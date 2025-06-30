package com.common.kafkastreams.service;

import com.common.kafkastreams.config.AggregationDefinition;
import com.common.kafkastreams.joins.DynamicPojoValueJoiner;
import com.common.kafkastreams.mapper.DynamicPojoKeyExtractor;
import com.common.kafkastreams.serdes.SerdeFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;


@Service
@Slf4j
public class DynamicTopologyBuilder {

    @Autowired
    private KTableRegistry kTableRegistry;

    private final ObjectMapper objectMapper; // Add ObjectMapper instance

    public DynamicTopologyBuilder() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false); // Ignore unknown fields in JSON
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }


    @SuppressWarnings("unchecked")
    public void buildAggregationTopology(StreamsBuilder streamsBuilder, AggregationDefinition aggDef) throws ClassNotFoundException {
        log.info("Building topology for aggregation ID: {}", aggDef.getId());

        KStream<Object, Object> finalOutputKStream; // The final stream to be pushed to output topic

        switch (aggDef.getProcessingMode()) {
            case JOIN_CHAIN:
                finalOutputKStream = buildChainedJoin(streamsBuilder, aggDef); // Handles both KStream-KTable and KTable-KTable chains
                break;
            case AGGREGATION:
                // Start with KStream from sourceTopic for aggregations
                KStream<Object, Object> aggSourceStream = kTableRegistry.createAndConfigureKStream(streamsBuilder, aggDef.getSourceTopic());

                if (aggDef.getGroupByKeyExtraction() == null || aggDef.getGroupByKeyType() == null) {
                    throw new IllegalArgumentException("For AGGREGATION mode, 'groupByKeyExtraction' and 'groupByKeyType' must be provided for ID: " + aggDef.getId());
                }
                KeyValueMapper<Object, Object, Object> groupByKeyMapper =
                        new DynamicPojoKeyExtractor<>(aggDef.getGroupByKeyExtraction());

                KStream<Object, Long> countStream = buildCountAggregation(aggSourceStream, aggDef, groupByKeyMapper);
                assert countStream != null;
                finalOutputKStream = countStream.mapValues((key, value) -> (Object) value);
                break;
            case SIMPLE_STREAM:
                // Start with KStream from sourceTopic
                finalOutputKStream = kTableRegistry.createAndConfigureKStream(streamsBuilder, aggDef.getSourceTopic());
                finalOutputKStream.peek((key, value) -> log.debug("Simple stream processing: key={}, value={}", key, value));
                break;
            default:
                throw new IllegalArgumentException("Unknown processing mode: " + aggDef.getProcessingMode() + " for aggregation ID: " + aggDef.getId());
        }

        // Final output to downstream topic
        if (aggDef.getOutputTopic().isEnabled()) {
            if (finalOutputKStream == null) {
                throw new IllegalStateException("Final output stream is null for aggregation ID: " + aggDef.getId() + ". Cannot output to topic.");
            }
//            Pair<Serde<Object>, Serde<Object>> serdes = SerdeFactory.createSerdesFromTopicConfig(aggDef.getOutputTopic().toTopicConfig());
            // Default to Object.class for generic key/value handling, assuming JSON or similar structure.
            // SerdeFactory should be configured to handle Object.class with appropriate JSON serdes.
            Serde<Object> keySerde = SerdeFactory.createSerde(Object.class);
            Serde<Object> valueSerde = SerdeFactory.createSerde(Object.class);

            log.info("Aggregation '{}' outputting to topic: {}", aggDef.getId(), aggDef.getOutputTopic().toTopicConfig().getName());
//            finalOutputKStream.to(
//                    aggDef.getOutputTopic().toTopicConfig().getName(),
//                    Produced.with(keySerde, valueSerde) // Use the default Object serde for both key and value
//            );

            log.info("Final output stream for aggregation ID '{}' has been sent to topic '{}'.", aggDef.getId(), aggDef.getOutputTopic().toTopicConfig().getName());
            finalOutputKStream.peek((key, value) -> {
                try {
                    String keyJson = objectMapper.writeValueAsString(key);
                    String valueJson = objectMapper.writeValueAsString(value);
                    log.info("Final output => Key: {}, Value: {}", keyJson, valueJson);
                } catch (Exception e) {
                    log.error("Failed to serialize key/value for aggregation ID '{}': {}", aggDef.getId(), e.getMessage());
                }
            });

        } else {
            log.info("Aggregation '{}' has no downstream output configured.", aggDef.getId());
        }
    }

    /**
     * Builds a chained join, which can start with a KStream or a KTable.
     *
     * @param aggDef The aggregation definition containing the chained join configuration.
     * @return The final KStream after all chained joins are applied.
     */
    @SuppressWarnings("unchecked")
    private KStream<Object, Object> buildChainedJoin(StreamsBuilder streamsBuilder, AggregationDefinition aggDef) {
        List<AggregationDefinition.JoinOperationConfig> joinOps = aggDef.getJoinOperations();
        if (joinOps == null || joinOps.isEmpty()) {
            throw new IllegalArgumentException("For JOIN_CHAIN mode, 'joinOperations' must be provided and not empty for ID: " + aggDef.getId());
        }
        if (aggDef.getSourceTopic() == null) {
            throw new IllegalArgumentException("For JOIN_CHAIN mode, 'sourceTopic' must be provided for ID: " + aggDef.getId());
        }

        log.info("Building chained join for aggregation ID: {}", aggDef.getId());

        // Get the first join operation to determine the initial source type
        KStream<Object, Object> currentKStream = null;
        KTable<Object, Object> currentKTable = null;

        AggregationDefinition.JoinOperationConfig firstJoinOp = joinOps.get(0);

        if (firstJoinOp.isInitialSourceIsStream()) {
            log.info("Initial source for join chain is KStream from topic: {}", aggDef.getSourceTopic().getName());
            currentKStream = kTableRegistry.createAndConfigureKStream(streamsBuilder, aggDef.getSourceTopic());
        } else {
            log.info("Initial source for join chain is KTable from topic: {}", aggDef.getSourceTopic().getName());
            currentKTable = kTableRegistry.getOrCreateKTable(streamsBuilder, aggDef.getSourceTopic());
        }
        // print currentKTable state for debugging
        if (currentKTable != null) {
            log.debug("Initial KTable state for aggregation ID '{}': {}", aggDef.getId(), currentKTable.toString());
        } else {
            log.debug("Initial KStream state for aggregation ID '{}': {}", aggDef.getId(), currentKStream.toString());
        }

        // Iterate through all join operations
        for (int i = 0; i < joinOps.size(); i++) {
            AggregationDefinition.JoinOperationConfig joinOp = joinOps.get(i);
            if (i == 0 && !joinOp.isInitialSourceIsStream()) {
                continue;
            }
            log.info("Applying join step {} ('{}') with enrichment topic '{}'",
                    (i + 1), joinOp.getId(), joinOp.getEnrichmentTopic().getName());

            // Build the 'right-hand' KTable for the current join step
            KTable<Object, Object> enrichmentKTable = kTableRegistry.getOrCreateKTable(streamsBuilder, joinOp.getEnrichmentTopic());

            if (joinOp.getOutputFieldsMapping() == null || joinOp.getOutputFieldsMapping().isEmpty()) {
                throw new IllegalArgumentException("For dynamic join, 'outputFieldsMapping' and 'outputPojoClass' must be provided in joinOperationConfig for ID: " + joinOp.getId());
            }

            // The V_OUT generic for ValueJoiner will now be Map<String, Object>
            ValueJoiner<Object, Object, Map<String, Object>> valueJoiner =
                    new DynamicPojoValueJoiner<>(joinOp.getOutputFieldsMapping());

            if (currentKStream != null) {
                try {
                    if (joinOp.getType() == AggregationDefinition.JoinType.LEFT_JOIN) {
                        currentKStream = currentKStream.leftJoin(enrichmentKTable, valueJoiner);
//                            .mapValues((key, value) -> (Object) value);
                    } else { // INNER_JOIN
                        currentKStream = currentKStream.join(enrichmentKTable, valueJoiner)
                                .mapValues((key, value) -> (Object) value);
                    }
                } catch (Exception e) {
                    log.error("Error during join operation {}: {}", joinOp.getId(), e.getMessage());
                    throw new RuntimeException("Failed to apply join operation: " + joinOp.getId(), e);
                }
                log.debug("Current chain state after join step {}: KStream<Object, Map<String, Object>>", (i + 1)); // Log changed type
            } else if (currentKTable != null) {
                if (joinOp.getType() == AggregationDefinition.JoinType.LEFT_JOIN) {
                    currentKTable = currentKTable.leftJoin(enrichmentKTable, valueJoiner)
                            .mapValues((key, value) -> (Object) value);
                } else { // INNER_JOIN
                    currentKTable = currentKTable.join(enrichmentKTable, valueJoiner)
                            .mapValues((key, value) -> (Object) value);
                }
                log.debug("Current chain state after join step {}: KTable<Object, Map<String, Object>>", (i + 1)); // Log changed type
            } else {
                throw new IllegalStateException("Neither KStream nor KTable initialized for join chain for ID: " + aggDef.getId());
            }
        }

        // Return the final result as a KStream, converting from KTable if necessary
        if (currentKStream != null) {
            return currentKStream;
        } else if (currentKTable != null) {
            return currentKTable.toStream();
        } else {
            throw new IllegalStateException("Final join chain result is null for ID: " + aggDef.getId());
        }
    }

    private KStream<Object, Long> buildCountAggregation(KStream<Object, Object> primaryStream, AggregationDefinition aggDef, KeyValueMapper<Object, Object, Object> groupByKeyMapper) {
//        return primaryStream
//                .selectKey(groupByKeyMapper)
//                .groupByKey(Grouped.with(SerdeFactory.createSerde((Class<Object>) safeGetClass(aggDef.getGroupByKeyType())), SerdeFactory.createSerde(Object.class)))
//                .count(Materialized.<Object, Long>as(aggDef.getStateStoreName())
//                        .withKeySerde(SerdeFactory.createSerde((Class<Object>) safeGetClass(aggDef.getGroupByKeyType())))
//                        .withValueSerde(org.apache.kafka.common.serialization.Serdes.Long()))
//                .toStream();
        return null; // Placeholder for actual implementation
    }



    private Class<?> safeGetClass(String className) {
        if (className == null) {
            return null;
        }
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            log.error("Class not found: {}", className, e);
            throw new RuntimeException("Class not found: " + className, e);
        }
    }
}