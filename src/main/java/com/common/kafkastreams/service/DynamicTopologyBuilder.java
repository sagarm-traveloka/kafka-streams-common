package com.common.kafkastreams.service;

import com.common.kafkastreams.config.AggregationDefinition;
import com.common.kafkastreams.joins.DynamicPojoValueJoiner;
import com.common.kafkastreams.mapper.DynamicPojoKeyExtractor;
import com.common.kafkastreams.serdes.SerdeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class DynamicTopologyBuilder {

//    @Autowired
//    private StreamsBuilder streamsBuilder;

    @Autowired
    private KTableRegistry kTableRegistry;

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
                KStream<Object, Object> aggSourceStream = createKStreamFromTopicConfig(streamsBuilder, aggDef.getSourceTopic());

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
                finalOutputKStream = createKStreamFromTopicConfig(streamsBuilder, aggDef.getSourceTopic());
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
            Pair<Serde<Object>, Serde<Object>> serdes = SerdeFactory.createSerdesFromTopicConfig(aggDef.getOutputTopic().toTopicConfig());

            log.info("Aggregation '{}' outputting to topic: {}", aggDef.getId(), aggDef.getOutputTopic().toTopicConfig().getName());
            finalOutputKStream.to(
                    aggDef.getOutputTopic().toTopicConfig().getName(),
                    Produced.with(serdes.getLeft(), serdes.getRight())
            );
        } else {
            log.info("Aggregation '{}' has no downstream output configured.", aggDef.getId());
        }
    }

    // define common function to create keySerde and valueSerde from TopicConfig


    /**
     * Helper method to create a KStream from a TopicConfig.
     */
    private KStream<Object, Object> createKStreamFromTopicConfig(StreamsBuilder streamsBuilder,  AggregationDefinition.TopicConfig topicConfig) {
        Pair<Serde<Object>, Serde<Object>> serdes = SerdeFactory.createSerdesFromTopicConfig(topicConfig);
        return streamsBuilder.stream(topicConfig.getName(), Consumed.with(serdes.getLeft(), serdes.getRight()));
    }

    /**
     * Builds a chained join, which can start with a KStream or a KTable.
     *
     * @param aggDef The aggregation definition containing the chained join configuration.
     * @return The final KStream after all chained joins are applied.
     */
    @SuppressWarnings("unchecked")
    private KStream<Object, Object> buildChainedJoin(StreamsBuilder streamsBuilder, AggregationDefinition aggDef) {
        if (aggDef.getJoinOperations() == null || aggDef.getJoinOperations().isEmpty()) {
            throw new IllegalArgumentException("For JOIN_CHAIN mode, 'joinOperations' must be provided and not empty for ID: " + aggDef.getId());
        }
        if (aggDef.getSourceTopic() == null) {
            throw new IllegalArgumentException("For JOIN_CHAIN mode, 'sourceTopic' must be provided for ID: " + aggDef.getId());
        }

        log.info("Building chained join for aggregation ID: {}", aggDef.getId());

        // Get the first join operation to determine the initial source type
        AggregationDefinition.JoinOperationConfig firstJoinOp = aggDef.getJoinOperations().get(0);

        KStream<Object, Object> currentKStream = null;
        KTable<Object, Object> currentKTable = null; // Will hold the result if the chain is KTable-based

        // Determine the starting point of the chain: KStream or KTable
        if (firstJoinOp.isInitialSourceIsStream()) {
            log.info("Initial source for join chain is KStream from topic: {}", aggDef.getSourceTopic().getName());
            currentKStream = createKStreamFromTopicConfig(streamsBuilder, aggDef.getSourceTopic());

            if (firstJoinOp.getPrimaryKeyExtraction() != null) {
                log.info("Applying primary key extractor for initial KStream in aggregation '{}' with config: {}",
                        aggDef.getId(), firstJoinOp.getPrimaryKeyExtraction());
                KeyValueMapper<Object, Object, Object> keyMapper =
                        new DynamicPojoKeyExtractor<>(firstJoinOp.getPrimaryKeyExtraction());
                currentKStream = currentKStream.selectKey(keyMapper);
            }
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
        for (int i = 0; i < aggDef.getJoinOperations().size(); i++) {
            AggregationDefinition.JoinOperationConfig joinOp = aggDef.getJoinOperations().get(i);
            log.info("Applying join step {} ('{}') with enrichment topic '{}'",
                    (i + 1), joinOp.getId(), joinOp.getEnrichmentTopic().getName());

            // Build the 'right-hand' KTable for the current join step
            KTable<Object, Object> enrichmentKTable = kTableRegistry.getOrCreateKTable(streamsBuilder, joinOp.getEnrichmentTopic());

            if (joinOp.getOutputFieldsMapping() == null || joinOp.getOutputPojoClass() == null) {
                throw new IllegalArgumentException("For dynamic join, 'outputFieldsMapping' and 'outputPojoClass' must be provided in joinOperationConfig for ID: " + joinOp.getId());
            }

            // The ValueJoiner will output the specified outputPojoClass
            ValueJoiner<Object, Object, Object> valueJoiner =
                    new DynamicPojoValueJoiner<>(joinOp.getOutputFieldsMapping(), joinOp.getOutputPojoClass());


            // Perform the join based on whether the 'left-hand' side is currently a KStream or KTable
            if (currentKStream != null) { // If the chain is currently KStream-based
                if (joinOp.getType() == AggregationDefinition.JoinType.LEFT_JOIN) {
                    currentKStream = currentKStream.leftJoin(enrichmentKTable, valueJoiner);
                } else { // INNER_JOIN
                    currentKStream = currentKStream.join(enrichmentKTable, valueJoiner);
                }
                log.debug("Current chain state after join step {}: KStream<Object, Object>", (i + 1));
            } else if (currentKTable != null) { // If the chain is currently KTable-based
                if (joinOp.getType() == AggregationDefinition.JoinType.LEFT_JOIN) {
                    currentKTable = currentKTable.leftJoin(enrichmentKTable, valueJoiner);
                } else { // INNER_JOIN
                    currentKTable = currentKTable.join(enrichmentKTable, valueJoiner);
                }
                log.debug("Current chain state after join step {}: KTable<Object, Object>", (i + 1));
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

    private KStream<Object, Long> buildCountAggregation(KStream<Object, Object> primaryStream,
                                                        AggregationDefinition aggDef,
                                                        KeyValueMapper<Object, Object, Object> groupByKeyMapper) throws ClassNotFoundException {
//        if (aggDef.getStateStoreName() == null || aggDef.getStateStoreName().trim().isEmpty()) {
//            throw new IllegalArgumentException("stateStoreName must be provided for AGGREGATION mode for ID: " + aggDef.getId());
//        }
//
//        Class<?> groupByKeyTypeClass;
//        try {
//            groupByKeyTypeClass = Class.forName(aggDef.getGroupByKeyType());
//        } catch (ClassNotFoundException e) {
//            log.error("Failed to find groupByKeyType class: {}", aggDef.getGroupByKeyType(), e);
//            throw new RuntimeException("Failed to load groupByKeyType class", e);
//        }
//
//        KGroupedStream<Object, Object> groupedStream = primaryStream
//                .selectKey(groupByKeyMapper)
//                .groupByKey(Grouped.with(SerdeFactory.createSerde((Class<Object>) groupByKeyTypeClass),
//                        SerdeFactory.createSerde((Class<Object>) Class.forName(aggDef.getSourceTopic().getValueClass()))));
//
////        KTable<Object, Object> aggregatedTable = groupedStream.count(
////                Materialized.as(aggDef.getStateStoreName())
////                        .withKeySerde(SerdeFactory.createSerde((Class<Object>) groupByKeyTypeClass))
////                        .withValueSerde(SerdeFactory.createSerde(Long.class))
////        );
//
//        KTable<Object, Long> aggregatedTable = groupedStream.count(
//                // Explicitly define K and V types for Materialized to help type inference
//                Materialized.as(aggDef.getStateStoreName())
//                        .withKeySerde(SerdeFactory.createSerde((Class<Object>) groupByKeyTypeClass))
//                        .withValueSerde(Serdes.Long())
//        );
//
//        return aggregatedTable.toStream();
        return null;
    }
}