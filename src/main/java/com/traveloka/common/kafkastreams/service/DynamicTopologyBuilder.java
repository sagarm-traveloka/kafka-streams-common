package com.traveloka.common.kafkastreams.service;

import com.traveloka.common.kafkastreams.config.AggregationDefinition;
import com.traveloka.common.kafkastreams.serdes.SerdeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class DynamicTopologyBuilder {

//    @Autowired
//    private StreamsBuilder streamsBuilder;

    @Autowired
    private KTableRegistry kTableRegistry;

    public void buildAggregationTopology(StreamsBuilder streamsBuilder, AggregationDefinition aggDef) {
        log.info("Building topology for aggregation ID: {}", aggDef.getId());

        KStream<Object, Object> finalOutputKStream; // The final stream to be pushed to output topic

        switch (aggDef.getProcessingMode()) {
            case JOIN_CHAIN:
                finalOutputKStream = buildChainedJoin(streamsBuilder, aggDef); // Handles both KStream-KTable and KTable-KTable chains
                break;
            case AGGREGATION:
                // Start with KStream from sourceTopic for aggregations
                KStream<Object, Object> aggSourceStream = createKStreamFromTopicConfig(streamsBuilder, aggDef.getSourceTopic());
                KStream<Object, Long> countStream = buildCountAggregation(aggSourceStream, aggDef);
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
            Serde<Object> outputKeySerde = SerdeFactory.createSerde(aggDef.getOutputTopic().getKeyClass());
            Serde<Object> outputValueSerde = SerdeFactory.createSerde(aggDef.getOutputTopic().getValueClass());

            log.info("Aggregation '{}' outputting to topic: {}", aggDef.getId(), aggDef.getOutputTopic().getName());
            finalOutputKStream.to(
                    aggDef.getOutputTopic().getName(),
                    Produced.with(outputKeySerde, outputValueSerde)
            );
        } else {
            log.info("Aggregation '{}' has no downstream output configured.", aggDef.getId());
        }
    }

    /**
     * Helper method to create a KStream from a TopicConfig.
     */
    private KStream<Object, Object> createKStreamFromTopicConfig(StreamsBuilder streamsBuilder,  AggregationDefinition.TopicConfig topicConfig) {
        Serde<Object> keySerde = SerdeFactory.createSerde(topicConfig.getKeyClass());
        Serde<Object> valueSerde = SerdeFactory.createSerde(topicConfig.getValueClass());
        return streamsBuilder.stream(topicConfig.getName(), Consumed.with(keySerde, valueSerde));
    }

    /**
     * Builds a chained join, which can start with a KStream or a KTable.
     *
     * @param aggDef The aggregation definition containing the chained join configuration.
     * @return The final KStream after all chained joins are applied.
     */
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

            // Apply primaryKeyExtractor if provided for the initial KStream
            if (firstJoinOp.getPrimaryKeyExtractorClass() != null) {
                log.info("Applying primary key extractor '{}' for initial KStream in aggregation '{}'",
                        firstJoinOp.getPrimaryKeyExtractorClass(), aggDef.getId());
                try {
                    KeyValueMapper<Object, Object, Object> keyMapper =
                            (KeyValueMapper<Object, Object, Object>) Class.forName(firstJoinOp.getPrimaryKeyExtractorClass()).getDeclaredConstructor().newInstance();
                    currentKStream = currentKStream.selectKey(keyMapper);
                } catch (Exception e) {
                    log.error("Failed to instantiate primary key extractor '{}' for ID '{}'", firstJoinOp.getPrimaryKeyExtractorClass(), aggDef.getId(), e);
                    throw new RuntimeException("Failed to instantiate primary key extractor", e);
                }
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

            ValueJoiner<Object, Object, Object> valueJoiner;
            try {
                valueJoiner = (ValueJoiner<Object, Object, Object>) Class.forName(joinOp.getValueJoinerClass()).getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                log.error("Failed to instantiate ValueJoiner '{}' for aggregation ID '{}', join step '{}'",
                        joinOp.getValueJoinerClass(), aggDef.getId(), joinOp.getId(), e);
                throw new RuntimeException("Failed to instantiate ValueJoiner", e);
            }

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

    // Existing buildCountAggregation method (no change needed)
    private KStream<Object, Long> buildCountAggregation(KStream<Object, Object> primaryStream, AggregationDefinition aggDef) {
        // ... (implementation as previously defined) ...
        return null; // Placeholder for brevity
    }
}