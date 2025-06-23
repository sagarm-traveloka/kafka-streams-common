package com.common.kafkastreams.config;

import lombok.Data;
import java.util.List;

@Data
public class AggregationDefinition {
    private String id;
    private String description;
    private ProcessingMode processingMode; // Defines the overall stream processing pattern

    // --- Source Topic Configuration ---
    // This is the very first topic in the pipeline.
    // Its type (KStream or KTable) will be inferred by the 'initialSourceIsStream' flag
    // in the *first* JoinOperationConfig if 'processingMode' is JOIN_CHAIN.
    private TopicConfig sourceTopic;

    // --- Join Operations (for JOIN_CHAIN mode) ---
    // A list of sequential join steps. Each entry defines one join with an enrichment table.
    // This list must be present and not empty if processingMode is JOIN_CHAIN.
    private List<JoinOperationConfig> joinOperations;

    // --- Aggregation Specific Configuration (for AGGREGATION mode) ---
    private String groupByKeyExtractorClass; // FQN of KeyValueMapper for grouping
    private String groupByKeyType;           // FQN of the type produced by groupByKeyExtractorClass
    private String stateStoreName;           // Name for the materialized state store (e.g., for KTable)

    // --- Output Topic Configuration ---
    private OutputTopicConfig outputTopic;

    // =====================================
    //           Nested Classes & Enums
    // =====================================

    public enum ProcessingMode {
        JOIN_CHAIN,        // Handles all KStream-KTable and KTable-KTable chained joins
        AGGREGATION,       // For count, sum, reduce, etc. (uses sourceTopic)
        SIMPLE_STREAM      // Basic KStream processing (uses sourceTopic)
    }

    public enum JoinType {
        LEFT_JOIN,
        INNER_JOIN
        // Add other join types as Kafka Streams supports them, e.g., OUTER_JOIN if needed
    }

    @Data
    public static class TopicConfig {
        private String name;
        private String keyClass;   // Fully Qualified Name of the key class
        private String valueClass; // Fully Qualified Name of the value class
    }

    // Configuration for a single step in a join chain
    @Data
    public static class JoinOperationConfig {
        private String id;               // Unique ID for this specific join step (e.g., "customer-enrichment-step")
        private JoinType type;           // Type of join (INNER_JOIN, LEFT_JOIN)

        // For the *first* join operation only (if initialSourceIsStream is true),
        // this defines the primary stream/table. Otherwise, it's the previous step's output.
        // This is always the 'right-hand side' for the join operation.
        private TopicConfig enrichmentTopic;

        // NEW: Flag to indicate if the *initial source* for this specific join operation is a KStream.
        // This flag is primarily relevant for the *first* JoinOperationConfig in the list for JOIN_CHAIN mode.
        // If true, the sourceTopic is treated as a KStream for this join step.
        // If false (or absent), the sourceTopic (or previous join result) is treated as a KTable.
        private boolean initialSourceIsStream = false; // Default to false (KTable-like source for chaining)

        // NEW: Relevant only if initialSourceIsStream is true for the FIRST joinOperationConfig.
        // This re-keys the initial KStream before its first join.
        private String primaryKeyExtractorClass; // FQN of KeyValueMapper for re-keying the initial KStream

        private String valueJoinerClass;    // FQN of ValueJoiner for this specific step.
        // Its input types must match the previous step's output and the enrichmentTopic's value.
    }

    @Data
    public static class OutputTopicConfig {
        private boolean enabled;
        private String name;
        private String keyClass;
        private String valueClass;
        private long retentionMs;
        private Integer partitions;
        private Short replicationFactor;
    }
}