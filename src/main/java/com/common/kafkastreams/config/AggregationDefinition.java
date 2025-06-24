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
//    private String groupByKeyExtractorClass; // FQN of KeyValueMapper for grouping
    private KeyExtractionConfig groupByKeyExtraction;
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
        private String keyClass="java.lang.String";   // Fully Qualified Name of the key class
        private String valueClass; // Fully Qualified Name of the value class (e.g., "com.service.kakfastreams.orders.model.Order")
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
        private KeyExtractionConfig primaryKeyExtraction;

        // --- NEW: Configuration for dynamic ValueJoiner ---
        // This list defines how fields from the left and right inputs are mapped to the output POJO.
        private List<JoinFieldMapping> outputFieldsMapping;
        // The Fully Qualified Name of the POJO class that this ValueJoiner should produce.
//        private String outputPojoClass;
        // private String valueJoinerClass;    // FQN of ValueJoiner for this specific step.
        // Its input types must match the previous step's output and the enrichmentTopic's value.
    }

    /**
     * Defines a single field mapping from an input POJO (left or right) to an output POJO.
     */
    @Data
    public static class JoinFieldMapping {
        public enum Source {
            LEFT, RIGHT
        }
        private Source source;          // Indicates whether the field comes from the 'left' or 'right' POJO
        private String sourceFieldName; // The name of the field in the source POJO (e.g., "orderId", "customerName")
        private String outputFieldName; // The name of the field in the target output POJO (e.g., "id", "customerFullName")
        // Future extensions could include: 'defaultValue', 'expression', 'transformationFunction'
    }

    /**
     * Defines how to extract a new key from either the existing key or the value.
     */
    @Data
    public static class KeyExtractionConfig {
        public enum Source {
            KEY, VALUE
        }
        private Source source;          // Indicates whether to extract from the 'KEY' or 'VALUE'
        private String fieldName;       // The name of the field to extract from the source (if source is VALUE)
        // If source is KEY, this field can be null/ignored, or specify 'value' to use the key directly.
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

        // getter to convert TopiConfig class
        public TopicConfig toTopicConfig() {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setName(name);
            topicConfig.setKeyClass(keyClass);
            topicConfig.setValueClass(valueClass);
            return topicConfig;
        }
    }
}