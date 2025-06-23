package com.common.kafkastreams.joins;

import com.common.kafkastreams.config.AggregationDefinition.JoinFieldMapping;
import com.common.kafkastreams.config.AggregationDefinition.JoinFieldMapping.Source;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A generic ValueJoiner that combines fields from two input POJOs (left and right)
 * into a new output POJO based on a configurable field mapping.
 *
 * @param <LEFT_VALUE> The type of the value from the left side of the join (e.g., Order).
 * @param <RIGHT_VALUE> The type of the value from the right side of the join (e.g., Customer).
 * @param <OUT_VALUE> The type of the output value (e.g., EnrichedOrder).
 */
@Slf4j
public class DynamicPojoValueJoiner<LEFT_VALUE, RIGHT_VALUE, OUT_VALUE>
        implements ValueJoiner<LEFT_VALUE, RIGHT_VALUE, OUT_VALUE> {

    private final List<JoinFieldMapping> outputFieldsMapping;
    private final Class<OUT_VALUE> outputPojoClass;
    private final ObjectMapper objectMapper;

    /**
     * Constructor for the DynamicPojoValueJoiner.
     *
     * @param outputFieldsMapping The list of field mappings defining how to construct the output POJO.
     * @param outputPojoClassFqn The Fully Qualified Name of the target output POJO class.
     * @throws IllegalArgumentException if mappings or output class FQN are invalid.
     * @throws RuntimeException if the output POJO class cannot be loaded.
     */
    @SuppressWarnings("unchecked")
    public DynamicPojoValueJoiner(List<JoinFieldMapping> outputFieldsMapping, String outputPojoClassFqn) {
        if (outputFieldsMapping == null || outputFieldsMapping.isEmpty()) {
            throw new IllegalArgumentException("outputFieldsMapping cannot be null or empty for DynamicPojoValueJoiner.");
        }
        if (outputPojoClassFqn == null || outputPojoClassFqn.trim().isEmpty()) {
            throw new IllegalArgumentException("outputPojoClassFqn cannot be null or empty for DynamicPojoValueJoiner.");
        }

        this.outputFieldsMapping = outputFieldsMapping;
        this.objectMapper = new ObjectMapper(); // Use a new ObjectMapper or inject a shared one if preferred

        try {
            this.outputPojoClass = (Class<OUT_VALUE>) Class.forName(outputPojoClassFqn);
            log.info("DynamicPojoValueJoiner initialized for output POJO: {}", outputPojoClassFqn);
        } catch (ClassNotFoundException e) {
            log.error("Output POJO class not found: {}", outputPojoClassFqn, e);
            throw new RuntimeException("Failed to load output POJO class: " + outputPojoClassFqn, e);
        }
    }

    @Override
    public OUT_VALUE apply(LEFT_VALUE leftValue, RIGHT_VALUE rightValue) {
        // If either input is null and it's a LEFT_JOIN scenario where nulls are expected, handle gracefully.
        // For INNER_JOIN, if either is null, the join wouldn't have happened anyway.
        if (leftValue == null && rightValue == null) {
            log.debug("Both left and right values are null. Returning null for join.");
            return null;
        }

        Map<String, Object> combinedData = new HashMap<>();

        try {
            // Convert input POJOs to Maps for easier field access via Jackson
            // This is robust as ObjectMapper can convert any POJO to Map
            Map<String, Object> leftMap = leftValue != null ? objectMapper.convertValue(leftValue, Map.class) : new HashMap<>();
            Map<String, Object> rightMap = rightValue != null ? objectMapper.convertValue(rightValue, Map.class) : new HashMap<>();

            for (JoinFieldMapping mapping : outputFieldsMapping) {
                Object valueToMap = null;
                if (mapping.getSource() == Source.LEFT) {
                    valueToMap = leftMap.get(mapping.getSourceFieldName());
                } else if (mapping.getSource() == Source.RIGHT) {
                    valueToMap = rightMap.get(mapping.getSourceFieldName());
                }

                if (valueToMap != null) {
                    combinedData.put(mapping.getOutputFieldName(), valueToMap);
                } else {
                    log.debug("Field '{}' from source '{}' was null. Not including in output.",
                            mapping.getSourceFieldName(), mapping.getSource());
                    // Optionally, you might want to put nulls or default values, depending on requirements.
                    // combinedData.put(mapping.getOutputFieldName(), null);
                }
            }

            // Convert the combined Map to the target output POJO
            return objectMapper.convertValue(combinedData, outputPojoClass);

        } catch (Exception e) {
            log.error("Error applying dynamic join logic: Left value={}, Right value={}. Error: {}",
                    leftValue, rightValue, e.getMessage(), e);
            // Handle error: return null, throw, or use a dead letter queue
            throw new RuntimeException("Failed to dynamically join values", e);
        }
    }
}