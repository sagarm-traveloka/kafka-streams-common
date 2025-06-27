package com.common.kafkastreams.joins;

import com.common.kafkastreams.config.AggregationDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A generic ValueJoiner that dynamically maps fields from two input POJOs (or Maps)
 * into a new output Map based on configuration.
 *
 * @param <V_LEFT> The type of the left value.
 * @param <V_RIGHT> The type of the right value.
 * // Changed V_OUT to Map<String, Object>
 */
@Slf4j
public class DynamicPojoValueJoiner<V_LEFT, V_RIGHT> implements ValueJoiner<V_LEFT, V_RIGHT, Map<String, Object>> { // V_OUT is now Map<String, Object>

    private final List<AggregationDefinition.JoinFieldMapping> outputFieldsMapping;
    private final ObjectMapper objectMapper;

    // Constructor no longer takes outputPojoClassName
    public DynamicPojoValueJoiner(List<AggregationDefinition.JoinFieldMapping> outputFieldsMapping) {
        if (outputFieldsMapping == null || outputFieldsMapping.isEmpty()) {
            throw new IllegalArgumentException("Output field mappings cannot be null or empty for DynamicPojoValueJoiner.");
        }
        this.outputFieldsMapping = outputFieldsMapping;
        this.objectMapper = new ObjectMapper();
        log.info("DynamicPojoValueJoiner initialized with mappings: {}", outputFieldsMapping);
    }

    @Override
    public Map<String, Object> apply(V_LEFT leftValue, V_RIGHT rightValue) {
        Map<String, Object> outputMap = new HashMap<>(); // Create a new HashMap for the output

        Map<String, Object> leftMap = (leftValue != null) ? objectMapper.convertValue(leftValue, Map.class) : null;
        Map<String, Object> rightMap = (rightValue != null) ? objectMapper.convertValue(rightValue, Map.class) : null;

        log.info("========== Applying DynamicPojoValueJoiner with leftValue: {}, rightValue: {} ==================== ", leftValue, rightValue);
        for (AggregationDefinition.JoinFieldMapping mapping : outputFieldsMapping) {
            Map<String, Object> sourceMap = null;
            if (mapping.getSource() == AggregationDefinition.JoinFieldMapping.Source.LEFT) {
                sourceMap = leftMap;
            } else if (mapping.getSource() == AggregationDefinition.JoinFieldMapping.Source.RIGHT) {
                sourceMap = rightMap;
            }

            if (sourceMap != null && sourceMap.containsKey(mapping.getSourceFieldName())) {
                Object valueToMap = sourceMap.get(mapping.getSourceFieldName());
                outputMap.put(mapping.getOutputFieldName(), valueToMap);
                log.debug("Mapped field '{}:{}' from {} to output field '{}' with value: {}",
                        mapping.getSource(), mapping.getSourceFieldName(),
                        (mapping.getSource() == AggregationDefinition.JoinFieldMapping.Source.LEFT ? "left" : "right"),
                        mapping.getOutputFieldName(), valueToMap);
            } else {
                log.warn("Source field '{}:{}' not found or source map is null. Skipping mapping to output field '{}'.",
                        mapping.getSource(), mapping.getSourceFieldName(), mapping.getOutputFieldName());
                // Optionally, put null or a default value if the field is not found
                outputMap.put(mapping.getOutputFieldName(), null);
            }
        }
        return outputMap;
    }
}