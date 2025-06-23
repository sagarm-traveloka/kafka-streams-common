package com.common.kafkastreams.mapper;

import com.common.kafkastreams.config.AggregationDefinition;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Map;

/**
 * A generic KeyValueMapper that extracts a new key from either the existing key
 * or a specified field within the value POJO, based on configuration.
 *
 * @param <K_IN> The type of the input key.
 * @param <V_IN> The type of the input value.
 * @param <K_OUT> The type of the output key.
 */
@Slf4j
public class DynamicPojoKeyExtractor<K_IN, V_IN, K_OUT> implements KeyValueMapper<K_IN, V_IN, K_OUT> {

    private final AggregationDefinition.KeyExtractionConfig config;
    private final ObjectMapper objectMapper;

    public DynamicPojoKeyExtractor(AggregationDefinition.KeyExtractionConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("KeyExtractionConfig cannot be null for DynamicPojoKeyExtractor.");
        }
        if (config.getSource() == AggregationDefinition.KeyExtractionConfig.Source.VALUE && (config.getFieldName() == null || config.getFieldName().trim().isEmpty())) {
            throw new IllegalArgumentException("fieldName must be specified when KeyExtractionConfig source is VALUE.");
        }
        this.config = config;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        log.info("DynamicPojoKeyExtractor initialized with config: {}", config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public K_OUT apply(K_IN key, V_IN value) {
        if (config.getSource() == AggregationDefinition.KeyExtractionConfig.Source.KEY) {
            log.debug("Extracting key directly from input key: {}", key);
            return (K_OUT) key;
        } else { // Source.VALUE
            if (value == null) {
                log.warn("Attempted to extract key from null value for field '{}'. Returning null.", config.getFieldName());
                return null;
            }
            try {
                Map<String, Object> valueMap = objectMapper.convertValue(value, Map.class);
                Object extractedKey = valueMap.get(config.getFieldName());
                log.debug("Extracted key '{}' from value field '{}'. Original value: {}", extractedKey, config.getFieldName(), value);
                return (K_OUT) extractedKey;
            } catch (Exception e) {
                log.error("Error extracting key from value for field '{}'. Value: {}. Error: {}",
                        config.getFieldName(), value, e.getMessage(), e);
                throw new RuntimeException("Failed to extract key from value", e);
            }
        }
    }
}