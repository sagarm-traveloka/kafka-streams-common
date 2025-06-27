package com.common.kafkastreams.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class DynamicPojoKeyExtractor<K, V> implements KeyValueMapper<K, V, Object> {

    // The configuration string is expected to be a JSON string
    // mapping output key field names to JSON paths in the input value.
    // Example: "{\"productId\": \"$.productId\", \"storeId\": \"$.storeId\"}"
    private final String keyExtractionConfig;
    private final ObjectMapper objectMapper;
    private final ObjectNode configJsonNode;

    public DynamicPojoKeyExtractor(String keyExtractionConfig) {
        if (keyExtractionConfig == null || keyExtractionConfig.isEmpty()) {
            throw new IllegalArgumentException("Key extraction configuration cannot be null or empty.");
        }
        this.keyExtractionConfig = keyExtractionConfig;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        try {
            this.configJsonNode = (ObjectNode) objectMapper.readTree(keyExtractionConfig);
        } catch (IOException e) {
            log.error("Failed to parse keyExtractionConfig JSON: {}", keyExtractionConfig, e);
            throw new IllegalArgumentException("Invalid JSON for keyExtractionConfig.", e);
        }
    }

    /**
     * Extracts a composite key from the input value (V).
     * The input value is expected to be a POJO or a Map (e.g., from JSON deserialization).
     * The extracted key will be a Map<String, Object> representing the composite key.
     *
     * @param key   The original key (ignored in this extractor, as key is derived from value).
     * @param value The input value (POJO or Map).
     * @return A Map<String, Object> representing the new composite key.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Object apply(K key, V value) {
        if (value == null) {
            return null; // Or throw an exception, depending on desired behavior for null values
        }

        // Convert value to JsonNode for flexible path extraction
        JsonNode valueNode;
        if (value instanceof JsonNode) {
            valueNode = (JsonNode) value;
        } else {
            // Convert POJO/Map to JsonNode. This is robust.
            valueNode = objectMapper.valueToTree(value);
        }

        ObjectNode newKeyNode = objectMapper.createObjectNode();

        // Iterate through the configured mappings
        Iterator<Map.Entry<String, JsonNode>> fields = configJsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String outputFieldName = entry.getKey(); // The field name in the new composite key
            String jsonPath = entry.getValue().asText(); // The JSON path in the input value

            // Simple JSON path implementation for top-level fields or direct nested paths.
            // For complex paths (e.g., arrays, filters), a dedicated JSONPath library would be needed.
            if (jsonPath.startsWith("$.") && jsonPath.length() > 2) {
                String sourceFieldName = jsonPath.substring(2); // Remove "$." prefix
                JsonNode extractedValue = valueNode.get(sourceFieldName);
                if (extractedValue != null && !extractedValue.isNull()) {
                    newKeyNode.set(outputFieldName, extractedValue);
                } else {
                    log.warn("Value not found for JSON path '{}' in input value for key field '{}'. Value: {}",
                            jsonPath, outputFieldName, valueNode.toPrettyString());
                    // Decide whether to include null, or skip the field, or throw error
                    // For now, it will be skipped if not found or null.
                }
            } else {
                log.warn("Unsupported JSON path format '{}' for key field '{}'. Only '$.fieldName' is currently supported directly. Value: {}",
                        jsonPath, outputFieldName, valueNode.toPrettyString());
                // Handle unsupported path formats, e.g., by skipping or throwing an error
            }
        }

        // Return the new composite key as a Map (which ObjectMappers often handle for JsonNode)
        return objectMapper.convertValue(newKeyNode, Map.class);
    }
}