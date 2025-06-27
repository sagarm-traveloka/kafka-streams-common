package com.common.kafkastreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JsonSerde<T> implements Serde<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> type;

    /**
     * Constructor for a specific type with an optional custom ObjectMapper.
     *
     * @param type The class type this Serde will handle.
     * @param customObjectMapper An optional custom ObjectMapper instance. If null, a default
     * configured ObjectMapper will be created.
     */
    public JsonSerde(Class<T> type, ObjectMapper customObjectMapper) {
        this.type = type;
        this.objectMapper = customObjectMapper != null ? customObjectMapper : new ObjectMapper();
        // Configure default ObjectMapper if created internally
        if (customObjectMapper == null) {
            this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            // Add any other common configurations for ObjectMapper here
        }
    }

    /**
     * Constructor for a specific type using a default configured ObjectMapper.
     *
     * @param type The class type this Serde will handle.
     */
    public JsonSerde(Class<T> type) {
        this(type, null);
    }

    /**
     * Default constructor for generic Object mapping (e.g., Map<String, Object>).
     * Uses a default configured ObjectMapper and sets the type to Object.class.
     */
    public JsonSerde() {
        this((Class<T>) Object.class, null);
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                // Serialize the data object to JSON bytes
                return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing JSON for topic '" + topic + "': " + data.getClass().getName(), e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                // If the target type is Object.class, deserialize to Map<String, Object> for generic JSON handling.
                // Otherwise, deserialize to the specific 'type'.
                if (Object.class.equals(type)) {
                    // This is a common pattern for generic JSON where objects become Maps
                    return objectMapper.readValue(data, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
                }
                return objectMapper.readValue(data, type);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing JSON for topic '" + topic + "': " + new String(data), e);
            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No specific configuration needed for this simple JsonSerde via Kafka Streams properties
    }

    @Override
    public void close() {
        // No resources to close
    }
}
