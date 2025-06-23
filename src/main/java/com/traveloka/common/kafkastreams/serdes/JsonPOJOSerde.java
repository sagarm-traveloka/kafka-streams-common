package com.traveloka.common.kafkastreams.serdes;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class JsonPOJOSerde<T> implements Serde<T> {

    // Using ObjectWriter and ObjectReader for thread-safety and performance
    // ObjectMapper itself is thread-safe for configuration, but read/write operations
    // are best done with dedicated ObjectWriter/ObjectReader instances.
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;

    private final Class<T> type; // The class of the POJO this Serde handles

    /**
     * Constructs a JsonPOJOSerde for the specified POJO type.
     * The ObjectMapper is configured to handle common serialization/deserialization needs.
     *
     * @param type The Class object representing the POJO type.
     */
    public JsonPOJOSerde(Class<T> type) {
        if (type == null) {
            throw new IllegalArgumentException("Type for JsonPOJOSerde cannot be null.");
        }
        this.type = type;
        ObjectMapper objectMapper = new ObjectMapper();
        // Configure ObjectMapper as needed, e.g.:
        // objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        // objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // objectMapper.findAndRegisterModules(); // For Java 8 Date/Time API support etc.

        this.objectWriter = objectMapper.writerFor(type);
        this.objectReader = objectMapper.readerFor(type);
    }

    /**
     * Constructor for Kafka's configure method (not typically used directly when creating via SerdeFactory).
     */
    public JsonPOJOSerde() {
        this(null); // Will throw IllegalArgumentException, for Kafka reflection primarily.
        // A more robust solution for Kafka's configure() would use Map config.
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // This method is called by Kafka to pass configurations.
        // If your POJO Serde needs dynamic configuration (e.g., specific Jackson features),
        // you would extract them from 'configs' map here.
        // For this simple implementation, it's often empty if the ObjectMapper is self-configured.
    }

    @Override
    public void close() {
        // No resources to close for ObjectMapper.
    }

    @Override
    public Serializer<T> serializer() {
        return new Serializer<T>() {
            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) {
                    return null;
                }
                try {
                    return objectWriter.writeValueAsBytes(data);
                } catch (IOException e) {
                    log.error("Error serializing data for topic {}: {}", topic, data, e);
                    throw new SerializationException("Error serializing JSON message for topic " + topic, e);
                }
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<T>() {
            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) {
                    return null;
                }
                try {
                    return objectReader.readValue(data);
                } catch (IOException e) {
                    log.error("Error deserializing data for topic {}: {}", topic, new String(data), e);
                    throw new SerializationException("Error deserializing JSON message for topic " + topic, e);
                }
            }
        };
    }
}
