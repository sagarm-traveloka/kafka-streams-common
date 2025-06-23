package com.common.kafkastreams.serdes;

import com.common.kafkastreams.config.AggregationDefinition;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class SerdeFactory {

    // Use a single, re-usable ObjectMapper instance for efficiency
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        // Configure ObjectMapper once for all Serdes.
        // Example configurations (uncomment as needed):
         OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false); // Ignore unknown fields in JSON
        // OBJECT_MAPPER.findAndRegisterModules(); // Register modules for Java 8 Date/Time API, etc.
    }

    // Private constructor to prevent instantiation
    private SerdeFactory() {}

    /**
     * Creates a Serde for a given Class.
     * This method first checks for Kafka's built-in Serdes for common types.
     * If the `dataClass` is not a standard type, it assumes it's a custom POJO
     * and creates a JSON Serde for it.
     *
     * @param dataClass The Class object for which to create the Serde.
     * @return A Serde instance compatible with the dataClass.
     * @throws IllegalArgumentException if the dataClass is null or no suitable Serde can be created.
     */
    @SuppressWarnings("unchecked")
    public static <T> Serde<T> createSerde(Class<T> dataClass) {
        if (dataClass == null) {
            log.warn("Attempted to create Serde for null dataClass. Returning Serdes.Bytes().");
            return (Serde<T>) Serdes.Bytes(); // Fallback or throw IllegalArgumentException
        }

        // Handle common Java types with built-in Serdes
        if (String.class.isAssignableFrom(dataClass)) {
            return (Serde<T>) Serdes.String();
        } else if (Long.class.isAssignableFrom(dataClass)) {
            return (Serde<T>) Serdes.Long();
        } else if (Integer.class.isAssignableFrom(dataClass)) {
            return (Serde<T>) Serdes.Integer();
        } else if (Double.class.isAssignableFrom(dataClass)) {
            return (Serde<T>) Serdes.Double();
        } else if (byte[].class.isAssignableFrom(dataClass)) {
            return (Serde<T>) Serdes.ByteArray();
        } else if (Void.class.isAssignableFrom(dataClass)) {
            return (Serde<T>) Serdes.Void();
        }
        // Add other primitive/standard types as needed (e.g., Boolean, Float, UUID)

        // For any other class, assume it's a custom POJO that needs JSON serialization/deserialization
        log.info("Creating JSON POJO Serde for custom class: {}", dataClass.getName());
        return new JsonPojoSerde<>(dataClass);
    }

    // --- Inner Classes for JSON POJO Serde Implementation ---

    /**
     * A generic Serde for POJOs using Jackson for JSON serialization/deserialization.
     * It wraps a custom Serializer and Deserializer.
     */
    public static class JsonPojoSerde<T> extends Serdes.WrapperSerde<T> {
        public JsonPojoSerde(Class<T> pojoClass) {
            // Pass the pojoClass to the Deserializer's constructor so it knows the target type
            super(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(pojoClass));
            log.debug("JsonPojoSerde created for class: {}", pojoClass.getName());
        }

        // No-arg constructor for reflection-based instantiation (e.g., if used in `Joined.with()`)
        // In such cases, the deserializer's `configure` method must be able to determine the type.
        // For this solution, we primarily rely on the parameterized constructor being used by SerdeFactory.
        public JsonPojoSerde() {
            // The Deserializer will need its targetType set via `configure` or other means later.
            super(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(null));
            log.warn("JsonPojoSerde instantiated with no-arg constructor. Deserializer requires target class configuration.");
        }
    }

    /**
     * Generic JSON Serializer for POJOs using Jackson's ObjectMapper.
     */
    public static class JsonPojoSerializer<T> implements Serializer<T> {

        private final ObjectWriter objectWriter = OBJECT_MAPPER.writer(); // Use shared ObjectMapper

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return null;
            }
            try {
                // Serialize the POJO to a byte array (JSON string)
                return objectWriter.writeValueAsBytes(data);
            } catch (IOException e) {
                log.error("Error serializing POJO {} to JSON for topic {}: {}", data.getClass().getName(), topic, e.getMessage(), e);
                throw new RuntimeException("Failed to serialize POJO to JSON", e);
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // No specific configuration needed for Jackson ObjectMapper here, as it's static.
        }

        @Override
        public void close() {
            // No-op, as ObjectMapper is static and managed externally
        }
    }

    /**
     * Generic JSON Deserializer for POJOs using Jackson's ObjectMapper.
     * It deserializes a byte array (JSON string) into the specified POJO class.
     */
    public static class JsonPojoDeserializer<T> implements Deserializer<T> {

        private Class<T> targetType;
        private ObjectReader objectReader;

        /**
         * Constructor that takes the target POJO class. This is the preferred way
         * to instantiate this deserializer when the type is known upfront.
         * @param targetType The Class object of the POJO to deserialize to.
         */
        public JsonPojoDeserializer(Class<T> targetType) {
            this.targetType = targetType;
            if (targetType != null) {
                this.objectReader = OBJECT_MAPPER.readerFor(targetType); // Create ObjectReader for efficiency
            } else {
                log.warn("JsonPojoDeserializer instantiated with null targetType. It must be configured later.");
            }
        }

        /**
         * No-arg constructor for reflection-based instantiation.
         * The `targetType` MUST be set via the `configure` method or other means before `deserialize` is called.
         */
        public JsonPojoDeserializer() {
            this(null); // Delegate to the parameterized constructor
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // If targetType was not set in the constructor (e.g., via no-arg constructor),
            // attempt to retrieve it from Kafka consumer/producer configurations.
            // This requires a custom configuration property, e.g., "value.deserializer.target.class".
            if (this.targetType == null) {
                String className = (String) configs.get("value.deserializer.target.class"); // Custom config key
                if (className != null) {
                    try {
                        this.targetType = (Class<T>) Class.forName(className);
                        this.objectReader = OBJECT_MAPPER.readerFor(targetType);
                        log.info("JsonPojoDeserializer configured with targetType from configs: {}", className);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException("Target class not found for deserializer: " + className, e);
                    }
                } else {
                    throw new IllegalStateException("JsonPojoDeserializer: Target type not configured. Cannot deserialize.");
                }
            }
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            if (targetType == null || objectReader == null) {
                throw new IllegalStateException("JsonPojoDeserializer is not properly configured with a target type for topic: " + topic);
            }
            try {
                // Deserialize the byte array (JSON content) into the target POJO class
                return objectReader.readValue(data);
            } catch (IOException e) {
                log.error("Error deserializing JSON bytes to POJO type {} for topic {}: {}", targetType.getName(), topic, e.getMessage(), e);
                // Depending on your error handling strategy, you might:
                // 1. Return null (to drop malformed records)
                // 2. Throw a RuntimeException (to fail the stream)
                // 3. Implement a Dead Letter Queue (DLQ) mechanism
                throw new RuntimeException("Failed to deserialize JSON bytes to " + targetType.getName(), e);
            }
        }

        @Override
        public void close() {
            // No-op, as ObjectMapper is static and managed externally
        }
    }

    /**
     * Creates keySerde and valueSerde from the given TopicConfig.
     *
     * @param topicConfig The TopicConfig containing the key and value class names.
     * @return A pair of Serdes for the key and value.
     */
    public static Pair<Serde<Object>, Serde<Object>> createSerdesFromTopicConfig(AggregationDefinition.TopicConfig topicConfig) {
        Class<?> keyClass;
        Class<?> valueClass;

        try {
            keyClass = Class.forName(topicConfig.getKeyClass());
            valueClass = Class.forName(topicConfig.getValueClass());
        } catch (ClassNotFoundException e) {
            log.error("Failed to find class for topic config: keyClass={}, valueClass={}", topicConfig.getKeyClass(), topicConfig.getValueClass(), e);
            throw new RuntimeException("Failed to load class from topic config", e);
        }

        Serde<Object> keySerde = SerdeFactory.createSerde((Class<Object>) keyClass);
        Serde<Object> valueSerde = SerdeFactory.createSerde((Class<Object>) valueClass);

        return Pair.of(keySerde, valueSerde);
    }
}