package com.common.kafkastreams.serdes;

import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class SerdeFactory {

    private static final ConcurrentMap<Class<?>, Serde<?>> serdeCache = new ConcurrentHashMap<>();
    private static final ObjectMapper sharedObjectMapper = new ObjectMapper(); // Shared ObjectMapper instance

    static {
        // Configure shared ObjectMapper once upon class loading
        sharedObjectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        // Add any other common configurations for ObjectMapper here, e.g.,
        sharedObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Creates or retrieves a cached Serde for the given class type.
     * This method is synchronized via ConcurrentMap's computeIfAbsent to ensure thread safety
     * and single instance creation for each type.
     *
     * @param type The Class object for which to create the Serde.
     * @param <T> The generic type of the Serde.
     * @return A Serde instance compatible with the given type.
     * @throws IllegalArgumentException if the type is null.
     */
    @SuppressWarnings("unchecked")
    public static <T> Serde<T> createSerde(Class<T> type) {
        if (type == null) {
            throw new IllegalArgumentException("Type for Serde cannot be null.");
        }

        log.info("Creating Serde for type: {}", type.getName());

        return (Serde<T>) serdeCache.computeIfAbsent(type, k -> {
            if (String.class.equals(k)) {
                log.debug("Creating String Serde for type: {}", k.getName());
                return Serdes.String();
            } else if (Long.class.equals(k) || long.class.equals(k)) {
                log.debug("Creating Long Serde for type: {}", k.getName());
                return Serdes.Long();
            } else if (Integer.class.equals(k) || int.class.equals(k)) {
                log.debug("Creating Integer Serde for type: {}", k.getName());
                return Serdes.Integer();
            } else if (Double.class.equals(k) || double.class.equals(k)) {
                log.debug("Creating Double Serde for type: {}", k.getName());
                return Serdes.Double();
            } else if (byte[].class.equals(k)) {
                log.debug("Creating ByteArray Serde for type: {}", k.getName());
                return Serdes.ByteArray();
            } else if (Object.class.equals(k) || Map.class.equals(k)) {
                // Crucial change: For generic Object or Map types, use JsonSerde.
                // This ensures LinkedHashMap keys are correctly serialized to JSON.
                log.info("Creating JsonSerde for generic Object/Map type: {}", k.getName());
                return new JsonSerde<>((Class<T>) k, sharedObjectMapper);
            } else {
                // Fallback for any other custom POJO types: use JsonSerde.
                // Assumes other custom types are also JSON-serializable.
                log.info("Creating JsonSerde for custom POJO type: {}", k.getName());
                return new JsonSerde<>(k, sharedObjectMapper);
            }
        });
    }

    /**
     * Provides access to the shared ObjectMapper instance used by the SerdeFactory.
     * This can be useful if you need to perform manual JSON operations consistent
     * with the Serde configuration.
     *
     * @return The shared ObjectMapper instance.
     */
    public static ObjectMapper getSharedObjectMapper() {
        return sharedObjectMapper;
    }
}