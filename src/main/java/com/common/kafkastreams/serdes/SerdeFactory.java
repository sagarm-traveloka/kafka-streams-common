package com.common.kafkastreams.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonSerde;

public class SerdeFactory {
    @SuppressWarnings("unchecked")
    public static <T> Serde<T> createSerde(String className) {
        if (className == null || className.isEmpty()) {
            throw new IllegalArgumentException("Class name for Serde cannot be null or empty.");
        }

        switch (className) {
            case "java.lang.String": return (Serde<T>) Serdes.String();
            case "java.lang.Long": return (Serde<T>) Serdes.Long();
            case "java.lang.Integer": return (Serde<T>) Serdes.Integer();
            case "java.lang.Double": return (Serde<T>) Serdes.Double();
            case "java.lang.Float": return (Serde<T>) Serdes.Float();
            case "java.lang.byte[]": return (Serde<T>) Serdes.ByteArray();
            // Add more standard Serdes as needed
            default:
                try {
                    // Assume custom classes use JsonSerde
                    Class<T> clazz = (Class<T>) Class.forName(className);
                    return new JsonSerde<>(clazz);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Could not find class for Serde: " + className, e);
                }
        }
    }
}
