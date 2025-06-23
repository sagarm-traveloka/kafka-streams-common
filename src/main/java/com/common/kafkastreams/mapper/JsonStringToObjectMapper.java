package com.common.kafkastreams.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.io.IOException;
import java.io.StringReader;

@Slf4j
public class JsonStringToObjectMapper<T> implements ValueMapper<Object, T> {

    private final Class<T> targetType;
    private final ObjectReader objectReader; // Use ObjectReader for better performance/safety

    public JsonStringToObjectMapper(Class<T> targetType) {
        if (targetType == null) {
            throw new IllegalArgumentException("Target POJO type for JsonStringToObjectMapper cannot be null.");
        }
        this.targetType = targetType;
        ObjectMapper objectMapper = new ObjectMapper();
        // Configure ObjectMapper as needed, e.g.:
        // objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // objectMapper.findAndRegisterModules(); // For Java 8 Date/Time API support etc.
        this.objectReader = objectMapper.readerFor(targetType);
    }

    @Override
    public T apply(Object value) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof String)) {
            String errorMessage = String.format(
                    "JsonStringToObjectMapper expected a String input, but received type: %s with value: %s. " +
                            "This typically means onWireValueClass was not correctly set to String.",
                    value.getClass().getName(), value.toString()
            );
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        String jsonString = (String) value;

        try {
            // FIX: Use new StringReader(jsonString) for ObjectReader.readValue()
            return objectReader.readValue(new StringReader(jsonString));
        } catch (IOException e) {
            String errorMessage = String.format(
                    "Error deserializing JSON string '%s' to POJO type '%s' in JsonStringToObjectMapper.",
                    jsonString, targetType.getName()
            );
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }
}
