package com.common.kafkastreams.service;

import com.common.kafkastreams.config.AggregationDefinition;
import com.common.kafkastreams.mapper.DynamicPojoKeyExtractor;
import com.common.kafkastreams.serdes.SerdeFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
@Slf4j
public class KTableRegistry {
    // A thread-safe map to store already created KTable instances, keyed by topic name
    private final ConcurrentMap<String, KTable<Object, Object>> kTableCache = new ConcurrentHashMap<>();

    // You could also add methods for GlobalKTable if you use them in a similar fashion,
    // to ensure they are also de-duplicated.
    private final ConcurrentMap<String, GlobalKTable<Object, Object>> GlobalKTableCache = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper; // Add ObjectMapper instance

    public KTableRegistry() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false); // Ignore unknown fields in JSON
    }

    // Helper method to determine if key extraction from value is configured
    private boolean isKeyExtractionConfigured(AggregationDefinition.TopicConfig topicConfig) {
        return topicConfig.getKeyFromValueExtractorClass() != null &&
                !topicConfig.getKeyFromValueExtractorClass().trim().isEmpty() &&
                topicConfig.getKeyFromValueExtractionConfig() != null &&
                !topicConfig.getKeyFromValueExtractionConfig().trim().isEmpty();
    }

    /**
     * Retrieves an existing KTable for a given topic, or creates and registers a new one if it doesn't exist.
     * This ensures only one KTable instance is created per topic within a single StreamsBuilder.
     *
     * @param topicConfig The configuration (name, keyClass, valueClass) of the topic to turn into a KTable.
     * @return A KTable instance for the given topic.
     */
//    public KTable<Object, Object> getOrCreateKTable(StreamsBuilder streamsBuilder, AggregationDefinition.TopicConfig topicConfig) {
//        return kTableCache.computeIfAbsent(topicConfig.getName(), topicName ->
//                buildKTableFromTopicConfigInternal(streamsBuilder, topicConfig)
//        );
//    }

    /**
     * Builds and returns a KTable from a given TopicConfig, leveraging the common KStream creation logic.
     * This method is primarily used internally by getOrCreateKTable.
     *
     * @param streamsBuilder The StreamsBuilder instance.
     * @param topicConfig The configuration for the source topic.
     * @return A KTable representing the stream from the configured topic.
     */
    @SuppressWarnings("unchecked")
    private KTable<Object, Object> buildKTableFromTopicConfigInternal(StreamsBuilder streamsBuilder, AggregationDefinition.TopicConfig topicConfig) {
        KStream<Object, Object> kStream = createAndConfigureKStream(streamsBuilder, topicConfig);
        return kStream.toTable();
    }


    /**
     * Creates and configures a KStream from a given TopicConfig. This includes:
     * - Determining key and value classes.
     * - Creating appropriate Serdes.
     * - Applying default key handling if 'defaultKeyIfNull' is configured.
     *
     * This method is common for both KStream and KTable sources.
     *
     * @param streamsBuilder The StreamsBuilder instance.
     * @param topicConfig The configuration for the source topic.
     * @return A KStream with the specified key/value types and default key handling.
     */
    @SuppressWarnings("unchecked")
//    public KStream<Object, Object> createAndConfigureKStream(StreamsBuilder streamsBuilder, AggregationDefinition.TopicConfig topicConfig) {
//        log.info("Creating KStream from topic config: {}", topicConfig.getName());
//        log.info("Creating KStream from topic config: {}", topicConfig.getName());
//
//        // Default to Object.class for generic key/value handling, assuming JSON or similar structure.
//        // SerdeFactory should be configured to handle Object.class with appropriate JSON serdes.
//        Serde<Object> keySerde = SerdeFactory.createSerde(Object.class);
//        Serde<Object> valueSerde = SerdeFactory.createSerde(Object.class);
//
//        KStream<Object, Object> kStream = streamsBuilder.stream(topicConfig.getName(), Consumed.with(keySerde, valueSerde));
//
//        // Apply key extraction from value if configured (for topics initially without keys, or to derive composite keys)
//        if (topicConfig.getKeyFromValueExtractorClass() != null && !topicConfig.getKeyFromValueExtractorClass().isEmpty()) {
//            log.info("Applying key extractor from value for topic '{}' with class: {}",
//                    topicConfig.getName(), topicConfig.getKeyFromValueExtractorClass());
//            try {
//                Class<?> extractorClass = Class.forName(topicConfig.getKeyFromValueExtractorClass());
//                KeyValueMapper<Object, Object, Object> keyMapper;
//
//                // Handle DynamicPojoKeyExtractor specifically if it requires a config string constructor
//                if (DynamicPojoKeyExtractor.class.isAssignableFrom(extractorClass)) {
//                    if (topicConfig.getKeyFromValueExtractionConfig() == null || topicConfig.getKeyFromValueExtractionConfig().isEmpty()) {
//                        throw new IllegalArgumentException("KeyFromValueExtractorClass " + extractorClass.getName() + " requires keyFromValueExtractionConfig for topic: " + topicConfig.getName());
//                    }
//                    Constructor<KeyValueMapper<Object, Object, Object>> constructor =
//                            (Constructor<KeyValueMapper<Object, Object, Object>>) extractorClass.getConstructor(String.class);
//                    keyMapper = constructor.newInstance(topicConfig.getKeyFromValueExtractionConfig());
//                } else {
//                    // For other custom KeyValueMappers, assume a no-arg constructor by default
//                    Constructor<KeyValueMapper<Object, Object, Object>> constructor =
//                            (Constructor<KeyValueMapper<Object, Object, Object>>) extractorClass.getConstructor();
//                    keyMapper = constructor.newInstance();
//                }
//
//                log.info("Using key extractor: {}", keyMapper.getClass().getName());
//                kStream = kStream.selectKey(keyMapper);
//            } catch (Exception e) {
//                log.error("Failed to instantiate or apply keyFromValueExtractorClass '{}' for topic '{}'",
//                        topicConfig.getKeyFromValueExtractorClass(), topicConfig.getName(), e);
//                throw new RuntimeException("Error with key from value extractor.", e);
//            }
//        }
//
//        return kStream;
//    }

    public KStream<Object, Object> createAndConfigureKStream(StreamsBuilder streamsBuilder, AggregationDefinition.TopicConfig topicConfig) {
        KStream<Object, Object> stream = streamsBuilder.stream(
                topicConfig.getName(),
                // Consume with generic Object Serdes for flexible JSON handling
                Consumed.with(SerdeFactory.createSerde(Object.class), SerdeFactory.createSerde(Object.class))
        );

        // Check if key extraction from value is configured
        if (isKeyExtractionConfigured(topicConfig)) {
            log.info("Re-keying KStream for topic '{}' using extractor: {}", topicConfig.getName(), topicConfig.getKeyFromValueExtractorClass());
            // Instantiate and apply the DynamicPojoKeyExtractor
            DynamicPojoKeyExtractor<Object, Object> keyExtractor = new DynamicPojoKeyExtractor<>(topicConfig.getKeyFromValueExtractionConfig());
            return stream.selectKey(keyExtractor);
        } else {
            log.info("Topic '{}' does not have key extraction configured. Using existing Kafka message key.", topicConfig.getName());
            stream.peek((key, value) -> {
                try {
                    String keyJson = objectMapper.writeValueAsString(key);
                    String valueJson = objectMapper.writeValueAsString(value);
                    log.info("Key: {}, Value: {}", keyJson, valueJson);
                } catch (Exception e) {
                    log.error("Failed to serialize key/value for topic '{}'", topicConfig.getName());
                }
            });
            return stream; // Use the stream as-is, its keys are already correct
        }
    }

    public KTable<Object, Object> getOrCreateKTable(StreamsBuilder streamsBuilder, AggregationDefinition.TopicConfig topicConfig) {
        // KTable is built from a KStream, then materialized.
        KStream<Object, Object> stream = streamsBuilder.stream(
                topicConfig.getName(),
                Consumed.with(SerdeFactory.createSerde(Object.class), SerdeFactory.createSerde(Object.class))
        );

        KStream<Object, Object> reKeyedStream;
        if (isKeyExtractionConfigured(topicConfig)) {
            log.info("Re-keying KStream for KTable topic '{}' using extractor: {}", topicConfig.getName(), topicConfig.getKeyFromValueExtractorClass());
            DynamicPojoKeyExtractor<Object, Object> keyExtractor = new DynamicPojoKeyExtractor<>(topicConfig.getKeyFromValueExtractionConfig());
            reKeyedStream = stream.selectKey(keyExtractor);
        } else {
            log.info("Topic '{}' does not have key extraction configured. Using existing Kafka message key for KTable source.", topicConfig.getName());
            log.info("======= Stream peek for KTable source: {}", topicConfig.getName());
            stream.peek((key, value) -> {
                try {
                    String keyJson = objectMapper.writeValueAsString(key);
                    String valueJson = objectMapper.writeValueAsString(value);
                    log.info("Key: {}, Value: {}", keyJson, valueJson);
                } catch (Exception e) {
                    log.error("Failed to serialize key/value for topic '{}'", topicConfig.getName());
                }
            });
            reKeyedStream = stream;
        }

        // Materialize the re-keyed stream into a KTable
        String stateStoreName = topicConfig.getName() + "-ktable-store"; // Example: derive a unique store name
        log.info("Materializing KTable for topic '{}' into state store '{}'", topicConfig.getName(), stateStoreName);

        return reKeyedStream.toTable(Materialized.with(SerdeFactory.createSerde(Object.class), SerdeFactory.createSerde(Object.class)));
//        return reKeyedStream.toTable(
//                Materialized.as(stateStoreName)
//                        .withKeySerde(SerdeFactory.createSerde(Object.class)) // Keys are Objects (Maps)
//                        .withValueSerde(SerdeFactory.createSerde(Object.class)) // Values are Objects (Maps)
//        );
    }

    public GlobalKTable<Object, Object> getOrCreateGlobalKTable(StreamsBuilder streamsBuilder, AggregationDefinition.TopicConfig topicConfig) {
        return GlobalKTableCache.computeIfAbsent(topicConfig.getName(), topicName -> {
//            Pair<Serde<Object>, Serde<Object>> serdes = SerdeFactory.createSerdesFromTopicConfig(topicConfig);
            Serde<Object> keySerde = SerdeFactory.createSerde(Object.class);
            Serde<Object> valueSerde = SerdeFactory.createSerde(Object.class);
            return streamsBuilder.globalTable(topicName, Consumed.with(keySerde, valueSerde));
        });
    }
}