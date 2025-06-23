package com.common.kafkastreams.service;

import com.common.kafkastreams.config.AggregationDefinition;
import com.common.kafkastreams.serdes.SerdeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
@Slf4j
public class KTableRegistry {

    // The single StreamsBuilder instance provided by Spring Kafka in this application
//    @Autowired
//    private StreamsBuilder streamsBuilder;

    // A thread-safe map to store already created KTable instances, keyed by topic name
    private final ConcurrentMap<String, KTable<Object, Object>> registeredKTables = new ConcurrentHashMap<>();

    // You could also add methods for GlobalKTable if you use them in a similar fashion,
    // to ensure they are also de-duplicated.
    private final ConcurrentMap<String, GlobalKTable<Object, Object>> registeredGlobalKTables = new ConcurrentHashMap<>();



    /**
     * Retrieves an existing KTable for a given topic, or creates and registers a new one if it doesn't exist.
     * This ensures only one KTable instance is created per topic within a single StreamsBuilder.
     *
     * @param topicConfig The configuration (name, keyClass, valueClass) of the topic to turn into a KTable.
     * @return A KTable instance for the given topic.
     */
    public KTable<Object, Object> getOrCreateKTable(StreamsBuilder streamsBuilder, AggregationDefinition.TopicConfig topicConfig) {
        return registeredKTables.computeIfAbsent(topicConfig.getName(), topicName -> {
            // This lambda is executed ONLY if the KTable for topicName is not already in the map
            Pair<Serde<Object>, Serde<Object>> serdes = SerdeFactory.createSerdesFromTopicConfig(topicConfig);

            log.info("Creating KTable for topic and registering to avoid duplicate KTable: {}", topicName);
            // Create and return a new KTable for this topic. This is where the actual materialization happens.
            return streamsBuilder.table(topicName, Consumed.with(serdes.getLeft(), serdes.getRight()));
        });
    }

    public GlobalKTable<Object, Object> getOrCreateGlobalKTable(StreamsBuilder streamsBuilder, AggregationDefinition.TopicConfig topicConfig) {
        return registeredGlobalKTables.computeIfAbsent(topicConfig.getName(), topicName -> {
            Pair<Serde<Object>, Serde<Object>> serdes = SerdeFactory.createSerdesFromTopicConfig(topicConfig);
            return streamsBuilder.globalTable(topicName, Consumed.with(serdes.getLeft(), serdes.getRight()));
        });
    }
}