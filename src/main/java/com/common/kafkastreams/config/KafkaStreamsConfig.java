package com.common.kafkastreams.config;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder; // Add this import
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfig {

    // You might still keep some global properties here if needed,
    // but specific aggregation properties are now in AggregationDefinition.

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig(@Value("${spring.application.name}") String applicationId) {
        log.info("Configuring Kafka Streams with application ID: {}", applicationId);
        if(!StringUtils.hasText(applicationId))
            throw new IllegalArgumentException("Application ID must be set in application properties");
        Map<String, Object> props = getStringObjectMap(applicationId);
        return new KafkaStreamsConfiguration(props);
    }

    private static Map<String, Object> getStringObjectMap(String applicationId) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Get from application.yml if preferred
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, "false"); // Disable metrics push to avoid conflicts with Spring Boot's metrics
        return props;
    }

    // Explicitly expose StreamsBuilder bean
//    @Bean
//    public StreamsBuilder streamsBuilder() {
//        return new StreamsBuilder();
//    }
}