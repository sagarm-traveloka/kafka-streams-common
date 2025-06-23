package com.common.kafkastreams.service;

import com.common.kafkastreams.config.AggregationDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
public class TopicProvisioner {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public void provisionTopic(AggregationDefinition.OutputTopicConfig topicConfig) {
        if (!topicConfig.isEnabled() || topicConfig.getName() == null) {
            return;
        }

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(
                    topicConfig.getName(),
                    topicConfig.getPartitions() != null ? topicConfig.getPartitions() : 3, // Default partitions
                    topicConfig.getReplicationFactor() != null ? topicConfig.getReplicationFactor() : (short) 1 // Default RF
            );

            if (topicConfig.getRetentionMs() != 0) { // 0 can be a sentinel for default
                if (topicConfig.getRetentionMs() == -1) { // -1 for compact
                    newTopic.configs(Collections.singletonMap("cleanup.policy", "compact"));
                } else {
                    newTopic.configs(Collections.singletonMap("retention.ms", String.valueOf(topicConfig.getRetentionMs())));
                }
            }

            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            log.info("Topic '{}' provisioned/ensured successfully with retention: {} ms",
                    topicConfig.getName(), topicConfig.getRetentionMs() == -1 ? "compact" : topicConfig.getRetentionMs());
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                log.info("Topic '{}' already exists. Skipping creation.", topicConfig.getName());
            } else {
                log.error("Failed to provision topic '{}'", topicConfig.getName(), e);
            }
        }
    }
}