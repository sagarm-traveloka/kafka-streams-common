package com.common.kafkastreams.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * A conceptual service to resolve POJO class names for Kafka topics.
 * In a real-world scenario, this would interact with a Schema Registry,
 * a custom metadata service, or even reflectively load classes based on convention.
 */
@Service
@Slf4j
public class SchemaService {

    // Mock data: In a real application, this would query a Schema Registry or a database
    private final Map<String, String> topicPojoMap = new HashMap<>();

    public SchemaService() {
        // Populate with example mappings
        // These would typically come from an external configuration or actual schema registry
        topicPojoMap.put("table1-topic", "com.yourcompany.pojo.Table1Value");
        topicPojoMap.put("table2-topic", "com.yourcompany.pojo.Table2Value");
        topicPojoMap.put("table3-topic", "com.yourcompany.pojo.Table3Value");
        topicPojoMap.put("table4-topic", "com.yourcompany.pojo.Table4Value");
        // Add mappings for other topics if needed, e.g., output topics
        topicPojoMap.put("final-enriched-orders-topic", "com.yourcompany.pojo.FinalJoinedValue");
        // For your 'order' topic from 'aggregation-customer-orders-enrichment.json'
        topicPojoMap.put("order", "com.service.kakfastreams.orders.model.Order");
        topicPojoMap.put("user", "com.service.kakfastreams.orders.model.Customer");
        topicPojoMap.put("enriched-orders", "com.service.kakfastreams.orders.model.EnrichedOrder");

        log.info("SchemaService initialized with mock topic-to-POJO mappings.");
    }

    /**
     * Retrieves the fully qualified class name of the POJO for a given topic.
     * In a real system, this would query a Schema Registry for the latest schema,
     * potentially resolving an Avro or Protobuf schema to a generated Java class.
     * Or, it could query a database of topic-to-POJO mappings.
     *
     * @param topicName The name of the Kafka topic.
     * @return The FQCN of the POJO class, or null if not found.
     */
    public String getPojoClassNameForTopic(String topicName) {
        String pojoClass = topicPojoMap.get(topicName);
        if (pojoClass == null) {
            log.warn("No POJO class mapping found for topic: {}", topicName);
        } else {
            log.debug("Resolved POJO class for topic {}: {}", topicName, pojoClass);
        }
        return pojoClass;
    }
}
