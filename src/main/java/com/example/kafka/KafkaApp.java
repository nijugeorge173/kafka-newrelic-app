package com.example.kafka;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.consumer.KafkaConsumerService;
import com.example.kafka.load.LoadGenerator;
import com.example.kafka.metrics.MetricsCollector;
import com.example.kafka.producer.KafkaProducerService;
import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaApp {

    private static final Logger log = LoggerFactory.getLogger(KafkaApp.class);

    public static void main(String[] args) throws Exception {
        log.info("=== Kafka New Relic APM Demo ===");
        log.info("  Bootstrap : {}", KafkaConfig.BOOTSTRAP_SERVERS);
        log.info("  Topic     : {}", KafkaConfig.TOPIC);
        log.info("  Load      : {} msg/sec", KafkaConfig.MESSAGES_PER_SECOND);
        log.info("  Consumers : {}", KafkaConfig.NUM_CONSUMERS);

        ensureTopicExists();

        KafkaProducerService producerService = new KafkaProducerService();

        List<KafkaConsumerService> consumerServices = new ArrayList<>();
        List<Thread> consumerThreads = new ArrayList<>();

        for (int i = 0; i < KafkaConfig.NUM_CONSUMERS; i++) {
            KafkaConsumerService cs = new KafkaConsumerService("consumer-" + i);
            consumerServices.add(cs);
            Thread t = new Thread(cs, "kafka-consumer-" + i);
            t.setDaemon(false);
            t.start();
            consumerThreads.add(t);
        }

        MetricsCollector metricsCollector = new MetricsCollector(producerService, consumerServices);
        metricsCollector.start();

        LoadGenerator loadGenerator = new LoadGenerator(producerService);
        loadGenerator.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping gracefully...");
            loadGenerator.close();
            consumerServices.forEach(KafkaConsumerService::stop);
            metricsCollector.close();
            producerService.close();

            long totalProduced = producerService.getMessagesSent();
            long totalConsumed = consumerServices.stream()
                    .mapToLong(KafkaConsumerService::getMessagesConsumed).sum();
            log.info("Final stats: produced={} consumed={} lag={}",
                    totalProduced, totalConsumed, totalProduced - totalConsumed);
        }, "shutdown-hook"));

        log.info("Application running — press Ctrl+C to stop.");
    }

    private static void ensureTopicExists() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get(15, TimeUnit.SECONDS);

            if (!existing.contains(KafkaConfig.TOPIC)) {
                NewTopic topic = new NewTopic(KafkaConfig.TOPIC, 3, (short) 1);
                topic.configs(Map.of(
                        "retention.ms",   "3600000",
                        "cleanup.policy", "delete"));
                admin.createTopics(Collections.singletonList(topic))
                        .all().get(15, TimeUnit.SECONDS);
                log.info("Created topic '{}' with 3 partitions", KafkaConfig.TOPIC);
            } else {
                log.info("Topic '{}' already exists", KafkaConfig.TOPIC);
            }
        } catch (Exception e) {
            log.warn("Could not verify/create topic '{}': {} — continuing anyway",
                    KafkaConfig.TOPIC, e.getMessage());
        }
    }
}
