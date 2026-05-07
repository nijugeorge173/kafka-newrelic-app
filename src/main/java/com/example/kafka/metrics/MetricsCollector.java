package com.example.kafka.metrics;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.consumer.KafkaConsumerService;
import com.example.kafka.producer.KafkaProducerService;
import com.newrelic.api.agent.NewRelic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsCollector implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "metrics-collector"));

    private final KafkaProducerService producerService;
    private final List<KafkaConsumerService> consumerServices;

    private final AtomicLong lastProducerCount = new AtomicLong(0);
    private final AtomicLong lastConsumerCount = new AtomicLong(0);

    public MetricsCollector(KafkaProducerService producerService,
                            List<KafkaConsumerService> consumerServices) {
        this.producerService = producerService;
        this.consumerServices = consumerServices;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(
                this::flush,
                KafkaConfig.METRICS_FLUSH_INTERVAL_SECONDS,
                KafkaConfig.METRICS_FLUSH_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
        log.info("MetricsCollector started (interval={}s)", KafkaConfig.METRICS_FLUSH_INTERVAL_SECONDS);
    }

    private void flush() {
        try {
            long currentProduced = producerService.getMessagesSent();
            long producerErrors  = producerService.getErrors();

            long currentConsumed = consumerServices.stream()
                    .mapToLong(KafkaConsumerService::getMessagesConsumed).sum();
            long consumerErrors  = consumerServices.stream()
                    .mapToLong(KafkaConsumerService::getErrors).sum();

            long producedDelta = currentProduced - lastProducerCount.getAndSet(currentProduced);
            long consumedDelta = currentConsumed - lastConsumerCount.getAndSet(currentConsumed);

            float producerTps = (float) producedDelta / KafkaConfig.METRICS_FLUSH_INTERVAL_SECONDS;
            float consumerTps = (float) consumedDelta / KafkaConfig.METRICS_FLUSH_INTERVAL_SECONDS;
            long lag           = currentProduced - currentConsumed;

            // Aggregate throughput metrics — visible in NR under Custom/ namespace
            NewRelic.recordMetric(KafkaConfig.NR_PRODUCER_PREFIX + "ThroughputPerSec", producerTps);
            NewRelic.recordMetric(KafkaConfig.NR_PRODUCER_PREFIX + "TotalMessages",    currentProduced);
            NewRelic.recordMetric(KafkaConfig.NR_PRODUCER_PREFIX + "ErrorCount",       producerErrors);

            NewRelic.recordMetric(KafkaConfig.NR_CONSUMER_PREFIX + "ThroughputPerSec", consumerTps);
            NewRelic.recordMetric(KafkaConfig.NR_CONSUMER_PREFIX + "TotalMessages",    currentConsumed);
            NewRelic.recordMetric(KafkaConfig.NR_CONSUMER_PREFIX + "ErrorCount",       consumerErrors);

            NewRelic.recordMetric("Custom/Kafka/ConsumerLag",      lag);
            NewRelic.recordMetric("Custom/Kafka/ActiveConsumers",  consumerServices.size());

            log.info("[metrics] produced={} ({}/s) consumed={} ({}/s) lag={} prodErr={} consErr={}",
                    currentProduced, String.format("%.1f", producerTps),
                    currentConsumed, String.format("%.1f", consumerTps),
                    lag, producerErrors, consumerErrors);

        } catch (Exception e) {
            log.error("Error flushing metrics to New Relic", e);
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
    }
}
