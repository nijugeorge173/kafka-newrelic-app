package com.example.kafka.consumer;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.model.EventMessage;
import com.example.kafka.wrapper.KafkaHeadersWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.TransportType;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaConsumerService implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong messagesConsumed = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);
    private final String consumerId;

    public KafkaConsumerService(String consumerId) {
        this.consumerId = consumerId;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + consumerId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        this.consumer = new KafkaConsumer<>(props);
        log.info("KafkaConsumerService {} initialized", consumerId);
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC));
        log.info("Consumer {} subscribed to topic '{}'", consumerId, KafkaConfig.TOPIC);

        while (running.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // Expected on shutdown
                if (running.get()) throw e;
            } catch (Exception e) {
                errors.incrementAndGet();
                NewRelic.noticeError(e);
                log.error("Consumer {} poll error", consumerId, e);
            }
        }
        log.info("Consumer {} stopped (consumed={})", consumerId, messagesConsumed.get());
    }

    @Trace(dispatcher = true)
    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            // Accept distributed trace headers to link this span back to the producer
            NewRelic.getAgent().getTransaction()
                    .acceptDistributedTraceHeaders(
                            TransportType.Kafka,
                            new KafkaHeadersWrapper(record.headers()));

            NewRelic.addCustomParameter("consumerId", consumerId);
            NewRelic.addCustomParameter("kafkaTopic", record.topic());
            NewRelic.addCustomParameter("kafkaPartition", record.partition());

            long lagMs = System.currentTimeMillis() - record.timestamp();
            long processStart = System.currentTimeMillis();

            EventMessage message = objectMapper.readValue(record.value(), EventMessage.class);
            NewRelic.addCustomParameter("eventType", message.getEventType());

            simulateProcessing(message.getEventType());

            long processingMs = System.currentTimeMillis() - processStart;
            messagesConsumed.incrementAndGet();

            NewRelic.recordMetric(KafkaConfig.NR_CONSUMER_PREFIX + "MessagesPerSecond", 1);
            NewRelic.recordMetric(KafkaConfig.NR_CONSUMER_PREFIX + "LagMs", lagMs);
            NewRelic.recordMetric(KafkaConfig.NR_CONSUMER_PREFIX + "ProcessingTimeMs", processingMs);
            NewRelic.recordMetric(KafkaConfig.NR_CONSUMER_PREFIX + "EventType/" + message.getEventType(), 1);

            log.debug("Consumer {} processed msg={} eventType={} lag={}ms processing={}ms",
                    consumerId, message.getMessageId(), message.getEventType(), lagMs, processingMs);

        } catch (Exception e) {
            errors.incrementAndGet();
            NewRelic.noticeError(e);
            log.error("Consumer {} failed to process partition={} offset={}",
                    consumerId, record.partition(), record.offset(), e);
        }
    }

    private void simulateProcessing(String eventType) throws InterruptedException {
        // Different event types have different processing costs — makes metrics interesting
        long sleepMs = switch (eventType) {
            case "ORDER_PLACED"         -> 5  + (long)(Math.random() * 15);
            case "PAYMENT_PROCESSED"    -> 10 + (long)(Math.random() * 20);
            case "INVENTORY_UPDATE"     -> 2  + (long)(Math.random() * 8);
            case "REFUND_REQUEST"       -> 15 + (long)(Math.random() * 30);
            case "SHIPMENT_UPDATE"      -> 3  + (long)(Math.random() * 10);
            default                     -> 1  + (long)(Math.random() * 5);
        };
        Thread.sleep(sleepMs);
    }

    public long getMessagesConsumed() { return messagesConsumed.get(); }
    public long getErrors() { return errors.get(); }

    public void stop() {
        running.set(false);
        consumer.wakeup();
    }

    @Override
    public void close() {
        stop();
        consumer.close();
    }
}
