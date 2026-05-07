package com.example.kafka.load;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.model.EventMessage;
import com.example.kafka.producer.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class LoadGenerator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LoadGenerator.class);

    private static final List<String> EVENT_TYPES = List.of(
            "ORDER_PLACED", "PAYMENT_PROCESSED", "INVENTORY_UPDATE",
            "USER_ACTION", "SHIPMENT_UPDATE", "REFUND_REQUEST",
            "SESSION_START", "SESSION_END");

    private final KafkaProducerService producerService;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong sequenceCounter = new AtomicLong(0);
    private final String hostname;

    public LoadGenerator(KafkaProducerService producerService) {
        this.producerService = producerService;
        this.scheduler = Executors.newScheduledThreadPool(
                2, r -> new Thread(r, "load-generator"));
        String h;
        try { h = InetAddress.getLocalHost().getHostName(); }
        catch (Exception e) { h = "unknown-host"; }
        this.hostname = h;
    }

    public void start() {
        // Schedule one send per (1_000_000 / MESSAGES_PER_SECOND) microseconds
        long intervalMicros = 1_000_000L / KafkaConfig.MESSAGES_PER_SECOND;
        scheduler.scheduleAtFixedRate(this::sendOne, 0, intervalMicros, TimeUnit.MICROSECONDS);
        log.info("LoadGenerator started: {} msg/sec on topic '{}'",
                KafkaConfig.MESSAGES_PER_SECOND, KafkaConfig.TOPIC);
    }

    private void sendOne() {
        String eventType = EVENT_TYPES.get((int)(Math.random() * EVENT_TYPES.size()));
        long seq = sequenceCounter.incrementAndGet();

        EventMessage msg = new EventMessage(
                UUID.randomUUID().toString(),
                eventType,
                buildPayload(eventType, seq),
                System.currentTimeMillis(),
                hostname,
                seq);

        producerService.send(msg);
    }

    private String buildPayload(String eventType, long seq) {
        return switch (eventType) {
            case "ORDER_PLACED" ->
                    String.format("{\"orderId\":\"ORD-%06d\",\"amount\":%.2f,\"items\":%d}",
                            seq, 10 + Math.random() * 990, (int)(1 + Math.random() * 10));
            case "PAYMENT_PROCESSED" ->
                    String.format("{\"transactionId\":\"TXN-%06d\",\"status\":\"SUCCESS\",\"amount\":%.2f}",
                            seq, 10 + Math.random() * 990);
            case "INVENTORY_UPDATE" ->
                    String.format("{\"sku\":\"SKU-%04d\",\"qty\":%d,\"warehouse\":\"WH-%02d\"}",
                            (int)(Math.random() * 9999), (int)(Math.random() * 500),
                            (int)(1 + Math.random() * 5));
            case "USER_ACTION" ->
                    String.format("{\"userId\":\"USR-%05d\",\"action\":\"click\",\"page\":\"/p/%d\"}",
                            (int)(Math.random() * 99999), (int)(Math.random() * 50));
            case "REFUND_REQUEST" ->
                    String.format("{\"orderId\":\"ORD-%06d\",\"reason\":\"damaged\",\"amount\":%.2f}",
                            (int)(Math.random() * seq + 1), 5 + Math.random() * 200);
            case "SHIPMENT_UPDATE" ->
                    String.format("{\"trackingId\":\"TRACK-%08d\",\"status\":\"IN_TRANSIT\",\"eta\":%d}",
                            (int)(Math.random() * 99999999), (int)(1 + Math.random() * 7));
            default ->
                    String.format("{\"seq\":%d,\"type\":\"%s\"}", seq, eventType);
        };
    }

    @Override
    public void close() {
        log.info("LoadGenerator stopping (sent {} messages)", sequenceCounter.get());
        scheduler.shutdown();
        try { scheduler.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }
}
