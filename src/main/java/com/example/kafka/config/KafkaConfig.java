package com.example.kafka.config;

public final class KafkaConfig {

    public static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    public static final String TOPIC =
            System.getenv().getOrDefault("KAFKA_TOPIC", "demo-events");

    public static final String CONSUMER_GROUP_ID = "nr-demo-consumer-group";
    public static final String PRODUCER_CLIENT_ID = "nr-demo-producer";

    // New Relic custom metric name prefixes — visible under Custom/ in the UI
    public static final String NR_PRODUCER_PREFIX = "Custom/Kafka/Producer/";
    public static final String NR_CONSUMER_PREFIX = "Custom/Kafka/Consumer/";

    // Load generator defaults — override via env vars
    public static final int MESSAGES_PER_SECOND =
            Integer.parseInt(System.getenv().getOrDefault("MESSAGES_PER_SECOND", "10"));

    public static final int NUM_CONSUMERS =
            Integer.parseInt(System.getenv().getOrDefault("NUM_CONSUMERS", "3"));

    // Metrics flush interval in seconds
    public static final int METRICS_FLUSH_INTERVAL_SECONDS = 10;

    private KafkaConfig() {}
}
