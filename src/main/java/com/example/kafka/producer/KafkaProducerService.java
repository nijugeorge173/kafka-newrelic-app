package com.example.kafka.producer;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.model.EventMessage;
import com.example.kafka.wrapper.KafkaHeadersWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaProducerService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicLong messagesSent = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);

    public KafkaProducerService() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConfig.PRODUCER_CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        this.producer = new KafkaProducer<>(props);
        log.info("KafkaProducerService initialized, bootstrap={}", KafkaConfig.BOOTSTRAP_SERVERS);
    }

    @Trace(dispatcher = true)
    public void send(EventMessage message) {
        try {
            String json = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    KafkaConfig.TOPIC, message.getEventType(), json);

            // Inject New Relic distributed trace headers so the consumer span links back
            NewRelic.getAgent().getTransaction()
                    .insertDistributedTraceHeaders(new KafkaHeadersWrapper(record.headers()));

            long sendStart = System.currentTimeMillis();

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    errors.incrementAndGet();
                    NewRelic.noticeError(exception);
                    log.error("Failed to send message {}", message.getMessageId(), exception);
                } else {
                    long latencyMs = System.currentTimeMillis() - sendStart;
                    messagesSent.incrementAndGet();
                    NewRelic.recordMetric(KafkaConfig.NR_PRODUCER_PREFIX + "MessagesPerSecond", 1);
                    NewRelic.recordMetric(KafkaConfig.NR_PRODUCER_PREFIX + "SendLatencyMs", latencyMs);
                    NewRelic.recordMetric(KafkaConfig.NR_PRODUCER_PREFIX + "RecordSizeBytes",
                            metadata.serializedValueSize());
                    log.debug("Sent msg={} partition={} offset={} latency={}ms",
                            message.getMessageId(), metadata.partition(), metadata.offset(), latencyMs);
                }
            });

        } catch (Exception e) {
            errors.incrementAndGet();
            NewRelic.noticeError(e);
            log.error("Error serializing/sending message", e);
        }
    }

    public long getMessagesSent() { return messagesSent.get(); }
    public long getErrors() { return errors.get(); }

    @Override
    public void close() {
        log.info("Flushing and closing producer (sent={})", messagesSent.get());
        producer.flush();
        producer.close();
    }
}
