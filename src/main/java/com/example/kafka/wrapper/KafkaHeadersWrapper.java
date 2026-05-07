package com.example.kafka.wrapper;

import com.newrelic.api.agent.HeaderType;
import com.newrelic.api.agent.Headers;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Bridges Kafka's byte-array header API to the New Relic Headers interface
 * so the agent can inject/extract distributed trace context.
 *
 * Used on the producer side via Transaction.insertDistributedTraceHeaders()
 * and on the consumer side via Transaction.acceptDistributedTraceHeaders().
 */
public class KafkaHeadersWrapper implements Headers {

    private final org.apache.kafka.common.header.Headers kafkaHeaders;

    public KafkaHeadersWrapper(org.apache.kafka.common.header.Headers kafkaHeaders) {
        this.kafkaHeaders = kafkaHeaders;
    }

    @Override
    public HeaderType getHeaderType() {
        return HeaderType.MESSAGE;
    }

    @Override
    public String getHeader(String name) {
        Header header = kafkaHeaders.lastHeader(name);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    @Override
    public Collection<String> getHeaders(String name) {
        return StreamSupport.stream(kafkaHeaders.spliterator(), false)
                .filter(h -> h.key().equals(name))
                .map(h -> new String(h.value(), StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    @Override
    public void setHeader(String name, String value) {
        kafkaHeaders.remove(name);
        kafkaHeaders.add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void addHeader(String name, String value) {
        kafkaHeaders.add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Collection<String> getHeaderNames() {
        return StreamSupport.stream(kafkaHeaders.spliterator(), false)
                .map(Header::key)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean containsHeader(String name) {
        return kafkaHeaders.lastHeader(name) != null;
    }
}
