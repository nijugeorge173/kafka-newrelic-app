package com.example.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EventMessage {

    @JsonProperty("messageId")
    private String messageId;

    @JsonProperty("eventType")
    private String eventType;

    @JsonProperty("payload")
    private String payload;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("producerHost")
    private String producerHost;

    @JsonProperty("sequenceNumber")
    private long sequenceNumber;

    public EventMessage() {}

    public EventMessage(String messageId, String eventType, String payload,
                        long timestamp, String producerHost, long sequenceNumber) {
        this.messageId = messageId;
        this.eventType = eventType;
        this.payload = payload;
        this.timestamp = timestamp;
        this.producerHost = producerHost;
        this.sequenceNumber = sequenceNumber;
    }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getProducerHost() { return producerHost; }
    public void setProducerHost(String producerHost) { this.producerHost = producerHost; }

    public long getSequenceNumber() { return sequenceNumber; }
    public void setSequenceNumber(long sequenceNumber) { this.sequenceNumber = sequenceNumber; }
}
