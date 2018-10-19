package com.bettercloud.kafka.kreader;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Record {
    @JsonProperty
    private final String topic;
    @JsonProperty
    private final int partition;
    @JsonProperty
    private final long offset;
    @JsonProperty
    private final Object value;

    public Record(String topic, int partition, long offset, Object value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.value = value;
    }
}
