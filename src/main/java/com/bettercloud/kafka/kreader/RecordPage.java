package com.bettercloud.kafka.kreader;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RecordPage {
    @JsonProperty
    private String topic;
    @JsonProperty
    private int partition;
    @JsonProperty
    private long nextOffset;
    @JsonProperty
    private List<? extends Object> records;

    public RecordPage(String topic, int partition, long nextOffset, List<? extends Object> records) {
        this.topic = topic;
        this.partition = partition;
        this.nextOffset = nextOffset;
        this.records = records;
    }
}
