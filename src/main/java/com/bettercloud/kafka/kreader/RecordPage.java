package com.bettercloud.kafka.kreader;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RecordPage {
    @JsonProperty
    private String topic;
    @JsonProperty
    private int partition;
    @JsonProperty
    private long startOffset;
    @JsonProperty
    private long nextOffset;
    @JsonProperty
    private List<Record> records;

    public RecordPage(String topic, int partition, long startOffset, long nextOffset, List<Record> records) {
        this.topic = topic;
        this.partition = partition;
        this.nextOffset = nextOffset;
        this.startOffset = startOffset;
        this.records = records;
    }
}
