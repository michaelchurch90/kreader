package com.bettercloud.kafka.kreader;

import java.util.List;

public class RecordPage {
    private String topic;
    private int partition;
    private long nextOffset;
    private List<? extends Object> records;

    public RecordPage(String topic, int partition, long nextOffset, List<? extends Object> records) {
        this.topic = topic;
        this.partition = partition;
        this.nextOffset = nextOffset;
        this.records = records;
    }
}
