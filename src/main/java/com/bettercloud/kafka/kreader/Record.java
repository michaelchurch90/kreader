package com.bettercloud.kafka.kreader;

public class Record {
    private final String topic;
    private final int partition;
    private final long offset;
    private final String value;

    public Record(String topic, int partition, long offset, String value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.value = value;
    }
}
