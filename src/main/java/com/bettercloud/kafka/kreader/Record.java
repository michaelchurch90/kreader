package com.bettercloud.kafka.kreader;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Record {
    @JsonProperty
    private String key;
    @JsonProperty
    private final long offset;
    @JsonProperty
    private final Object value;

    public Record(String key, long offset, Object value) {
        this.offset = offset;
        this.value = value;
    }
}
