package com.kt.vd.Redis;


import lombok.Data;

public class LongResult extends RedisResult{
    private String key;
    private Long value;

    public LongResult(String key, Long value) {
        this.key = key;
        this.value = value;
    }

    public LongResult(String key) {
        this.key = key;
        this.value = null;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}