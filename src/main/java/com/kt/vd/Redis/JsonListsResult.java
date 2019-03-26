package com.kt.vd.Redis;


import lombok.Data;

import java.util.List;
import java.util.Map;

public class JsonListsResult extends RedisResult{
    private String key;
    private List<Map<String, List<Map<String, Object>>>> value;

    public JsonListsResult(String key) {
        this.key = key;
        this.value = null;
    }

    public JsonListsResult(String key, List<Map<String, List<Map<String, Object>>>> value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<Map<String, List<Map<String, Object>>>> getValue() {
        return value;
    }

    public void setValue(List<Map<String, List<Map<String, Object>>>> value) {
        this.value = value;
    }
}