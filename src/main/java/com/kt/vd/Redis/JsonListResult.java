package com.kt.vd.Redis;

import java.util.List;
import java.util.Map;

public class JsonListResult extends RedisResult {
    private String key;
    private List<Map<String,Object>> value;

    public JsonListResult(String key, List<Map<String,Object>> value) {
        this.key = key;
        this.value = value;
    }
    public JsonListResult(String key) {
        this.key = key;
        this.value = null;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<Map<String, Object>> getValue() {
        return value;
    }

    public void setValue(List<Map<String, Object>> value) {
        this.value = value;
    }
}
