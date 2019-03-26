package com.kt.vd.Redis;


import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ValueOperations;

import javax.annotation.Resource;

public class IntegerResult extends RedisResult{
    private String key;
    private Integer value;



    public IntegerResult(String key, Integer value) {
        this.key = key;
        this.value = value;
    }

    public IntegerResult(String key) {
        this.key = key;
        this.value = null;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }
}