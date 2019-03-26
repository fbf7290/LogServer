package com.kt.vd.Redis;

import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class RedisManager {


    @Resource(name="redisTemplate")
    ValueOperations<String, List<Map<String,Object>>> jsonListOpts;

    @Resource(name="redisTemplate")
    ValueOperations<String, Integer> integerOpts;

    @Resource(name="redisTemplate")
    ValueOperations<String,  List<Map<String, List<Map<String, Object>>>>> jsonListsOpts;

    /**
     * 당일 이전 데이터 요청에 대해서 레디스 접근 허용
     * @param date
     * @return
     */
    private boolean isLegal(LocalDate date){
        LocalDate today = LocalDate.now();
        if(date.isBefore(today))
            return true;

        return false;
    }

    /**
     * 인자값을 토대로 레디스 키 생성
     * @param prefix
     * @param parameters
     * @return
     */
    private String generateRedisKey(String prefix, String... parameters){
        StringBuffer sb = new StringBuffer();
        sb.append(prefix);

        for (String parameter: parameters) {
            sb.append(":");
            sb.append(parameter);
        }
        return sb.toString();
    }


    public JsonListResult getJsonListResult(LocalDate end, String prefix, String... parameters){
        String key = this.generateRedisKey(prefix, parameters);

        if(!this.isLegal(end))
            return new JsonListResult(key);

        List<Map<String, Object>> value = jsonListOpts.get(key);
        return new JsonListResult(key, value);
    }

    public IntegerResult getIntegerResult(LocalDate end, String prefix, String... parameters){

        String key = this.generateRedisKey(prefix, parameters);

        if(!this.isLegal(end))
            return new IntegerResult(key);

        return new IntegerResult(key, integerOpts.get(key));
    }


    public JsonListsResult getJsonListsResult(LocalDate end, String prefix, String... parameters){

        String key = this.generateRedisKey(prefix, parameters);

        if(!this.isLegal(end))
            return new JsonListsResult(key);

        List<Map<String, List<Map<String, Object>>>> value = jsonListsOpts.get(key);

        return new JsonListsResult(key, value);
    }

    public void setJsonListOpts(String key, List<Map<String,Object>> value, int time, TimeUnit timeUnit){
        jsonListOpts.set(key, value, time, timeUnit);
    }

    public void setJsonListsOpts(String key, List<Map<String,List<Map<String,Object>>>> value, int time, TimeUnit timeUnit){
        jsonListsOpts.set(key, value, time, timeUnit);
    }

    public void setIntegerOpts(String key, Integer value, int time, TimeUnit timeUnit){
        integerOpts.set(key, value, time, timeUnit);
    }
}
