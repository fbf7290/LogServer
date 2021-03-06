package com.kt.vd.ElasticSearch;


import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IndexManager {


    @Autowired
    ElasticsearchTemplate esTemplate;

    void test(){
        esTemplate.getClient().admin().indices().getIndex(new GetIndexRequest().indices("sell-*")).actionGet().getIndices();
    }


    /**
     *
     *  Generate date between startDate and endDate
     *
     * @param startDate
     * @param endDate
     * @return
     */
    public static String[] generateIndex(String index, LocalDate startDate, LocalDate endDate){
        LocalDate now = LocalDate.now();
        if(endDate.isAfter(now))
            endDate = now;

        long numOfDaysBetween = ChronoUnit.DAYS.between(startDate, endDate) + 1;
        List<String> collect = IntStream.iterate(0, i -> i + 1)
                .limit(numOfDaysBetween)
                .mapToObj(i -> index+startDate.plusDays(i).toString())
                .collect(Collectors.toList());

        return collect.toArray(new String[collect.size()]);
    }
}
