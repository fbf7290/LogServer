package com.kt.vd.visit;


import com.kt.vd.common.Generator;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

@RestController
@RequestMapping(value = "/visit", method =  RequestMethod.GET )
public class VisitController {
    @Autowired
    ElasticsearchTemplate esTemplate;

    static final String index = "visit-";

    @RequestMapping("{user}/{machine}")
    public List<Map<String,Object>> getVisitByMachine(@PathVariable String user, @PathVariable int machine,
                                                       @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                       @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end){

        String[] index_names = Generator.generateIndex(index, start, end);

        BoolQueryBuilder query = boolQuery().must(matchQuery("user_id", user))
                .must(matchQuery("machine_id", machine));


        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(query)
                .withIndices(index_names)
                .addAggregation(AggregationBuilders.terms("agg").field("hour_of_day").size(24).order(Terms.Order.term(true)))
                .build();



        Aggregations aggregations = esTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });
        Terms agg = aggregations.get("agg");


        List<Map<String, Object>> responseData= new ArrayList<>();
        for (Terms.Bucket entry : agg.getBuckets()) {
            Map<String, Object> data = new HashMap<>();
            data.put("hour", entry.getKey());
            data.put("count", entry.getDocCount());

            responseData.add(data);
        }
        return responseData;


    }
}
