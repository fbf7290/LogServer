package com.kt.vd.sell;


import com.kt.vd.common.Generator;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.*;

@RestController
@RequestMapping(value = "/sell", method =  RequestMethod.GET )
public class SellController {

    @Autowired
    ElasticsearchTemplate esTemplate;

    static final String index = "sell-";


    @RequestMapping(value = {"/test/{user}","/test/{user}/{machine}"})
    public String test(@PathVariable String user, @PathVariable(required = false) Optional<Integer> machine){
        if(machine.isPresent()){
            System.out.println("hi");
        }else{
            System.out.println("SD");
        }

        return "SDFDSF";
    }

    @RequestMapping(value = {"/{user}","/{user}/{machine}"})
    public List<Map<String,Object>> getSellInfoByMachine(@PathVariable String user, @PathVariable(required = false) Optional<Integer> machine,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                       @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end){


        String[] index_names = Generator.generateIndex(index, start, end);

        BoolQueryBuilder query;
        if(machine.isPresent()){
            query = boolQuery().must(matchQuery("user_id", user))
                    .must(matchQuery("machine_id", machine.get()));
        }else{
            query = boolQuery().must(matchQuery("user_id", user));
        }


        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(query)
                .withIndices(index_names)
                .addAggregation(AggregationBuilders.terms("agg").field("drink_type"))
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
            data.put("drink_type", entry.getKey());
            data.put("sell", entry.getDocCount());

            responseData.add(data);
        }
        return responseData;
    }
}
