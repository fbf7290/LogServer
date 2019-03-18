package com.kt.vd.sell;


import com.kt.vd.common.Generator;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;

@RestController
@RequestMapping(value = "/sell", method =  RequestMethod.GET )
public class SellController {

    @Autowired
    ElasticsearchTemplate esTemplate;

    static final String index = "sell-";

    /**
     *
     *  Return sales volume according to the type of beverage
     *
     * @param user
     * @param machine
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"/{user}","/{user}/{machine}"})
    public List<Map<String,Object>> getSellByDrink(@PathVariable String user, @PathVariable(required = false) Optional<Integer> machine,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                       @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end){


        String[] index_names = Generator.generateIndex(index, start, end);

        QueryBuilder query;
        if(machine.isPresent()){
            query = constantScoreQuery(boolQuery().must(termQuery("user_id", user))
                    .must(termQuery("machine_id", machine.get())));
        }else{
            query = constantScoreQuery(boolQuery().must(termQuery("user_id", user)));
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

    /**
     *
     * Return sales volume according to the hour
     *
     * @param user
     * @param machine
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"/time/{user}", "/time/{user}/{machine}"})
    public List<Map<String,Object>> getSellByTime(@PathVariable String user, @PathVariable(required = false) Optional<Integer> machine,
                                                  @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                  @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end){

        String[] index_names = Generator.generateIndex(index, start, end);

        QueryBuilder query;
        if(machine.isPresent()){
            query = constantScoreQuery(boolQuery().must(termQuery("user_id", user))
                    .must(termQuery("machine_id", machine.get())));
        }else{
            query = constantScoreQuery(boolQuery().must(termQuery("user_id", user)));
        }


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


    /**
     *
     *  Return drink sell volume by drink
     *
     * @param user
     * @param machine
     * @param drink
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"/drink/{user}/{drink}", "/drink/{user}/{machine}/{drink}"})
    public long getSellDrink(@PathVariable String user, @PathVariable(required = false, name= "machine") Optional<Integer> machine,
                                                 @PathVariable(name="drink") String drink,
                                                 @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                 @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end){

        String[] index_names = Generator.generateIndex(index, start, end);

        QueryBuilder query;

        if(machine.isPresent()){
            query = constantScoreQuery(boolQuery().must(termQuery("user_id", user))
                    .must(termQuery("machine_id", machine.get()))
                    .must(termQuery("drink_type", drink)));
        }else{
            query = constantScoreQuery(boolQuery().must(termQuery("user_id", user))
                    .must(termQuery("drink_type", drink)));
        }


        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(query)
                .withIndices(index_names)
                .build();


        long count = esTemplate.count(searchQuery);
        return count;
    }
}
