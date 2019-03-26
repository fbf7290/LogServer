package com.kt.vd.visit;


import com.kt.vd.ElasticSearch.IndexManager;
import com.kt.vd.Redis.JsonListResult;
import com.kt.vd.Redis.RedisManager;
import com.kt.vd.Redis.RedisResult;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.*;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping(value = "/visit", method =  RequestMethod.GET )
public class VisitController {
    @Autowired
    ElasticsearchTemplate esTemplate;
    @Autowired
    RedisManager redisManager;

    static final private String index = "visit-";
    static final private String visitByMahcinePrefix = "visit";
    static final private String visitByLocPrefix = "visit:loc";

    /**
     *
     * Return visit count according to the hour
     *
     * @param user
     * @param machine
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"/{user}","/{user}/{machine}"})
    public List<Map<String,Object>> getVisitByMachine(@PathVariable String user, @PathVariable(required = false) Optional<String> machine,
                                                       @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                       @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end){

        boolean cacheFlag = false;

        JsonListResult cacheData;
        if(machine.isPresent()){
            cacheData = redisManager.getJsonListResult(end, visitByMahcinePrefix, user, machine.get(), start.toString(), end.toString());
        }else{
            cacheData = redisManager.getJsonListResult(end, visitByMahcinePrefix, user,  start.toString(), end.toString());
        }
        if(cacheData.getValue() != null)
            return cacheData.getValue();
        else
            cacheFlag = true;


        String[] index_names = IndexManager.generateIndex(index, start, end);

        BoolQueryBuilder boolQuery = boolQuery().must(termQuery("user", user));
        if(machine.isPresent())
            boolQuery.must(termQuery("machine", machine.get()));

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(constantScoreQuery(boolQuery))
                .withIndices(index_names)
                .withRoute(user)
                .addAggregation(AggregationBuilders.terms("agg").field("hour_of_date").size(24).order(Terms.Order.term(true)))
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

        if(cacheFlag){
            redisManager.setJsonListOpts(cacheData.getKey(), responseData, 1, TimeUnit.HOURS);
        }

        return responseData;
    }


    /**
     *
     * Return visit count according to the location
     *
     * @param province
     * @param user
     * @param municipality
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"loc/{province}/{user}","loc/{province}/{municipality}/{user}"})
    public List<Map<String,Object>> getVisitByLoc(@PathVariable String province, @PathVariable(value = "user") String user,
                                                  @PathVariable(required = false, value = "municipality") Optional<String> municipality,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end){


        boolean cacheFlag = false;

        JsonListResult cacheData;
        if(municipality.isPresent()){
            cacheData = redisManager.getJsonListResult(end, visitByLocPrefix, province, municipality.get(), user, start.toString(), end.toString());
        }else{
            cacheData = redisManager.getJsonListResult(end, visitByLocPrefix, province, user, start.toString(), end.toString());
        }
        if(cacheData.getValue() != null)
            return cacheData.getValue();
        else
            cacheFlag = true;



        String agg_term = "municipality";
        String[] index_names = IndexManager.generateIndex(index, start, end);

        BoolQueryBuilder boolQuery = boolQuery().must(termQuery("user", user))
                .must(termQuery("province", province));
        if(municipality.isPresent()){
            boolQuery.must(termQuery("municipality", municipality.get()));
            agg_term = "submunicipality";
        }

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(constantScoreQuery(boolQuery))
                .withIndices(index_names)
                .withRoute(user)
                .addAggregation(AggregationBuilders.terms("agg").field(agg_term))
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
            data.put("loc", entry.getKey());
            data.put("count", entry.getDocCount());

            responseData.add(data);
        }

        if(cacheFlag){
            redisManager.setJsonListOpts(cacheData.getKey(), responseData, 1, TimeUnit.HOURS);
        }

        return responseData;
    }
}
