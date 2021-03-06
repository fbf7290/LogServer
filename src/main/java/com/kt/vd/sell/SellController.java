package com.kt.vd.sell;


import com.kt.vd.ElasticSearch.IndexManager;
import com.kt.vd.Redis.IntegerResult;
import com.kt.vd.Redis.JsonListResult;
import com.kt.vd.Redis.JsonListsResult;
import com.kt.vd.Redis.RedisManager;
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
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping(value = "/sell", method =  RequestMethod.GET )
public class SellController {

    @Autowired
    ElasticsearchTemplate esTemplate;
    @Autowired
    RedisManager redisManager;

    static final private String index = "sell-";
    static final private String sellByDrinkPrefix = "sell";
    static final private String sellByTimePrefix = "sell:time";
    static final private String sellDrinkPrefix = "sell:drink";
    static final private String sellByLocPrefix = "sell:loc";
    static final private String sellByDrinkAllPrefix = "sell:all";

    /**
     * Return sales volume according to the type of beverage
     *
     * @param user
     * @param machine
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"/{user}", "/{user}/{machine}"})
    public List<Map<String, Object>> getSellByDrink(@PathVariable String user, @PathVariable(required = false) Optional<String> machine,
                                                    @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                    @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end) {

        boolean cacheFlag = false;
        JsonListResult cacheData = null;

        if(!redisManager.permitCache(end)){
            if(machine.isPresent()){
                cacheData = redisManager.getJsonListResult(sellByDrinkPrefix, user, machine.get(), start.toString(), end.toString());
            }else{
                cacheData = redisManager.getJsonListResult(sellByDrinkPrefix, user,  start.toString(), end.toString());
            }
            if(cacheData.getValue() != null)
                return cacheData.getValue();
            else
                cacheFlag = true;
        }



        String[] index_names = IndexManager.generateIndex(index, start, end);

        BoolQueryBuilder boolQuery = boolQuery().must(termQuery("user", user));
        if (machine.isPresent())
            boolQuery.must(termQuery("machine", machine.get()));

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(constantScoreQuery(boolQuery))
                .withIndices(index_names)
                .withRoute(user)
                .addAggregation(AggregationBuilders.terms("agg").field("drink_type").size(50))
                .build();

        Aggregations aggregations = esTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });
        Terms agg = aggregations.get("agg");


        List<Map<String, Object>> responseData = new ArrayList<>();
        for (Terms.Bucket entry : agg.getBuckets()) {
            Map<String, Object> data = new HashMap<>();
            data.put("drink_type", entry.getKey());
            data.put("sell", entry.getDocCount());

            responseData.add(data);
        }

        if(cacheFlag){
            redisManager.setJsonListOpts(cacheData.getKey(), responseData, 1, TimeUnit.HOURS);
        }

        return responseData;
    }

    /**
     * Return sales volume according to the hour
     *
     * @param user
     * @param machine
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"/time/{user}", "/time/{user}/{machine}"})
    public List<Map<String, Object>> getSellByTime(@PathVariable String user, @PathVariable(required = false) Optional<String> machine,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end) {


        boolean cacheFlag = false;

        JsonListResult cacheData= null;

        if(!redisManager.permitCache(end)) {
            if (machine.isPresent()) {
                cacheData = redisManager.getJsonListResult(sellByTimePrefix, user, machine.get(), start.toString(), end.toString());
            } else {
                cacheData = redisManager.getJsonListResult(sellByTimePrefix, user, start.toString(), end.toString());
            }
            if (cacheData.getValue() != null)
                return cacheData.getValue();
            else
                cacheFlag = true;
        }

        String[] index_names = IndexManager.generateIndex(index, start, end);


        BoolQueryBuilder boolQuery = boolQuery().must(termQuery("user", user));
        if (machine.isPresent())
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


        List<Map<String, Object>> responseData = new ArrayList<>();
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
     * Return drink sell volume by drink
     *
     * @param user
     * @param machine
     * @param drink
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"/drink/{user}/{drink}", "/drink/{user}/{machine}/{drink}"})
    public Integer getSellDrink(@PathVariable String user, @PathVariable(required = false, value = "machine") Optional<String> machine,
                             @PathVariable(value = "drink") String drink,
                             @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                             @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end) {

        boolean cacheFlag = false;
        IntegerResult cacheData = null;

        if(!redisManager.permitCache(end)) {
            if (machine.isPresent()) {
                cacheData = redisManager.getIntegerResult(sellDrinkPrefix, drink, user, machine.get(), start.toString(), end.toString());
            } else {
                cacheData = redisManager.getIntegerResult(sellDrinkPrefix, drink, user, start.toString(), end.toString());
            }
            if (cacheData.getValue() != null)
                return cacheData.getValue();
            else
                cacheFlag = true;
        }

        String[] index_names = IndexManager.generateIndex(index, start, end);


        BoolQueryBuilder boolQuery = boolQuery().must(termQuery("user", user))
                        .must(termQuery("drink_type", drink));
        if (machine.isPresent())
            boolQuery.must(termQuery("machine", machine.get()));

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(constantScoreQuery(boolQuery))
                .withIndices(index_names)
                .withRoute(user)
                .build();


        Long count = esTemplate.count(searchQuery);

        if(cacheFlag){
            redisManager.setIntegerOpts(cacheData.getKey(), count.intValue(), 1, TimeUnit.HOURS);
        }

        return count.intValue();
    }


    /**
     * Return drink sell volume according to the location
     *
     * @param province
     * @param user
     * @param municipality
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"loc/{province}/{user}", "loc/{province}/{municipality}/{user}"})
    public List<Map<String, List<Map<String, Object>>>> getSellByLoc(@PathVariable String province, @PathVariable(value = "user") String user,
                                                                     @PathVariable(required = false, value = "municipality") Optional<String> municipality,
                                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end) {

        boolean cacheFlag = false;

        JsonListsResult cacheData = null;

        if(!redisManager.permitCache(end)) {
            if (municipality.isPresent()) {
                cacheData = redisManager.getJsonListsResult(sellByLocPrefix, province, municipality.get(), user, start.toString(), end.toString());
            } else {
                cacheData = redisManager.getJsonListsResult(sellByLocPrefix, province, user, start.toString(), end.toString());
            }
            if (cacheData.getValue() != null)
                return cacheData.getValue();
            else
                cacheFlag = true;
        }

        String agg_term = "municipality";
        String[] index_names = IndexManager.generateIndex(index, start, end);

        BoolQueryBuilder boolQuery = boolQuery().must(termQuery("user", user))
                .must(termQuery("province", province));
        if (municipality.isPresent()) {
            boolQuery.must(termQuery("municipality", municipality.get()));
            agg_term = "submunicipality";
        }

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(constantScoreQuery(boolQuery))
                .withIndices(index_names)
                .withRoute(user)
                .addAggregation(AggregationBuilders.terms("main").field(agg_term)
                        .subAggregation(AggregationBuilders.terms("sub").field("drink_type")))
                .build();

        Aggregations aggregations = esTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });

        Terms mainAggs = aggregations.get("main");
        List<Map<String, List<Map<String, Object>>>> responseData = new ArrayList<>();
        String mainKey;

        for (Terms.Bucket subAgg : mainAggs.getBuckets()) {
            Map<String, List<Map<String, Object>>> mainData = new HashMap<>();
            List<Map<String, Object>> subData = new ArrayList<>();

            Terms subTerms = subAgg.getAggregations().get("sub");

            for (Terms.Bucket entry : subTerms.getBuckets()) {
                Map<String, Object> data = new HashMap<>();
                data.put("drink", entry.getKey());
                data.put("count", entry.getDocCount());

                subData.add(data);
            }
            mainData.put(subAgg.getKeyAsString(), subData);
            responseData.add(mainData);
        }


        if(cacheFlag){
            redisManager.setJsonListsOpts(cacheData.getKey(), responseData, 1, TimeUnit.HOURS);
        }

        return responseData;
    }

    /**
     *
     * Return drink sell volume according to the location, drink
     *
     * @param drink
     * @param province
     * @param user
     * @param municipality
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"/{drink}/loc/{province}/{user}", "/{drink}/loc/{province}/{municipality}/{user}"})
    public List<Map<String,Object>> getSellDrinkByLoc(@PathVariable String drink,
                                                      @PathVariable String province, @PathVariable(value = "user") String user,
                                                      @PathVariable(required = false, value = "municipality") Optional<String> municipality,
                                                      @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                      @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end){

        boolean cacheFlag = false;

        JsonListResult cacheData = null;

        if(!redisManager.permitCache(end)) {
            if (municipality.isPresent()) {
                cacheData = redisManager.getJsonListResult(sellByLocPrefix, province, municipality.get(), drink, start.toString(), end.toString());
            } else {
                cacheData = redisManager.getJsonListResult(sellByLocPrefix, province, drink, start.toString(), end.toString());
            }
            if (cacheData.getValue() != null)
                return cacheData.getValue();
            else
                cacheFlag = true;
        }

        String agg_term = "municipality";
        String[] index_names = IndexManager.generateIndex(index, start, end);

        BoolQueryBuilder boolQuery = boolQuery().must(termQuery("user", user))
                .must(termQuery("drink_type", drink))
                .must(termQuery("province", province));
        if (municipality.isPresent()) {
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


        List<Map<String, Object>> responseData = new ArrayList<>();
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

    @RequestMapping(value = {"/all/{top}","/all"})
    public List<Map<String,Object>> getSellByDrinkAll(@PathVariable(required = false) Optional<Integer> top,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end){

        boolean cacheFlag = false;

        JsonListResult cacheData = null;

        if(!redisManager.permitCache(end)) {
            cacheData = redisManager.getJsonListResult(sellByDrinkAllPrefix, top.orElse(10).toString(), start.toString(), end.toString());
            if (cacheData.getValue() != null)
                return cacheData.getValue();
            else
                cacheFlag = true;
        }


        String[] index_names = IndexManager.generateIndex(index, start, end);

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(constantScoreQuery(matchAllQuery()))
                .withIndices(index_names)
                .addAggregation(AggregationBuilders.terms("agg").field("drink_type").size(top.orElse(10)))
                .build();

        Aggregations aggregations = esTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });

        Terms agg = aggregations.get("agg");


        List<Map<String, Object>> responseData = new ArrayList<>();
        for (Terms.Bucket entry : agg.getBuckets()) {
            Map<String, Object> data = new HashMap<>();
            data.put("drink_type", entry.getKey());
            data.put("count", entry.getDocCount());

            responseData.add(data);
        }

        if(cacheFlag){
            redisManager.setJsonListOpts(cacheData.getKey(), responseData, 30, TimeUnit.DAYS);
        }

        return responseData;


    }
}

