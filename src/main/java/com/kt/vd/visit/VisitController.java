package com.kt.vd.visit;


import com.kt.vd.common.Generator;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.*;

@RestController
@RequestMapping(value = "/visit", method =  RequestMethod.GET )
public class VisitController {
    @Autowired
    ElasticsearchTemplate esTemplate;

    static final String index = "visit-";


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

        String[] index_names = Generator.generateIndex(index, start, end);

        BoolQueryBuilder boolQuery = boolQuery().must(termQuery("user", user));
        if(machine.isPresent())
            boolQuery.must(termQuery("machine", machine.get()));

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(constantScoreQuery(boolQuery))
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
     * Return visit count according to the location
     *
     * @param megal
     * @param user
     * @param district
     * @param start
     * @param end
     * @return
     */
    @RequestMapping(value = {"loc/{megal}/{user}","loc/{megal}/{district}/{user}"})
    public List<Map<String,Object>> getVisitByLoc(@PathVariable String megal, @PathVariable(value = "user") String user,
                                                  @PathVariable(required = false, value = "district") Optional<String> district,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate start,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate end){

        String agg_term = "district";
        String[] index_names = Generator.generateIndex(index, start, end);

        BoolQueryBuilder boolQuery = boolQuery().must(termQuery("user", user))
                .must(termQuery("megalopolis", megal));
        if(district.isPresent()){
            boolQuery.must(termQuery("district", district.get()));
            agg_term = "avenue";
        }

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(constantScoreQuery(boolQuery))
                .withIndices(index_names)
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
        return responseData;
    }
}
