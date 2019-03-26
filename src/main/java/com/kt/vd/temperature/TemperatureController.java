package com.kt.vd.temperature;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.data.util.CloseableIterator;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.*;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping(value = "/temperature", method =  RequestMethod.GET )
public class TemperatureController {

    @Autowired
    ElasticsearchTemplate esTemplate;

    static final String index = "temperature-";
    static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");



    /**
     *
     * Return temperature data between now()- 'interval'minutes and now()
     *
     * @param user
     * @param machine
     * @param lane
     * @return
     */
    private List<Map<String,Object>> getTemperatureByInterval(String user, String machine, int lane, int interval){

        LocalDateTime datetime = LocalDateTime.now();
        LocalDate date = datetime.toLocalDate();
        LocalDateTime datetimeMinus10 = datetime.minusMinutes(interval);

        String index_name = index + date.toString();


        QueryBuilder query = constantScoreQuery(boolQuery().must(termQuery("user", user))
                .must(termQuery("machine", machine))
                .must(termQuery("lane", lane)));

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date")
                .gte(datetimeMinus10.format(formatter))
                .lte(datetime.format(formatter));

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(query)
                .withFilter(rangeQueryBuilder)
                .withIndices(index_name)
                .withRoute(user)
                .build();

        CloseableIterator<Temperature> stream = esTemplate.stream(searchQuery, Temperature.class);

        Temperature temperature;
        List<Map<String, Object>> result = new ArrayList<>();

        while(stream.hasNext()){
            temperature = stream.next();

            Map<String, Object> data = new HashMap<>();
            data.put("degree",temperature.getDegree());
            data.put("date",temperature.getDate());

            result.add(data);
        }

        return result;
    }




    /**
     *
     * Response temperature init data
     * Return temperature data between now()-10minutes and now()
     *
     * @param user
     * @param machine
     * @param lane
     * @return
     */
    @RequestMapping("/init/{user}/{machine}/{lane}")
    public List<Map<String,Object>> getTemperatureInit(@PathVariable String user, @PathVariable String machine,
                                                   @PathVariable int lane){
        return getTemperatureByInterval(user, machine, lane, 10);
    }


    /**
     *
     * Response recent temperature data
     * Return temperature data between now()-1minutes and now()
     *
     * @param user
     * @param machine
     * @param lane
     * @return
     */
    @RequestMapping("/{user}/{machine}/{lane}")
    public List<Map<String,Object>> getTemperature(@PathVariable String user, @PathVariable String machine,
                                                       @PathVariable int lane){
        return getTemperatureByInterval(user, machine, lane, 1);
    }



    private List<Map<String, List<Map<String, Object>>>> getTemperaturesByInterval(String user, String machine, int interval){

        LocalDateTime datetime = LocalDateTime.now();
        LocalDate date = datetime.toLocalDate();
        LocalDateTime datetimeMinus10 = datetime.minusMinutes(interval);


        String index_name = index + date.toString();

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date")
                .gte(datetimeMinus10.format(formatter))
                .lte(datetime.format(formatter));

        QueryBuilder query = boolQuery().must(termQuery("user", user))
                .must(termQuery("machine", machine)).must(rangeQueryBuilder);


        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(constantScoreQuery(query))
                .withIndices(index_name)
                .withRoute(user)
                .addAggregation(AggregationBuilders.terms("main").field("lane")
                        .subAggregation(AggregationBuilders.topHits("sub").size(60)))
                .build();



        Aggregations aggregations = esTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });

        Terms mainAggs = aggregations.get("main");
        List<Map<String, List<Map<String, Object>>>> responseData = new ArrayList<>();


        for (Terms.Bucket subAgg : mainAggs.getBuckets()) {
            Map<String, List<Map<String, Object>>> mainData = new HashMap<>();
            List<Map<String, Object>> subData = new ArrayList<>();

            TopHits topHits = subAgg.getAggregations().get("sub");

            for (SearchHit hit : topHits.getHits().getHits()) {
                Map<String, Object> data = new HashMap<>();
                Map<String, Object> source = hit.getSourceAsMap();
//                data.put("date", source.get("date"));
                data.put("date", source.get("date"));
                data.put("degree", source.get("degree"));
                subData.add(data);
            }

            mainData.put("data", subData);
//            mainData.put(subAgg.getKeyAsString(), subData);
            responseData.add(mainData);
        }

        return responseData;
    }

    @RequestMapping("/init/{user}/{machine}")
    public List<Map<String, List<Map<String, Object>>>> getTemperaturesInit(@PathVariable String user, @PathVariable String machine){
        return getTemperaturesByInterval(user, machine, 10);
    }


    @RequestMapping("/{user}/{machine}")
    public List<Map<String, List<Map<String, Object>>>> getTemperature(@PathVariable String user, @PathVariable String machine){
        return getTemperaturesByInterval(user, machine, 1);
    }

//
//    @RequestMapping("/init/{user}/{machine}")
//    public List<Map<String, List<Map<String, Object>>>> test(@PathVariable String user, @PathVariable String machine){
//
//
//        LocalDateTime datetime = LocalDateTime.now();
//        LocalDate date = datetime.toLocalDate();
//        LocalDateTime datetimeMinus10 = datetime.minusMinutes(interval);
//
//
//        String index_name = index + date.toString();
//
//        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date")
//                .gte(datetimeMinus10.format(formatter))
//                .lte(datetime.format(formatter));
//
//        QueryBuilder query = boolQuery().must(termQuery("user", user))
//                .must(termQuery("machine", machine)).must(rangeQueryBuilder);
//
//
//        SearchQuery searchQuery = new NativeSearchQueryBuilder()
//                .withQuery(constantScoreQuery(query))
//                .withIndices(index_name)
//                .withRoute(user)
//                .addAggregation(AggregationBuilders.terms("main").field("lane")
//                    .subAggregation(AggregationBuilders.topHits("sub").size(60)))
//                .build();
//
//
//
//        Aggregations aggregations = esTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
//            @Override
//            public Aggregations extract(SearchResponse response) {
//                return response.getAggregations();
//            }
//        });
//
//        Terms mainAggs = aggregations.get("main");
//        List<Map<String, List<Map<String, Object>>>> responseData = new ArrayList<>();
//        String mainKey;
//
//
//        for (Terms.Bucket subAgg : mainAggs.getBuckets()) {
//            Map<String, List<Map<String, Object>>> mainData = new HashMap<>();
//            List<Map<String, Object>> subData = new ArrayList<>();
//
//            TopHits topHits = subAgg.getAggregations().get("sub");
//
//            for (SearchHit hit : topHits.getHits().getHits()) {
//                Map<String, Object> data = new HashMap<>();
//                Map<String, Object> source = hit.getSourceAsMap();
//                data.put("date", source.get("date"));
//                data.put("degree", source.get("degree"));
//                subData.add(data);
//            }
//
//            mainData.put(subAgg.getKeyAsString(), subData);
//            responseData.add(mainData);
//        }
//
//        return responseData;
//    }

}
