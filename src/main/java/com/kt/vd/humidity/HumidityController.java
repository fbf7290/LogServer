package com.kt.vd.humidity;

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
@RequestMapping(value = "/humidity", method =  RequestMethod.GET )
public class HumidityController {

    @Autowired
    ElasticsearchTemplate esTemplate;

    static final String index = "humidity-";
    static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");



    /**
     *
     * Return humidity data between now()- 'interval'minutes and now()
     *
     * @param user
     * @param machine
     * @param lane
     * @return
     */
    private List<Map<String,Object>> getHumidityByInterval(String user, String machine, int lane, int interval){

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

        CloseableIterator<Humidity> stream = esTemplate.stream(searchQuery, Humidity.class);

        Humidity humidity;
        List<Map<String, Object>> result = new ArrayList<>();

        while(stream.hasNext()){
            humidity = stream.next();

            Map<String, Object> data = new HashMap<>();
            data.put("degree",humidity.getDegree());
            data.put("date",humidity.getDate());

            result.add(data);
        }

        return result;
    }




    /**
     *
     * Response humidity init data
     * Return humidity data between now()-10minutes and now()
     *
     * @param user
     * @param machine
     * @param lane
     * @return
     */
    @RequestMapping("/init/{user}/{machine}/{lane}")
    public List<Map<String,Object>> getHumidityInit(@PathVariable String user, @PathVariable String machine,
                                                       @PathVariable int lane){
        return getHumidityByInterval(user, machine, lane, 10);
    }


    /**
     *
     * Response recent humidity data
     * Return humidity data between now()-1minutes and now()
     *
     * @param user
     * @param machine
     * @param lane
     * @return
     */
    @RequestMapping("/{user}/{machine}/{lane}")
    public List<Map<String,Object>> getHumidity(@PathVariable String user, @PathVariable String machine,
                                                   @PathVariable int lane){
        return getHumidityByInterval(user, machine, lane, 1);
    }



    private List<Map<String, List<Map<String, Object>>>> getHumiditysByInterval(String user, String machine, int interval){

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
    public List<Map<String, List<Map<String, Object>>>> getHumiditysInit(@PathVariable String user, @PathVariable String machine){
        return getHumiditysByInterval(user, machine, 10);
    }


    @RequestMapping("/{user}/{machine}")
    public List<Map<String, List<Map<String, Object>>>> getHumiditys(@PathVariable String user, @PathVariable String machine){
        return getHumiditysByInterval(user, machine, 1);
    }

}
