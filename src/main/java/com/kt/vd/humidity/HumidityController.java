package com.kt.vd.humidity;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.data.util.CloseableIterator;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

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
    private List<Map<String,Object>> getHumidityByInterval(String user, int machine, int lane, int interval){

        LocalDateTime datetime = LocalDateTime.now();
        LocalDate date = datetime.toLocalDate();
        LocalDateTime datetimeMinus10 = datetime.minusMinutes(interval);

        String index_name = index + date.toString();


        BoolQueryBuilder query = boolQuery().must(matchQuery("user_id", user))
                .must(matchQuery("machine_id", machine))
                .must(matchQuery("lane", lane));

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date")
                .gte(datetimeMinus10.format(formatter))
                .lte(datetime.format(formatter));

        query.filter(rangeQueryBuilder);

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(query).withQuery(query)
                .withIndices(index_name)
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
    public List<Map<String,Object>> getHumidityInit(@PathVariable String user, @PathVariable int machine,
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
    public List<Map<String,Object>> getHumidity(@PathVariable String user, @PathVariable int machine,
                                                   @PathVariable int lane){
        return getHumidityByInterval(user, machine, lane, 1);
    }




}
