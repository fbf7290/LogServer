package com.kt.vd.temperature;


import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
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
    private List<Map<String,Object>> getTemperatureByInterval(String user, int machine, int lane, int interval){

        LocalDateTime datetime = LocalDateTime.now();
        LocalDate date = datetime.toLocalDate();
        LocalDateTime datetimeMinus10 = datetime.minusMinutes(interval);

        String index_name = index + date.toString();


        QueryBuilder query = constantScoreQuery(boolQuery().must(termQuery("user_id", user))
                .must(termQuery("machine_id", machine))
                .must(termQuery("lane", lane)));

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date")
                .gte(datetimeMinus10.format(formatter))
                .lte(datetime.format(formatter));

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(query)
                .withFilter(rangeQueryBuilder)
                .withIndices(index_name)
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
    public List<Map<String,Object>> getTemperatureInit(@PathVariable String user, @PathVariable int machine,
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
    public List<Map<String,Object>> getTemperature(@PathVariable String user, @PathVariable int machine,
                                                       @PathVariable int lane){
        return getTemperatureByInterval(user, machine, lane, 1);
    }





//
//    @RequestMapping("/{user}/{machine}/{lane}")
//    public List<Map<String,Object>> getTemperature(@PathVariable String user, @PathVariable int machine,
//                                                   @PathVariable int lane,
//                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start,
//                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime end){
//
//
//        BoolQueryBuilder query = boolQuery().must(matchQuery("user_id", user))
//                .must(matchQuery("machine_id", machine))
//                .must(matchQuery("lane", lane));
//
//        String[] a = Generator.generateIndex(index_name, start.toLocalDate(), end.toLocalDate());
//        for (String b :
//                a) {
//            System.out.println(b);
//        }
//
//        SearchQuery searchQuery = new NativeSearchQueryBuilder()
//                .withQuery(query)
//                .withIndices(Generator.generateIndex(index_name, start.toLocalDate(), end.toLocalDate()))
//                .build();
//
//        CloseableIterator<temperature> stream = esTemplate.stream(searchQuery, temperature.class);
//
//        temperature temperature;
//        List<Map<String, Object>> responseData= new ArrayList<>();
//
//        while(stream.hasNext()){
//            temperature = stream.next();
//
//            Map<String, Object> data = new HashMap<>();
//            data.put("degree",temperature.getDegree());
//            data.put("date",temperature.getDate());
//
//            responseData.add(data);
//        }
//
//        return responseData;
//    }

}
