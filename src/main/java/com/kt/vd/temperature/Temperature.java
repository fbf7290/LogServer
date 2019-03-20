package com.kt.vd.temperature;


import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.Date;

@Data
@Builder
@Document(indexName = "temperature-index", type = "log", createIndex = false)
class Temperature {
    @Id
    private String _id;
    private String user;
    private String machine;
    private int lane;
    private float degree;


    @JsonFormat(pattern ="yyyy-MM-dd HH:mm:ss")
    private Date date;

    public Temperature() {}

    public Temperature(String _id, String user, String machine, int lane, float degree, Date date) {
        this._id = _id;
        this.user = user;
        this.machine = machine;
        this.lane = lane;
        this.degree = degree;
        this.date = date;
    }
}
