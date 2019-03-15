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
    private String user_id;
    private int machine_id;
    private int lane;
    private float degree;


    @JsonFormat(pattern ="yyyy-MM-dd HH:mm:ss")
    private Date date;

    public Temperature() {}

    public Temperature(String _id, String user_id, int machine_id, int lane, float degree, Date date) {
        this._id = _id;
        this.user_id = user_id;
        this.machine_id = machine_id;
        this.lane = lane;
        this.degree = degree;
        this.date = date;
    }
}
