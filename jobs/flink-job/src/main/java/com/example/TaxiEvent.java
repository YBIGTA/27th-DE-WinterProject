package com.example;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaxiEvent {
    public Long trip_id;
    
    @JsonProperty("event") 
    public String event; 
    
    public String ts;
    public Double lat;
    public Double lon;
    public Integer zone_id;

    public Integer PULocationID;   
    public Integer DOLocationID;

    public void setZone_id(Integer zone_id) {
        this.zone_id = zone_id;
    }

    public Long getTrip_id() {
        return this.trip_id;
    }

    public TaxiEvent() {}
}