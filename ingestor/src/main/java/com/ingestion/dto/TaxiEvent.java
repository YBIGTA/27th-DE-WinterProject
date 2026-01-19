package com.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaxiEvent {

    // 공통 필드
    private String event;       // PICKUP, IN_TRANSIT, DROPOFF

    @JsonProperty("trip_id")
    private Long tripId;

    @JsonProperty("ts")
    private String ts;

    // 위치 정보 (PICKUP, DROPOFF, IN_TRANSIT)
    private Double lat;
    private Double lon;

    // PICKUP / DROPOFF 전용 필드
    @JsonProperty("PULocationID")
    private Long puLocationId;

    @JsonProperty("DOLocationID")
    private Long doLocationId;

    @JsonProperty("VendorID")
    private Long vendorId;

    @JsonProperty("passenger_count")
    private Long passengerCount;

    @JsonProperty("RatecodeID")
    private Long ratecodeId;

    @JsonProperty("payment_type")
    private Long paymentType;

    private Double extra;

    @JsonProperty("mta_tax")
    private Double mtaTax;

    @JsonProperty("tip_amount")
    private Double tipAmount;

    @JsonProperty("tolls_amount")
    private Double tollsAmount;

    @JsonProperty("improvement_surcharge")
    private Double improvementSurcharge;

    @JsonProperty("congestion_surcharge")
    private Double congestionSurcharge;

    @JsonProperty("Airport_fee")
    private Double airportFee;

    // DROPOFF 전용 필드 (요금 등)
    @JsonProperty("fare_amount")
    private Double fareAmount;

    @JsonProperty("total_amount")
    private Double totalAmount;

    @JsonProperty("trip_distance")
    private Double tripDistance;
}
