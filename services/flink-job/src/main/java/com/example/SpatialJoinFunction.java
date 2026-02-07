package com.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class SpatialJoinFunction extends RichMapFunction<TaxiEvent, TaxiEvent> {
    private List<ZoneCoords> zoneList;

    private static class ZoneCoords {
        int id;
        double lat;
        double lon;
        ZoneCoords(int id, double lon, double lat) { this.id = id; this.lon = lon; this.lat = lat; }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        zoneList = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                getClass().getClassLoader().getResourceAsStream("taxi_zone_median_coords.csv")))) {
            String line;
            br.readLine(); 
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                zoneList.add(new ZoneCoords(
                    Integer.parseInt(parts[0]),
                    Double.parseDouble(parts[1]),
                    Double.parseDouble(parts[2])
                ));
            }
        }
    }

    @Override
    public TaxiEvent map(TaxiEvent event) {
        if (event == null || event.lat == null || event.lon == null) {
            event.setZone_id(-1);
            return event;
        }

        int nearestId = -1;
        double minDistance = Double.MAX_VALUE;

        for (ZoneCoords zone : zoneList) {
            double dist = calculateDistance(event.lat, event.lon, zone.lat, zone.lon);
            if (dist < minDistance) {
                minDistance = dist;
                nearestId = zone.id;
            }
        }
        event.setZone_id(nearestId);
        return event;
    }

    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}