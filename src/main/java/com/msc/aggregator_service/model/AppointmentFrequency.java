package com.msc.aggregator_service.model;

import lombok.Data;

@Data
public class AppointmentFrequency {
    private String period = "";
    private Integer count = 0;
}
