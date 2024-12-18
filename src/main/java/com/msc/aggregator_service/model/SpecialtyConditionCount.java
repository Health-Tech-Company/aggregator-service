package com.msc.aggregator_service.model;

import lombok.Data;

@Data
public class SpecialtyConditionCount {
    private String specialty;
    private String condition;
    private Integer count = 0;
}
