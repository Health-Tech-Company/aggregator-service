package com.msc.aggregator_service.model;

import lombok.Data;

@Data
public class DoctorAppointmentCount {
    private String doctorId;
    private Integer appointmentCount;
}
