package com.msc.aggregator_service.service;

import com.msc.aggregator_service.model.AppointmentFrequency;
import com.msc.aggregator_service.model.DoctorAppointmentCount;
import com.msc.aggregator_service.model.SpecialtyConditionCount;

import java.util.List;

public interface AggregationService {
    List<DoctorAppointmentCount> getAppointmentCountsPerDoctor();
    List<AppointmentFrequency> getAppointmentFrequencyOverTime();
    List<SpecialtyConditionCount> getCommonConditionsBySpecialty();
}
