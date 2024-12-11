package com.msc.aggregator_service.service;

import com.msc.aggregator_service.model.Appointment;
import com.msc.aggregator_service.model.AppointmentFrequency;
import com.msc.aggregator_service.model.DoctorAppointmentCount;
import com.msc.aggregator_service.model.SpecialtyConditionCount;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class AggregationServiceImpl implements AggregationService {

    private final MongoTemplate mongoTemplate;

    @Override
    public List<DoctorAppointmentCount> getAppointmentCountsPerDoctor() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.group("doctorId").count().as("appointmentCount"),
                Aggregation.project("appointmentCount").and("doctorId").previousOperation()
        );

        AggregationResults<DoctorAppointmentCount> results =
                mongoTemplate.aggregate(aggregation, Appointment.class, DoctorAppointmentCount.class);

        return results.getMappedResults();
    }

    @Override
    public List<AppointmentFrequency> getAppointmentFrequencyOverTime() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.project("appointmentDate")
                        .andExpression("{$dateToString: {format: '%Y-%m-%d', date: '$appointmentDate'}}").as("date"),
                Aggregation.group("date").count().as("count"),
                Aggregation.project("count").and("date").previousOperation()
        );

        AggregationResults<AppointmentFrequency> results =
                mongoTemplate.aggregate(aggregation, Appointment.class, AppointmentFrequency.class);

        return results.getMappedResults();
    }

    @Override
    public List<SpecialtyConditionCount> getCommonConditionsBySpecialty() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.group("specialty", "condition").count().as("count"),
                Aggregation.project("count").and("specialty").previousOperation()
                        .and("condition").previousOperation()
        );

        AggregationResults<SpecialtyConditionCount> results =
                mongoTemplate.aggregate(aggregation, "patients", SpecialtyConditionCount.class);

        return results.getMappedResults();
    }
}
