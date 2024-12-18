package com.msc.aggregator_service.service;

import com.msc.aggregator_service.model.Appointment;
import com.msc.aggregator_service.model.AppointmentFrequency;
import com.msc.aggregator_service.model.DoctorAppointmentCount;
import com.msc.aggregator_service.model.SpecialtyConditionCount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Service implementation for aggregation operations using MongoDB.
 * Provides methods for aggregating data related to doctor appointments,
 * appointment frequency, and conditions by specialty.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AggregationServiceImpl implements AggregationService {

    private final MongoTemplate mongoTemplate;

    /**
     * Retrieves the count of appointments per doctor.
     *
     * @return List of DoctorAppointmentCount containing doctor IDs and appointment counts.
     */
    @Override
    public List<DoctorAppointmentCount> getAppointmentCountsPerDoctor() {
        log.info("Starting aggregation: Appointment counts per doctor.");

        // Define aggregation pipeline
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.group("doctorId").count().as("appointmentCount"),
                Aggregation.project("appointmentCount").and("doctorId").previousOperation()
        );

        // Execute aggregation
        AggregationResults<DoctorAppointmentCount> results =
                this.mongoTemplate.aggregate(aggregation, Appointment.class, DoctorAppointmentCount.class);

        log.info("Completed aggregation: Appointment counts per doctor. Records retrieved: {}", results.getMappedResults().size());
        return results.getMappedResults();
    }

    /**
     * Retrieves the frequency of appointments over time.
     *
     * @return List of AppointmentFrequency containing periods (e.g., daily) and counts.
     */
    @Override
    public List<AppointmentFrequency> getAppointmentFrequencyOverTime() {
        log.info("Starting aggregation: Appointment frequency over time.");

        // Define aggregation pipeline
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.project("appointmentDate")
                        .andExpression("{$dateToString: {format: '%Y-%m-%d', date: '$appointmentDate'}}").as("date"),
                Aggregation.group("date").count().as("count"),
                Aggregation.project("count").and("date").previousOperation()
        );

        // Execute aggregation
        AggregationResults<AppointmentFrequency> results =
                this.mongoTemplate.aggregate(aggregation, Appointment.class, AppointmentFrequency.class);

        log.info("Completed aggregation: Appointment frequency over time. Records retrieved: {}", results.getMappedResults().size());
        return results.getMappedResults();
    }

    /**
     * Retrieves the most common conditions by medical specialty.
     *
     * @return List of SpecialtyConditionCount containing specialties, conditions, and counts.
     */
    @Override
    public List<SpecialtyConditionCount> getCommonConditionsBySpecialty() {
        log.info("Starting aggregation: Common conditions by specialty.");

        // Define aggregation pipeline
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.group("specialty", "condition").count().as("count"),
                Aggregation.project("count")
                        .and("specialty").previousOperation()
                        .and("condition").previousOperation()
        );

        // Execute aggregation
        AggregationResults<SpecialtyConditionCount> results =
                this.mongoTemplate.aggregate(aggregation, "patients", SpecialtyConditionCount.class);

        log.info("Completed aggregation: Common conditions by specialty. Records retrieved: {}", results.getMappedResults().size());
        return results.getMappedResults();
    }
}
