package com.msc.aggregator_service.cronjob;

import com.msc.aggregator_service.model.AppointmentFrequency;
import com.msc.aggregator_service.model.DoctorAppointmentCount;
import com.msc.aggregator_service.model.SpecialtyConditionCount;
import com.msc.aggregator_service.repository.ETLRepository;
import com.msc.aggregator_service.service.AggregationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * This class handles daily data aggregation jobs.
 * It retrieves aggregated metrics and uploads them to the ETL repository for reporting.
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class DataAggregationJob {

    private final ETLRepository etlRepository;
    private final AggregationService aggregationService;

    /**
     * Scheduled job that runs daily at midnight (00:00).
     * It performs the following tasks:
     * - Aggregates doctor appointment counts and uploads them.
     * - Aggregates appointment frequencies over time and uploads them.
     * - Aggregates common conditions by specialty and uploads them.
     */
    @Scheduled(cron = "0 0 0 * * *")
    public void runDailyAggregations() {
        log.info("Starting daily data aggregation job...");

        // Aggregate and upload doctor appointment counts
        log.info("Fetching appointment counts per doctor...");
        List<DoctorAppointmentCount> doctorCounts = this.aggregationService.getAppointmentCountsPerDoctor();
        doctorCounts.forEach(count -> {
            log.debug("Uploading metrics for doctorId: {}, appointmentCount: {}", count.getDoctorId(), count.getAppointmentCount());
            this.etlRepository.uploadDoctorMetrics(count.getDoctorId(), count.getAppointmentCount());
        });
        log.info("Completed uploading doctor appointment counts.");

        // Aggregate and upload appointment frequency
        log.info("Fetching appointment frequency over time...");
        List<AppointmentFrequency> frequency = this.aggregationService.getAppointmentFrequencyOverTime();
        frequency.forEach(freq -> {
            log.debug("Uploading frequency for period: {}, count: {}", freq.getPeriod(), freq.getCount());
            this.etlRepository.uploadAppointmentFrequency(freq.getPeriod(), freq.getCount());
        });
        log.info("Completed uploading appointment frequency.");

        // Aggregate and upload common conditions by specialty
        log.info("Fetching common conditions by specialty...");
        List<SpecialtyConditionCount> conditions = this.aggregationService.getCommonConditionsBySpecialty();
        conditions.forEach(cond -> {
            log.debug("Uploading condition for specialty: {}, condition: {}, count: {}", cond.getSpecialty(), cond.getCondition(), cond.getCount());
            this.etlRepository.uploadSpecialtyConditions(cond.getSpecialty(), cond.getCondition(), cond.getCount());
        });
        log.info("Completed uploading specialty condition counts.");

        log.info("Daily data aggregation job finished successfully.");
    }
}
