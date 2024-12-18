package com.msc.aggregator_service.resource;

import com.msc.aggregator_service.cronjob.DataAggregationJob;
import com.msc.aggregator_service.model.DoctorAppointmentCount;
import com.msc.aggregator_service.model.AppointmentFrequency;
import com.msc.aggregator_service.model.SpecialtyConditionCount;
import com.msc.aggregator_service.service.AggregationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/aggregations")
public class DataAggregatorResource {

    private final AggregationService aggregationService;
    private final DataAggregationJob dataAggregationJob;

    /**
     * Trigger the aggregation process manually.
     *
     * @return ResponseEntity indicating the status of the operation.
     */
    @PostMapping("/run")
    public ResponseEntity<String> runDataAggregation() {
        try {
            this.dataAggregationJob.runDailyAggregations();
            return ResponseEntity.ok("Data aggregation completed successfully.");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error during data aggregation: " + e.getMessage());
        }
    }

    /**
     * Get aggregated data for doctor appointment counts.
     *
     * @return List of DoctorAppointmentCount.
     */
    @GetMapping("/doctor-appointment-counts")
    public ResponseEntity<List<DoctorAppointmentCount>> getDoctorAppointmentCounts() {
        return ResponseEntity.ok(aggregationService.getAppointmentCountsPerDoctor());
    }

    /**
     * Get aggregated data for appointment frequencies over time.
     *
     * @return List of AppointmentFrequency.
     */
    @GetMapping("/appointment-frequencies")
    public ResponseEntity<List<AppointmentFrequency>> getAppointmentFrequencies() {
        return ResponseEntity.ok(aggregationService.getAppointmentFrequencyOverTime());
    }

    /**
     * Get aggregated data for common conditions by specialty.
     *
     * @return List of SpecialtyConditionCount.
     */
    @GetMapping("/specialty-conditions")
    public ResponseEntity<List<SpecialtyConditionCount>> getSpecialtyConditions() {
        return ResponseEntity.ok(aggregationService.getCommonConditionsBySpecialty());
    }
}
