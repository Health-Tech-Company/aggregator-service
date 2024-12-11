package com.msc.aggregator_service.cronjob;

import com.msc.aggregator_service.model.AppointmentFrequency;
import com.msc.aggregator_service.model.DoctorAppointmentCount;
import com.msc.aggregator_service.model.SpecialtyConditionCount;
import com.msc.aggregator_service.repository.ETLRepository;
import com.msc.aggregator_service.service.AggregationService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class DataAggregationJob {

    private final ETLRepository etlRepository;
    private final AggregationService aggregationService;

    @Scheduled(cron = "0 0 0 * * *")
    public void runDailyAggregations() {
        List<DoctorAppointmentCount> doctorCounts = aggregationService.getAppointmentCountsPerDoctor();
        doctorCounts.forEach(count ->
                etlRepository.uploadDoctorMetrics(count.getDoctorId(), count.getAppointmentCount())
        );

        List<AppointmentFrequency> frequency = aggregationService.getAppointmentFrequencyOverTime();
        frequency.forEach(freq ->
                etlRepository.uploadAppointmentFrequency(freq.getPeriod(), freq.getCount())
        );

        List<SpecialtyConditionCount> conditions = aggregationService.getCommonConditionsBySpecialty();
        conditions.forEach(cond ->
                etlRepository.uploadSpecialtyConditions(cond.getSpecialty(), cond.getCondition(), cond.getCount())
        );
    }
}
