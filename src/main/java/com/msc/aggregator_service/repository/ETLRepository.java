package com.msc.aggregator_service.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;


@Repository
@RequiredArgsConstructor
public class ETLRepository {
    private final JdbcTemplate jdbcTemplate;


    public void uploadDoctorMetrics(String doctorId, int appointmentCount) {
        String sql = "INSERT INTO DoctorMetrics (doctor_id, appointment_count, report_date) VALUES (?, ?, CURRENT_DATE)";
        jdbcTemplate.update(sql, doctorId, appointmentCount);
    }

    public void uploadAppointmentFrequency(String period, int count) {
        String sql = "INSERT INTO AppointmentFrequency (period, count, report_date) VALUES (?, ?, CURRENT_DATE)";
        jdbcTemplate.update(sql, period, count);
    }

    public void uploadSpecialtyConditions(String specialty, String condition, int count) {
        String sql = "INSERT INTO SpecialtyConditions (specialty, condition, count, report_date) VALUES (?, ?, ?, CURRENT_DATE)";
        jdbcTemplate.update(sql, specialty, condition, count);
    }
}
