package com.msc.aggregator_service.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@Document(collection = "appointments")
public class Appointment {
    @Id
    private String id;
    private String doctorId;
    private String patientId;
    private LocalDateTime appointmentDate;
    private String status;
    private String notes;
}
