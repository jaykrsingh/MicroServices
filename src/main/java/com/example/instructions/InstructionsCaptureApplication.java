package com.example.instructions;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class InstructionsCaptureApplication {
    public static void main(String[] args) {
        SpringApplication.run(InstructionsCaptureApplication.class, args);
    }
}