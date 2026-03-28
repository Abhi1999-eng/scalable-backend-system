package com.abhishek.scalable_backend_system;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class ScalableBackendSystemApplication {

    public static void main(String[] args) {
        SpringApplication.run(ScalableBackendSystemApplication.class, args);
    }

}
