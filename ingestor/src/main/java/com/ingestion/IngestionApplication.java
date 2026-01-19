package com.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class IngestionApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(IngestionApplication.class, args);

        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[SHUTDOWN] Graceful shutdown initiated...");
            context.close();
            System.out.println("[SHUTDOWN] Application stopped");
        }));
    }
}