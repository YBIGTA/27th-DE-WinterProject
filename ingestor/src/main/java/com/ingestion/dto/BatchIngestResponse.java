package com.ingestion.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response DTO for batch ingestion endpoint.
 * Provides feedback on which events were accepted and which failed.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BatchIngestResponse {
    /**
     * Number of events successfully accepted
     */
    private int acceptedCount;

    /**
     * Number of events that were rejected
     */
    private int rejectedCount;

    /**
     * Indices of events that failed (for debugging/retry purposes)
     */
    private List<Integer> failedIndices;
}
