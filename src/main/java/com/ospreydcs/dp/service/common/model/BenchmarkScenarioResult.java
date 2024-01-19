package com.ospreydcs.dp.service.common.model;

public class BenchmarkScenarioResult {
    public final boolean success;
    public final double valuesPerSecond;
    public BenchmarkScenarioResult(boolean success, double valuesPerSecond) {
        this.success = success;
        this.valuesPerSecond = valuesPerSecond;
    }
}
