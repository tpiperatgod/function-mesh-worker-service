package io.functionmesh.compute.models;

import io.kubernetes.client.openapi.models.V2beta2HorizontalPodAutoscalerBehavior;
import io.kubernetes.client.openapi.models.V2beta2MetricSpec;
import java.util.Arrays;
import java.util.List;
import lombok.Data;

@Data
public class HPASpec {
    private String builtinCPURule;
    private String builtinMemoryRule;
    private List<V2beta2MetricSpec> autoScalingMetrics;
    private V2beta2HorizontalPodAutoscalerBehavior autoScalingBehavior;
    public static final List<String> builtinCPUAutoscalerRules = Arrays.asList("AverageUtilizationCPUPercent80", "AverageUtilizationCPUPercent50", "AverageUtilizationCPUPercent20");
    public static final List<String> builtinMemoryAutoscalerRules = Arrays.asList("AverageUtilizationMemoryPercent80", "AverageUtilizationMemoryPercent50", "AverageUtilizationMemoryPercent20");
}
