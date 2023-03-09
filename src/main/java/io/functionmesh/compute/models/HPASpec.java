/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
