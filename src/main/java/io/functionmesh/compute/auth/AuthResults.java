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

package io.functionmesh.compute.auth;

import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumeMounts;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumes;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVolumeMounts;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVolumes;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodVolumeMounts;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodVolumes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class AuthResults {
    // Auth providers needs to generate the secret data which contains two keys:
    // brokerClientAuthenticationPlugin, brokerClientAuthenticationParameters
    private Map<String, byte[]> authSecretData = new HashMap<>();

    // Auth providers may need to mount some files to pod for authentication
    private List<V1alpha1FunctionSpecPodVolumes> functionVolumes;
    private List<V1alpha1FunctionSpecPodVolumeMounts> functionVolumeMounts;

    private List<V1alpha1SinkSpecPodVolumes> sinkVolumes;
    private List<V1alpha1SinkSpecPodVolumeMounts> sinkVolumeMounts;

    private List<V1alpha1SourceSpecPodVolumes> sourceVolumes;
    private List<V1alpha1SourceSpecPodVolumeMounts> sourceVolumeMounts;
}
