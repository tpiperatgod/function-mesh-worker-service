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
package io.functionmesh.compute.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodImagePullSecrets;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodInitContainers;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumes;
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodInitContainers;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVolumes;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodInitContainers;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodVolumes;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.functions.runtime.RuntimeUtils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.junit.Test;

@Slf4j
public class WorkerConfigTest {
    @Test
    public void testCustomConfigs() throws Exception {
        WorkerConfig workerConfig = WorkerConfig.load(getClass().getClassLoader().getResource("test_worker_config.yaml")
                .toURI().getPath());
        log.info("Got worker config [{}]", workerConfig);
        Map<String, Object> customConfigs = workerConfig.getFunctionsWorkerServiceCustomConfigs();
        log.info("Got custom configs [{}]", customConfigs);
        MeshWorkerServiceCustomConfig customConfig = RuntimeUtils.getRuntimeFunctionConfig(
                customConfigs, MeshWorkerServiceCustomConfig.class);

        assertEquals(customConfigs.get("uploadEnabled"), customConfig.isUploadEnabled());
        assertEquals("service-account", customConfig.getDefaultServiceAccountName());

        V1OwnerReference ownerRef = CommonUtil.getOwnerReferenceFromCustomConfigs(customConfig);
        log.info("Got owner ref [{}]", ownerRef);
        assertEquals("pulsar.streamnative.io/v1alpha1", ownerRef.getApiVersion());
        assertEquals("PulsarBroker", ownerRef.getKind());
        assertEquals("test", ownerRef.getName());
        assertEquals("4627a402-35f2-40ac-b3fc-1bae5a2bd626", ownerRef.getUid());
        assertNull(ownerRef.getBlockOwnerDeletion());
        assertNull(ownerRef.getController());

        List<V1alpha1SourceSpecPodVolumes> v1alpha1SourceSpecPodVolumesList =
                customConfig.asV1alpha1SourceSpecPodVolumesList();
        assertEquals(1, v1alpha1SourceSpecPodVolumesList.size());
        assertEquals("secret-pulsarcluster-data", v1alpha1SourceSpecPodVolumesList.get(0).getName());
        assertNotNull("pulsarcluster-data", v1alpha1SourceSpecPodVolumesList.get(0).getSecret());
        assertEquals("pulsarcluster-data", v1alpha1SourceSpecPodVolumesList.get(0).getSecret().getSecretName());

        List<V1alpha1SinkSpecPodVolumes> v1alpha1SinkSpecPodVolumesList =
                customConfig.asV1alpha1SinkSpecPodVolumesList();
        assertEquals(1, v1alpha1SinkSpecPodVolumesList.size());
        assertEquals("secret-pulsarcluster-data", v1alpha1SinkSpecPodVolumesList.get(0).getName());
        assertNotNull("pulsarcluster-data", v1alpha1SinkSpecPodVolumesList.get(0).getSecret());
        assertEquals("pulsarcluster-data", v1alpha1SinkSpecPodVolumesList.get(0).getSecret().getSecretName());

        List<V1alpha1FunctionSpecPodVolumes> v1alpha1FunctionSpecPodVolumesList =
                customConfig.asV1alpha1FunctionSpecPodVolumesList();
        assertEquals(1, v1alpha1FunctionSpecPodVolumesList.size());
        assertEquals("secret-pulsarcluster-data", v1alpha1FunctionSpecPodVolumesList.get(0).getName());
        assertNotNull("pulsarcluster-data", v1alpha1FunctionSpecPodVolumesList.get(0).getSecret());
        assertEquals("pulsarcluster-data", v1alpha1FunctionSpecPodVolumesList.get(0).getSecret().getSecretName());

        assertEquals("always", customConfig.getImagePullPolicy());

        List<V1alpha1FunctionSpecPodImagePullSecrets> functionSpecPodImagePullSecrets =
                customConfig.asV1alpha1FunctionSpecPodImagePullSecrets();
        assertEquals(1, customConfig.getImagePullSecrets().size());
        assertEquals(1, functionSpecPodImagePullSecrets.size());
        assertEquals("registry-secret", functionSpecPodImagePullSecrets.get(0).getName());

        assertEquals(3, customConfig.getFunctionRunnerImages().size());
        assertEquals("streamnative/pulsar-functions-java-runner", customConfig.getFunctionRunnerImages().get("JAVA"));
        assertEquals("streamnative/pulsar-functions-python-runner",
                customConfig.getFunctionRunnerImages().get("PYTHON"));
        assertEquals("streamnative/pulsar-functions-go-runner", customConfig.getFunctionRunnerImages().get("GO"));

        assertEquals(2, customConfig.getLabels().size());
        assertEquals("function-mesh", customConfig.getLabels().get("functionmesh.io/managedBy"));
        assertEquals("bar", customConfig.getLabels().get("foo"));
        assertEquals(1, customConfig.getAnnotations().size());
        assertEquals("barAnnotation", customConfig.getAnnotations().get("fooAnnotation"));

        assertEquals(1, customConfig.getFunctionLabels().size());
        assertEquals(1, customConfig.getSinkLabels().size());
        assertEquals(1, customConfig.getSourceLabels().size());
        assertEquals(1, customConfig.getFunctionAnnotations().size());
        assertEquals(1, customConfig.getSinkAnnotations().size());
        assertEquals(1, customConfig.getSourceAnnotations().size());
        assertEquals("function", customConfig.getFunctionLabels().get("appType"));
        assertEquals("function", customConfig.getFunctionAnnotations().get("appType"));
        assertEquals("sink", customConfig.getSinkLabels().get("appType"));
        assertEquals("sink", customConfig.getSinkAnnotations().get("appType"));
        assertEquals("source", customConfig.getSourceLabels().get("appType"));
        assertEquals("source", customConfig.getSourceAnnotations().get("appType"));


        List<String> cmds = Arrays.asList("sh", "-c", "echo hello world");
        List<V1alpha1FunctionSpecPodInitContainers> functionSpecPodInitContainers =
                customConfig.asV1alpha1FunctionSpecPodInitContainers();
        assertEquals(1, functionSpecPodInitContainers.size());
        assertEquals("init", functionSpecPodInitContainers.get(0).getName());
        assertEquals("streamnative/init:latest", functionSpecPodInitContainers.get(0).getImage());
        assertEquals(cmds, functionSpecPodInitContainers.get(0).getCommand());

        List<V1alpha1SourceSpecPodInitContainers> sourceSpecPodInitContainers =
                customConfig.asV1alpha1SourceSpecPodInitContainers();
        assertEquals(1, sourceSpecPodInitContainers.size());
        assertEquals("init", sourceSpecPodInitContainers.get(0).getName());
        assertEquals("streamnative/init:latest", sourceSpecPodInitContainers.get(0).getImage());
        assertEquals(cmds, sourceSpecPodInitContainers.get(0).getCommand());

        List<V1alpha1SinkSpecPodInitContainers> sinkSpecPodInitContainers =
                customConfig.asV1alpha1SinkSpecPodInitContainers();
        assertEquals(1, sinkSpecPodInitContainers.size());
        assertEquals("init", sinkSpecPodInitContainers.get(0).getName());
        assertEquals("streamnative/init:latest", sinkSpecPodInitContainers.get(0).getImage());
        assertEquals(cmds, sinkSpecPodInitContainers.get(0).getCommand());

        Resources resources = customConfig.getDefaultResources();
        assertEquals(2, resources.getCpu(), 0.1);
        assertEquals(17179869184L, resources.getRam().longValue());

        assertEquals(120L, customConfig.getConnectorSearchIntervalSeconds());
        assertEquals("connector-definitions/conf.yaml", customConfig.getConnectorDefinitionsFilePath());
    }
}
