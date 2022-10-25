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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.functions.models.*;
import io.functionmesh.compute.models.CustomRuntimeOptions;
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import io.functionmesh.compute.testdata.Generate;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;

import java.util.*;
import java.util.stream.Collectors;

import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import okhttp3.Response;
import okhttp3.internal.http.RealResponseBody;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactoryConfig;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        Response.class,
        RealResponseBody.class,
        CommonUtil.class,
        FunctionsUtil.class,
        InstanceControlGrpc.InstanceControlFutureStub.class})
@PowerMockIgnore({"javax.management.*", "jdk.internal.reflect.*"})
public class FunctionsUtilTest {
    @Test
    public void testCreateV1alpha1FunctionFromFunctionConfig() throws JsonProcessingException {
        String tenant = "public";
        String namespace = "default";
        String functionName = "word-count";
        String group = "compute.functionmesh.io";
        String version = "v1alpha1";
        String kind = "Function";
        String className = "org.example.functions.WordCountFunction";
        String typeClassName = "java.lang.String";
        int parallelism = 1;
        String input = "persistent://public/default/sentences";
        String output = "persistent://public/default/count";
        String clusterName = "test-pulsar";
        String jar = "/pulsar/function-executable";
        Map<String, Object> configs = new HashMap<>();
        configs.put("foo", "bar");

        MeshWorkerService meshWorkerService = PowerMockito.mock(MeshWorkerService.class);
        CustomObjectsApi customObjectsApi = PowerMockito.mock(CustomObjectsApi.class);
        PowerMockito.when(meshWorkerService.getCustomObjectsApi()).thenReturn(customObjectsApi);
        WorkerConfig workerConfig = PowerMockito.mock(WorkerConfig.class);
        KubernetesRuntimeFactoryConfig factoryConfig = PowerMockito.mock(KubernetesRuntimeFactoryConfig.class);
        PowerMockito.when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);
        PowerMockito.when(meshWorkerService.getFactoryConfig()).thenReturn(factoryConfig);
        PowerMockito.when(factoryConfig.getExtraFunctionDependenciesDir()).thenReturn("");
        PowerMockito.when(workerConfig.isAuthorizationEnabled()).thenReturn(false);
        PowerMockito.when(workerConfig.isAuthenticationEnabled()).thenReturn(false);
        PowerMockito.when(workerConfig.getFunctionsWorkerServiceCustomConfigs()).thenReturn(Collections.emptyMap());
        PulsarAdmin pulsarAdmin = PowerMockito.mock(PulsarAdmin.class);
        PowerMockito.when(meshWorkerService.getBrokerAdmin()).thenReturn(pulsarAdmin);
        PowerMockito.stub(PowerMockito.method(CommonUtil.class, "downloadPackageFile")).toReturn(null);
        PowerMockito.stub(PowerMockito.method(CommonUtil.class, "getFilenameFromPackageMetadata")).toReturn(null);

        Map<String, String> env = new HashMap<String, String>(){
            {
                put("unique", "unique");
                put("shared", "shared");
                put("shared2", "shared2");
            }
        };
        Map<String, String> functionEnv = new HashMap<String, String>(){
            {
                put("shared", "shared-function");
                put("shared2", "shared2-function");
                put("function", "function");
            }
        };

        List<V1alpha1FunctionSpecPodVolumes> volumeList = new ArrayList<V1alpha1FunctionSpecPodVolumes>() {
            {
                add(new V1alpha1FunctionSpecPodVolumes().name("volume1"));
                add(new V1alpha1FunctionSpecPodVolumes().name("volume2"));
            }
        };
        List<V1alpha1FunctionSpecPodVolumeMounts> volumeMountList = new ArrayList<V1alpha1FunctionSpecPodVolumeMounts>() {
            {
                add(new V1alpha1FunctionSpecPodVolumeMounts().name("volumeMount1"));
                add(new V1alpha1FunctionSpecPodVolumeMounts().name("volumeMount2"));
            }
        };
        MeshWorkerServiceCustomConfig meshWorkerServiceCustomConfig =
                PowerMockito.mock(MeshWorkerServiceCustomConfig.class);
        PowerMockito.when(meshWorkerServiceCustomConfig.isUploadEnabled()).thenReturn(true);
        PowerMockito.when(meshWorkerServiceCustomConfig.isFunctionEnabled()).thenReturn(true);
        PowerMockito.when(meshWorkerServiceCustomConfig.isAllowUserDefinedServiceAccountName()).thenReturn(false);
        PowerMockito.when(meshWorkerServiceCustomConfig.getEnv()).thenReturn(env);
        PowerMockito.when(meshWorkerServiceCustomConfig.getFunctionEnv()).thenReturn(functionEnv);
        PowerMockito.when(meshWorkerServiceCustomConfig.asV1alpha1FunctionSpecPodVolumesList()).thenReturn(volumeList);
        PowerMockito.when(meshWorkerServiceCustomConfig.asV1alpha1FunctionSpecPodVolumeMounts()).thenReturn(volumeMountList);
        PowerMockito.when(meshWorkerService.getMeshWorkerServiceCustomConfig())
                .thenReturn(meshWorkerServiceCustomConfig);

        FunctionConfig functionConfig =
                Generate.createJavaFunctionWithPackageURLConfig(tenant, namespace, functionName);

        V1alpha1Function v1alpha1Function = FunctionsUtil.createV1alpha1FunctionFromFunctionConfig(kind, group, version,
                functionName, functionConfig.getJar(), functionConfig, null, meshWorkerService);

        Assert.assertEquals(v1alpha1Function.getKind(), kind);

        V1alpha1FunctionSpec v1alpha1FunctionSpec = v1alpha1Function.getSpec();
        Assert.assertEquals(v1alpha1FunctionSpec.getClassName(), className);
        Assert.assertEquals(v1alpha1FunctionSpec.getCleanupSubscription(), true);
        Assert.assertEquals(v1alpha1FunctionSpec.getReplicas().intValue(), parallelism);
        Assert.assertEquals(v1alpha1FunctionSpec.getInput().getTopics().get(0), input);
        Assert.assertEquals(v1alpha1FunctionSpec.getOutput().getTopic(), output);
        Assert.assertEquals(v1alpha1FunctionSpec.getPulsar().getPulsarConfig(),
                CommonUtil.getPulsarClusterConfigMapName(clusterName));
        Assert.assertEquals(v1alpha1FunctionSpec.getInput().getTypeClassName(), typeClassName);
        Assert.assertEquals(v1alpha1FunctionSpec.getOutput().getTypeClassName(), typeClassName);
        Assert.assertEquals(v1alpha1FunctionSpec.getJava().getJar(), jar);
        Assert.assertEquals(v1alpha1FunctionSpec.getForwardSourceMessageProperty(), true);
        Assert.assertEquals(v1alpha1FunctionSpec.getFuncConfig(), configs);
        Assert.assertNotNull(v1alpha1FunctionSpec.getSecretsMap());
        Assert.assertFalse(v1alpha1FunctionSpec.getSecretsMap().isEmpty());
        Assert.assertEquals(v1alpha1FunctionSpec.getSecretsMap().size(), 2);
        Assert.assertEquals(v1alpha1FunctionSpec.getSecretsMap().get("secret1").getKey(), "secretKey1");
        Assert.assertEquals(v1alpha1FunctionSpec.getSecretsMap().get("secret1").getPath(), "secretPath1");
        Assert.assertEquals(v1alpha1FunctionSpec.getSecretsMap().get("secret2").getKey(), "secretKey2");
        Assert.assertEquals(v1alpha1FunctionSpec.getSecretsMap().get("secret2").getPath(), "secretPath2");
        Assert.assertEquals(v1alpha1FunctionSpec.getPod().getEnv().stream().collect(Collectors.toMap(
                V1alpha1FunctionSpecPodEnv::getName, V1alpha1FunctionSpecPodEnv::getValue)), new HashMap<String, String>(){
            {
                put("unique", "unique");
                put("shared", "shared-function");
                put("function", "function");
                put("runtime", "runtime-env");
                put("shared2", "shared2-runtime");
            }
        } );
        Assert.assertEquals(v1alpha1FunctionSpec.getSubscriptionName(), "test-sub");
        Assert.assertEquals(v1alpha1FunctionSpec.getSubscriptionPosition(), V1alpha1FunctionSpec.SubscriptionPositionEnum.LATEST);
        Assert.assertEquals(v1alpha1FunctionSpec.getPod().getVolumes(), volumeList);
        Assert.assertEquals(v1alpha1FunctionSpec.getVolumeMounts(), volumeMountList);
    }

    @Test
    public void testCreateFunctionConfigFromV1alpha1Function() throws JsonProcessingException {
        String tenant = "public";
        String namespace = "default";
        String functionName = "word-count";
        String group = "compute.functionmesh.io";
        String version = "v1alpha1";
        String kind = "Function";

        MeshWorkerService meshWorkerService = PowerMockito.mock(MeshWorkerService.class);
        CustomObjectsApi customObjectsApi = PowerMockito.mock(CustomObjectsApi.class);
        PowerMockito.when(meshWorkerService.getCustomObjectsApi()).thenReturn(customObjectsApi);
        WorkerConfig workerConfig = PowerMockito.mock(WorkerConfig.class);
        KubernetesRuntimeFactoryConfig factoryConfig = PowerMockito.mock(KubernetesRuntimeFactoryConfig.class);
        PowerMockito.when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);
        PowerMockito.when(meshWorkerService.getFactoryConfig()).thenReturn(factoryConfig);
        PowerMockito.when(factoryConfig.getExtraFunctionDependenciesDir()).thenReturn("");
        PowerMockito.when(workerConfig.isAuthorizationEnabled()).thenReturn(false);
        PowerMockito.when(workerConfig.isAuthenticationEnabled()).thenReturn(false);
        PowerMockito.when(workerConfig.getFunctionsWorkerServiceCustomConfigs()).thenReturn(Collections.emptyMap());
        PulsarAdmin pulsarAdmin = PowerMockito.mock(PulsarAdmin.class);
        PowerMockito.when(meshWorkerService.getBrokerAdmin()).thenReturn(pulsarAdmin);
        PowerMockito.stub(PowerMockito.method(CommonUtil.class, "downloadPackageFile")).toReturn(null);
        PowerMockito.stub(PowerMockito.method(CommonUtil.class, "getFilenameFromPackageMetadata"))
                .toReturn("word-count.jar");

        Map<String, String> env = new HashMap<String, String>(){
            {
                put("unique", "unique");
                put("shared", "shared");
                put("shared2", "shared2");
            }
        };
        Map<String, String> functionEnv = new HashMap<String, String>(){
            {
                put("shared", "shared-function");
                put("shared2", "shared2-function");
                put("function", "function");
            }
        };
        MeshWorkerServiceCustomConfig meshWorkerServiceCustomConfig =
                PowerMockito.mock(MeshWorkerServiceCustomConfig.class);
        PowerMockito.when(meshWorkerServiceCustomConfig.isUploadEnabled()).thenReturn(true);
        PowerMockito.when(meshWorkerServiceCustomConfig.isFunctionEnabled()).thenReturn(true);
        PowerMockito.when(meshWorkerServiceCustomConfig.isAllowUserDefinedServiceAccountName()).thenReturn(false);
        PowerMockito.when(meshWorkerServiceCustomConfig.getEnv()).thenReturn(env);
        PowerMockito.when(meshWorkerServiceCustomConfig.getFunctionEnv()).thenReturn(functionEnv);
        PowerMockito.when(meshWorkerService.getMeshWorkerServiceCustomConfig())
                .thenReturn(meshWorkerServiceCustomConfig);

        FunctionConfig functionConfig =
                Generate.createJavaFunctionWithPackageURLConfig(tenant, namespace, functionName);
        // test whether will it filter out env got from the custom config
        String expectedCustomRuntimeOptions = functionConfig.getCustomRuntimeOptions();
        CustomRuntimeOptions customRuntimeOptions =
                CommonUtil.getCustomRuntimeOptions(functionConfig.getCustomRuntimeOptions());
        customRuntimeOptions.getEnv().put("unique", "unique");
        customRuntimeOptions.getEnv().put("shared", "shared-function");
        customRuntimeOptions.getEnv().put("function", "function");
        String customRuntimeOptionsJSON = new Gson().toJson(customRuntimeOptions, CustomRuntimeOptions.class);
        functionConfig.setCustomRuntimeOptions(customRuntimeOptionsJSON);

        V1alpha1Function v1alpha1Function = FunctionsUtil.createV1alpha1FunctionFromFunctionConfig(kind, group, version,
                functionName, functionConfig.getJar(), functionConfig, null, meshWorkerService);

        FunctionConfig newFunctionConfig = FunctionsUtil.createFunctionConfigFromV1alpha1Function(tenant, namespace,
                functionName, v1alpha1Function, meshWorkerService);

        Assert.assertEquals(functionConfig.getClassName(), newFunctionConfig.getClassName());
        Assert.assertEquals(functionConfig.getJar(), newFunctionConfig.getJar());
        Assert.assertEquals(functionConfig.getPy(), newFunctionConfig.getPy());
        Assert.assertEquals(functionConfig.getGo(), newFunctionConfig.getGo());
        Assert.assertArrayEquals(functionConfig.getInputs().toArray(), newFunctionConfig.getInputs().toArray());
        Assert.assertEquals(functionConfig.getOutput(), newFunctionConfig.getOutput());
        Assert.assertEquals(functionConfig.getMaxMessageRetries(), newFunctionConfig.getMaxMessageRetries());
        Assert.assertEquals(functionConfig.getAutoAck(), newFunctionConfig.getAutoAck());
        Assert.assertEquals(functionConfig.getBatchBuilder(), newFunctionConfig.getBatchBuilder());
        Assert.assertEquals(expectedCustomRuntimeOptions, newFunctionConfig.getCustomRuntimeOptions());
        Assert.assertEquals(functionConfig.getSubName(), newFunctionConfig.getSubName());
        Assert.assertEquals(functionConfig.getCleanupSubscription(), newFunctionConfig.getCleanupSubscription());
        Assert.assertEquals(functionConfig.getCustomSchemaInputs(), newFunctionConfig.getCustomSchemaInputs());
        Assert.assertEquals(functionConfig.getCustomSerdeInputs(), newFunctionConfig.getCustomSerdeInputs());
        Assert.assertEquals(functionConfig.getCustomSchemaOutputs(), newFunctionConfig.getCustomSchemaOutputs());
        Assert.assertEquals(functionConfig.getUserConfig(), newFunctionConfig.getUserConfig());
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode oldSecretsJson = objectMapper.readTree(objectMapper.writeValueAsString(functionConfig.getSecrets()));
        JsonNode newSecretsJson =
                objectMapper.readTree(objectMapper.writeValueAsString(newFunctionConfig.getSecrets()));
        Assert.assertEquals(oldSecretsJson, newSecretsJson);
        Assert.assertEquals(functionConfig.getSubName(), newFunctionConfig.getSubName());
        Assert.assertEquals(functionConfig.getSubscriptionPosition(), newFunctionConfig.getSubscriptionPosition());
    }

    @Test
    public void testCreateV1alpha1FunctionWithWindowConfig() throws JsonProcessingException {
        String tenant = "public";
        String namespace = "default";
        String functionName = "get-input-topics";
        String group = "compute.functionmesh.io";
        String version = "v1alpha1";
        String kind = "Function";
        Integer windowLengthCount = 5;
        Integer slidingIntervalCount = 10;
        WindowConfig windowConfig = new WindowConfig();
        windowConfig.setWindowLengthCount(windowLengthCount);
        windowConfig.setSlidingIntervalCount(slidingIntervalCount);

        MeshWorkerService meshWorkerService = PowerMockito.mock(MeshWorkerService.class);
        CustomObjectsApi customObjectsApi = PowerMockito.mock(CustomObjectsApi.class);
        PowerMockito.when(meshWorkerService.getCustomObjectsApi()).thenReturn(customObjectsApi);
        WorkerConfig workerConfig = PowerMockito.mock(WorkerConfig.class);
        KubernetesRuntimeFactoryConfig factoryConfig = PowerMockito.mock(KubernetesRuntimeFactoryConfig.class);
        PowerMockito.when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);
        PowerMockito.when(meshWorkerService.getFactoryConfig()).thenReturn(factoryConfig);
        PowerMockito.when(factoryConfig.getExtraFunctionDependenciesDir()).thenReturn("");
        PowerMockito.when(workerConfig.isAuthorizationEnabled()).thenReturn(false);
        PowerMockito.when(workerConfig.isAuthenticationEnabled()).thenReturn(false);
        PowerMockito.when(workerConfig.getFunctionsWorkerServiceCustomConfigs()).thenReturn(Collections.emptyMap());
        PulsarAdmin pulsarAdmin = PowerMockito.mock(PulsarAdmin.class);
        PowerMockito.when(meshWorkerService.getBrokerAdmin()).thenReturn(pulsarAdmin);
        PowerMockito.stub(PowerMockito.method(CommonUtil.class, "downloadPackageFile")).toReturn(null);
        PowerMockito.stub(PowerMockito.method(CommonUtil.class, "getFilenameFromPackageMetadata")).toReturn(null);

        MeshWorkerServiceCustomConfig meshWorkerServiceCustomConfig =
                PowerMockito.mock(MeshWorkerServiceCustomConfig.class);
        PowerMockito.when(meshWorkerServiceCustomConfig.isUploadEnabled()).thenReturn(true);
        PowerMockito.when(meshWorkerServiceCustomConfig.isFunctionEnabled()).thenReturn(true);
        PowerMockito.when(meshWorkerServiceCustomConfig.isAllowUserDefinedServiceAccountName()).thenReturn(false);
        PowerMockito.when(meshWorkerService.getMeshWorkerServiceCustomConfig())
                .thenReturn(meshWorkerServiceCustomConfig);

        FunctionConfig functionConfig =
                Generate.createJavaFunctionWithWindowConfig(tenant, namespace, functionName);

        V1alpha1Function v1alpha1Function = FunctionsUtil.createV1alpha1FunctionFromFunctionConfig(kind, group, version,
                functionName, functionConfig.getJar(), functionConfig, null, meshWorkerService);

        Assert.assertEquals(v1alpha1Function.getKind(), kind);

        V1alpha1FunctionSpec v1alpha1FunctionSpec = v1alpha1Function.getSpec();
        Assert.assertNotNull(v1alpha1FunctionSpec.getWindowConfig());
        Assert.assertEquals(v1alpha1FunctionSpec.getWindowConfig().getWindowLengthCount(), windowLengthCount);
        Assert.assertEquals(v1alpha1FunctionSpec.getWindowConfig().getSlidingIntervalCount(), slidingIntervalCount);
    }
}
