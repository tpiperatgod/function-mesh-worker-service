/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.functionmesh.compute.util;

import static io.functionmesh.compute.models.SecretRef.KEY_KEY;
import static io.functionmesh.compute.models.SecretRef.PATH_KEY;
import static io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVpaUpdatePolicy.UpdateModeEnum.AUTO;
import static io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVpaUpdatePolicy.UpdateModeEnum.INITIAL;
import static io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVpaUpdatePolicy.UpdateModeEnum.OFF;
import static io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVpaUpdatePolicy.UpdateModeEnum.RECREATE;
import static io.functionmesh.compute.util.CommonUtil.ANNOTATION_MANAGED;
import static io.functionmesh.compute.util.CommonUtil.buildDownloadPath;
import static io.functionmesh.compute.util.CommonUtil.getClassNameFromFile;
import static io.functionmesh.compute.util.CommonUtil.getCustomLabelClaims;
import static io.functionmesh.compute.util.CommonUtil.getExceptionInformation;
import static org.apache.pulsar.common.functions.Utils.BUILTIN;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.models.CustomRuntimeOptions;
import io.functionmesh.compute.models.FunctionMeshConnectorDefinition;
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import io.functionmesh.compute.models.VPAContainerPolicy;
import io.functionmesh.compute.models.VPASpec;
import io.functionmesh.compute.models.VPAUpdatePolicy;
import io.functionmesh.compute.sinks.models.V1alpha1Sink;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpec;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecInput;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecInputCryptoConfig;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecInputSourceSpecs;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecJava;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPod;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodEnv;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodResources;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVpa;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVpaResourcePolicy;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVpaResourcePolicyContainerPolicies;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVpaUpdatePolicy;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPulsar;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecSecretsMap;
import io.functionmesh.compute.worker.MeshConnectorsManager;
import io.kubernetes.client.custom.Quantity;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.SinkStatus.SinkInstanceStatus.SinkInstanceStatusData;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.SinkConfigUtils;

@Slf4j
public class SinksUtil {
    public static final String CPU_KEY = "cpu";
    public static final String MEMORY_KEY = "memory";

    public static V1alpha1Sink createV1alpha1SkinFromSinkConfig(String kind, String group, String version
            , String sinkName, String sinkPkgUrl, InputStream uploadedInputStream, SinkConfig sinkConfig,
                                                                MeshConnectorsManager connectorsManager,
                                                                String cluster, MeshWorkerService worker) {
        MeshWorkerServiceCustomConfig customConfig = worker.getMeshWorkerServiceCustomConfig();
        CustomRuntimeOptions customRuntimeOptions =
                CommonUtil.getCustomRuntimeOptions(sinkConfig.getCustomRuntimeOptions());
        String clusterName = CommonUtil.getClusterName(cluster, customRuntimeOptions);
        String serviceAccountName = customRuntimeOptions.getServiceAccountName();

        String location =
                String.format(
                        "%s/%s/%s",
                        sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName());
        if (StringUtils.isNotEmpty(sinkPkgUrl)) {
            location = sinkPkgUrl;
        }
        String archive = sinkConfig.getArchive();
        SinkConfigUtils.ExtractedSinkDetails extractedSinkDetails =
                new SinkConfigUtils.ExtractedSinkDetails("", customRuntimeOptions.getInputTypeClassName());
        Map<String, String> customLabelClaims =
                getCustomLabelClaims(clusterName, sinkConfig.getTenant(), sinkConfig.getNamespace(),
                        sinkConfig.getName(), worker, kind);

        Function.FunctionDetails functionDetails = null;
        try {
            functionDetails = SinkConfigUtils.convert(sinkConfig, extractedSinkDetails);
        } catch (Exception ex) {
            log.error("cannot convert SinkConfig to FunctionDetails", ex);
            throw new RestException(Response.Status.BAD_REQUEST,
                    "functionConfig cannot be parsed into functionDetails");
        }

        V1alpha1Sink v1alpha1Sink = new V1alpha1Sink();
        v1alpha1Sink.setKind(kind);
        v1alpha1Sink.setApiVersion(String.format("%s/%s", group, version));
        v1alpha1Sink.setMetadata(CommonUtil.makeV1ObjectMeta(sinkConfig.getName(),
                worker.getJobNamespace(),
                functionDetails.getNamespace(),
                functionDetails.getTenant(),
                clusterName,
                CommonUtil.getOwnerReferenceFromCustomConfigs(customConfig),
                customLabelClaims));

        V1alpha1SinkSpec v1alpha1SinkSpec = new V1alpha1SinkSpec();
        v1alpha1SinkSpec.setTenant(sinkConfig.getTenant());
        v1alpha1SinkSpec.setNamespace(sinkConfig.getNamespace());

        if (StringUtils.isNotEmpty(customConfig.getImagePullPolicy())) {
            v1alpha1SinkSpec.setImagePullPolicy(customConfig.getImagePullPolicy());
        }
        v1alpha1SinkSpec.setClassName(sinkConfig.getClassName());

        Integer parallelism = sinkConfig.getParallelism() == null ? 1 : sinkConfig.getParallelism();
        v1alpha1SinkSpec.setReplicas(parallelism);

        V1alpha1SinkSpecJava v1alpha1SinkSpecJava = new V1alpha1SinkSpecJava();
        v1alpha1SinkSpecJava.setJavaOpts(customConfig.getJavaOPTs());
        String extraDependenciesDir = "";
        if (worker.getFactoryConfig() != null && StringUtils.isNotEmpty(
                worker.getFactoryConfig().getExtraFunctionDependenciesDir())) {
            if (Paths.get(worker.getFactoryConfig().getExtraFunctionDependenciesDir()).isAbsolute()) {
                extraDependenciesDir = worker.getFactoryConfig().getExtraFunctionDependenciesDir();
            } else {
                extraDependenciesDir = "/pulsar/" + worker.getFactoryConfig().getExtraFunctionDependenciesDir();
            }
        } else if (StringUtils.isNotEmpty(customConfig.getExtraDependenciesDir())) {
            if (Paths.get(customConfig.getExtraDependenciesDir()).isAbsolute()) {
                extraDependenciesDir = customConfig.getExtraDependenciesDir();
            } else {
                extraDependenciesDir = "/pulsar/" + customConfig.getExtraDependenciesDir();
            }
        } else {
            extraDependenciesDir = "/pulsar/instances/deps";
        }
        v1alpha1SinkSpecJava.setExtraDependenciesDir(extraDependenciesDir);

        if (connectorsManager != null && archive.startsWith(BUILTIN)) {
            String connectorType = archive.replaceFirst("^builtin://", "");
            FunctionMeshConnectorDefinition definition = connectorsManager.getConnectorDefinition(connectorType);
            if (definition != null) {
                v1alpha1SinkSpec.setImage(definition.toFullImageURL());
                if (definition.getSinkClass() != null && v1alpha1SinkSpec.getClassName() == null) {
                    v1alpha1SinkSpec.setClassName(definition.getSinkClass());
                    extractedSinkDetails.setSinkClassName(definition.getSinkClass());
                }
                v1alpha1SinkSpecJava.setJar(definition.getJar());
                v1alpha1SinkSpecJava.setJarLocation("");
                v1alpha1SinkSpec.setJava(v1alpha1SinkSpecJava);
            } else {
                log.warn("cannot find built-in connector {}", connectorType);
                throw new RestException(Response.Status.BAD_REQUEST,
                        String.format("connectorType %s is not supported yet", connectorType));
            }
        } else if (StringUtils.isNotEmpty(sinkConfig.getArchive())) {
            v1alpha1SinkSpecJava.setJar(
                    buildDownloadPath(worker.getWorkerConfig().getDownloadDirectory(), sinkConfig.getArchive()));
            if (StringUtils.isNotEmpty(sinkPkgUrl)) {
                v1alpha1SinkSpecJava.setJarLocation(location);
            }
            v1alpha1SinkSpec.setJava(v1alpha1SinkSpecJava);
            extractedSinkDetails.setSinkClassName(sinkConfig.getClassName());
        }

        if (CommonUtil.getRunnerImageFromConfig("JAVA", worker) != null
                && StringUtils.isEmpty(v1alpha1SinkSpec.getImage())) {
            v1alpha1SinkSpec.setImage(CommonUtil.getRunnerImageFromConfig("JAVA", worker));
            if (StringUtils.isNotEmpty(v1alpha1SinkSpec.getImage())
                    && StringUtils.isNotEmpty(customRuntimeOptions.getRunnerImageTag())) {
                if (v1alpha1SinkSpec.getImage().contains(":")) {
                    // replace the image tag
                    String[] parts = v1alpha1SinkSpec.getImage().split(":");
                    if (parts.length == 2) {
                        v1alpha1SinkSpec.setImage(parts[0] + ":" + customRuntimeOptions.getRunnerImageTag());
                    }
                }
            }
        }

        V1alpha1SinkSpecInput v1alpha1SinkSpecInput = new V1alpha1SinkSpecInput();

        for (Map.Entry<String, Function.ConsumerSpec> inputSpecs : functionDetails.getSource().getInputSpecsMap()
                .entrySet()) {
            V1alpha1SinkSpecInputSourceSpecs inputSourceSpecsItem = new V1alpha1SinkSpecInputSourceSpecs();
            if (Strings.isNotEmpty(inputSpecs.getValue().getSerdeClassName())) {
                inputSourceSpecsItem.setSerdeClassname(inputSpecs.getValue().getSerdeClassName());
            }
            if (Strings.isNotEmpty(inputSpecs.getValue().getSchemaType())) {
                inputSourceSpecsItem.setSchemaType(inputSpecs.getValue().getSchemaType());
            }
            if (inputSpecs.getValue().hasReceiverQueueSize()) {
                inputSourceSpecsItem.setReceiverQueueSize(inputSpecs.getValue().getReceiverQueueSize().getValue());
            }
            if (inputSpecs.getValue().hasCryptoSpec()) {
                inputSourceSpecsItem.setCryptoConfig(convertFromCryptoSpec(inputSpecs.getValue().getCryptoSpec()));
            }
            inputSourceSpecsItem.setIsRegexPattern(inputSpecs.getValue().getIsRegexPattern());
            inputSourceSpecsItem.setSchemaProperties(inputSpecs.getValue().getSchemaPropertiesMap());
            v1alpha1SinkSpecInput.putSourceSpecsItem(inputSpecs.getKey(), inputSourceSpecsItem);
        }

        if (Strings.isNotEmpty(customRuntimeOptions.getInputTypeClassName())) {
            v1alpha1SinkSpecInput.setTypeClassName(customRuntimeOptions.getInputTypeClassName());
        } else {
            if (connectorsManager == null) {
                v1alpha1SinkSpecInput.setTypeClassName("[B");
            } else {
                String connectorType = archive.replaceFirst("^builtin://", "");
                FunctionMeshConnectorDefinition functionMeshConnectorDefinition =
                        connectorsManager.getConnectorDefinition(connectorType);
                if (functionMeshConnectorDefinition == null) {
                    v1alpha1SinkSpecInput.setTypeClassName("[B");
                } else {
                    v1alpha1SinkSpecInput.setTypeClassName(functionMeshConnectorDefinition.getSinkTypeClassName());
                    if (StringUtils.isEmpty(v1alpha1SinkSpecInput.getTypeClassName())) {
                        v1alpha1SinkSpecInput.setTypeClassName("[B");
                    }
                    // we only handle user provide --inputs but also with defaultSchemaType defined
                    if (sinkConfig.getInputs() != null && sinkConfig.getInputs().size() > 0) {
                        if (v1alpha1SinkSpecInput.getSourceSpecs() == null) {
                            v1alpha1SinkSpecInput.setSourceSpecs(new HashMap<>());
                        }
                        for (String input : sinkConfig.getInputs()) {
                            V1alpha1SinkSpecInputSourceSpecs inputSourceSpecsItem =
                                    v1alpha1SinkSpecInput.getSourceSpecs().getOrDefault(input,
                                            new V1alpha1SinkSpecInputSourceSpecs());
                            boolean updated = false;
                            if (StringUtils.isNotEmpty(functionMeshConnectorDefinition.getDefaultSchemaType())
                                    && StringUtils.isEmpty(inputSourceSpecsItem.getSchemaType())) {
                                inputSourceSpecsItem.setSchemaType(
                                        functionMeshConnectorDefinition.getDefaultSchemaType());
                                updated = true;
                            }
                            if (StringUtils.isNotEmpty(functionMeshConnectorDefinition.getDefaultSerdeClassName())
                                    && StringUtils.isEmpty(inputSourceSpecsItem.getSerdeClassname())) {
                                inputSourceSpecsItem.setSerdeClassname(
                                        functionMeshConnectorDefinition.getDefaultSerdeClassName());
                                updated = true;
                            }
                            if (updated) {
                                v1alpha1SinkSpecInput.putSourceSpecsItem(input, inputSourceSpecsItem);
                            }
                        }
                    }
                }
            }
        }

        if (sinkConfig.getInputs() != null) {
            v1alpha1SinkSpecInput.setTopics(new ArrayList<>(sinkConfig.getInputs()));
        }

        if ((v1alpha1SinkSpecInput.getTopics() == null || v1alpha1SinkSpecInput.getTopics().size() == 0) &&
                (v1alpha1SinkSpecInput.getSourceSpecs() == null || v1alpha1SinkSpecInput.getSourceSpecs().size() == 0)
        ) {
            log.warn("invalid SinkSpecInput {}", v1alpha1SinkSpecInput);
            throw new RestException(Response.Status.BAD_REQUEST, "invalid SinkSpecInput");
        }
        v1alpha1SinkSpec.setInput(v1alpha1SinkSpecInput);

        if (Strings.isNotEmpty(functionDetails.getSource().getSubscriptionName())) {
            v1alpha1SinkSpec.setSubscriptionName(functionDetails.getSource().getSubscriptionName());
        }
        switch (functionDetails.getSource().getSubscriptionPosition()) {
            case LATEST:
                v1alpha1SinkSpec.setSubscriptionPosition(V1alpha1SinkSpec.SubscriptionPositionEnum.LATEST);
                break;
            case EARLIEST:
                v1alpha1SinkSpec.setSubscriptionPosition(V1alpha1SinkSpec.SubscriptionPositionEnum.EARLIEST);
                break;
            default:
        }
        v1alpha1SinkSpec.setRetainOrdering(functionDetails.getRetainOrdering());

        v1alpha1SinkSpec.setCleanupSubscription(functionDetails.getSource().getCleanupSubscription());
        v1alpha1SinkSpec.setAutoAck(functionDetails.getAutoAck());

        if (functionDetails.getSource().getTimeoutMs() != 0) {
            v1alpha1SinkSpec.setTimeout((int) functionDetails.getSource().getTimeoutMs());
        }

        if (functionDetails.hasRetryDetails()) {
            v1alpha1SinkSpec.setMaxMessageRetry(functionDetails.getRetryDetails().getMaxMessageRetries());
            if (Strings.isNotEmpty(functionDetails.getRetryDetails().getDeadLetterTopic())) {
                v1alpha1SinkSpec.setDeadLetterTopic(functionDetails.getRetryDetails().getDeadLetterTopic());
            }
        }

        v1alpha1SinkSpec.setReplicas(functionDetails.getParallelism());
        if (customRuntimeOptions.getMaxReplicas() > functionDetails.getParallelism()) {
            v1alpha1SinkSpec.setMaxReplicas(customRuntimeOptions.getMaxReplicas());
        }

        Resources resources =
                CommonUtil.mergeWithDefault(worker.getMeshWorkerServiceCustomConfig().getDefaultResources(),
                        sinkConfig.getResources());

        double cpu = resources.getCpu();
        long ramRequest = resources.getRam();

        Map<String, Object> limits = new HashMap<>();
        Map<String, Object> requests = new HashMap<>();

        long padding = Math.round(ramRequest * (10.0 / 100.0)); // percentMemoryPadding is 0.1
        long ramWithPadding = ramRequest + padding;

        limits.put(CPU_KEY, Quantity.fromString(Double.toString(cpu)).toSuffixedString());
        limits.put(MEMORY_KEY, Quantity.fromString(Long.toString(ramWithPadding)).toSuffixedString());

        requests.put(CPU_KEY, Quantity.fromString(Double.toString(cpu)).toSuffixedString());
        requests.put(MEMORY_KEY, Quantity.fromString(Long.toString(ramRequest)).toSuffixedString());

        V1alpha1SinkSpecPodResources v1alpha1SinkSpecResources = new V1alpha1SinkSpecPodResources();
        v1alpha1SinkSpecResources.setLimits(limits);
        v1alpha1SinkSpecResources.setRequests(requests);
        v1alpha1SinkSpec.setResources(v1alpha1SinkSpecResources);

        V1alpha1SinkSpecPulsar v1alpha1SinkSpecPulsar = new V1alpha1SinkSpecPulsar();
        v1alpha1SinkSpecPulsar.setPulsarConfig(CommonUtil.getPulsarClusterConfigMapName(clusterName));
        // TODO: auth
        // v1alpha1SinkSpecPulsar.setAuthConfig(CommonUtil.getPulsarClusterAuthConfigMapName(clusterName));
        v1alpha1SinkSpec.setPulsar(v1alpha1SinkSpecPulsar);

        v1alpha1SinkSpec.setClusterName(clusterName);

        if (sinkConfig.getConfigs() != null && !sinkConfig.getConfigs().isEmpty()) {
            v1alpha1SinkSpec.setSinkConfig(sinkConfig.getConfigs());
        } else {
            v1alpha1SinkSpec.setSinkConfig(new HashMap<>());
        }

        V1alpha1SinkSpecPod specPod = new V1alpha1SinkSpecPod();
        if (worker.getMeshWorkerServiceCustomConfig().isAllowUserDefinedServiceAccountName() &&
                StringUtils.isNotEmpty(serviceAccountName)) {
            specPod.setServiceAccountName(serviceAccountName);
        }
        if (!CommonUtil.isMapEmpty(customLabelClaims)) {
            specPod.setLabels(customLabelClaims);
        }
        Map<String, String> customAnnotations = new HashMap<>();
        CommonUtil.mergeMap(customConfig.getAnnotations(), customAnnotations);
        CommonUtil.mergeMap(customConfig.getSinkAnnotations(), customAnnotations);
        if (!CommonUtil.isMapEmpty(customAnnotations)) {
            specPod.setAnnotations(customAnnotations);
        }
        List<V1alpha1SinkSpecPodEnv> env = new ArrayList<>();
        Map<String, String> finalEnv = new HashMap<>();
        CommonUtil.mergeMap(customConfig.getEnv(), finalEnv);
        CommonUtil.mergeMap(customConfig.getSinkEnv(), finalEnv);
        CommonUtil.mergeMap(customRuntimeOptions.getEnv(), finalEnv);
        if (!CommonUtil.isMapEmpty(finalEnv)) {
            finalEnv.entrySet().forEach(entry -> {
                V1alpha1SinkSpecPodEnv podEnv = new V1alpha1SinkSpecPodEnv();
                podEnv.setName(entry.getKey());
                podEnv.setValue(entry.getValue());
                env.add(podEnv);
            });
        }
        specPod.setEnv(env);

        try {
            specPod.setVolumes(customConfig.asV1alpha1SinkSpecPodVolumesList());
            v1alpha1SinkSpec.setVolumeMounts(customConfig.asV1alpha1SinkSpecPodVolumeMountsList());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Error while converting volumes/volumeMounts resources from custom config", e);
        }

        if (customRuntimeOptions.getVpaSpec() != null) {
            V1alpha1SinkSpecPodVpa vpaSpec = generateVPASpecFromCustomRuntimeOptions(customRuntimeOptions.getVpaSpec());
            specPod.setVpa(vpaSpec);
        }

        v1alpha1SinkSpec.setPod(specPod);

        if (sinkConfig.getSecrets() != null && !sinkConfig.getSecrets().isEmpty()) {
            Map<String, Object> secrets = sinkConfig.getSecrets();
            Map<String, V1alpha1SinkSpecSecretsMap> secretsMapMap = new HashMap<>();
            for (Map.Entry<String, Object> entry : secrets.entrySet()) {
                Map<String, String> kv = (Map<String, String>) entry.getValue();
                if (kv == null || !kv.containsKey(PATH_KEY) || !kv.containsKey(KEY_KEY)) {
                    log.error("Invalid secrets from sink config for sink {}, "
                                    + "the secret must contains path and key {}: {}",
                            sinkName, entry.getKey(), entry.getValue());
                    continue;
                }
                V1alpha1SinkSpecSecretsMap v1alpha1SinkSpecSecretsMap = new V1alpha1SinkSpecSecretsMap();
                v1alpha1SinkSpecSecretsMap.path(kv.get(PATH_KEY));
                v1alpha1SinkSpecSecretsMap.key(kv.get(KEY_KEY));
                secretsMapMap.put(entry.getKey(), v1alpha1SinkSpecSecretsMap);
            }
            if (!secretsMapMap.isEmpty()) {
                v1alpha1SinkSpec.setSecretsMap(secretsMapMap);
            }
        }

        if (StringUtils.isEmpty(v1alpha1SinkSpec.getClassName())) {
            boolean isPkgUrlProvided = StringUtils.isNotEmpty(sinkPkgUrl);
            if (isPkgUrlProvided) {
                try {
                    String className = getClassNameFromFile(worker, sinkPkgUrl,
                            Function.FunctionDetails.ComponentType.SINK);
                    if (StringUtils.isNotEmpty(className)) {
                        v1alpha1SinkSpec.setClassName(className);
                    }
                } catch (Exception e) {
                    log.error("Invalid register sink request {}", sinkName, e);
                    throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
                }
            } else {
                log.error("Invalid register sink request {}: not provide className", sinkName);
                throw new RestException(Response.Status.BAD_REQUEST, "no className provided");
            }
        }

        v1alpha1Sink.setSpec(v1alpha1SinkSpec);

        return v1alpha1Sink;
    }

    public static SinkConfig createSinkConfigFromV1alpha1Sink(
            String tenant, String namespace, String sinkName, V1alpha1Sink v1alpha1Sink, MeshWorkerService worker) {
        SinkConfig sinkConfig = new SinkConfig();

        sinkConfig.setName(sinkName);
        sinkConfig.setNamespace(namespace);
        sinkConfig.setTenant(tenant);

        MeshWorkerServiceCustomConfig customConfig = worker.getMeshWorkerServiceCustomConfig();

        V1alpha1SinkSpec v1alpha1SinkSpec = v1alpha1Sink.getSpec();

        if (v1alpha1SinkSpec == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Sink CRD without Spec defined.");
        }
        sinkConfig.setParallelism(v1alpha1SinkSpec.getReplicas());
        if (v1alpha1SinkSpec.getProcessingGuarantee() != null) {
            sinkConfig.setProcessingGuarantees(
                    CommonUtil.convertProcessingGuarantee(v1alpha1SinkSpec.getProcessingGuarantee().getValue()));
        }

        CustomRuntimeOptions customRuntimeOptions = new CustomRuntimeOptions();

        Map<String, ConsumerConfig> consumerConfigMap = new HashMap<>();
        V1alpha1SinkSpecInput v1alpha1SinkSpecInput = v1alpha1SinkSpec.getInput();
        if (v1alpha1SinkSpecInput == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "SinkSpec CRD without Input defined.");
        }
        if (Strings.isNotEmpty(v1alpha1SinkSpecInput.getTypeClassName())) {
            customRuntimeOptions.setInputTypeClassName(v1alpha1SinkSpecInput.getTypeClassName());
        }
        if (Strings.isNotEmpty(v1alpha1SinkSpec.getClusterName())) {
            customRuntimeOptions.setClusterName(v1alpha1SinkSpec.getClusterName());
        }
        if (v1alpha1SinkSpec.getMaxReplicas() != null && v1alpha1SinkSpec.getMaxReplicas() > 0) {
            customRuntimeOptions.setMaxReplicas(v1alpha1SinkSpec.getMaxReplicas());
        }

        CommonUtil.setManaged(customRuntimeOptions, v1alpha1Sink.getMetadata());

        if (v1alpha1SinkSpec.getPod() != null) {
            if (Strings.isNotEmpty(v1alpha1SinkSpec.getPod().getServiceAccountName())) {
                customRuntimeOptions.setServiceAccountName(v1alpha1SinkSpec.getPod().getServiceAccountName());
            }
            if (v1alpha1SinkSpec.getPod().getEnv() != null) {
                Map<String, String> customConfigEnv = new HashMap<>();
                CommonUtil.mergeMap(customConfig.getEnv(), customConfigEnv);
                CommonUtil.mergeMap(customConfig.getSinkEnv(), customConfigEnv);
                Map<String, String> runtimeEnv =
                        CommonUtil.getRuntimeEnv(customConfigEnv, v1alpha1SinkSpec.getPod().getEnv().stream().collect(
                                Collectors.toMap(V1alpha1SinkSpecPodEnv::getName, V1alpha1SinkSpecPodEnv::getValue))
                        );
                customRuntimeOptions.setEnv(runtimeEnv);
            }
            V1alpha1SinkSpecPodVpa vpaSpec = v1alpha1SinkSpec.getPod().getVpa();
            if (vpaSpec != null) {
                VPASpec configVPASpec = generateVPASpecFromSinkConfig(vpaSpec);
                customRuntimeOptions.setVpaSpec(configVPASpec);
            }
        }

        if (v1alpha1SinkSpecInput.getTopics() != null) {
            for (String topic : v1alpha1SinkSpecInput.getTopics()) {
                ConsumerConfig consumerConfig = new ConsumerConfig();
                consumerConfig.setRegexPattern(false);
                consumerConfigMap.put(topic, consumerConfig);
            }
        }

        if (Strings.isNotEmpty(v1alpha1SinkSpecInput.getTopicPattern())) {
            String patternTopic = v1alpha1SinkSpecInput.getTopicPattern();
            ConsumerConfig consumerConfig = consumerConfigMap.getOrDefault(patternTopic, new ConsumerConfig());
            consumerConfig.setRegexPattern(true);
            consumerConfigMap.put(patternTopic, consumerConfig);
        }

        if (v1alpha1SinkSpecInput.getCustomSerdeSources() != null) {
            for (Map.Entry<String, String> source : v1alpha1SinkSpecInput.getCustomSerdeSources().entrySet()) {
                String topic = source.getKey();
                String serdeClassName = source.getValue();
                ConsumerConfig consumerConfig = consumerConfigMap.getOrDefault(topic, new ConsumerConfig());
                consumerConfig.setRegexPattern(false);
                consumerConfig.setSerdeClassName(serdeClassName);
                consumerConfigMap.put(topic, consumerConfig);
            }
        }

        if (v1alpha1SinkSpecInput.getSourceSpecs() != null) {
            for (Map.Entry<String, V1alpha1SinkSpecInputSourceSpecs> source : v1alpha1SinkSpecInput.getSourceSpecs()
                    .entrySet()) {
                String topic = source.getKey();
                V1alpha1SinkSpecInputSourceSpecs sourceSpecs = source.getValue();
                ConsumerConfig consumerConfig = consumerConfigMap.getOrDefault(topic, new ConsumerConfig());
                if (sourceSpecs.getIsRegexPattern() != null) {
                    consumerConfig.setRegexPattern(sourceSpecs.getIsRegexPattern());
                }
                consumerConfig.setSchemaType(sourceSpecs.getSchemaType());
                consumerConfig.setSerdeClassName(sourceSpecs.getSerdeClassname());
                consumerConfig.setReceiverQueueSize(sourceSpecs.getReceiverQueueSize());
                consumerConfig.setSchemaProperties(sourceSpecs.getSchemaProperties());
                consumerConfig.setConsumerProperties(sourceSpecs.getConsumerProperties());
                if (sourceSpecs.getCryptoConfig() != null) {
                    // TODO: convert CryptoConfig to function config
                }
                consumerConfigMap.put(topic, consumerConfig);
            }
        }

        sinkConfig.setInputSpecs(consumerConfigMap);
        sinkConfig.setInputs(consumerConfigMap.keySet());

        if (Strings.isNotEmpty(v1alpha1SinkSpec.getSubscriptionName())) {
            sinkConfig.setSourceSubscriptionName(v1alpha1SinkSpec.getSubscriptionName());
        }
        if (v1alpha1SinkSpec.getSubscriptionPosition() != null) {
            switch (v1alpha1SinkSpec.getSubscriptionPosition()) {
                case LATEST:
                    sinkConfig.setSourceSubscriptionPosition(SubscriptionInitialPosition.Latest);
                    break;
                case EARLIEST:
                    sinkConfig.setSourceSubscriptionPosition(SubscriptionInitialPosition.Earliest);
                    break;
                default:
            }
        }
        if (v1alpha1SinkSpec.getRetainOrdering() != null) {
            sinkConfig.setRetainOrdering(v1alpha1SinkSpec.getRetainOrdering());
        }
        if (v1alpha1SinkSpec.getCleanupSubscription() != null) {
            sinkConfig.setCleanupSubscription(v1alpha1SinkSpec.getCleanupSubscription());
        }
        if (v1alpha1SinkSpec.getAutoAck() != null) {
            sinkConfig.setAutoAck(v1alpha1SinkSpec.getAutoAck());
        } else {
            sinkConfig.setAutoAck(true);
        }
        if (v1alpha1SinkSpec.getTimeout() != null && v1alpha1SinkSpec.getTimeout() != 0) {
            sinkConfig.setTimeoutMs(v1alpha1SinkSpec.getTimeout().longValue());
        }
        if (v1alpha1SinkSpec.getMaxMessageRetry() != null) {
            sinkConfig.setMaxMessageRetries(v1alpha1SinkSpec.getMaxMessageRetry());
            if (Strings.isNotEmpty(v1alpha1SinkSpec.getDeadLetterTopic())) {
                sinkConfig.setDeadLetterTopic(v1alpha1SinkSpec.getDeadLetterTopic());
            }
        }
        sinkConfig.setClassName(v1alpha1SinkSpec.getClassName());
        if (v1alpha1SinkSpec.getSinkConfig() != null) {
            sinkConfig.setConfigs((Map<String, Object>) v1alpha1SinkSpec.getSinkConfig());
        }
        if (v1alpha1SinkSpec.getSecretsMap() != null && !v1alpha1SinkSpec.getSecretsMap().isEmpty()) {
            Map<String, V1alpha1SinkSpecSecretsMap> secretsMapMap = v1alpha1SinkSpec.getSecretsMap();
            Map<String, Object> secrets = new HashMap<>(secretsMapMap);
            sinkConfig.setSecrets(secrets);
        }

        Resources resources = new Resources();
        Map<String, Object> sinkResources = v1alpha1SinkSpec.getResources().getRequests();
        Quantity cpuQuantity = Quantity.fromString((String) sinkResources.get(CPU_KEY));
        Quantity memoryQuantity = Quantity.fromString((String) sinkResources.get(MEMORY_KEY));
        resources.setCpu(cpuQuantity.getNumber().doubleValue());
        resources.setRam(memoryQuantity.getNumber().longValue());
        sinkConfig.setResources(resources);

        String customRuntimeOptionsJSON = new Gson().toJson(customRuntimeOptions, CustomRuntimeOptions.class);
        sinkConfig.setCustomRuntimeOptions(customRuntimeOptionsJSON);

        if (Strings.isNotEmpty(v1alpha1SinkSpec.getRuntimeFlags())) {
            sinkConfig.setRuntimeFlags(v1alpha1SinkSpec.getRuntimeFlags());
        }

        if (v1alpha1SinkSpec.getJava() != null && Strings.isNotEmpty(v1alpha1SinkSpec.getJava().getJar())) {
            sinkConfig.setArchive(v1alpha1SinkSpec.getJava().getJar());
        }

        return sinkConfig;
    }

    private static V1alpha1SinkSpecInputCryptoConfig convertFromCryptoSpec(Function.CryptoSpec cryptoSpec) {
        // TODO: convertFromCryptoSpec
        return null;
    }

    public static void convertFunctionStatusToInstanceStatusData(InstanceCommunication.FunctionStatus functionStatus,
                                                                 SinkInstanceStatusData sinkInstanceStatusData) {
        if (functionStatus == null || sinkInstanceStatusData == null) {
            return;
        }
        sinkInstanceStatusData.setRunning(functionStatus.getRunning());
        sinkInstanceStatusData.setError(functionStatus.getFailureException());
        sinkInstanceStatusData.setNumReadFromPulsar(functionStatus.getNumReceived());
        sinkInstanceStatusData.setNumSystemExceptions(functionStatus.getNumSystemExceptions()
                + functionStatus.getNumUserExceptions() + functionStatus.getNumSourceExceptions());
        List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry :
                functionStatus.getLatestUserExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            systemExceptionInformationList.add(exceptionInformation);
        }
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry :
                functionStatus.getLatestSystemExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            systemExceptionInformationList.add(exceptionInformation);
        }
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry :
                functionStatus.getLatestSourceExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            systemExceptionInformationList.add(exceptionInformation);
        }
        sinkInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);
        sinkInstanceStatusData.setNumSinkExceptions(functionStatus.getNumSinkExceptions());
        List<ExceptionInformation> sinkExceptionInformationList = new LinkedList<>();
        for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry :
                functionStatus.getLatestSinkExceptionsList()) {
            ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
            sinkExceptionInformationList.add(exceptionInformation);
        }
        sinkInstanceStatusData.setLatestSinkExceptions(sinkExceptionInformationList);

        sinkInstanceStatusData.setNumWrittenToSink(functionStatus.getNumSuccessfullyProcessed());
        sinkInstanceStatusData.setLastReceivedTime(functionStatus.getLastInvocationTime());
    }

    public static void mergeTrustedConfigs(final SinkConfig sinkConfig, V1alpha1Sink v1alpha1Sink) {
        CustomRuntimeOptions customRuntimeOptions =
                CommonUtil.getCustomRuntimeOptions(sinkConfig.getCustomRuntimeOptions());
        if (v1alpha1Sink.getSpec().getPod() == null) {
            v1alpha1Sink.getSpec().setPod(new V1alpha1SinkSpecPod());
        }
        if (StringUtils.isNotEmpty(customRuntimeOptions.getRunnerImage())) {
            v1alpha1Sink.getSpec().setImage(customRuntimeOptions.getRunnerImage());
        }
        if (StringUtils.isNotEmpty(customRuntimeOptions.getServiceAccountName())) {
            v1alpha1Sink.getSpec().getPod().setServiceAccountName(customRuntimeOptions.getServiceAccountName());
        }
        if (!customRuntimeOptions.isManaged()) {
            Map<String, String> currentAnnotations = v1alpha1Sink.getMetadata().getAnnotations();
            if (currentAnnotations == null) {
                currentAnnotations = new HashMap<>();
            }
            currentAnnotations.put(ANNOTATION_MANAGED, "false");
            v1alpha1Sink.getMetadata().setAnnotations(currentAnnotations);
        }
    }

    private static V1alpha1SinkSpecPodVpa generateVPASpecFromCustomRuntimeOptions(
            VPASpec configVPASpec) {
        V1alpha1SinkSpecPodVpa vpaSpec = new V1alpha1SinkSpecPodVpa();
        VPAUpdatePolicy updatePolicy = configVPASpec.getUpdatePolicy();
        if (updatePolicy != null) {
            V1alpha1SinkSpecPodVpaUpdatePolicy functionUpdatePolicy = new V1alpha1SinkSpecPodVpaUpdatePolicy();
            if (updatePolicy.getMinReplicas() > 0) {
                functionUpdatePolicy.setMinReplicas(updatePolicy.getMinReplicas());
            }
            switch (updatePolicy.getUpdateMode().toLowerCase()) {
                case "off":
                    functionUpdatePolicy.setUpdateMode(OFF);
                    break;
                case "initial":
                    functionUpdatePolicy.setUpdateMode(INITIAL);
                    break;
                case "recreate":
                    functionUpdatePolicy.setUpdateMode(RECREATE);
                    break;
                case "auto":
                    functionUpdatePolicy.setUpdateMode(AUTO);
                    break;
                default:
                    break;
            }
            vpaSpec.setUpdatePolicy(functionUpdatePolicy);
        }

        List<VPAContainerPolicy> containerPolicies = configVPASpec.getResourcePolicy();
        if (containerPolicies != null) {
            V1alpha1SinkSpecPodVpaResourcePolicy functionResourcePolicy =
                    new V1alpha1SinkSpecPodVpaResourcePolicy();
            List<V1alpha1SinkSpecPodVpaResourcePolicyContainerPolicies> functionContainerPolicies =
                    new ArrayList<>();
            for (VPAContainerPolicy containerPolicy : containerPolicies) {
                V1alpha1SinkSpecPodVpaResourcePolicyContainerPolicies functionContainerPolicy =
                        new V1alpha1SinkSpecPodVpaResourcePolicyContainerPolicies();
                functionContainerPolicy.containerName(containerPolicy.getContainerName())
                        .maxAllowed(containerPolicy.getMaxAllowed()).minAllowed(containerPolicy.getMinAllowed());
                switch (containerPolicy.getMode().toLowerCase()) {
                    case "off":
                        functionContainerPolicy.mode(
                                V1alpha1SinkSpecPodVpaResourcePolicyContainerPolicies.ModeEnum.OFF);
                        break;
                    case "auto":
                        functionContainerPolicy.mode(
                                V1alpha1SinkSpecPodVpaResourcePolicyContainerPolicies.ModeEnum.AUTO);
                        break;
                    default:
                        break;
                }
                if (containerPolicy.getControlledResources().size() > 0) {
                    functionContainerPolicy.controlledResources(containerPolicy.getControlledResources());
                }
                switch (containerPolicy.getControlledValues().toLowerCase()) {
                    case "requestsonly":
                        functionContainerPolicy.setControlledValues(
                                V1alpha1SinkSpecPodVpaResourcePolicyContainerPolicies.ControlledValuesEnum.REQUESTSONLY);
                        break;
                    case "requestsandlimits":
                        functionContainerPolicy.setControlledValues(
                                V1alpha1SinkSpecPodVpaResourcePolicyContainerPolicies.ControlledValuesEnum.REQUESTSANDLIMITS);
                        break;
                    default:
                        break;
                }
                functionContainerPolicies.add(functionContainerPolicy);
            }
            functionResourcePolicy.containerPolicies(functionContainerPolicies);
            vpaSpec.resourcePolicy(functionResourcePolicy);
        }
        return vpaSpec;
    }

    private static VPASpec generateVPASpecFromSinkConfig(V1alpha1SinkSpecPodVpa vpaSpec) {
        VPASpec configVPASpec = new VPASpec();
        V1alpha1SinkSpecPodVpaUpdatePolicy updatePolicy = vpaSpec.getUpdatePolicy();
        if (updatePolicy != null) {
            VPAUpdatePolicy vpaUpdatePolicy = new VPAUpdatePolicy();
            if (updatePolicy.getUpdateMode() != null) {
                vpaUpdatePolicy.setUpdateMode(updatePolicy.getUpdateMode().getValue());
            }
            if (updatePolicy.getMinReplicas() != null && updatePolicy.getMinReplicas() > 0) {
                vpaUpdatePolicy.setMinReplicas(updatePolicy.getMinReplicas());
            }
            configVPASpec.setUpdatePolicy(vpaUpdatePolicy);
        }
        V1alpha1SinkSpecPodVpaResourcePolicy resourcePolicy = vpaSpec.getResourcePolicy();
        if (resourcePolicy != null && resourcePolicy.getContainerPolicies() != null) {
            List<VPAContainerPolicy> vpaContainerPolicies = new ArrayList<>();
            for (V1alpha1SinkSpecPodVpaResourcePolicyContainerPolicies containerPolicy :
                    resourcePolicy.getContainerPolicies()) {
                if (containerPolicy == null) {
                    continue;
                }
                VPAContainerPolicy vpaContainerPolicy = new VPAContainerPolicy();
                vpaContainerPolicy.setContainerName(containerPolicy.getContainerName());
                if (containerPolicy.getMode() != null) {
                    vpaContainerPolicy.setMode(containerPolicy.getMode().getValue());
                }
                vpaContainerPolicy.setMinAllowed(containerPolicy.getMinAllowed());
                vpaContainerPolicy.setMaxAllowed(containerPolicy.getMaxAllowed());
                if (containerPolicy.getControlledValues() != null) {
                    vpaContainerPolicy.setControlledValues(containerPolicy.getControlledValues().getValue());
                }
                vpaContainerPolicy.setControlledResources(containerPolicy.getControlledResources());
                vpaContainerPolicies.add(vpaContainerPolicy);
            }
            configVPASpec.setResourcePolicy(vpaContainerPolicies);
        }
        return configVPASpec;
    }
}
