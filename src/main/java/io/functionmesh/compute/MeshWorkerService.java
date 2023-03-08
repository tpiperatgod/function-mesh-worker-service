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
package io.functionmesh.compute;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import io.functionmesh.compute.rest.api.FunctionsImpl;
import io.functionmesh.compute.rest.api.SinksImpl;
import io.functionmesh.compute.rest.api.SourcesImpl;
import io.functionmesh.compute.util.KubernetesUtils;
import io.functionmesh.compute.worker.MeshConnectorsManager;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import java.io.FileReader;
import java.io.IOException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.functions.runtime.RuntimeUtils;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactoryConfig;
import org.apache.pulsar.functions.worker.ErrorNotifier;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.service.api.Functions;
import org.apache.pulsar.functions.worker.service.api.FunctionsV2;
import org.apache.pulsar.functions.worker.service.api.Sinks;
import org.apache.pulsar.functions.worker.service.api.Sources;
import org.apache.pulsar.functions.worker.service.api.Workers;

/**
 * Function mesh proxy implement.
 */
@Slf4j
@Getter
public class MeshWorkerService implements WorkerService {

    private static final String KUBE_CONFIG_ENV = "KUBE_CONFIG";
    private static final String API_GROUP = "compute.functionmesh.io";
    private static final String API_VERSION = "v1alpha1";
    private static final String API_PLURAL = "functions";
    final PulsarWorkerService.PulsarClientCreator clientCreator;
    private volatile boolean isInitialized = false;
    private WorkerConfig workerConfig;
    private boolean authenticationEnabled;
    private Functions<MeshWorkerService> functions;
    private FunctionsV2<MeshWorkerService> functionsV2;
    private Sinks<MeshWorkerService> sinks;
    private Sources<MeshWorkerService> sources;
    private CoreV1Api coreV1Api;
    private AppsV1Api appsV1Api;
    private CustomObjectsApi customObjectsApi;
    private ApiClient apiClient;
    private PulsarAdmin brokerAdmin;
    @Deprecated
    private KubernetesRuntimeFactoryConfig factoryConfig;
    private MeshWorkerServiceCustomConfig meshWorkerServiceCustomConfig;
    private AuthenticationService authenticationService;
    private AuthorizationService authorizationService;
    private MeshConnectorsManager connectorsManager;
    private ServiceConfiguration brokerConfig;
    private StorageAdminClient stateStoreAdminClient;

    public MeshWorkerService() {

        this.clientCreator = new PulsarWorkerService.PulsarClientCreator() {
            @Override
            public PulsarAdmin newPulsarAdmin(String pulsarServiceUrl, WorkerConfig workerConfig) {
                // using isBrokerClientAuthenticationEnabled instead of isAuthenticationEnabled in function-worker
                if (workerConfig.isBrokerClientAuthenticationEnabled()) {
                    return WorkerUtils.getPulsarAdminClient(
                            pulsarServiceUrl,
                            workerConfig.getBrokerClientAuthenticationPlugin(),
                            workerConfig.getBrokerClientAuthenticationParameters(),
                            workerConfig.getBrokerClientTrustCertsFilePath(),
                            workerConfig.isTlsAllowInsecureConnection(),
                            workerConfig.isTlsEnableHostnameVerification(),
                            workerConfig);
                } else {
                    return WorkerUtils.getPulsarAdminClient(
                            pulsarServiceUrl,
                            "",
                            "",
                            "",
                            null,
                            null,
                            workerConfig);
                }
            }

            @Override
            public PulsarClient newPulsarClient(String pulsarServiceUrl, WorkerConfig workerConfig) {
                // using isBrokerClientAuthenticationEnabled instead of isAuthenticationEnabled in function-worker
                if (workerConfig.isBrokerClientAuthenticationEnabled()) {
                    return WorkerUtils.getPulsarClient(
                            pulsarServiceUrl,
                            workerConfig.getBrokerClientAuthenticationPlugin(),
                            workerConfig.getBrokerClientAuthenticationParameters(),
                            workerConfig.isUseTls(),
                            workerConfig.getBrokerClientTrustCertsFilePath(),
                            workerConfig.isTlsAllowInsecureConnection(),
                            workerConfig.isTlsEnableHostnameVerification());
                } else {
                    return WorkerUtils.getPulsarClient(
                            pulsarServiceUrl,
                            "",
                            "",
                            null,
                            "",
                            null,
                            null);
                }
            }
        };
    }

    @Override
    public void initAsStandalone(WorkerConfig workerConfig) throws Exception {
        this.init(workerConfig);
    }

    @Override
    public void initInBroker(ServiceConfiguration brokerConfig,
                             WorkerConfig workerConfig, PulsarResources pulsarResources,
                             InternalConfigurationData internalConf) throws Exception {
        this.brokerConfig = brokerConfig;
        this.init(workerConfig);
    }

    public void init(WorkerConfig workerConfig) throws Exception {
        this.workerConfig = workerConfig;
        this.initKubernetesClient();
        this.authenticationEnabled = this.workerConfig.isAuthenticationEnabled();
        this.functions = new FunctionsImpl(() -> MeshWorkerService.this);
        this.sources = new SourcesImpl(() -> MeshWorkerService.this);
        this.sinks = new SinksImpl(() -> MeshWorkerService.this);
        this.factoryConfig = RuntimeUtils.getRuntimeFunctionConfig(
                workerConfig.getFunctionRuntimeFactoryConfigs(), KubernetesRuntimeFactoryConfig.class);
        this.meshWorkerServiceCustomConfig = RuntimeUtils.getRuntimeFunctionConfig(
                workerConfig.getFunctionsWorkerServiceCustomConfigs(), MeshWorkerServiceCustomConfig.class);
    }

    public void validateExternalServices() throws Exception {
        if (this.brokerConfig != null
                && this.meshWorkerServiceCustomConfig.isUploadEnabled()
                && !this.brokerConfig.isEnablePackagesManagement()) {
            throw new RuntimeException("uploadEnabled requires enablePackagesManagement"
                    + " to be enabled from broker's configuration");
        }
    }

    private void initKubernetesClient() throws IOException {
        try {
            if (StringUtils.isNotEmpty(System.getenv(KUBE_CONFIG_ENV))) {
                String kubeConfigPath = System.getenv(KUBE_CONFIG_ENV);
                log.info("Initialization kubernetes client from config file: {}", kubeConfigPath);
                // loading the out-of-cluster config, a kubeconfig from file-system
                apiClient =
                        ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();
            } else {
                apiClient = Config.defaultClient();
            }
            coreV1Api = new CoreV1Api(apiClient);
            appsV1Api = new AppsV1Api(apiClient);
            customObjectsApi = new CustomObjectsApi(apiClient);
            // check whether FunctionMesh is installed
            customObjectsApi.listNamespacedCustomObject(API_GROUP, API_VERSION, getJobNamespace(), API_PLURAL, "false", null, null, null, null, null, null, null);
        } catch (java.io.IOException e) {
            log.error("Initialization kubernetes client failed", e);
            throw e;
        } catch (ApiException e) {
            if (e.getCode() == HTTP_NOT_FOUND) {
                throw new RuntimeException("FunctionMesh is required, please install it first");
            }
        }
    }

    public void start(AuthenticationService authenticationService,
                      AuthorizationService authorizationService,
                      ErrorNotifier errorNotifier) throws Exception {
        this.authenticationService = authenticationService;
        this.authorizationService = authorizationService;
        this.brokerAdmin = clientCreator.newPulsarAdmin(workerConfig.getPulsarWebServiceUrl(), workerConfig);
        this.connectorsManager = new MeshConnectorsManager(meshWorkerServiceCustomConfig);
        this.validateExternalServices();
        this.initStateStorageService();
        this.isInitialized = true;
        log.info("/** Started mesh worker service **/");
    }

    public void stop() {
        if (null != getBrokerAdmin()) {
            getBrokerAdmin().close();
        }
        if (null != stateStoreAdminClient) {
            stateStoreAdminClient.close();
        }
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    public Workers<? extends WorkerService> getWorkers() {
        // No need to implement since in this mode, there's no function worker
        // Consider ban this api in the production environment
        return null;
    }

    public void generateFunctionsStats(SimpleTextOutputStream out) {
        // to do https://github.com/streamnative/function-mesh/issues/56
    }

    public String getJobNamespace() {
        return KubernetesUtils.getNamespace(getMeshWorkerServiceCustomConfig(), this.getFactoryConfig());
    }

    private void initStateStorageService() {
        if (workerConfig.getStateStorageServiceUrl() != null) {
            StorageClientSettings clientSettings = StorageClientSettings.newBuilder()
                    .serviceUri(workerConfig.getStateStorageServiceUrl())
                    .build();
            this.stateStoreAdminClient = StorageClientBuilder.newBuilder()
                    .withSettings(clientSettings)
                    .buildAdmin();
        }
    }
}