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

import static io.functionmesh.compute.auth.AuthHandlerOauth.KEY_NAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPulsarAuthConfig;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPulsarAuthConfigOauth2Config;
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import io.functionmesh.compute.util.CommonUtil;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretList;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.junit.Test;

public class AuthHandlerOauthTest {

    @Test
    public void testHandle() throws ApiException {
        String annotationKey = "cloud.streamnative.io/service-account.email";
        String secretName = "admin-secret";
        MeshWorkerService meshWorkerService = mock(MeshWorkerService.class);
        WorkerConfig workerConfig = mock(WorkerConfig.class);
        String oauth2Parameters = "{\"audience\":\"test-audience\",\"issuerUrl\":\"https://test.com/\""
                + ",\"privateKey\":\"file:///mnt/secrets/auth.json\",\"type\":\"client_credentials\"}";
        when(workerConfig.getBrokerClientAuthenticationPlugin()).thenReturn(CommonUtil.OAUTH_PLUGIN_NAME);
        when(workerConfig.getBrokerClientAuthenticationParameters()).thenReturn(oauth2Parameters);

        MeshWorkerServiceCustomConfig customConfig = mock(MeshWorkerServiceCustomConfig.class);
        when(customConfig.getOauth2SecretAnnotationKey()).thenReturn(annotationKey);
        when(meshWorkerService.getMeshWorkerServiceCustomConfig()).thenReturn(customConfig);

        CoreV1Api coreV1Api = mock(CoreV1Api.class);
        V1Secret v1Secret = new V1Secret()
                .metadata(new V1ObjectMeta().name(secretName).annotations(
                        java.util.Collections.singletonMap(annotationKey, "admin")));
        V1SecretList secrets = new V1SecretList().items(java.util.Collections.singletonList(v1Secret));

        when(meshWorkerService.getCoreV1Api()).thenReturn(coreV1Api);
        when(coreV1Api.listNamespacedSecret("default", null, null, null, null, null, null, null, null, null, null)).thenReturn(secrets);
        when(meshWorkerService.getCoreV1Api()).thenReturn(coreV1Api);

        when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);
        when(meshWorkerService.getJobNamespace()).thenReturn("default");

        AuthResults results = new AuthHandlerOauth().handle(meshWorkerService, "admin", null, "Function");

        AuthResults expected = new AuthResults();
        V1alpha1FunctionSpecPulsarAuthConfigOauth2Config oauth2 =
                new V1alpha1FunctionSpecPulsarAuthConfigOauth2Config()
                        .audience("test-audience")
                        .issuerUrl("https://test.com/")
                        .keySecretName(secretName)
                        .keySecretKey(KEY_NAME);
        expected.setFunctionAuthConfig(new V1alpha1FunctionSpecPulsarAuthConfig().oauth2Config(oauth2));

        assertEquals(expected.getFunctionAuthConfig(), results.getFunctionAuthConfig());
    }
}