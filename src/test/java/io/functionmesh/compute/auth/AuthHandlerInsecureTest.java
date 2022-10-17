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

import static io.functionmesh.compute.auth.AuthHandler.CLIENT_AUTHENTICATION_PARAMETERS_CLAIM;
import static io.functionmesh.compute.auth.AuthHandler.CLIENT_AUTHENTICATION_PLUGIN_CLAIM;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPulsarAuthConfig;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPulsarAuthConfigOauth2Config;
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import io.functionmesh.compute.util.CommonUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.junit.Test;

public class AuthHandlerInsecureTest {

    @Test
    public void testHandle() {
        MeshWorkerService meshWorkerService = mock(MeshWorkerService.class);
        WorkerConfig workerConfig = mock(WorkerConfig.class);
        String oauth2Parameters = "{\"audience\":\"test-audience\",\"issuerUrl\":\"https://test.com/\""
                + ",\"privateKey\":\"file:///mnt/secrets/auth.json\",\"type\":\"client_credentials\"}";
        when(workerConfig.getBrokerClientAuthenticationPlugin()).thenReturn("fake");
        when(workerConfig.getBrokerClientAuthenticationParameters()).thenReturn(oauth2Parameters);
        when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);

        AuthResults results = new AuthHandlerInsecure().handle(meshWorkerService, "admin", null, "Function");

        Map<String, byte[]> secretData = new HashMap<>();
        secretData.put(CLIENT_AUTHENTICATION_PLUGIN_CLAIM, "fake".getBytes());
        secretData.put(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM, oauth2Parameters.getBytes());
        AuthResults expected = new AuthResults().setAuthSecretData(secretData);

        assertArrayEquals(expected.getAuthSecretData().get(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM),
                results.getAuthSecretData().get(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM));
        assertArrayEquals(expected.getAuthSecretData().get(CLIENT_AUTHENTICATION_PLUGIN_CLAIM),
                results.getAuthSecretData().get(CLIENT_AUTHENTICATION_PLUGIN_CLAIM));

        // test oauth2 provider
        WorkerConfig workerConfig2 = mock(WorkerConfig.class);
        when(workerConfig2.getBrokerClientAuthenticationPlugin()).thenReturn(CommonUtil.OAUTH_PLUGIN_NAME);
        when(workerConfig2.getBrokerClientAuthenticationParameters()).thenReturn(
                "{\"audience\":\"test-audience\",\"issuerUrl\":\"https://test.com/\""
                        + ",\"privateKey\":\"file:///mnt/secrets/auth.json\",\"type\":\"client_credentials\"}");
        when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig2);
        MeshWorkerServiceCustomConfig customConfig = mock(MeshWorkerServiceCustomConfig.class);
        when(customConfig.getOauth2SecretName()).thenReturn("test-secret");
        when(meshWorkerService.getMeshWorkerServiceCustomConfig()).thenReturn(customConfig);

        AuthResults results2 = new AuthHandlerInsecure().handle(meshWorkerService, "admin", null, "Function");

        AuthResults expected2 = new AuthResults();
        V1alpha1FunctionSpecPulsarAuthConfigOauth2Config oauth2 =
                new V1alpha1FunctionSpecPulsarAuthConfigOauth2Config()
                        .audience("test-audience")
                        .issuerUrl("https://test.com/")
                        .keySecretName("test-secret")
                        .keySecretKey("auth.json");
        expected2.setFunctionAuthConfig(new V1alpha1FunctionSpecPulsarAuthConfig().oauth2Config(oauth2));
        assertEquals(expected2.getFunctionAuthConfig(), results2.getFunctionAuthConfig());
    }
}