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
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import io.functionmesh.compute.MeshWorkerService;
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
        when(workerConfig.getBrokerClientAuthenticationPlugin()).thenReturn(CommonUtil.OAUTH_PLUGIN_NAME);
        when(workerConfig.getBrokerClientAuthenticationParameters()).thenReturn(
                "{\"audience\":\"test-audience\",\"issuerUrl\":\"https://test.com/\""
                        + ",\"privateKey\":\"file:///mnt/secrets/auth.json\",\"type\":\"client_credentials\"}");
        when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);

        AuthResults results = new AuthHandlerInsecure().handle(meshWorkerService, "admin", null, "Function");

        String oauth2ExtendedParameters = "{\"audience\":\"test-audience\",\"issuerUrl\":\"https://test.com/\""
                + ",\"privateKey\":\"file:///mnt/secrets/auth.json\",\"type\":\"client_credentials\""
                + ",\"issuer_url\":\"https://test.com/\",\"private_key\":\"/mnt/secrets/auth.json\"}";
        Map<String, byte[]> secretData = new HashMap<>();
        secretData.put(CLIENT_AUTHENTICATION_PLUGIN_CLAIM, CommonUtil.OAUTH_PLUGIN_NAME.getBytes());
        secretData.put(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM, oauth2ExtendedParameters.getBytes());
        AuthResults expected = new AuthResults().setAuthSecretData(secretData);

        assertArrayEquals(expected.getAuthSecretData().get(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM),
                results.getAuthSecretData().get(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM));
        assertArrayEquals(expected.getAuthSecretData().get(CLIENT_AUTHENTICATION_PLUGIN_CLAIM),
                results.getAuthSecretData().get(CLIENT_AUTHENTICATION_PLUGIN_CLAIM));
    }
}