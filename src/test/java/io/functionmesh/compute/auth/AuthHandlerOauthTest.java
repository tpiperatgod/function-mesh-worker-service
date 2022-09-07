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
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodConfigMapItems;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodSecret;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumeMounts;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumes;
import io.functionmesh.compute.util.CommonUtil;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Secret;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.junit.Test;

public class AuthHandlerOauthTest {

    @Test
    public void testHandle() throws ApiException {
        MeshWorkerService meshWorkerService = mock(MeshWorkerService.class);
        WorkerConfig workerConfig = mock(WorkerConfig.class);
        when(workerConfig.getBrokerClientAuthenticationPlugin()).thenReturn(CommonUtil.OAUTH_PLUGIN_NAME);
        CoreV1Api coreV1Api = mock(CoreV1Api.class);
        Map<String, byte[]> secretData = new HashMap<>();
        secretData.put(CLIENT_AUTHENTICATION_PLUGIN_CLAIM, CommonUtil.OAUTH_PLUGIN_NAME.getBytes());
        String oauth2Parameters = "{\"audience\":\"test-audience\",\"issuerUrl\":\"https://test.com/\""
                + ",\"privateKey\":\"file:///mnt/secrets/auth.json\",\"type\":\"client_credentials\"}";
        secretData.put(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM, oauth2Parameters.getBytes());
        V1Secret v1Secret = new V1Secret()
                .metadata(new V1ObjectMeta().name("admin-secret"))
                .data(secretData);
        when(coreV1Api.readNamespacedSecret("admin", "default", null, null, null)).thenReturn(v1Secret);

        when(meshWorkerService.getCoreV1Api()).thenReturn(coreV1Api);
        when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);
        when(meshWorkerService.getJobNamespace()).thenReturn("default");

        AuthResults results = new AuthHandlerOauth().handle(meshWorkerService, "admin", null, "Function");

        String oauth2ExtendedParameters = "{\"audience\":\"test-audience\",\"issuerUrl\":\"https://test.com/\""
                + ",\"privateKey\":\"file:///mnt/secrets/auth.json\",\"type\":\"client_credentials\""
                + ",\"issuer_url\":\"https://test.com/\",\"private_key\":\"/mnt/secrets/auth.json\"}";
        secretData.put(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM, oauth2ExtendedParameters.getBytes());
        AuthResults expected = new AuthResults().setAuthSecretData(secretData);
        List<V1alpha1FunctionSpecPodConfigMapItems> items =
                new ArrayList<V1alpha1FunctionSpecPodConfigMapItems>() {{
                    add(new V1alpha1FunctionSpecPodConfigMapItems()
                            .key("auth.json")
                            .path("auth.json"));
                }};
        List<V1alpha1FunctionSpecPodVolumes> volumes =
                new ArrayList<V1alpha1FunctionSpecPodVolumes>() {{
                    add(new V1alpha1FunctionSpecPodVolumes().name("oauth-secret")
                            .secret(new V1alpha1FunctionSpecPodSecret().secretName("admin")
                                    .defaultMode(420).items(items)));
                }};
        List<V1alpha1FunctionSpecPodVolumeMounts>
                vms =
                new ArrayList<V1alpha1FunctionSpecPodVolumeMounts>() {{
                    add(new V1alpha1FunctionSpecPodVolumeMounts().name("oauth-secret")
                            .mountPath("/mnt/secrets/auth.json")
                            .subPath("auth.json"));
                }};
        expected.setFunctionVolumeMounts(vms).setFunctionVolumes(volumes);

        assertArrayEquals(expected.getAuthSecretData().get(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM), results.getAuthSecretData().get(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM));
        assertArrayEquals(expected.getAuthSecretData().get(CLIENT_AUTHENTICATION_PLUGIN_CLAIM), results.getAuthSecretData().get(CLIENT_AUTHENTICATION_PLUGIN_CLAIM));
        assertEquals(expected.getFunctionVolumes(), results.getFunctionVolumes());
        assertEquals(expected.getFunctionVolumeMounts(), results.getFunctionVolumeMounts());
    }
}