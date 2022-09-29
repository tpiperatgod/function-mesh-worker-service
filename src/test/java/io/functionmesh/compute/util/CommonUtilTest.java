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

import static io.functionmesh.compute.auth.AuthHandler.CLIENT_AUTHENTICATION_PARAMETERS_CLAIM;
import static io.functionmesh.compute.auth.AuthHandler.CLIENT_AUTHENTICATION_PLUGIN_CLAIM;
import static io.functionmesh.compute.util.CommonUtil.DEFAULT_FUNCTION_DOWNLOAD_DIRECTORY;
import static io.functionmesh.compute.util.CommonUtil.DEFAULT_FUNCTION_EXECUTABLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.auth.AuthHandlerOauth;
import io.functionmesh.compute.auth.AuthResults;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Secret;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.junit.Assert;
import org.junit.Test;

public class CommonUtilTest {
    @Test
    public void testBuildDownloadPath() {
        // no data provided
        String downloadDirectory = "";
        String archivePath = "";
        String path = CommonUtil.buildDownloadPath(downloadDirectory, archivePath);
        assertEquals(DEFAULT_FUNCTION_DOWNLOAD_DIRECTORY + DEFAULT_FUNCTION_EXECUTABLE, path);

        // with downloadDirectory
        downloadDirectory = "/download";
        archivePath = "";
        path = CommonUtil.buildDownloadPath(downloadDirectory, archivePath);
        assertEquals(downloadDirectory + "/" + DEFAULT_FUNCTION_EXECUTABLE, path);

        // with downloadDirectory & archivePath
        downloadDirectory = "/download";
        archivePath = "a.jar";
        path = CommonUtil.buildDownloadPath(downloadDirectory, archivePath);
        assertEquals(downloadDirectory + "/" + archivePath, path);

        // with downloadDirectory & full archivePath
        downloadDirectory = "/download";
        archivePath = "/Users/sample/a.jar";
        path = CommonUtil.buildDownloadPath(downloadDirectory, archivePath);
        assertEquals(downloadDirectory + "/a.jar", path);

        // with full archivePath
        downloadDirectory = null;
        archivePath = "/Users/sample/a.jar";
        path = CommonUtil.buildDownloadPath(downloadDirectory, archivePath);
        assertEquals(DEFAULT_FUNCTION_DOWNLOAD_DIRECTORY + "a.jar", path);
    }

    @Test
    public void testDoAuth() throws ApiException {
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

        AuthResults results = CommonUtil.doAuth(meshWorkerService, "admin", null, "Function");
        AuthResults expectedResults = new AuthHandlerOauth().handle(meshWorkerService, "admin", null, "Function");
        assertEquals(expectedResults.getFunctionAuthConfig(), results.getFunctionAuthConfig());

        when(workerConfig.getBrokerClientAuthenticationPlugin()).thenReturn("Un-Supported-Auth-Provider");
        try {
            CommonUtil.doAuth(meshWorkerService, "admin", null, "Function");
            Assert.fail("Expecting a RestException is thrown");
        } catch (RestException e) {
            assertEquals(e.getMessage(), "No handler for given auth plugin: Un-Supported-Auth-Provider");
        }
    }
}
