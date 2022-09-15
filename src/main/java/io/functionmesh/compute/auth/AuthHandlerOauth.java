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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodConfigMapItems;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodSecret;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumeMounts;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPodVolumes;
import io.functionmesh.compute.models.OAuth2Parameters;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodConfigMapItems;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodSecret;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVolumeMounts;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPodVolumes;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodConfigMapItems;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodSecret;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodVolumeMounts;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPodVolumes;
import io.functionmesh.compute.util.CommonUtil;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1Secret;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

public class AuthHandlerOauth implements AuthHandler {
    private static final String SECRET_VOLUME_NAME = "oauth-secret";
    private static final int DEFAULT_MODE = 0644;
    // the provided secret should use this to store the oauth2 parameters
    private static final String KEY_NAME = "auth.json";

    @Override
    public AuthResults handle(MeshWorkerService workerService, String clientRole,
                              AuthenticationDataSource authDataHttps, String component) {
        if (StringUtils.isNotEmpty(clientRole)) {
            String secretName = getSecretNameFromClientRole(clientRole);
            try {
                V1Secret secret = workerService.getCoreV1Api()
                        .readNamespacedSecret(secretName, workerService.getJobNamespace(), null, null, null);

                // create auth secret for function-mesh
                byte[] params = secret.getData().get(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM);
                Map<String, byte[]> valueMap = new HashMap<>();
                valueMap.put(CLIENT_AUTHENTICATION_PLUGIN_CLAIM, CommonUtil.OAUTH_PLUGIN_NAME.getBytes());
                ObjectMapper mapper = new ObjectMapper();
                OAuth2Parameters oauth2Parameters = mapper.readValue(params, OAuth2Parameters.class);
                byte[] extendedParams = mapper.writeValueAsBytes(oauth2Parameters);
                valueMap.put(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM, extendedParams);

                // the private key should be an absolute path of a file with a `file://` prefix
                // generate volume and volume mount for oauth secret
                String privateKey = oauth2Parameters.getPrivateKey().replace("file://", "");
                Path path = Paths.get(privateKey);
                AuthResults results = new AuthResults().setAuthSecretData(valueMap);

                switch (component.toLowerCase()) {
                    case CommonUtil.COMPONENT_FUNCTION:
                        List<V1alpha1FunctionSpecPodConfigMapItems> items =
                                new ArrayList<V1alpha1FunctionSpecPodConfigMapItems>() {{
                                    add(new V1alpha1FunctionSpecPodConfigMapItems()
                                            .key(KEY_NAME)
                                            .path(path.getFileName().toString()));
                                }};
                        List<V1alpha1FunctionSpecPodVolumes> volumes =
                                new ArrayList<V1alpha1FunctionSpecPodVolumes>() {{
                                    add(new V1alpha1FunctionSpecPodVolumes().name(SECRET_VOLUME_NAME)
                                            .secret(new V1alpha1FunctionSpecPodSecret().secretName(secretName)
                                                    .defaultMode(DEFAULT_MODE).items(items)));
                                }};
                        List<V1alpha1FunctionSpecPodVolumeMounts>
                                vms =
                                new ArrayList<V1alpha1FunctionSpecPodVolumeMounts>() {{
                                    add(new V1alpha1FunctionSpecPodVolumeMounts().name(SECRET_VOLUME_NAME)
                                            .mountPath(privateKey)
                                            .subPath(path.getFileName().toString()));
                                }};
                        results.setFunctionVolumes(volumes).setFunctionVolumeMounts(vms);
                        break;
                    case CommonUtil.COMPONENT_SINK:
                        List<V1alpha1SinkSpecPodConfigMapItems> sinkItems =
                                new ArrayList<V1alpha1SinkSpecPodConfigMapItems>() {{
                                    add(new V1alpha1SinkSpecPodConfigMapItems()
                                            .key(KEY_NAME)
                                            .path(path.getFileName().toString()));
                                }};
                        List<V1alpha1SinkSpecPodVolumes> sinkVolumes =
                                new ArrayList<V1alpha1SinkSpecPodVolumes>() {{
                                    add(new V1alpha1SinkSpecPodVolumes().name(SECRET_VOLUME_NAME)
                                            .secret(new V1alpha1SinkSpecPodSecret().secretName(secretName)
                                                    .defaultMode(DEFAULT_MODE).items(sinkItems)));
                                }};
                        List<V1alpha1SinkSpecPodVolumeMounts>
                                sinkVms =
                                new ArrayList<V1alpha1SinkSpecPodVolumeMounts>() {{
                                    add(new V1alpha1SinkSpecPodVolumeMounts().name(SECRET_VOLUME_NAME)
                                            .mountPath(privateKey)
                                            .subPath(path.getFileName().toString()));
                                }};
                        results.setSinkVolumes(sinkVolumes).setSinkVolumeMounts(sinkVms);
                        break;
                    case CommonUtil.COMPONENT_SOURCE:
                        List<V1alpha1SourceSpecPodConfigMapItems> sourceItems =
                                new ArrayList<V1alpha1SourceSpecPodConfigMapItems>() {{
                                    add(new V1alpha1SourceSpecPodConfigMapItems()
                                            .key(KEY_NAME)
                                            .path(path.getFileName().toString()));
                                }};
                        List<V1alpha1SourceSpecPodVolumes> sourceVolumes =
                                new ArrayList<V1alpha1SourceSpecPodVolumes>() {{
                                    add(new V1alpha1SourceSpecPodVolumes().name(SECRET_VOLUME_NAME)
                                            .secret(new V1alpha1SourceSpecPodSecret().secretName(secretName)
                                                    .defaultMode(DEFAULT_MODE).items(sourceItems)));
                                }};
                        List<V1alpha1SourceSpecPodVolumeMounts>
                                sourceVms =
                                new ArrayList<V1alpha1SourceSpecPodVolumeMounts>() {{
                                    add(new V1alpha1SourceSpecPodVolumeMounts().name(SECRET_VOLUME_NAME)
                                            .mountPath(privateKey)
                                            .subPath(path.getFileName().toString()));
                                }};
                        results.setSourceVolumes(sourceVolumes).setSourceVolumeMounts(sourceVms);
                        break;
                    default:
                        break;
                }
                return results;
            } catch (IOException e) {
                throw new RuntimeException("Failed to generate secret data", e);
            } catch (ApiException e) {
                throw new RuntimeException(
                        "Failed to get secret " + secretName + " in namespace " + workerService.getJobNamespace(), e);
            } catch (NullPointerException e) {
                throw new RuntimeException(
                        "Failed to get secret data " + secretName + " in namespace " + workerService.getJobNamespace(),
                        e);
            }
        }
        throw new RuntimeException("Client role is empty");
    }

    /*
     * Get secret name from the client role.
     */
    private String getSecretNameFromClientRole(String clientRole) {
        if (clientRole == null) {
            return "";
        }

        // for Google Cloud
        String secret = clientRole.split("@")[0];

        // for Azure Active Directory
        secret = secret.replaceAll("^api://", "");

        return secret;
    }
}
