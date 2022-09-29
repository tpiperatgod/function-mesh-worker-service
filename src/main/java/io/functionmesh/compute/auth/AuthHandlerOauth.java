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
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPulsarAuthConfig;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPulsarAuthConfigOauth2Config;
import io.functionmesh.compute.models.OAuth2Parameters;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPulsarAuthConfig;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPulsarAuthConfigOauth2Config;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPulsarAuthConfig;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPulsarAuthConfigOauth2Config;
import io.functionmesh.compute.util.CommonUtil;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1Secret;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

public class AuthHandlerOauth implements AuthHandler {
    // the provided secret should use this to store the oauth2 parameters
    public static final String KEY_NAME = "auth.json";

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
                ObjectMapper mapper = new ObjectMapper();
                OAuth2Parameters oauth2Parameters = mapper.readValue(params, OAuth2Parameters.class);

                AuthResults results = new AuthResults();

                switch (component.toLowerCase()) {
                    case CommonUtil.COMPONENT_FUNCTION:
                        V1alpha1FunctionSpecPulsarAuthConfigOauth2Config oauth2 =
                                new V1alpha1FunctionSpecPulsarAuthConfigOauth2Config()
                                        .audience(oauth2Parameters.getAudience())
                                        .issuerUrl(oauth2Parameters.getIssuerUrl())
                                        .keySecretName(secretName)
                                        .keySecretKey(KEY_NAME);
                        if (StringUtils.isNotEmpty(oauth2Parameters.getScope())) {
                            oauth2.setScope(oauth2Parameters.getScope());
                        }
                        results.setFunctionAuthConfig(new V1alpha1FunctionSpecPulsarAuthConfig().oauth2Config(oauth2));
                        break;
                    case CommonUtil.COMPONENT_SINK:
                        V1alpha1SinkSpecPulsarAuthConfigOauth2Config sinkOAuth2 =
                                new V1alpha1SinkSpecPulsarAuthConfigOauth2Config()
                                        .audience(oauth2Parameters.getAudience())
                                        .issuerUrl(oauth2Parameters.getIssuerUrl())
                                        .keySecretName(secretName)
                                        .keySecretKey(KEY_NAME);
                        if (StringUtils.isNotEmpty(oauth2Parameters.getScope())) {
                            sinkOAuth2.setScope(oauth2Parameters.getScope());
                        }
                        results.setSinkAuthConfig(new V1alpha1SinkSpecPulsarAuthConfig().oauth2Config(sinkOAuth2));
                        break;
                    case CommonUtil.COMPONENT_SOURCE:
                        V1alpha1SourceSpecPulsarAuthConfigOauth2Config sourceOAuth2 =
                                new V1alpha1SourceSpecPulsarAuthConfigOauth2Config()
                                        .audience(oauth2Parameters.getAudience())
                                        .issuerUrl(oauth2Parameters.getIssuerUrl())
                                        .keySecretName(secretName)
                                        .keySecretKey(KEY_NAME);
                        if (StringUtils.isNotEmpty(oauth2Parameters.getScope())) {
                            sourceOAuth2.setScope(oauth2Parameters.getScope());
                        }
                        results.setSourceAuthConfig(new V1alpha1SourceSpecPulsarAuthConfig().oauth2Config(sourceOAuth2));
                        break;
                    default:
                        break;
                }
                return results;
            } catch (IOException | NullPointerException e) {
                throw new RuntimeException(
                        "Failed to get secret data " + secretName + " in namespace " + workerService.getJobNamespace(),
                        e);
            } catch (ApiException e) {
                throw new RuntimeException(
                        "Failed to get secret " + secretName + " in namespace " + workerService.getJobNamespace(), e);
            }
        }
        throw new RuntimeException("Client role is empty");
    }

    @Override
    public void cleanUp(MeshWorkerService workerService, String clientRole, AuthenticationDataSource authDataHttps,
                        String component, String clusterName, String tenant, String namespace, String componentName) {
        // Do nothing for oauth2 handler
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
