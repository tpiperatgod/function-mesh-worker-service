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

import io.functionmesh.compute.MeshWorkerService;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;

public class AuthHandlerJWTToken implements AuthHandler {

    private static final String HEADER_NAME = "Authorization";
    private static final String TOKEN_TYPE  = "Bearer";

    @Override
    public AuthResults handle(MeshWorkerService workerService, String clientRole, AuthenticationDataHttps authDataHttps,
                              String component) {
        AuthResults results = new AuthResults();
        Map<String, byte[]> valueMap = new HashMap<>();
        String token = authDataHttps.getHttpHeader(HEADER_NAME).replaceAll(TOKEN_TYPE, "").trim();
        if (StringUtils.isEmpty(token)) {
            throw new RuntimeException("Failed to get token from Authorization header");
        }
        valueMap.put(CLIENT_AUTHENTICATION_PLUGIN_CLAIM,
                workerService.getWorkerConfig().getBrokerClientAuthenticationPlugin().getBytes());
        valueMap.put(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM, token.getBytes());
        results.setAuthSecretData(valueMap);
        return results;
    }
}
