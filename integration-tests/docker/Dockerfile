#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

ARG PULSAR_IMAGE_TAG

FROM streamnative/sn-platform:${PULSAR_IMAGE_TAG} as lib

FROM streamnative/pulsar-all:${PULSAR_IMAGE_TAG}
ARG PULSAR_IMAGE_TAG
COPY ./target/mesh-worker-service*.nar /pulsar/mesh-worker-service.nar
COPY ./integration-tests/docker/connectors.yaml /pulsar/conf/connectors.yaml
COPY --from=lib /pulsar/lib/pulsar-broker-auth-oauth2-${PULSAR_IMAGE_TAG}.jar /pulsar/lib/pulsar-broker-auth-oauth2.jar
RUN mkdir -p /pulsar-nar
