#!/usr/bin/env bash
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

set -e

BINDIR=`dirname "$0"`
PULSAR_HOME=`cd ${BINDIR}/..;pwd`
VALUES_FILE=$1
TLS=${TLS:-"false"}
SYMMETRIC=${SYMMETRIC:-"false"}
FUNCTION=${FUNCTION:-"false"}
WITH_AUTH=${WITH_AUTH:-"false"}

source ${PULSAR_HOME}/.ci/helm.sh

# create cluster
ci::create_cluster

extra_opts=""
if [[ "x${SYMMETRIC}" == "xtrue" ]]; then
    extra_opts="-s"
fi

# install storage provisioner
#ci::install_storage_provisioner

# install metrics server
#ci::install_metrics_server

# install cert manager chart
ci::install_cert_manager_charts

# install function-mesh chart
ci::install_function_mesh_charts

# install pulsar chart
ci::install_pulsar_charts "$VALUES_FILE"

# test producer
if [ "x${WITH_AUTH}" = "xtrue" ]; then
  ci::test_pulsar_producer_with_auth
else
  ci::test_pulsar_producer
fi

