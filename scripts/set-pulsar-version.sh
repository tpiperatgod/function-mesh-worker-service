#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

version=${1#v}
if [[ "x$version" == "x" ]]; then
  echo "You need to provide the version of the Pulsar"
  exit 1
fi

OLDVERSION=$(mvn -q -Dexec.executable=echo -Dexec.args='${pulsar.version}' --non-recursive exec:exec)
echo "OLDVERSION=${OLDVERSION}"
mvn versions:set-property -Dproperty=pulsar.version -DnewVersion=${version}

# bump integration tests
sed -i.bak -E "s/${OLDVERSION}/${version}/g" integration-tests/docker/connectors.yaml
sed -i.bak -E "s/${OLDVERSION}/${version}/g" .ci/clusters/values_mesh_worker_service.yaml
sed -i.bak -E "s/${OLDVERSION}/${version}/g" .ci/clusters/values_mesh_worker_service_with_oauth.yaml
sed -i.bak -E "s/${OLDVERSION}/${version}/g" .ci/clusters/values_mesh_worker_service_with_insecure_auth.yaml
