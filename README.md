## Mesh Worker Service

This is a proxy that is used to forward requests to the k8s.

### Run with pulsar broker

#### Prerequisite

* Pulsar 2.8.0 or later

#### Update configuration file

Add the following configuration to the `functions_worker.yml` configuration file:

```$xslt
functionsWorkerServiceNarPackage: /YOUR-NAR-PATH/mesh-worker-service-1.0-SNAPSHOT.nar
```
Replace the `YOUR-NAR-PATH` variable with your real path.

### Run in standalone mode

#### Run in docker

1. Add the following configuration to the `functions_worker.yml` configuration file:

```$xslt
functionsWorkerServiceNarPackage: /YOUR-NAR-PATH/mesh-worker-service-${VERSION}-SNAPSHOT.nar
```
Replace the `YOUR-NAR-PATH` variable with your real path.

2. Update the ZooKeeper and Broker URLs in the `functions_worker.yml` configuration file.

3. Start the container. 

  You must provide the `KUBE_CONFIG` environment variable and mount related volumes to the container to manage resources in the target Kubernetes cluster. 

```
docker run -td --name worker -p 6750:6750 -v /YOUR-CONF/functions_worker.yml:/pulsar/conf/functions_worker.yml -v /YOUR-NAR-PATH/:/ANY-PATH -e KUBE_CONFIG=/kube/config -v /YOUR-KUBE-CONFIG:/kube/config apachepulsar/pulsar:2.9.2 bin/pulsar functions-worker
```

To combine the worker service and pulsar cluster's admin endpoints, you should start a Pulsar proxy. For details, see [configure-proxies](https://pulsar.apache.org/docs/next/functions-worker-run-separately#configure-proxies-for-standalone-function-workers).


#### Run in k8s

For details, see a sample [YAML file](./examples/standalone.yaml). You can update the configuration based on your requirement and apply it.

(Optional) If you want to use another Kubernetes cluster to manage Function mesh resources, you must add the `KUBE_CONFIG` environment variable and related volume to the Pod.

```yaml
...
env:
  - name: KUBE_CONFIG
    value: /kube/config
...
volumeMounts:
  - name: outside-k8s-config
    mountPath: /kube/config
    subPath: config
...
volumes:
  - name: outside-k8s-config
    secret:
      secretName: k8s-config # create your own secret for k8s config
      items:
      - key: config
        path: config
...
```


### Configuring the development environment


#### Start service

Importing this project into the idea development environment.

Configuration environment variable `KUBECONFIG` for idea development environment.

#### Test Interface

#### getFunctionStatus
```shell script
./bin/pulsar-admin --admin-url http://localhost:6750 functions status --tenant public --namespace default --name functionmesh-sample-ex1
```

or 

```shell script
curl http://localhost:6750/admin/v3/functions/test/default/functionmesh-sample-ex1/status
```

#### registerFunction
 ```shell script
./bin/pulsar-admin --admin-url http://localhost:6750 functions create \
   --jar target/my-jar-with-dependencies.jar \
   --classname org.example.functions.WordCountFunction \
   --tenant public \
   --namespace default \
   --name word-count \
   --inputs persistent://public/default/sentences \
   --output persistent://public/default/count \
   --cpu 0.1 \
   --ram 1 \
   --custom-runtime-options \
   "{"clusterName": "test-pulsar", "inputTypeClassName": "java.lang.String", "outputTypeClassName": "java.lang.String"}"
```

##### sink connector
 ```shell script
./bin/pulsar-admin --admin-url http://localhost:6750 sinks create \
   --sink-type data-generator \
   --classname org.apache.pulsar.io.datagenerator.DataGeneratorPrintSink \
   --tenant public \
   --namespace default \
   --name data-generator-sink \
   --inputs persistent://public/default/random-data-topic \
   --auto-ack true \
   --custom-runtime-options \
   "{"clusterName": "test-pulsar", "inputTypeClassName": "org.apache.pulsar.io.datagenerator.Person"}"
```

##### source connector
 ```shell script
./bin/pulsar-admin --admin-url http://localhost:6750 sources create \
   --source-type data-generator \
   --tenant public \
   --namespace default \
   --name data-generator-source \
   --destination-topic-name persistent://public/default/random-data-topic \
   --source-config "{"sleepBetweenMessages": "5000"}"
   --custom-runtime-options \
   "{"clusterName": "test-pulsar", "outputTypeClassName": "org.apache.pulsar.io.datagenerator.Person"}"
```

#### updateFunction
 ```shell script
./bin/pulsar-admin --admin-url http://localhost:6750 functions update \
   --jar target/my-jar-with-dependencies.jar \
   --classname org.example.functions.WordCountFunction \
   --tenant public \
   --namespace default \
   --name word-count \
   --inputs persistent://public/default/sentences \
   --output persistent://public/default/count \
   --input-specs "{"source": {"serdeClassName": "java.lang.String"}}" \
   --output-serde-classname java.lang.String \
   --cpu 0.2 \
   --ram 1 \
   --user-config "{"clusterName": "test-pulsar", "typeClassName": "java.lang.String"}"
```

#### getFunctionInfo
 ```shell script
./bin/pulsar-admin --admin-url http://localhost:6750 functions get \
   --tenant public \
   --namespace default \
   --name word-count
```

#### deregisterFunction
 ```shell script
./bin/pulsar-admin --admin-url http://localhost:6750 functions delete \
   --tenant public \
   --namespace default \
   --name word-count
```

## More tools

### Automatic generation java [crd model](https://github.com/kubernetes-client/java/blob/master/docs/generate-model-from-third-party-resources.md)

crd yaml [file](https://github.com/streamnative/function-mesh/tree/master/config/crd/bases)

Note: add the field `preserveUnknownFields: false` to spec for avoid this [issue](https://github.com/kubernetes-client/java/issues/1254)

```shell script
CRD_FILE=compute.functionmesh.io_sources.yaml # Target CRD file

GEN_DIR=/tmp/functions-mesh/crd
mkdir -p $GEN_DIR
cp ../config/crd/bases/* $GEN_DIR
cd $GEN_DIR

LOCAL_MANIFEST_FILE=$GEN_DIR/$CRD_FILE

# yq site: https://mikefarah.gitbook.io/yq/
yq e ".spec.preserveUnknownFields = false" -i $CRD_FILE 

docker rm -f kind-control-plane
docker run \
  --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd)":"$(pwd)" \
  -ti \
  --network host \
  ghcr.io/kubernetes-client/java/crd-model-gen:v1.0.4 \
  /generate.sh \
  -u $LOCAL_MANIFEST_FILE \
  -n io.functionmesh.compute \
  -p io.functionmesh.compute \
  -o "$(pwd)"

open $GEN_DIR
```

### Generated CRD's Java Model

```shell script
./scripts/generate-crd.sh
```

Then add license for crd model file
```shell script
mvn license:format
```
