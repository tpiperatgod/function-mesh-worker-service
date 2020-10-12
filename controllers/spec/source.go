package spec

import (
	"github.com/gogo/protobuf/jsonpb"
	"github.com/streamnative/mesh-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func MakeSourceService(source *v1alpha1.Source) *corev1.Service {
	labels := makeSourceLabels(source)
	objectMeta := MakeSourceObjectMeta(source)
	return MakeService(objectMeta, labels)
}

func MakeSourceStatefulSet(source *v1alpha1.Source) *appsv1.StatefulSet {
	objectMeta := MakeSourceObjectMeta(source)
	return MakeStatefulSet(objectMeta, &source.Spec.Parallelism, MakeSourceContainer(source),
		makeSourceLabels(source), source.Spec.Pulsar.PulsarConfig)
}

func MakeSourceObjectMeta(source *v1alpha1.Source) *metav1.ObjectMeta {
	return &metav1.ObjectMeta{
		Name:      source.Name,
		Namespace: source.Namespace,
		OwnerReferences: []metav1.OwnerReference{
			*metav1.NewControllerRef(source, source.GroupVersionKind()),
		},
	}
}

func MakeSourceContainer(source *v1alpha1.Source) *corev1.Container {
	return &corev1.Container{
		// TODO new container to pull user code image and upload jars into bookkeeper
		Name:    "source-instance",
		Image:   "apachepulsar/pulsar-all",
		Command: makeSourceCommand(source),
		Ports:   []corev1.ContainerPort{GRPCPort, MetricsPort},
		Env: []corev1.EnvVar{{
			Name:      "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}},
		}},
		// TODO calculate resource precisely
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0.2"),
				corev1.ResourceMemory: resource.MustParse("2G")},
			Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0.2"),
				corev1.ResourceMemory: resource.MustParse("2G")},
		},
		ImagePullPolicy: corev1.PullIfNotPresent,
		VolumeMounts: []corev1.VolumeMount{{
			Name:      PULSAR_CONFIG,
			ReadOnly:  true,
			MountPath: PathPulsarClusterConfigs,
		}},
		EnvFrom: []corev1.EnvFromSource{{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: v1.LocalObjectReference{Name: source.Spec.Pulsar.PulsarConfig},
			},
		}},
	}
}

func makeSourceLabels(source *v1alpha1.Source) map[string]string {
	labels := make(map[string]string)
	labels["component"] = "source"
	labels["name"] = source.Spec.Name
	labels["namespace"] = source.Namespace

	return labels
}

func makeSourceCommand(source *v1alpha1.Source) []string {
	return MakeCommand(source.Spec.Java.JarLocation, source.Spec.Java.Jar,
		source.Spec.Name, source.Spec.Pulsar.PulsarConfig, generateSourceDetailsInJson(source))
}

func generateSourceDetailsInJson(source *v1alpha1.Source) string {
	sourceDetails := convertSourceDetails(source)
	marshaler := &jsonpb.Marshaler{}
	json, error := marshaler.MarshalToString(sourceDetails)
	if error != nil {
		// TODO
		panic(error)
	}
	return json
}