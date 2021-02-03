package oci

import (
	"fmt"
	"strings"
)

// PrototypeKryptonSandboxTypeAnnotation specifies whether a krypton sandbox will be created.
const PrototypeKryptonSandboxTypeAnnotation = "io.kubernetes.cri.kryptonsandbox"

// PrototypeKryptonSandboxType defines the valid types of the
// `PrototypeKryptonSandboxTypeAnnotation` annotation.
type PrototypeKryptonSandboxType bool

// IsKryptonSandboxMode searches `a` for `key` and if found verifies that the
// value is `true` or `false` in any case. If `key` is not found returns `def`.
func IsKryptonSandboxMode(a map[string]string) (bool, error) {
	if v, ok := a[PrototypeKryptonSandboxTypeAnnotation]; ok {
		switch strings.ToLower(v) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return false, fmt.Errorf("invalid '%s': '%s'", PrototypeKryptonSandboxTypeAnnotation, v)
		}
	}

	// No value was set, use the default (false).
	return false, nil
}

// KubernetesContainerTypeAnnotation is the annotation used by CRI to define the `ContainerType`.
const KubernetesContainerTypeAnnotation = "io.kubernetes.cri.container-type"

// KubernetesSandboxIDAnnotation is the annotation used by CRI to define the
// KubernetesContainerTypeAnnotation == "sandbox"` ID.
const KubernetesSandboxIDAnnotation = "io.kubernetes.cri.sandbox-id"

// KubernetesContainerType defines the valid types of the
// `KubernetesContainerTypeAnnotation` annotation.
type KubernetesContainerType string

const (
	// KubernetesContainerTypeNone is only valid when
	// `KubernetesContainerTypeAnnotation` is not set.
	KubernetesContainerTypeNone KubernetesContainerType = ""
	// KubernetesContainerTypeContainer is valid when
	// `KubernetesContainerTypeAnnotation == "container"`.
	KubernetesContainerTypeContainer KubernetesContainerType = "container"
	// KubernetesContainerTypeSandbox is valid when
	// `KubernetesContainerTypeAnnotation == "sandbox"`.
	KubernetesContainerTypeSandbox KubernetesContainerType = "sandbox"
)

// GetSandboxTypeAndID parses `specAnnotations` searching for the
// `KubernetesContainerTypeAnnotation` and `KubernetesSandboxIDAnnotation`
// annotations and if found validates the set before returning.
func GetSandboxTypeAndID(specAnnotations map[string]string) (KubernetesContainerType, string, error) {
	var ct KubernetesContainerType
	if t, ok := specAnnotations[KubernetesContainerTypeAnnotation]; ok {
		switch t {
		case string(KubernetesContainerTypeContainer):
			ct = KubernetesContainerTypeContainer
		case string(KubernetesContainerTypeSandbox):
			ct = KubernetesContainerTypeSandbox
		default:
			return KubernetesContainerTypeNone, "", fmt.Errorf("invalid '%s': '%s'", KubernetesContainerTypeAnnotation, t)
		}
	}

	id := specAnnotations[KubernetesSandboxIDAnnotation]

	switch ct {
	case KubernetesContainerTypeContainer, KubernetesContainerTypeSandbox:
		if id == "" {
			return KubernetesContainerTypeNone, "", fmt.Errorf("cannot specify '%s' without '%s'", KubernetesContainerTypeAnnotation, KubernetesSandboxIDAnnotation)
		}
	default:
		if id != "" {
			return KubernetesContainerTypeNone, "", fmt.Errorf("cannot specify '%s' without '%s'", KubernetesSandboxIDAnnotation, KubernetesContainerTypeAnnotation)
		}
	}
	return ct, id, nil
}
