//go:build integration
// +build integration

/*
Integration test for the external connection migration path.
Requires a running Kubernetes cluster (e.g., minikube) with:
- The KVCache CRD installed
- A secret "valkey-credentials" in the default namespace

Run with:
  go test -tags=integration -run TestMigrationPath ./pkg/controller/kvcache/backends/ -v
*/

package backends

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/constants"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func setupRealClient(t *testing.T) client.Client {
	t.Helper()

	// Use default kubeconfig (minikube sets this up)
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)

	cfg, err := kubeConfig.ClientConfig()
	require.NoError(t, err, "failed to load kubeconfig — is minikube running?")

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, orchestrationv1alpha1.AddToScheme(scheme))

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err, "failed to create Kubernetes client")

	return c
}

// TestMigrationPath_CleanupInClusterRedis validates the migration path:
// 1. Create in-cluster Redis Pod + Service (simulating existing state)
// 2. Run reconcileRedisService with externalConnection configured
// 3. Confirm the in-cluster resources are deleted
func TestMigrationPath_CleanupInClusterRedis(t *testing.T) {
	ctx := context.Background()
	c := setupRealClient(t)
	namespace := "default"
	kvCacheName := fmt.Sprintf("migration-test-%d", time.Now().Unix())

	// --- Step 1: Create in-cluster Redis Pod + Service (simulating prior state) ---
	redisPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-redis", kvCacheName),
			Namespace: namespace,
			Labels: map[string]string{
				constants.KVCacheLabelKeyIdentifier: kvCacheName,
				constants.KVCacheLabelKeyRole:       constants.KVCacheLabelValueRoleMetadata,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "redis",
					Image: "valkey/valkey:8",
					Command: []string{"valkey-server"},
				},
			},
		},
	}
	redisSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-redis", kvCacheName),
			Namespace: namespace,
			Labels: map[string]string{
				constants.KVCacheLabelKeyIdentifier: kvCacheName,
				constants.KVCacheLabelKeyRole:       constants.KVCacheLabelValueRoleMetadata,
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Port: 6379, Protocol: corev1.ProtocolTCP},
			},
			Selector: map[string]string{
				constants.KVCacheLabelKeyIdentifier: kvCacheName,
			},
		},
	}

	require.NoError(t, c.Create(ctx, redisPod))
	require.NoError(t, c.Create(ctx, redisSvc))
	t.Logf("Created in-cluster Redis Pod and Service: %s-redis", kvCacheName)

	// Verify they exist
	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: redisPod.Name}, &corev1.Pod{})
	require.NoError(t, err, "Pod should exist before migration")
	err = c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: redisSvc.Name}, &corev1.Service{})
	require.NoError(t, err, "Service should exist before migration")

	// --- Step 2: Run reconcileRedisService with externalConnection ---
	reconciler := NewDistributedReconciler(c, constants.KVCacheBackendInfinistore)
	reconciler.Backend = InfiniStoreBackend{}

	kvCache := &orchestrationv1alpha1.KVCache{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kvCacheName,
			Namespace: namespace,
		},
		Spec: orchestrationv1alpha1.KVCacheSpec{
			Metadata: &orchestrationv1alpha1.MetadataSpec{
				Redis: &orchestrationv1alpha1.MetadataConfig{
					ExternalConnection: &orchestrationv1alpha1.ExternalConnectionConfig{
						Address:           "valkey.example.com:6379",
						PasswordSecretRef: "valkey-credentials",
					},
				},
			},
		},
	}

	err = reconciler.reconcileRedisService(ctx, kvCache)
	require.NoError(t, err, "reconcileRedisService should succeed with valid external connection")
	t.Log("reconcileRedisService completed successfully with external connection")

	// --- Step 3: Confirm in-cluster resources are deleted ---
	// Give the API server a moment to process the deletes
	time.Sleep(1 * time.Second)

	// Pod may still be in Terminating state (graceful deletion), so check
	// either NotFound or DeletionTimestamp is set.
	pod := &corev1.Pod{}
	err = c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: redisPod.Name}, pod)
	if err == nil {
		assert.NotNil(t, pod.DeletionTimestamp, "Redis Pod should be deleted (or have DeletionTimestamp set) after migration")
	} else {
		assert.True(t, apierrors.IsNotFound(err), "Redis Pod should be deleted after migration, got: %v", err)
	}

	svc := &corev1.Service{}
	err = c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: redisSvc.Name}, svc)
	if err == nil {
		assert.NotNil(t, svc.DeletionTimestamp, "Redis Service should be deleted (or have DeletionTimestamp set) after migration")
	} else {
		assert.True(t, apierrors.IsNotFound(err), "Redis Service should be deleted after migration, got: %v", err)
	}

	t.Log("✓ Migration path validated: in-cluster Redis Pod and Service successfully cleaned up")
}

// TestMigrationPath_InvalidExternalConfig_PreservesResources validates that
// an invalid external config does NOT delete existing in-cluster resources.
func TestMigrationPath_InvalidExternalConfig_PreservesResources(t *testing.T) {
	ctx := context.Background()
	c := setupRealClient(t)
	namespace := "default"
	kvCacheName := fmt.Sprintf("safe-test-%d", time.Now().Unix())

	// Create in-cluster Redis resources
	redisPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-redis", kvCacheName),
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "redis", Image: "valkey/valkey:8", Command: []string{"valkey-server"}},
			},
		},
	}
	redisSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-redis", kvCacheName),
			Namespace: namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports:    []corev1.ServicePort{{Port: 6379, Protocol: corev1.ProtocolTCP}},
			Selector: map[string]string{"app": "redis"},
		},
	}

	require.NoError(t, c.Create(ctx, redisPod))
	require.NoError(t, c.Create(ctx, redisSvc))

	// Run reconcile with INVALID external connection (no port)
	reconciler := NewDistributedReconciler(c, constants.KVCacheBackendInfinistore)
	reconciler.Backend = InfiniStoreBackend{}

	kvCache := &orchestrationv1alpha1.KVCache{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kvCacheName,
			Namespace: namespace,
		},
		Spec: orchestrationv1alpha1.KVCacheSpec{
			Metadata: &orchestrationv1alpha1.MetadataSpec{
				Redis: &orchestrationv1alpha1.MetadataConfig{
					ExternalConnection: &orchestrationv1alpha1.ExternalConnectionConfig{
						Address: "valkey.example.com", // invalid — no port
					},
				},
			},
		},
	}

	err := reconciler.reconcileRedisService(ctx, kvCache)
	assert.Error(t, err, "should fail validation")
	assert.Contains(t, err.Error(), "must be in host:port format")

	// Confirm resources were NOT deleted
	err = c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: redisPod.Name}, &corev1.Pod{})
	assert.NoError(t, err, "Redis Pod should still exist after validation failure")

	err = c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: redisSvc.Name}, &corev1.Service{})
	assert.NoError(t, err, "Redis Service should still exist after validation failure")

	t.Log("✓ Safety validated: invalid external config did NOT delete in-cluster resources")

	// Cleanup
	_ = c.Delete(ctx, redisPod)
	_ = c.Delete(ctx, redisSvc)
}
