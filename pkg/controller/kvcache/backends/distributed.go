/*
Copyright 2025 The Aibrix Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backends

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	orchestrationv1alpha1 "github.com/vllm-project/aibrix/api/orchestration/v1alpha1"
	"github.com/vllm-project/aibrix/pkg/constants"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type DistributedReconciler struct {
	client.Client
	*BaseReconciler
	Backend KVCacheBackend
}

func NewDistributedReconciler(c client.Client, backend string) *DistributedReconciler {
	reconciler := &DistributedReconciler{
		Client:         c,
		BaseReconciler: &BaseReconciler{Client: c},
	}

	if backend == constants.KVCacheBackendInfinistore {
		reconciler.Backend = InfiniStoreBackend{}
	} else if backend == constants.KVCacheBackendHPKV {
		reconciler.Backend = HpKVBackend{}
	} else {
		panic(fmt.Sprintf("unsupported backend: %s", backend))
	}
	return reconciler
}

func (r *DistributedReconciler) Reconcile(ctx context.Context, kvCache *orchestrationv1alpha1.KVCache) (ctrl.Result, error) {
	if err := r.Backend.ValidateObject(kvCache); err != nil {
		return ctrl.Result{}, err
	}

	if kvCache.Spec.Metadata != nil && kvCache.Spec.Metadata.Redis != nil {
		if err := r.reconcileRedisService(ctx, kvCache); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Handle infinistore kvCache Deployment
	if err := r.ReconcileStatefulsetObject(ctx, r.Backend.BuildCacheStatefulSet(kvCache)); err != nil {
		return ctrl.Result{}, err
	}

	// Handle Hpkv/infinistore Services
	if err := r.ReconcileServiceObject(ctx, r.Backend.BuildService(kvCache)); err != nil {
		return ctrl.Result{}, err
	}

	if kvCache.Spec.Watcher != nil {
		if err := r.reconcileWatcherPodServiceAccount(ctx, r.Backend.BuildWatcherPodServiceAccount(kvCache)); err != nil {
			return ctrl.Result{}, err
		}

		if err := r.reconcileWatcherPodRole(ctx, r.Backend.BuildWatcherPodRole(kvCache)); err != nil {
			return ctrl.Result{}, err
		}

		if err := r.reconcileWatcherPodRoleBinding(ctx, r.Backend.BuildWatcherPodRoleBinding(kvCache)); err != nil {
			return ctrl.Result{}, err
		}

		// Handle Hpkv/infinistore watcher Pod
		if err := r.ReconcilePodObject(ctx, r.Backend.BuildWatcherPod(kvCache)); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *DistributedReconciler) reconcileMetadataService(ctx context.Context, kvCache *orchestrationv1alpha1.KVCache) error {
	if kvCache.Spec.Metadata != nil && kvCache.Spec.Metadata.Etcd == nil && kvCache.Spec.Metadata.Redis == nil {
		return errors.New("either etcd or redis configuration is required")
	}

	if kvCache.Spec.Metadata.Redis != nil {
		return r.reconcileRedisService(ctx, kvCache)
	}

	return nil
}

func (r *DistributedReconciler) reconcileRedisService(ctx context.Context, kvCache *orchestrationv1alpha1.KVCache) error {
	redisConfig := kvCache.Spec.Metadata.Redis

	// If an external connection is configured, skip in-cluster Redis deployment.
	// The operator is providing their own managed Valkey/Redis-compatible endpoint.
	if redisConfig.ExternalConnection != nil && redisConfig.ExternalConnection.Address != "" {
		klog.Infof("Using external metadata connection at %s, skipping in-cluster Redis deployment", redisConfig.ExternalConnection.Address)
		return r.validateExternalConnection(ctx, kvCache)
	}

	// Fall back to in-cluster Redis deployment (existing behavior).
	if redisConfig.Runtime == nil {
		return errors.New("redis metadata config requires either externalConnection or runtime to be set")
	}

	replicas := int(redisConfig.Runtime.Replicas)
	if replicas != 1 {
		klog.Warningf("replica %d > 1 is not supported at this moment, we will change to single replica", replicas)
	}

	pod := r.Backend.BuildMetadataPod(kvCache)
	if err := r.ReconcilePodObject(ctx, pod); err != nil {
		return err
	}

	// Create or update the metadata service for each pod
	metadataService := r.Backend.BuildMetadataService(kvCache)
	if err := r.ReconcileServiceObject(ctx, metadataService); err != nil {
		return err
	}

	return nil
}

// validateExternalConnection validates the external connection configuration
// and resolves the PasswordSecretRef if provided.
func (r *DistributedReconciler) validateExternalConnection(ctx context.Context, kvCache *orchestrationv1alpha1.KVCache) error {
	extConn := kvCache.Spec.Metadata.Redis.ExternalConnection

	// Validate address format (host:port)
	if _, _, err := net.SplitHostPort(extConn.Address); err != nil {
		return fmt.Errorf("external connection address %q must be in host:port format: %w", extConn.Address, err)
	}

	// If a password secret reference is provided, verify it exists.
	if extConn.PasswordSecretRef != "" {
		if _, err := r.resolveSecretValue(ctx, kvCache.Namespace, extConn.PasswordSecretRef); err != nil {
			return fmt.Errorf("failed to resolve PasswordSecretRef %q: %w", extConn.PasswordSecretRef, err)
		}
	}

	return nil
}

// resolveSecretValue reads a value from a Kubernetes Secret.
// The secretRef format is "secretName/key" or just "secretName" (defaults to key "password").
func (r *DistributedReconciler) resolveSecretValue(ctx context.Context, namespace, secretRef string) (string, error) {
	parts := strings.SplitN(secretRef, "/", 2)
	secretName := parts[0]
	secretKey := "password"
	if len(parts) == 2 {
		secretKey = parts[1]
	}

	secret := &corev1.Secret{}
	if err := r.Client.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      secretName,
	}, secret); err != nil {
		return "", fmt.Errorf("secret %q not found in namespace %q: %w", secretName, namespace, err)
	}

	value, ok := secret.Data[secretKey]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %q", secretKey, secretName)
	}

	return string(value), nil
}
