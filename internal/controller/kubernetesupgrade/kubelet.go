package kubernetesupgrade

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	talosconstants "github.com/siderolabs/talos/pkg/machinery/constants"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tupprv1alpha1 "github.com/home-operations/tuppr/api/v1alpha1"
	"github.com/home-operations/tuppr/internal/controller/nodeutil"
)

// kubeletUpgradeWait is how long a succeeded job is kept while the selected
// kubelets restart onto the target image. It stays under
// KubernetesJobTTLAfterFinished: once the job is gone, a fresh one runs.
const kubeletUpgradeWait = 4 * time.Minute

// upgradeKubeletArgs leaves kubelets out of upgrade-k8s when the upgrade has a
// node selector. talosctl upgrades the kubelet on every node it finds without
// the control plane label, unselected ones included (a virtual kubelet has no
// Talos API to answer it), and has no flag to narrow that set, so tuppr
// upgrades the selected kubelets itself (upgradeSelectedKubelets).
func upgradeKubeletArgs(kubernetesUpgrade *tupprv1alpha1.KubernetesUpgrade) []string {
	if kubernetesUpgrade.Spec.NodeSelector == nil {
		return nil
	}
	return []string{"--upgrade-kubelet=false"}
}

// kubeletImageRepository is the image upgrade-k8s would give the kubelet:
// <imageRepository>/kubelet, or the Talos default.
func kubeletImageRepository(repository string) string {
	repo := strings.TrimRight(repository, "/")
	if repo == "" {
		return talosconstants.KubeletImage
	}
	return repo + "/kubelet"
}

// awaitSelectedKubelets runs after an upgrade-k8s job with a node selector
// succeeds: it upgrades the selected kubelets and keeps the job, reporting
// waiting, until they report the target version or kubeletUpgradeWait runs
// out. Past the wait, a node that still lags is left to the usual
// verification and a node that can't be patched fails the upgrade.
func (r *Reconciler) awaitSelectedKubelets(ctx context.Context, kubernetesUpgrade *tupprv1alpha1.KubernetesUpgrade, job *batchv1.Job) (ctrl.Result, bool, error) {
	logger := log.FromContext(ctx)
	targetVersion := kubernetesUpgrade.Spec.Kubernetes.Version

	withinWait := job.Status.CompletionTime == nil || r.Now.Now().Sub(job.Status.CompletionTime.Time) < kubeletUpgradeWait

	if err := r.upgradeSelectedKubelets(ctx, kubernetesUpgrade, job.Labels[targetNodeLabelKey]); err != nil {
		if withinWait {
			logger.Error(err, "Failed to upgrade kubelets on selected nodes, requeueing")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, true, nil
		}
		message := fmt.Sprintf("Failed to upgrade kubelets on selected nodes: %s", err)
		result, err := r.failJob(ctx, kubernetesUpgrade, job, message, message)
		return result, true, err
	}

	upgraded, err := r.areAllNodesUpgraded(ctx, kubernetesUpgrade.Spec.NodeSelector, targetVersion)
	if err != nil {
		logger.Error(err, "Failed to verify kubelet versions, requeueing")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, true, nil
	}
	if upgraded || !withinWait {
		return ctrl.Result{}, false, nil
	}

	message := fmt.Sprintf("Upgrading kubelets on selected nodes to %s", targetVersion)
	if err := r.setPhaseWithUpdates(ctx, kubernetesUpgrade, tupprv1alpha1.JobPhaseUpgrading, "", kubernetesUpgrade.Status.ControllerNode, message, nil); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("set Upgrading phase while kubelets restart: %w", err)
	}
	logger.V(1).Info("Waiting for selected kubelets to report the target version", "targetVersion", targetVersion)
	return ctrl.Result{RequeueAfter: 15 * time.Second}, true, nil
}

// upgradeSelectedKubelets points every selected node whose kubelet reports
// another version at the target kubelet image, the controller node first.
// Nodes already patched are left alone, so it is safe to repeat while their
// kubelets restart.
func (r *Reconciler) upgradeSelectedKubelets(ctx context.Context, kubernetesUpgrade *tupprv1alpha1.KubernetesUpgrade, controllerNode string) error {
	nodes, err := nodeutil.ListSelectedNodes(ctx, r.Client, kubernetesUpgrade.Spec.NodeSelector)
	if err != nil {
		return err
	}
	slices.SortStableFunc(nodes, func(a, b corev1.Node) int {
		switch controllerNode {
		case a.Name:
			return -1
		case b.Name:
			return 1
		}
		return 0
	})

	targetVersion := kubernetesUpgrade.Spec.Kubernetes.Version
	repository := kubeletImageRepository(kubernetesUpgrade.Spec.Kubernetes.ImageRepository)
	for _, node := range nodes {
		current := node.Status.NodeInfo.KubeletVersion
		// Empty means the kubelet has not reported yet; areAllNodesUpgraded
		// skips such nodes too.
		if current == "" || current == targetVersion {
			continue
		}
		nodeIP, err := nodeutil.GetNodeIP(&node)
		if err != nil {
			return fmt.Errorf("kubernetesupgrade: kubelet on node %s: %w", node.Name, err)
		}
		changed, err := r.TalosClient.SetNodeKubeletVersion(ctx, nodeIP, repository, targetVersion)
		if err != nil {
			return fmt.Errorf("kubernetesupgrade: kubelet on node %s: %w", node.Name, err)
		}
		if changed {
			log.FromContext(ctx).Info("Upgrading kubelet", "node", node.Name, "from", current, "to", targetVersion)
		}
	}
	return nil
}
