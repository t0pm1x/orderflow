package k8s_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestK8sSmoke_KindCluster_HelmRenders is an integration smoke test that:
//
//  1. Provisions a kind cluster using deploy/kind/kind.yaml
//  2. Waits for the control-plane node to be Ready
//  3. Renders the four service Helm charts (orderflow-{order,payment,inventory,saga})
//     plus the infra chart (orderflow-postgres) with `helm template` to ensure they
//     produce valid YAML containing a Deployment manifest
//
// The test is skipped when KIND_SKIP=1 or -short is set, since it requires
// docker, kind, kubectl, and helm on the host (or CI runner).
//
// For v1.0 this test verifies cluster bring-up + chart renderability. Loading
// the ghcr.io/t0pm1x/orderflow-*:dev images into the cluster and asserting
// on full pod readiness is left as a follow-up tracked in the v1.0 runbook.
func TestK8sSmoke_KindCluster_HelmRenders(t *testing.T) {
	if testing.Short() {
		t.Skip("kind smoke requires docker + kubectl + kind + helm on host")
	}
	if os.Getenv("KIND_SKIP") == "1" {
		t.Skip("KIND_SKIP=1 set")
	}

	kindPath, err := resolveKind()
	if err != nil {
		t.Skipf("kind not found on host: %v", err)
	}
	kubectlPath, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skipf("kubectl not found on host: %v", err)
	}
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skipf("helm not found on host: %v", err)
	}

	root, err := findRepoRoot()
	if err != nil {
		t.Fatalf("locate repo root: %v", err)
	}

	clusterName := "orderflow-smoke"
	const nodeImage = "kindest/node:v1.30.0"

	// Use a fresh background context for cleanup so a cancelled/expired
	// test context cannot prevent us from tearing the cluster down.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(func() {
		cleanupCancel()
		if out, err := exec.CommandContext(cleanupCtx, kindPath,
			"delete", "cluster", "--name", clusterName).CombinedOutput(); err != nil {
			t.Logf("kind delete cluster (best-effort) failed: %v\n%s", err, out)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	kindCfg := filepath.Join(root, "deploy", "kind", "kind.yaml")
	t.Logf(">>> creating kind cluster %q with %s", clusterName, kindCfg)
	createOut, err := exec.CommandContext(ctx, kindPath, "create", "cluster",
		"--name", clusterName,
		"--config", kindCfg,
		"--image", nodeImage,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("kind create cluster: %v\n%s", err, createOut)
	}

	t.Log(">>> waiting for control-plane node Ready (timeout 120s)")
	waitOut, err := exec.CommandContext(ctx, kubectlPath, "wait",
		"--for=condition=Ready", "node", "--all", "--timeout=120s",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl wait node --all: %v\n%s", err, waitOut)
	}

	t.Log(">>> verifying kind cluster reachable via kubectl")
	getOut, err := exec.CommandContext(ctx, kubectlPath, "get", "nodes",
		"-o", "name",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl get nodes: %v\n%s", err, getOut)
	}
	if !strings.Contains(string(getOut), "node/") {
		t.Fatalf("expected at least one node from kubectl get nodes, got: %s", getOut)
	}

	charts := []struct {
		release string
		path    string
	}{
		{"orderflow-order", filepath.Join(root, "deploy", "helm", "orderflow-order")},
		{"orderflow-payment", filepath.Join(root, "deploy", "helm", "orderflow-payment")},
		{"orderflow-inventory", filepath.Join(root, "deploy", "helm", "orderflow-inventory")},
		{"orderflow-saga", filepath.Join(root, "deploy", "helm", "orderflow-saga")},
		{"orderflow-infra", filepath.Join(root, "deploy", "helm", "orderflow-postgres")},
	}
	for _, c := range charts {
		t.Logf(">>> helm template %s (%s)", c.release, c.path)
		out, err := exec.CommandContext(ctx, "helm", "template", c.release, c.path).CombinedOutput()
		if err != nil {
			t.Errorf("helm template %s: %v\n%s", c.release, err, out)
			continue
		}
		rendered := string(out)
		if !strings.Contains(rendered, "kind: Deployment") {
			t.Errorf("helm template %s: rendered output missing kind: Deployment", c.release)
		}
	}

	t.Log(">>> smoke PASS")
}

// resolveKind searches PATH first, then the known winget install location
// on Windows. Returns os.ErrNotExist when no candidate resolves.
func resolveKind() (string, error) {
	candidates := []string{"kind"}
	if runtime.GOOS == "windows" {
		candidates = append(candidates,
			`C:\Users\t0p_m\AppData\Local\Microsoft\WinGet\Packages\Kubernetes.kind_Microsoft.Winget.Source_8wekyb3d8bbwe\kind.exe`)
	}
	for _, c := range candidates {
		if path, err := exec.LookPath(c); err == nil && path != "" {
			return path, nil
		}
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}
	return "", os.ErrNotExist
}

// findRepoRoot walks up from this test file looking for go.work, which is
// the canonical marker of the orderflow monorepo root.
func findRepoRoot() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", os.ErrNotExist
	}
	dir := filepath.Dir(thisFile)
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", os.ErrNotExist
}
