package dockerdeploy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/couchbaselabs/cbdinocluster/clusterdef"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestGetClusterInfoEx_PreferPreFetchedState(t *testing.T) {
	// Count local connection attempts to verify we bypass them for cached nodes
	var localConnectionsAttempted atomic.Int64

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/pools/default" {
			// Track requests to /pools/default to see if local nodes are hit
			if !strings.Contains(r.Host, "10.0.0.1") {
				localConnectionsAttempted.Add(1)
			}

			if strings.Contains(r.Host, "10.0.0.3") {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{
					"nodes": [
						{
							"thisNode": true,
							"otpNode": "ns_3@10.0.0.3",
							"hostname": "10.0.0.3:8091",
							"status": "healthy",
							"services": ["kv"]
						}
					],
					"servicesNeedRebalance": []
				}`))
				return
			}

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"nodes": [
					{
						"thisNode": true,
						"otpNode": "ns_1@10.0.0.1",
						"hostname": "10.0.0.1:8091",
						"status": "healthy",
						"services": ["kv"]
					},
					{
						"otpNode": "ns_2@10.0.0.2",
						"hostname": "10.0.0.2:8091",
						"status": "healthy",
						"services": ["kv"]
					}
				],
				"servicesNeedRebalance": []
			}`))
			return
		}

		if r.URL.Path == "/pools/default/terseClusterInfo" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"clusterUUID": "mock-uuid",
				"orchestrator": "ns_1@10.0.0.1",
				"isBalanced": true,
				"clusterCompatVersion": "7.0.0"
			}`))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockServer.Close()

	u, err := url.Parse(mockServer.URL)
	require.NoError(t, err)
	mockHost := u.Host

	// Redirect all requests going to port 8091 to our mock server
	originalTransport := http.DefaultTransport
	http.DefaultTransport = &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if strings.HasSuffix(addr, ":8091") {
				addr = mockHost
			}
			dialer := &net.Dialer{}
			return dialer.DialContext(ctx, network, addr)
		},
	}
	defer func() {
		http.DefaultTransport = originalTransport
	}()

	deployer := &Deployer{
		logger: zap.NewNop(),
	}

	cluster := &clusterInfo{
		ClusterID: "test-cluster",
		Nodes: []*nodeInfo{
			{
				IPAddress: "10.0.0.1",
				Type:      "server-node",
				Name:      "node-1",
			},
			{
				IPAddress: "10.0.0.2",
				Type:      "server-node",
				Name:      "node-2",
			},
			{
				IPAddress: "10.0.0.3",
				Type:      "server-node",
				Name:      "node-3",
			},
		},
	}

	// Run getClusterInfoEx
	res, err := deployer.getClusterInfoEx(context.Background(), cluster)
	require.NoError(t, err)
	require.NotNil(t, res)

	// Verify fields populated correctly from pre-fetched state
	require.Len(t, res.NodesEx, 3)
	require.Equal(t, "ns_1@10.0.0.1", res.NodesEx[0].OTPNode)
	require.Equal(t, "healthy", res.NodesEx[0].Status)
	require.Equal(t, []clusterdef.Service{clusterdef.KvService}, res.NodesEx[0].Services)
	require.True(t, res.NodesEx[0].IsClusterOrchestrator)

	require.Equal(t, "ns_2@10.0.0.2", res.NodesEx[1].OTPNode)
	require.Equal(t, "healthy", res.NodesEx[1].Status)
	require.Equal(t, []clusterdef.Service{clusterdef.KvService}, res.NodesEx[1].Services)
	require.False(t, res.NodesEx[1].IsClusterOrchestrator)

	// Node 3 fallback verification
	require.Equal(t, "ns_3@10.0.0.3", res.NodesEx[2].OTPNode)
	require.Equal(t, "healthy", res.NodesEx[2].Status)
	require.Equal(t, []clusterdef.Service{clusterdef.KvService}, res.NodesEx[2].Services)
	require.False(t, res.NodesEx[2].IsClusterOrchestrator)

	// Verify that local API was only polled for node 3 fallback, and node 2 was matched in cache!
	require.Equal(t, int64(1), localConnectionsAttempted.Load())
}
