//go:build integration || chaos || soak

package kafkatest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DefaultImage is used when KAAS_KAFKA_IMAGE is unset. CI sets it per
// matrix entry (Kafka 3.9, 4.2, 4.4).
const DefaultImage = "apache/kafka:4.2.0"

const (
	externalPort = "9092/tcp"
	startScript  = "/tmp/kaas_start.sh"
)

// The advertised external listener must carry the host-mapped port, which is
// only known after the container starts, so the container waits for this
// script to appear and the PostStarts hook writes it. The Testcontainers
// Kafka module does the same but only supports Confluent images.
const startScriptTmpl = `#!/bin/bash
export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://%s,BROKER://$(hostname):9093"
exec /etc/kafka/docker/run
`

// Real starts a single-node KRaft broker from an apache/kafka image and
// returns its seed addresses. The container is removed when the test ends.
func Real(t testing.TB) []string { return RealBroker(t).Seeds }

// Broker is a running broker container a chaos test can disturb.
type Broker struct {
	Seeds []string
	c     testcontainers.Container
}

// Pause freezes the broker's process, as a long GC pause, a stalled
// disk or a partitioned network would: connections stay open and
// requests go unanswered. Unpause lets it carry on where it was.
func (b *Broker) Pause(t testing.TB)   { b.docker(t, "pause") }
func (b *Broker) Unpause(t testing.TB) { b.docker(t, "unpause") }

func (b *Broker) docker(t testing.TB, verb string) {
	t.Helper()
	if out, err := exec.Command("docker", verb, b.c.GetContainerID()).CombinedOutput(); err != nil {
		t.Fatalf("docker %s: %v: %s", verb, err, out)
	}
}

// RealBroker is Real with the container, for chaos tests.
func RealBroker(t testing.TB) *Broker {
	t.Helper()
	img := os.Getenv("KAAS_KAFKA_IMAGE")
	if img == "" {
		img = DefaultImage
	}
	ctx := context.Background()
	c, err := testcontainers.Run(ctx, img,
		testcontainers.WithExposedPorts(externalPort),
		testcontainers.WithEnv(map[string]string{
			"CLUSTER_ID":                                     "a2Fhcy10ZXN0LWNsdXN0ZXI",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9094",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		}),
		testcontainers.WithEntrypoint("sh", "-c",
			"while [ ! -f "+startScript+" ]; do sleep 0.1; done; exec bash "+startScript),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{writeStartScript},
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatalf("start %s: %v", img, err)
	}
	endpoint, err := c.PortEndpoint(ctx, externalPort, "")
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	return &Broker{Seeds: []string{endpoint}, c: c}
}

func writeStartScript(ctx context.Context, c testcontainers.Container) error {
	if err := wait.ForMappedPort(externalPort).WaitUntilReady(ctx, c); err != nil {
		return fmt.Errorf("wait for mapped port: %w", err)
	}
	endpoint, err := c.PortEndpoint(ctx, externalPort, "")
	if err != nil {
		return fmt.Errorf("port endpoint: %w", err)
	}
	script := fmt.Sprintf(startScriptTmpl, endpoint)
	if err := c.CopyToContainer(ctx, []byte(script), startScript, 0o755); err != nil {
		return fmt.Errorf("copy start script: %w", err)
	}
	return nil
}
