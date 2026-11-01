//go:build integration

package pipe

import (
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// The kfake checks of kafka_test.go, on a real broker of each version CI
// runs: what a pipe acknowledges is written, and what it doesn't comes
// round again.

func TestIntegrationCopy(t *testing.T) { checkCopy(t, kafkatest.Real(t)) }

func TestIntegrationResume(t *testing.T) { checkResume(t, kafkatest.Real(t)) }

func TestIntegrationRebalance(t *testing.T) { checkRebalance(t, kafkatest.Real(t), 20_000) }
