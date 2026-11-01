package topics

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Grow sets topic's partition count to n and lets the controller place
// the new replicas. Kafka only adds partitions, so n must be above the
// current count. With dry, the controller checks the change without
// making it.
func Grow(ctx context.Context, adm *kadm.Client, topic string, n int, dry bool) error {
	update := adm.UpdatePartitions
	if dry {
		update = adm.ValidateUpdatePartitions
	}
	rs, err := update(ctx, n, topic)
	if err != nil {
		return err
	}
	r, ok := rs[topic]
	switch {
	case !ok:
		return fmt.Errorf("the controller did not answer for %s", topic)
	case r.Err != nil && r.ErrMessage != "":
		return fmt.Errorf("%w: %s", r.Err, r.ErrMessage)
	}
	return r.Err
}
