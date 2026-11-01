package topics

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Delete asks the controller to delete names. The result maps each name
// to why it was not deleted, nil when it was. Kafka finishes deleting in
// the background: metadata can list a deleted topic for a moment after.
func Delete(ctx context.Context, adm *kadm.Client, names []string) (map[string]error, error) {
	rs, err := adm.DeleteTopics(ctx, names...)
	if err != nil {
		return nil, err
	}
	out := make(map[string]error, len(names))
	for _, n := range names {
		r, ok := rs[n]
		switch {
		case !ok:
			out[n] = fmt.Errorf("the controller did not answer for %s", n)
		case r.Err != nil && r.ErrMessage != "":
			out[n] = fmt.Errorf("%w: %s", r.Err, r.ErrMessage)
		default:
			out[n] = r.Err
		}
	}
	return out, nil
}
