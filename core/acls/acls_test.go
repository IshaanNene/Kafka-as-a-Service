package acls

import (
	"context"
	"testing"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestNormalize(t *testing.T) {
	a, err := Normalize(ACL{Principal: "User:a", Resource: "TransactionalId", Name: "tx", Operation: "DESCRIBE_CONFIGS"})
	if err != nil {
		t.Fatal(err)
	}
	want := ACL{Principal: "User:a", Host: "*", Resource: "transactional-id", Name: "tx", Pattern: "literal", Operation: "describe-configs", Permission: "allow"}
	if a != want {
		t.Fatalf("got %+v\nwant %+v", a, want)
	}
	c, err := Normalize(ACL{Principal: "User:a", Resource: "cluster", Operation: "alter"})
	if err != nil || c.Name != "kafka-cluster" {
		t.Fatalf("cluster name default: %+v %v", c, err)
	}
	for _, bad := range []ACL{
		{Principal: "a", Resource: "topic", Name: "x", Operation: "read"},
		{Principal: "User:a", Resource: "any", Name: "x", Operation: "read"},
		{Principal: "User:a", Resource: "topic", Name: "x", Operation: "any"},
		{Principal: "User:a", Resource: "topic", Name: "x", Operation: "read", Pattern: "match"},
		{Principal: "User:a", Resource: "topic", Name: "x", Operation: "read", Permission: "maybe"},
	} {
		if _, err := Normalize(bad); err == nil {
			t.Errorf("accepted %+v", bad)
		}
	}
}

func TestCreateListDelete(t *testing.T) {
	_, cl := kafkatest.Fake(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	read, _ := Normalize(ACL{Principal: "User:a", Resource: "topic", Name: "orders", Operation: "read"})
	pre, _ := Normalize(ACL{Principal: "User:a", Resource: "topic", Name: "orders", Pattern: "prefixed", Operation: "read"})
	deny, _ := Normalize(ACL{Principal: "User:b", Resource: "group", Name: "g", Operation: "read", Permission: "deny"})
	if err := Create(ctx, cl, []ACL{read, pre, deny}); err != nil {
		t.Fatal(err)
	}
	got, err := List(ctx, cl)
	if err != nil || len(got) != 3 {
		t.Fatalf("list: %+v %v", got, err)
	}
	for _, a := range got {
		if _, err := Normalize(a); err != nil {
			t.Errorf("listed ACL doesn't round-trip: %+v: %v", a, err)
		}
	}
	// Deleting the literal grant leaves the prefixed one with the same name.
	if err := Delete(ctx, cl, []ACL{read}); err != nil {
		t.Fatal(err)
	}
	got, _ = List(ctx, cl)
	if len(got) != 2 || got[0] != pre || got[1] != deny {
		t.Fatalf("after delete: %+v", got)
	}
	if s := pre.String(); s != "allow User:a read on topic orders*" {
		t.Errorf("String: %q", s)
	}
}
