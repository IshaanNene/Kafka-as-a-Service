# ADR-0009: The operator wraps Strimzi, with controller-runtime

- Status: accepted
- Date: 2026-11-02
- Deciders: `IshaanNene`

## Context
Phase 3 runs Kafka clusters for teams on Kubernetes: create them, roll them and size them to load, and show them in the console. Running brokers well on Kubernetes needs a lot of machinery: ordered restarts that wait for in-sync replicas, KRaft bootstrapping, certificates, storage, and partition moves when brokers come and go. Strimzi has that machinery, and the dev stack already runs it (`task dev`).

## Decision
The `kaas operator` owns a small resource, `KafkaService` (`kaas.io/v1alpha1`), and renders each one into Strimzi resources. It adds only what Strimzi leaves to people:

- a roll on request;
- load measurement;
- autoscaling with a scale-down window;
- a status the console can show.

It is built on controller-runtime and client-go, which come into the one `kaas` binary. Strimzi's resources are handled as unstructured objects, and KafkaService deep copies go through JSON, so the repository gains no code-generation step.

The console reaches managed clusters through `controlplane/lifecycle.Manager`. Its Kubernetes implementation writes KafkaService specs and nothing else.

## Alternatives considered
- **Brokers as our own StatefulSets.** This rebuilds Strimzi's hardest parts, and the first versions would restart brokers less safely than Strimzi does today.
- **A hand-written Kubernetes REST client** to keep the binary small. Kubeconfig auth plugins, watches, caches and leader election would all be ours to get right.
- **controller-gen for deep copies and the CRD.** This would be the repository's only generator. The types are small, and the CRD is written by hand with the validation it needs.
- **A separate operator binary.** It would give a smaller CLI, but two release artefacts and two version streams. One binary keeps `kaas operator` and `kaas console --kube` from drifting apart.

## Consequences
- `kaas` grows by the Kubernetes client libraries: about 15 MB, and a slower first build. The CLI paths don't load them at run time.
- Strimzi is a hard dependency of Phase 3, at version 1.0 or later (its v1 API). Strimzi upgrades need checking against the rendered resources and the restart annotation.
- Autoscaling measures load from inside the cluster. Run from a laptop, the operator can't reach in-cluster brokers, so autoscaling holds. It logs why, and the console shows no load.
- Deleting a KafkaService deletes its Strimzi resources through owner references. Broker volumes are kept (`deleteClaim: false`), so data outlives an accidental delete until its claims are removed.
