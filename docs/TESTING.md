# Testing guide: how kaas is proven

A Kafka tool is only useful if it is trustworthy when the cluster isn't. This guide covers:
- the test tiers kaas runs;
- what each one proves;
- the bugs the suite has caught;
- the evidence each CI run leaves behind.

The tiers follow the usual approaches to testing Kafka applications: unit tests with mocks, integration tests against a real broker, performance and load tests, chaos tests, contract (schema) tests, replaying production records, and soak tests.

## The tiers

| Tier | Runs on | What it proves | Run it |
|---|---|---|---|
| **Unit and kfake** | Every PR | Logic against an in-memory Kafka (franz-go's kfake), including fault injection | `task test` |
| **Console and GitOps** | Every PR | Plans converge on kfake, the console API's shapes and guards, the event stream's replay, and the web app's formatting | `task test`, `task test:web` |
| **Screen goldens** | Every PR | Every screen draws the same at fixed sizes, and live screens never truncate | `task test` |
| **Fuzz** | Every PR (60s a target) | No input crashes a parser of untrusted bytes, and round trips are lossless | `task test:fuzz` |
| **Contract** | Every PR | The JSON record format, the plugin protocol and generator fields stay compatible | `task test` |
| **Integration** | Every PR, on Kafka 3.9.2, 4.2.1 and 4.4.0 | The same properties against real brokers in containers, and the CLI end to end | `task test:integration` |
| **Chaos** | Every PR | Nothing is lost, and nothing is written twice by a retry, when the broker freezes, slows or rebalances | `task test:chaos` |
| **Benchmarks** | Every PR (results as an artifact) | The cost of each stage, so regressions show in benchstat | `task bench` |
| **Soak** | Weekly, and on demand | Hours of load with lag, heap and goroutines bounded | `task test:soak` |

Integration, chaos and soak tests need Docker. `KAAS_KAFKA_IMAGE=apache/kafka:4.4.0` picks the broker version.

## Unit tests and mocks

Most packages are tested against **kfake**, an in-memory Kafka that speaks the real protocol. That is better than a mock: the client code under test runs unchanged. Where a test needs a failure, it injects one at the protocol level, the way Java tests use `MockProducer.errorNext()`.

- **`TestKafkaProduceRefused`.** The broker refuses the first two produce requests with a non-retriable error. The pipe must fail without committing its source offsets, and the next run must write every record. (`core/pipe/kafka_test.go`)
- **`TestFailedWriteIsNotAcked`.** When one of two sinks fails, the batch isn't acknowledged to its source.
- **`TestKafkaLatency`.** A client dialer adds latency to every write, as a proxy like Toxiproxy would. The copy slows down but stays complete.

The checks that matter most are written once and run on every tier: `checkCopy`, `checkResume` and `checkRebalance` run on kfake in unit tests, and on real brokers in integration and chaos tests.

## Console and GitOps

The management plane is tested the same way, on kfake clusters:

- **`core/plan`.**
  - A desired-state file is planned, applied and re-planned until nothing is left (`TestPlanApplyConverges`).
  - Every refusal in parsing has a case: unknown keys, bad operations and resources, principals without a type, sizes where a number belongs, CEL rules that don't compile or don't return a bool.
  - Plan IDs are stable for the same changes. Policies block, prune only touches the kinds a file manages, and retention decreases are rated caution.
- **`core/acls` and `core/quotas`.** Exact create, list and delete. Deleting a literal grant must leave the prefixed grant of the same name alone.
- **`controlplane/schemas`.** The registry client against an in-memory registry, including compatibility levels and basic auth from the URL.
- **`controlplane/console`.**
  - `TestFleetAPI` starts a fleet of two clusters: one healthy and drifting from its file, and one unreachable. It checks each cluster's status, that no list is ever `null`, the 404s and the security headers.
  - `TestApplyIsGuarded` walks the whole guard matrix:
    1. apply disabled;
    2. wrong content type;
    3. cross-origin request;
    4. wrong name typed;
    5. stale plan ID.

    Then it applies for real and checks the cluster converged and a `PlanApplied` event was published.
  - `TestEventsReplayAfterReconnect` drops the stream and checks that `Last-Event-ID` brings back what was missed.
- **`cmd/kaas`.** `TestPlanAndApply` runs `kaas plan` and `kaas apply` end to end:
  - the plan text;
  - `--exit-code` exiting 5;
  - JSON output;
  - `--yes` required without a terminal;
  - a stale `--plan` refused;
  - a real apply.
- **The web app.** `controlplane/console/webtest` runs with `node --test` and checks the formatting every number on screen goes through. The `web` CI job also checks every module parses.

## Integration: real Kafka in containers

`test/kafkatest` starts an `apache/kafka` image as a single-node KRaft broker with Testcontainers. CI runs every integration test on three Kafka versions.

- **`TestIntegrationCopy`, `Resume` and `Rebalance`.** These are the pipe's delivery properties on real brokers.
- **`TestIntegrationCLI`.** The CLI end to end, as an operator would use it:
  1. create topics;
  2. load one with a pipe;
  3. consume it back;
  4. move it through a consumer-group pipe and check a second run has nothing to do;
  5. rewind the group with `kaas offsets reset` and check the replay matches;
  6. edit a config and take a snapshot.
- The snapshot and engine packages have their own integration tests against the same matrix.

## Chaos

Chaos tests (`core/pipe/chaos_test.go`, build tag `chaos`) disturb a real broker while records flow. Each one asserts what a pipe promises.

| Test | Disturbance | Asserts |
|---|---|---|
| `TestChaosPauseMidStream` | `docker pause` freezes the broker for 8s while 6,000 records stream in | Every record lands **exactly once**: the idempotent producer's retries are deduplicated by the broker |
| `TestChaosStopDuringPause` | The broker freezes with a batch in flight, and the pipe is stopped | The pipe waits for the broker's real answer, acknowledges what it wrote, and the next run neither loses nor repeats records |
| `TestChaosLatency` | 25ms added to every write to the broker | The copy completes whole |
| `TestChaosRebalance` | Pipe members join and leave a consumer group mid-stream, over 50,000 records | No record is lost; redeliveries from rebalances are counted |

A frozen broker is the case that's hardest to reason about. The client has sent a request, the broker has said nothing, and the caller's deadline passes. Some clients report a timeout, and the record may still land when the broker wakes. franz-go doesn't give up on a request it has sent, so kaas never reports a write as failed when it may have succeeded. These tests pin that behaviour. If a future client version changed it, the at-least-once design still holds: an unacknowledged batch is read again, and nothing is lost.

## Fuzzing

Every parser that reads bytes from outside kaas has a fuzz target, run for 60 seconds per target on each PR:

| Target | Reads |
|---|---|
| `FuzzParseBatches` | Record batch headers from fetch responses (`kaas segments`) |
| `FuzzBatchFields`, `FuzzSkipTags` | Frames passing through the `kaas sniff` proxy |
| `FuzzParseFile` | What a user types into `kaas config edit`, which must also write back and re-read the same |
| `FuzzParseLine` | Lines piped into `kaas produce`: key + separator + value always rejoin into the line |
| `FuzzDecodeJSONL` | JSON lines from files, HTTP bodies and plugins: decode, encode, decode is lossless |
| `FuzzCSVValues` | Any key and value survive a CSV file |
| `FuzzFilter` | CEL filters over arbitrary values answer or error, but never panic |

Crashers are kept under `testdata/fuzz/` and run as regular tests from then on.

## Contracts, in place of a schema registry

kaas doesn't include a Schema Registry, so it doesn't test Avro or Protobuf schema evolution. Use your registry's compatibility checks for that. What kaas does own are formats other programs depend on, and `core/pipe/contract_test.go` pins them:

- **`TestRecordFormatV1`.** Lines written in the first version of the record format must always read the same. Fields may be added, never renamed or reinterpreted, and unknown fields are ignored.
- **`TestRecordFormatFields`.** The field names written.
- **`TestPluginProtocol`.** What kaas sends to and accepts from plugins.
- **`TestGenContract`.** The generators' fields and JSON types, which demos and load tests filter on.

A pipe can also act as a shape gate: `--where 'has(value.order_id) && has(value.amount)'` rejects records without the fields and counts them.

## Replaying production records

`TestPipeReplaysConsumeSample` takes a sample with `kaas consume -o json` from one cluster and replays it into another with `kaas pipe`. Keys, text and binary values, and headers must all arrive intact. The same two commands work against real clusters (see [use case 4](USE-CASES.md#4-testing-with-production-shaped-data)).

## Performance and load

- **Benchmarks** (`core/pipe/bench_test.go`, `core/engine/bench_test.go`) measure the engine, the CEL filter, the codecs, the generator and a whole topic copy. CI runs them five times and uploads the results for `benchstat`.
- **Cluster load tests** are `kaas bench produce` and `kaas bench consume`, the equivalents of the perf-test tools, plus `kaas pipe --from gen:…?rate=N` for sustained traffic.

## Soak

`test/soak` runs two pipes against a real broker for `KAAS_SOAK_DURATION`. One loads a topic at a steady rate, and one moves it on as a consumer group. Every few seconds the test samples lag, heap and goroutines. It fails if:
- lag ever exceeds ten seconds of load;
- the heap more than doubles over the run, which would mean a leak;
- goroutines grow;
- any record is missing at the end.

The weekly CI run writes a Markdown report as an artifact.

## Bugs the suite has caught

Tests are only evidence if they find things. These were found by the tiers above, and each now has a regression test:

| Found by | Bug | Fix |
|---|---|---|
| Fuzzing | `logmap.ParseBatches` panicked on a control batch whose size claimed less than a batch header, reading past its end. A corrupt or hostile fetch response could crash `kaas segments` | Batches shorter than a v2 header stop the parse; the crasher is a seed |
| Integration (real broker) | A `stop=end` pipe as a new consumer group failed with `COORDINATOR_NOT_AVAILABLE` on a fresh cluster. kfake has no such startup window | Retriable coordinator errors are retried |
| Replay test | `kaas consume -o json` base64-encoded only the binary half of a record but marked the whole record `encoding: base64`, so a reader couldn't tell which half to decode | Both halves are encoded when either is binary |
| Chaos | A pipe stopped with Ctrl-C counted its last batch as acknowledged when the commit had failed | Written batches are acknowledged even while stopping, and a failed ack isn't counted |
| Usage guide check | `--plugin-dir .` couldn't run a plugin: the joined path lost its `./` and exec looked it up on PATH | Plugin paths are made absolute |
| kfake tests | Closing a consumer-group source with an unacknowledged batch hung while leaving the group | Rebalances are allowed before closing |

## Evidence for each change

Each pull request leaves records a reviewer or auditor can check:

| Evidence | Where |
|---|---|
| Coverage, as a total in the job summary and the profile as an artifact | `test` job |
| Every chaos test's result and its measurements (records written, redelivered, lost) | `chaos` job summary |
| Fuzz crashers, if any | `fuzz` job artifacts |
| Benchmark results, five runs for `benchstat` | `bench` job artifact |
| Results across three Kafka versions | `integration` matrix |
| Soak report: lag, heap and goroutines over time | weekly `soak` job artifact |
| Signed releases with an SBOM and SLSA provenance | `release` workflow |

These support the DORA measures a team tracks. Every change to main passes the same gates, which keeps the change failure rate low. The gates run in minutes, which keeps lead time short. A failed tier names the property that broke, which shortens time to restore. kaas doesn't compute DORA metrics itself; it produces the per-change evidence they are built from.
