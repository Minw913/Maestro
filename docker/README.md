# Live container provisioning

The packaged live factory is `maestro.backends.coral.backend:default_factory`.
It needs Linux, rootless Docker with cgroup v2, installed editable pinned Coral,
credential environment variables, and two locally installed **digest-pinned**
images. Windows/mock tests cannot establish those physical guarantees.

Both Dockerfiles require an explicit audited base-image digest. The agent base
must already contain Python 3, Node.js and npm. Select an exact coding CLI
version, record its package integrity/dependency lock in deployment records,
and build/publish once in the operator's registry. For example, with real
digests and a real version substituted:

```sh
docker build -f docker/Dockerfile.agent \
  --build-arg BASE_IMAGE="$AUDITED_NODE_PYTHON_IMAGE_DIGEST" \
  --build-arg CLAUDE_CODE_VERSION="$PINNED_CLAUDE_CODE_VERSION" \
  -t registry.example/maestro-agent:deployment-1 .
docker build -f docker/Dockerfile.candidate \
  --build-arg BASE_IMAGE="$AUDITED_SOLVER_TOOLCHAIN_IMAGE_DIGEST" \
  -t registry.example/maestro-candidate:deployment-1 .
```

Resolve each pushed image's repository digest with `docker image inspect` and
configure `coral.generation_image` and `pace.contract.image` with that digest.
Local-only image IDs/tags are deliberately rejected. Dependency resolution at
build time is not a claim of byte-reproducible builds: the final image digest
is the immutable execution identity. Archive the build inputs and package
lock/integrity metadata to reproduce the build independently.

No provider key, benchmark, ledger, host Docker socket, MAESTRO authority
directory, or peer workspace belongs in either image. Coral runs in the host's
trusted Python environment; the generation image contains its coding CLI.
The host injects the trusted stdlib monitor/relay at launch, so it does not
depend on an unversioned script inside the image.

During generation the candidate CLI runs as UID 65534 beneath a separate UID 0
monitor, with only the monitor's SETUID/SETGID/KILL capabilities retained. The whole
container has one CPU, memory/pid/output/file limits, a monotonic wall deadline,
and aggregate cgroup CPU metering. The network namespace has no external
interface. A localhost HTTP relay reaches only the bind-mounted Unix socket of
the persistent quota gateway. Only the phase token enters the container.

The solver/Git repository, private notes, outbox and runtime home are separate
bounded tmpfs views (64 MiB combined data; 8192 declared file entries by default,
with one quarter of each limit per view). They are initialized from this agent's
read-only seed workspace. After every child in the PID namespace stops, the
trusted monitor creates a size/count-bounded archive in its private authority
mount. The host shim rejects path traversal, duplicate entries, symlinks and
hardlinks before synchronizing only this agent's four directories. Candidate
file writes never target an unbounded host bind mount. Each child also inherits
a per-file size ceiling. Other filesystem paths and the shared verified champion
are read-only. Source-planning is a single structured provider request with no
coding CLI or shell. Runtime histories are fresh per policy arm; only declared
semantic warm memory is cloned. Generation inference/CPU caps persist across
repair/restart; unresolved resource receipts consume the reservation and block
further scope starts.

Formal candidates run in separate networkless containers with a frozen panel
instance on stdin. They do not inherit runtime homes, memory, tickets or quota
credentials. The 20-instance contract reserves 600 candidate CPU seconds plus
20 CPU seconds for a separately isolated trusted verifier; wall time, memory,
pid and output caps are independent limits. The receipt records actual CPU;
the reserved maximum is reported separately.

Only the exact frozen `coral.model` is admitted by the gateway. Requests must
contain exactly one output ceiling. Anthropic `/v1/messages?beta=true` is
allowlisted; beta headers come only from trusted `coral.anthropic_beta` settings.
Token-count/model-list/other auxiliary endpoints are denied until explicitly
qualified and given a separate accounting policy. A particular CLI version may
therefore require a documented compatibility extension; requests never bypass
the gateway as a fallback.

Qualification still requires a real provider/CLI pilot: verify buffered SSE,
tool calls, provider output ceilings, stop/resume, CPU fork trees, memory/pid
exhaustion, filesystem escape attempts, and gateway bypass denial. Do not label
the mock/FakeRuntime contract suite as this deployment qualification.
