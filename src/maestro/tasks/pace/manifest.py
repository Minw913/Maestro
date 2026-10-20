"""Frozen, ordered, checksummed panels and CPU execution contracts."""

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path

from .parser import parse_graph
from .scoring import validate_reference


class ManifestError(ValueError):
    pass


def canonical_hash(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode()).hexdigest()


VERIFIER_SOURCE_FILES = ("maestro/__init__.py", "maestro/tasks/__init__.py",
                         "maestro/tasks/pace/__init__.py", "maestro/tasks/pace/parser.py",
                         "maestro/tasks/pace/verifier.py", "maestro/tasks/pace/scoring.py",
                         "maestro/tasks/pace/manifest.py", "maestro/tasks/pace/verification_worker.py")


def verifier_source_digest(source_root: Path | None = None) -> str:
    """Bind the worker and imported trusted implementation to the contract."""
    source_root = source_root or Path(__file__).resolve().parents[3]
    return canonical_hash({name: hashlib.sha256((source_root / name).read_bytes()).hexdigest()
                           for name in VERIFIER_SOURCE_FILES})


@dataclass(frozen=True)
class EvaluationContract:
    schema_version: int = 1
    label: str = "pace_style_internal"
    instances: int = 20
    cpu_seconds_per_instance: float = 30.0
    verification_cpu_seconds: float = 20.0
    verification_wall_seconds: float = 30.0
    verification_code_hash: str = ""
    wall_seconds_per_instance: float = 45.0
    memory_mb: int = 512
    pids: int = 32
    output_bytes: int = 1_000_000
    cpus: int = 1
    cpu_workers: int = 4
    instance_workers: int = 1
    image: str = ""
    solver_argv: tuple[str, ...] = ("/candidate/run",)
    hardware_id: str = "UNSPECIFIED"
    toolchain_digest: str = "UNSPECIFIED"
    seed: int = 0

    def __post_init__(self):
        import math
        if self.schema_version != 1 or self.label != "pace_style_internal":
            raise ManifestError("unsupported evaluation contract")
        if type(self.cpus) is not int or self.cpus != 1:
            raise ManifestError("V1 PACE requires exactly one core per instance")
        for name in ("instances", "memory_mb", "pids", "output_bytes", "cpu_workers", "instance_workers"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ManifestError(f"{name} must be a positive integer")
        for name in ("cpu_seconds_per_instance", "verification_cpu_seconds", "wall_seconds_per_instance", "verification_wall_seconds"):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ManifestError(f"{name} must be positive and finite")
        if not self.solver_argv or any(not isinstance(x, str) or not x for x in self.solver_argv):
            raise ManifestError("solver argv must be a nonempty list of nonempty strings")
        if not self.verification_code_hash:
            object.__setattr__(self, "verification_code_hash", verifier_source_digest())

    @classmethod
    def standard(cls, **kwargs):
        contract = cls(**kwargs)
        if (contract.instances, contract.cpu_seconds_per_instance,
                contract.verification_cpu_seconds) != (20, 30.0, 20.0):
            raise ManifestError("standard V1 contract is 20 instances x 30 CPU seconds + 20 reserve")
        return contract

    @property
    def reserved_cpu_seconds(self) -> float:
        return self.instances * self.cpu_seconds_per_instance + self.verification_cpu_seconds

    @property
    def contract_hash(self) -> str:
        return canonical_hash(asdict(self))


@dataclass(frozen=True)
class PanelInstance:
    instance_id: str
    path: Path
    relative_path: str
    sha256: str
    reference_size: int
    n: int
    graph_hash: str


@dataclass(frozen=True)
class PanelManifest:
    panel_id: str
    split: str
    instances: tuple[PanelInstance, ...]
    seed: int = 0
    schema_version: int = 1

    @property
    def panel_hash(self) -> str:
        # Physical data root is excluded: byte-identical panels can be relocated.
        return canonical_hash({"schema_version": self.schema_version, "panel_id": self.panel_id,
                               "split": self.split, "seed": self.seed, "instances": [
            {"instance_id": item.instance_id, "path": item.relative_path,
             "sha256": item.sha256, "reference_size": item.reference_size,
             "n": item.n, "graph_hash": item.graph_hash}
            for item in self.instances]})

    def assert_disjoint(self, other: "PanelManifest") -> None:
        if ({x.graph_hash for x in self.instances} & {x.graph_hash for x in other.instances}
                or {x.path.resolve() for x in self.instances} & {x.path.resolve() for x in other.instances}):
            raise ManifestError("training, smoke and held-out panels must be disjoint")


def normalized_graph_hash(graph) -> str:
    """Identify a labeled graph independently of comments and edge line order."""
    return canonical_hash({"n": graph.n, "edges": [
        [vertex, neighbor] for vertex in range(1, graph.n + 1)
        for neighbor in sorted(graph.adjacency[vertex]) if vertex < neighbor]})


def assert_smoke_disjoint(panel: PanelManifest, graph_path: str | Path) -> None:
    graph = parse_graph(Path(graph_path).read_text(encoding="utf-8"))
    if normalized_graph_hash(graph) in {entry.graph_hash for entry in panel.instances}:
        raise ManifestError("fixed smoke graph must not occur in the formal panel")


def load_panel(path: str | Path, *, data_root: str | Path | None = None,
               expected_split: str | None = None) -> PanelManifest:
    source = Path(path).resolve(strict=True)
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (ValueError, UnicodeError) as exc:
        raise ManifestError(f"invalid panel JSON: {source}") from exc
    if not isinstance(raw, dict) or raw.get("schema_version") != 1:
        raise ManifestError("panel requires schema_version=1")
    split = raw.get("split")
    if split not in {"training", "heldout", "smoke"} or (expected_split and split != expected_split):
        raise ManifestError("incorrect or missing panel split")
    entries = raw.get("instances")
    if not isinstance(entries, list) or not entries:
        raise ManifestError("panel must have at least one ordered instance")
    root = Path(data_root).resolve(strict=True) if data_root is not None else source.parent
    instances: list[PanelInstance] = []
    seen_ids: set[str] = set()
    seen_paths: set[Path] = set()
    seen_hashes: set[str] = set()
    seen_graphs: set[str] = set()
    for item in entries:
        if not isinstance(item, dict):
            raise ManifestError("each instance must be an object")
        instance_id = item.get("instance_id")
        if not isinstance(instance_id, str) or not instance_id or instance_id in seen_ids:
            raise ManifestError("instance IDs must be nonempty and unique")
        relative = item.get("path")
        if not isinstance(relative, str) or Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise ManifestError("instance paths must stay under the declared data root")
        actual = (root / relative).resolve(strict=True)
        if not actual.is_relative_to(root) or not actual.is_file() or actual in seen_paths:
            raise ManifestError("instance is outside data root, not a file or repeated")
        data = actual.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        if item.get("sha256") != digest:
            raise ManifestError(f"checksum mismatch or missing checksum for {instance_id}")
        if digest in seen_hashes:
            raise ManifestError("a panel cannot repeat byte-identical graph instances")
        try:
            graph = parse_graph(data.decode("utf-8"))
            reference = item["reference_size"]
            validate_reference(graph.n, reference)
        except (ValueError, KeyError, UnicodeError) as exc:
            raise ManifestError(f"invalid graph/reference for {instance_id}: {exc}") from exc
        graph_hash = normalized_graph_hash(graph)
        if graph_hash in seen_graphs:
            raise ManifestError("a panel cannot repeat the same labeled graph")
        seen_ids.add(instance_id)
        seen_paths.add(actual)
        seen_hashes.add(digest)
        seen_graphs.add(graph_hash)
        instances.append(PanelInstance(instance_id, actual, Path(relative).as_posix(), digest, reference, graph.n, graph_hash))
    panel_id = raw.get("panel_id")
    if not isinstance(panel_id, str) or not panel_id:
        raise ManifestError("panel_id must be a nonempty string")
    seed = raw.get("seed", 0)
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ManifestError("panel seed must be an integer")
    return PanelManifest(panel_id, split, tuple(instances), seed)
