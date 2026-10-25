"""PACE 2025 Dominating Set adapter; wraps the existing manifest, runner and held-out code."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

from maestro.backends.base import BackendUnavailable
from maestro.tasks import fields
from maestro.types import IntegrityError, MaestroError

if TYPE_CHECKING:
    from maestro.config import RunConfig
    from maestro.execution.cpu_pool import GlobalCpuPool
    from maestro.execution.sandbox import ContainmentRunner
    from maestro.loop import ExperimentEngine
    from maestro.tasks.base import ArtifactStore, SmokeCheck
    from maestro.tasks.pace.manifest import EvaluationContract, PanelManifest
    from maestro.tasks.pace.runner import PaceEvaluator


def _contract(config: RunConfig) -> dict[str, object]:
    return fields.section(config.pace, 'contract')


@dataclass(frozen=True)
class LivePace:
    panel: PanelManifest
    contract: EvaluationContract
    config: RunConfig

    @property
    def panel_hash(self) -> str:
        return self.panel.panel_hash

    @property
    def contract_hash(self) -> str:
        return self.contract.contract_hash

    def evaluator(self, *, runner: ContainmentRunner | None, artifact_store: ArtifactStore | None,
                  cpu_pool: GlobalCpuPool) -> PaceEvaluator:
        from maestro.tasks.pace.runner import PaceEvaluator
        return PaceEvaluator(self.panel, self.contract, runner=runner, artifact_store=artifact_store,
                             cpu_workers=self.config.cpu_workers,
                             instance_workers=self.config.instance_workers, cpu_pool=cpu_pool)


class PaceBenchmark:
    name: str = 'pace'
    scratch_exec: bool = False
    directions: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
        ("structural_reduction", ("reduction", "initialization"),
         "Explore safe reduction rules and seed construction.",
         "Improve the initial dominating set without changing validity."),
        ("local_improvement", ("local_search", "postprocess"),
         "Explore bounded local moves and postprocessing.",
         "Improve solution quality through verified local exchanges."),
        ("search_diversification", ("restart", "tie_break"),
         "Explore restarts and deterministic tie handling.",
         "Reduce sensitivity to an unproductive search trajectory."),
        ("execution_efficiency", ("data_structure", "time_management"),
         "Explore incremental data structures and time allocation.",
         "Perform more useful search within the fixed CPU contract."),
    )

    def settings(self, config: RunConfig) -> dict[str, object]:
        return fields.mapping(config.pace, 'pace')

    def validate(self, config: RunConfig) -> None:
        if config.task:
            raise ValueError('benchmark=pace takes its settings from pace:, not task:')

    def live(self, config: RunConfig) -> LivePace:
        from maestro.tasks.pace.manifest import EvaluationContract, load_panel
        manifest = self.settings(config).get('panel_manifest')
        if not isinstance(manifest, str) or not manifest:
            raise MaestroError('execution_mode=real with benchmark=pace requires pace.panel_manifest, seed_solver, pinned runner and Coral backend')
        panel = load_panel(manifest, expected_split='training')
        contract_settings = dict(_contract(config))
        for name, value in (('cpu_workers', config.cpu_workers), ('instance_workers', config.instance_workers)):
            if name in contract_settings and contract_settings[name] != value:
                raise IntegrityError(f'Top-level {name} and PACE contract disagree')
            contract_settings[name] = value
        contract = EvaluationContract.standard(**contract_settings)
        if config.evaluation_instance_count != contract.instances:
            raise IntegrityError('Config and PACE panel contract disagree')
        return LivePace(panel, contract, config)

    def image(self, config: RunConfig) -> str:
        image = _contract(config).get('image', '')
        return image if isinstance(image, str) else ''

    def seed_solver(self, config: RunConfig) -> str:
        seed = self.settings(config).get('seed_solver', '')
        return seed if isinstance(seed, str) else ''

    def solver_argv(self, config: RunConfig) -> tuple[str, ...]:
        argv = _contract(config).get('solver_argv', ('/candidate/run',))
        if not isinstance(argv, list | tuple):
            raise fields.FieldError('pace.contract.solver_argv must be a list of strings')
        return tuple(str(part) for part in cast(list[object] | tuple[object, ...], argv))

    def smoke_check(self, config: RunConfig, *, require_file: bool = False,
                    missing_message: str = 'Configured fixed toy graph is missing') -> SmokeCheck:
        from maestro.tasks.pace.manifest import assert_smoke_disjoint, load_panel
        from maestro.tasks.pace.runner import smoke_test
        from maestro.tasks.pace.sanity import default_sanity_graph_path
        toy_setting = fields.mapping(config.coral, 'coral').get('smoke_graph')
        if toy_setting is not None and not isinstance(toy_setting, str | Path):
            raise fields.FieldError('coral.smoke_graph must be a path')
        toy = Path(toy_setting) if toy_setting is not None else default_sanity_graph_path()
        if require_file and not toy.is_file():
            raise BackendUnavailable(missing_message)
        assert_smoke_disjoint(load_panel(fields.text(config.pace, 'panel_manifest'), expected_split='training'), toy)
        image, solver_argv = self.image(config), self.solver_argv(config)

        def check(candidate_path: str | Path, runner: ContainmentRunner) -> tuple[bool, str]:
            return smoke_test(candidate_path, toy, runner, image=image, solver_argv=solver_argv)
        return check

    def evaluate_final(self, engine: ExperimentEngine, source_episode_id: str,
                       manifest_path: str) -> dict[str, object]:
        from maestro.tasks.pace.heldout import evaluate_final
        return evaluate_final(engine, source_episode_id, manifest_path)


PACE = PaceBenchmark()
