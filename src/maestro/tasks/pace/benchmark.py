"""PACE 2025 Dominating Set adapter; wraps the existing manifest, runner and held-out code."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from maestro.backends.base import BackendUnavailable
from maestro.types import IntegrityError, MaestroError


@dataclass(frozen=True)
class LivePace:
    panel: object
    contract: object
    config: object

    @property
    def panel_hash(self) -> str:
        return self.panel.panel_hash

    @property
    def contract_hash(self) -> str:
        return self.contract.contract_hash

    def evaluator(self, *, runner, artifact_store, cpu_pool):
        from maestro.tasks.pace.runner import PaceEvaluator
        return PaceEvaluator(self.panel, self.contract, runner=runner, artifact_store=artifact_store,
                             cpu_workers=self.config.cpu_workers,
                             instance_workers=self.config.instance_workers, cpu_pool=cpu_pool)


class PaceBenchmark:
    name = 'pace'
    directions = (
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

    def settings(self, config) -> dict:
        return config.pace

    def live(self, config) -> LivePace:
        from maestro.tasks.pace.manifest import EvaluationContract, load_panel
        manifest = config.pace.get('panel_manifest')
        if not manifest:
            raise MaestroError('execution_mode=real with benchmark=pace requires pace.panel_manifest, seed_solver, pinned runner and Coral backend')
        panel = load_panel(manifest, expected_split='training')
        contract_settings = dict(config.pace.get('contract', {}))
        for name in ('cpu_workers', 'instance_workers'):
            value = getattr(config, name)
            if name in contract_settings and contract_settings[name] != value:
                raise IntegrityError(f'Top-level {name} and PACE contract disagree')
            contract_settings[name] = value
        contract = EvaluationContract.standard(**contract_settings)
        if config.evaluation_instance_count != contract.instances:
            raise IntegrityError('Config and PACE panel contract disagree')
        return LivePace(panel, contract, config)

    def image(self, config) -> str:
        return config.pace.get('contract', {}).get('image', '')

    def seed_solver(self, config) -> str:
        return config.pace.get('seed_solver', '')

    def solver_argv(self, config) -> tuple[str, ...]:
        return tuple(config.pace.get('contract', {}).get('solver_argv', ('/candidate/run',)))

    def smoke_check(self, config, *, require_file=False, missing_message='Configured fixed toy graph is missing'):
        from maestro.tasks.pace.manifest import assert_smoke_disjoint, load_panel
        from maestro.tasks.pace.runner import smoke_test
        from maestro.tasks.pace.sanity import default_sanity_graph_path
        toy = Path(config.coral.get('smoke_graph', default_sanity_graph_path()))
        if require_file and not toy.is_file():
            raise BackendUnavailable(missing_message)
        assert_smoke_disjoint(load_panel(config.pace['panel_manifest'], expected_split='training'), toy)
        image = self.image(config)
        solver_argv = self.solver_argv(config)
        return lambda candidate_path, runner: smoke_test(candidate_path, toy, runner,
                                                         image=image, solver_argv=solver_argv)

    def evaluate_final(self, engine, source_episode_id, manifest_path):
        from maestro.tasks.pace.heldout import evaluate_final
        return evaluate_final(engine, source_episode_id, manifest_path)


PACE = PaceBenchmark()
