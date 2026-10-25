"""MLE-bench operations that need its own venv: carve a validation split, and grade a submission.

Run with MLE-bench's interpreter (repos/mle-bench/.venv/bin/python -I); prints one JSON object.
"""
from __future__ import annotations

import argparse
import json
import tempfile
import zipfile
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import Protocol, cast


class Leaderboard(Protocol):
    def __getitem__(self, column: str) -> list[float]: ...


class Grader(Protocol):
    def __call__(self, submission: object, answers: object) -> float | None: ...
    def is_lower_better(self, leaderboard: Leaderboard) -> bool: ...
    def rank_score(self, score: float | None, leaderboard: Leaderboard) -> dict[str, object]: ...


class Competition(Protocol):
    id: str
    answers: Path
    sample_submission: Path
    public_dir: Path
    grader: Grader
    prepare_fn: Callable[[Path, Path, Path], None]


class Registry(Protocol):
    def set_data_dir(self, data_dir: Path) -> Registry: ...
    def get_competition(self, competition_id: str) -> Competition: ...


class RegistryModule(Protocol):
    registry: Registry


class Utils(Protocol):
    def read_csv(self, path: Path) -> Leaderboard: ...
    def load_answers(self, path: Path) -> object: ...


def _zip(source: Path, archive: Path) -> None:
    with zipfile.ZipFile(archive, 'w') as out:
        out.write(source, source.name)


def _spooky(public: Path, raw: Path) -> None:
    _zip(public / 'train.csv', raw / 'train.zip')


# Competition -> rebuilds the raw Kaggle layout its prepare() reads, from MLE-bench's public train split.
RAW_VIEWS: dict[str, Callable[[Path, Path], None]] = {
    'spooky-author-identification': _spooky,
}


def competition(competition_id: str, data_dir: Path) -> Competition:
    module = cast(RegistryModule, cast(object, import_module('mlebench.registry')))
    return module.registry.set_data_dir(data_dir).get_competition(competition_id)


def split(competition_id: str, data_dir: Path, public_out: Path, private_out: Path) -> dict[str, object]:
    """Run the competition's own prepare() on its public train split, giving a validation split."""
    if competition_id not in RAW_VIEWS:
        raise SystemExit(f'{competition_id} has no validation raw view yet; add one to RAW_VIEWS')
    comp = competition(competition_id, data_dir)
    with tempfile.TemporaryDirectory(prefix='mlebench-raw-') as scratch:
        raw = Path(scratch)
        RAW_VIEWS[competition_id](comp.public_dir, raw)
        public_out.mkdir(parents=True)
        private_out.mkdir(parents=True)
        comp.prepare_fn(raw, public_out, private_out)
    for written in (private_out / comp.answers.name, public_out / comp.sample_submission.name):
        if not written.is_file():
            raise SystemExit(f'prepare() wrote no {written.name} into the validation split')
    return {'answers': comp.answers.name, 'sample_submission': comp.sample_submission.name}


def grade(competition_id: str, data_dir: Path, submission: Path, answers: Path, leaderboard: Path) -> dict[str, object]:
    """The competition's own score for `submission`, and where it lands on the Kaggle leaderboard."""
    utils = cast(Utils, cast(object, import_module('mlebench.utils')))
    comp = competition(competition_id, data_dir)
    board = utils.read_csv(leaderboard)
    lower = comp.grader.is_lower_better(board)
    score = comp.grader(utils.read_csv(submission), utils.load_answers(answers)) if submission.is_file() else None
    scores = [float(s) for s in board['score']]
    beaten = None if score is None else sum((s > score) if lower else (s < score) for s in scores) / len(scores)
    rank = comp.grader.rank_score(score, board)
    return {'score': score, 'lower_is_better': lower, 'beaten': beaten, 'teams': len(scores),
            'any_medal': bool(rank['gold_medal'] or rank['silver_medal'] or rank['bronze_medal']),
            'above_median': bool(rank['above_median'])}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    carve = commands.add_parser('split')
    marks = commands.add_parser('grade')
    for command in (carve, marks):
        _ = command.add_argument('--competition', required=True)
        _ = command.add_argument('--data-dir', required=True, type=Path, help='mlebench prepare --data-dir')
    _ = carve.add_argument('--public-out', required=True, type=Path)
    _ = carve.add_argument('--private-out', required=True, type=Path)
    for name in ('--submission', '--answers', '--leaderboard'):
        _ = marks.add_argument(name, required=True, type=Path)
    options = parser.parse_args()
    command = cast(str, options.command)
    if command == 'split':
        result = split(cast(str, options.competition), cast(Path, options.data_dir), cast(Path, options.public_out),
                       cast(Path, options.private_out))
    else:
        result = grade(cast(str, options.competition), cast(Path, options.data_dir), cast(Path, options.submission),
                       cast(Path, options.answers), cast(Path, options.leaderboard))
    print(json.dumps(result))
