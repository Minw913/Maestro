"""Finding a finished episode's champion for a benchmark's own final evaluation."""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

from maestro.tasks import fields
from maestro.types import IntegrityError, MaestroError

if TYPE_CHECKING:
    from maestro.loop import ExperimentEngine


def champion_path(engine: ExperimentEngine, episode_id: str) -> tuple[str, Path]:
    """(champion hash, frozen candidate directory) from the episode's last settled checkpoint."""
    connection = engine.ledger.connection
    row = cast(tuple[str] | None, connection.execute(
        'SELECT payload FROM checkpoints WHERE episode_id=? ORDER BY round_index DESC LIMIT 1',
        (episode_id,)).fetchone())
    if row is None:
        raise MaestroError('source episode has no settled checkpoint')
    board = fields.section(fields.parse_object(row[0], 'checkpoint'), 'board')
    champion = fields.text(board, 'champion_hash')
    candidate = cast(tuple[str] | None, connection.execute(
        'SELECT payload FROM candidates WHERE hash=?', (champion,)).fetchone())
    if candidate is None:
        raise IntegrityError('champion has no recorded candidate')
    return champion, Path(fields.text(fields.parse_object(candidate[0], 'candidate'), 'candidate_path'))
