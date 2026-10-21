"""Persistent, fail-closed inference budget reservations, independent of CLI turns."""
from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import threading
import time
import uuid


class BudgetExceeded(PermissionError):
    pass


class BudgetStore:
    """Reserve calls, UTF-8 request bytes and provider output-token ceilings.

    Reservations survive crashes and are conservatively consumed on ambiguous
    failures. No timeout/reset path replenishes a budget. Create one account for
    each episode/round/agent/phase; never use CLI ``max_turns`` as call accounting.
    """
    def __init__(self, path: str | Path):
        self.connection = sqlite3.connect(str(path), isolation_level=None,
                                          check_same_thread=False, timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.RLock()
        self.connection.executescript("""
        CREATE TABLE IF NOT EXISTS inference_budgets(
          account TEXT PRIMARY KEY, max_calls INTEGER NOT NULL,
          max_request_bytes INTEGER NOT NULL,max_output_tokens INTEGER NOT NULL,
          max_concurrent INTEGER NOT NULL,deadline REAL NOT NULL,
          calls INTEGER NOT NULL DEFAULT 0,request_bytes INTEGER NOT NULL DEFAULT 0,
          output_tokens INTEGER NOT NULL DEFAULT 0,active INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS inference_reservations(
          reservation_id TEXT PRIMARY KEY,account TEXT NOT NULL REFERENCES inference_budgets(account),
          request_id TEXT NOT NULL,request_bytes INTEGER NOT NULL,output_tokens INTEGER NOT NULL,
          status TEXT NOT NULL,actual_output_tokens INTEGER,UNIQUE(account,request_id));
        CREATE TABLE IF NOT EXISTS provider_receipts(
          reservation_id TEXT PRIMARY KEY REFERENCES inference_reservations(reservation_id),
          provider_request_id TEXT NOT NULL,request_hash TEXT NOT NULL,protocol TEXT NOT NULL,
          usage_json TEXT NOT NULL,input_tokens INTEGER,output_tokens INTEGER,
          cache_read_input_tokens INTEGER,cache_creation_input_tokens INTEGER,
          usage_complete INTEGER NOT NULL,recorded_at REAL NOT NULL);
        """)

    @contextmanager
    def _transaction(self):
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield
                self.connection.execute("COMMIT")
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def create(self, account: str, *, max_calls: int, max_request_bytes: int,
               max_output_tokens: int, deadline: float, max_concurrent: int = 1) -> None:
        limits = (max_calls, max_request_bytes, max_output_tokens, max_concurrent)
        if any(n <= 0 for n in limits) or deadline <= time.time():
            raise ValueError("Budget limits and future deadline are required")
        with self._transaction():
            old = self.connection.execute("SELECT * FROM inference_budgets WHERE account=?", (account,)).fetchone()
            desired = (max_calls, max_request_bytes, max_output_tokens, max_concurrent, deadline)
            if old:
                actual = tuple(old[k] for k in ("max_calls", "max_request_bytes", "max_output_tokens", "max_concurrent", "deadline"))
                if actual != desired:
                    raise BudgetExceeded("Existing budget cannot be reset or expanded")
                return
            self.connection.execute("INSERT INTO inference_budgets(account,max_calls,max_request_bytes,max_output_tokens,max_concurrent,deadline) VALUES(?,?,?,?,?,?)", (account, *desired))

    def reserve(self, account: str, request_id: str, *, request_bytes: int,
                output_tokens: int) -> str:
        if request_bytes < 0 or output_tokens <= 0:
            raise BudgetExceeded("Explicit positive output-token bound is required")
        with self._transaction():
            if self.connection.execute("SELECT 1 FROM inference_reservations WHERE account=? AND request_id=?", (account, request_id)).fetchone():
                raise BudgetExceeded("Request ID already consumed; never redispatch an ambiguous request")
            row = self.connection.execute("SELECT * FROM inference_budgets WHERE account=?", (account,)).fetchone()
            if row is None or time.time() >= row["deadline"]:
                raise BudgetExceeded("Missing or expired phase account")
            if (row["calls"] + 1 > row["max_calls"] or
                row["request_bytes"] + request_bytes > row["max_request_bytes"] or
                row["output_tokens"] + output_tokens > row["max_output_tokens"] or
                row["active"] >= row["max_concurrent"]):
                raise BudgetExceeded("Inference phase quota exhausted")
            reservation_id = uuid.uuid4().hex
            self.connection.execute("UPDATE inference_budgets SET calls=calls+1,request_bytes=request_bytes+?,output_tokens=output_tokens+?,active=active+1 WHERE account=?", (request_bytes, output_tokens, account))
            self.connection.execute("INSERT INTO inference_reservations VALUES(?,?,?,?,?,'RESERVED',NULL)", (reservation_id, account, request_id, request_bytes, output_tokens))
            return reservation_id

    def finish(self, reservation_id: str, *, actual_output_tokens: int | None = None,
               failed: bool = False) -> None:
        with self._transaction():
            row = self.connection.execute("SELECT * FROM inference_reservations WHERE reservation_id=?", (reservation_id,)).fetchone()
            if row is None:
                raise BudgetExceeded("Unknown reservation")
            if row["status"] != "RESERVED":
                return
            if failed:
                # A disconnected/timed-out client does not prove the provider
                # stopped. Keep the concurrency slot and full output ceiling
                # reserved until explicit authoritative reconciliation.
                self.connection.execute("UPDATE inference_reservations SET status='UNKNOWN',actual_output_tokens=NULL WHERE reservation_id=?",
                                        (reservation_id,))
                return
            if actual_output_tokens is not None and (type(actual_output_tokens) is not int
                    or not 0 <= actual_output_tokens <= row["output_tokens"]):
                raise BudgetExceeded("Provider exceeded the enforced output-token ceiling")
            # Refund unused output only after an authoritative success. Calls and
            # request bytes remain charged even if transport or generation fails.
            used = row["output_tokens"] if actual_output_tokens is None else actual_output_tokens
            refund = row["output_tokens"] - used
            self.connection.execute("UPDATE inference_budgets SET active=active-1,output_tokens=output_tokens-? WHERE account=?", (refund, row["account"]))
            self.connection.execute("UPDATE inference_reservations SET status='COMPLETED',actual_output_tokens=? WHERE reservation_id=?",
                                    (actual_output_tokens, reservation_id))

    def confirm_finished(self, reservation_id: str, *, proof: dict,
                         actual_output_tokens: int | None = None) -> None:
        """Operator/transport reconciliation, never a timer-based quota reset.

        The trusted caller must supply provider/executor evidence that the
        request finished or was cancelled. No agent-facing API exposes this.
        Calls stay consumed; absent actual usage retains the original ceiling.
        """
        if not isinstance(proof, dict) or proof.get("confirmed_finished") is not True or not proof.get("provider_evidence"):
            raise BudgetExceeded("Provider evidence of completion/cancellation is required")
        with self._transaction():
            row = self.connection.execute("SELECT * FROM inference_reservations WHERE reservation_id=?",
                                          (reservation_id,)).fetchone()
            if row is None or row["status"] not in {"UNKNOWN", "COMPLETED"}:
                raise BudgetExceeded("Only an unknown provider reservation can be reconciled")
            if row["status"] == "COMPLETED":
                return
            if actual_output_tokens is not None and (type(actual_output_tokens) is not int
                    or not 0 <= actual_output_tokens <= row["output_tokens"]):
                raise BudgetExceeded("Invalid reconciled provider usage")
            used = row["output_tokens"] if actual_output_tokens is None else actual_output_tokens
            self.connection.execute("UPDATE inference_budgets SET active=active-1,output_tokens=output_tokens-? WHERE account=?",
                                    (row["output_tokens"] - used, row["account"]))
            self.connection.execute("UPDATE inference_reservations SET status='COMPLETED',actual_output_tokens=? WHERE reservation_id=?",
                                    (actual_output_tokens, reservation_id))
            self.connection.execute("""CREATE TABLE IF NOT EXISTS inference_reconciliations(
                reservation_id TEXT PRIMARY KEY REFERENCES inference_reservations(reservation_id),
                proof_json TEXT NOT NULL,recorded_at REAL NOT NULL)""")
            self.connection.execute("INSERT INTO inference_reconciliations VALUES(?,?,?)",
                                    (reservation_id, json.dumps(proof, sort_keys=True, allow_nan=False), time.time()))

    @staticmethod
    def normalize_usage(usage: dict, *, protocol: str = "") -> dict:
        """Normalize actual provider usage without inventing missing values.

        Anthropic top-level cache read/write tokens are *additional* to ordinary
        input_tokens. OpenAI cached_tokens is already a subset of prompt/input.
        The canonical input_tokens includes all input tokens in both protocols;
        cache fields remain separate cost categories and are never summed twice.
        """
        invalid = []
        if not isinstance(usage, dict):
            usage = {}
            invalid.append("usage_envelope")
        def token(value, name):
            if value is None:
                return None
            if type(value) is not int or value < 0:
                invalid.append(name)
                return None
            return value
        anthropic = protocol.startswith("/v1/messages") or any(
            name in usage for name in ("cache_read_input_tokens", "cache_creation_input_tokens"))
        ordinary = token(usage.get("input_tokens", usage.get("prompt_tokens")), "input_tokens")
        output = token(usage.get("output_tokens", usage.get("completion_tokens")), "output_tokens")
        if anthropic:
            cache_read = token(usage.get("cache_read_input_tokens", 0), "cache_read_input_tokens")
            cache_write = token(usage.get("cache_creation_input_tokens", 0), "cache_creation_input_tokens")
            total_input = (ordinary + cache_read + cache_write
                           if all(value is not None for value in (ordinary, cache_read, cache_write)) else None)
        else:
            details = usage.get("input_tokens_details", usage.get("prompt_tokens_details", {})) or {}
            if not isinstance(details, dict):
                details = {}
                invalid.append("input_tokens_details")
            cache_read = token(details.get("cached_tokens", 0), "cached_tokens")
            cache_write = 0
            total_input = ordinary
            if cache_read is not None and ordinary is not None and cache_read > ordinary:
                invalid.append("cached_tokens_exceed_input")
        return {"input_tokens": total_input, "output_tokens": output,
                "cache_read_input_tokens": cache_read, "cache_creation_input_tokens": cache_write,
                "usage_complete": not invalid and total_input is not None and output is not None,
                "invalid_fields": invalid, "input_semantics": "includes_cache"}

    def save_receipt(self, reservation_id: str, usage: dict, request_hash: str,
                     provider_request_id: str = "", *, protocol: str = "") -> dict:
        """Persist authoritative usage independently of conservative quota charges.

        Byte hashes identify requests without storing prompts or credentials.
        Replaying an identical receipt is idempotent; changed data under the same
        reservation is rejected. Failed/no-usage requests remain visibly unknown.
        """
        if len(request_hash) != 64 or any(ch not in "0123456789abcdef" for ch in request_hash):
            raise ValueError("provider receipt requires a SHA-256 request hash")
        normalized = self.normalize_usage(usage, protocol=protocol)
        text = json.dumps(usage, sort_keys=True, separators=(",", ":"), allow_nan=False)
        request_identity = str(provider_request_id or "")[:1024]
        values = (reservation_id, request_identity, request_hash, protocol, text,
                  normalized["input_tokens"], normalized["output_tokens"],
                  normalized["cache_read_input_tokens"], normalized["cache_creation_input_tokens"],
                  int(normalized["usage_complete"]))
        with self._transaction():
            if self.connection.execute("SELECT 1 FROM inference_reservations WHERE reservation_id=?",
                                       (reservation_id,)).fetchone() is None:
                raise BudgetExceeded("Cannot attach provider usage to an unknown reservation")
            old = self.connection.execute("SELECT * FROM provider_receipts WHERE reservation_id=?",
                                          (reservation_id,)).fetchone()
            if old:
                fields = ("reservation_id", "provider_request_id", "request_hash", "protocol", "usage_json",
                          "input_tokens", "output_tokens", "cache_read_input_tokens",
                          "cache_creation_input_tokens", "usage_complete")
                if tuple(old[key] for key in fields) != values:
                    raise BudgetExceeded("Conflicting provider receipt for the same inference reservation")
                return normalized
            self.connection.execute("INSERT INTO provider_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                                    (*values, time.time()))
        return normalized

    def usage(self, account: str) -> dict:
        """Actual known usage, unknown markers, and requested ceilings separately."""
        with self._lock:
            snapshot = self.snapshot(account)
            rows = self.connection.execute("""SELECT r.output_tokens AS ceiling,r.status AS reservation_status,p.*
                FROM inference_reservations r LEFT JOIN provider_receipts p
                ON p.reservation_id=r.reservation_id WHERE r.account=?""", (account,)).fetchall()
            complete = len(rows) == snapshot["calls"] and all(
                row["usage_complete"] == 1 and row["reservation_status"] == "COMPLETED" for row in rows)
            known_input = sum(row["input_tokens"] or 0 for row in rows)
            known_output = sum(row["output_tokens"] or 0 for row in rows)
            return {"calls": snapshot["calls"], "provider_receipts": sum(row["usage_complete"] is not None for row in rows),
                    "input_tokens": known_input if complete else None,
                    "output_tokens": known_output if complete else None,
                    "known_input_tokens": known_input, "known_output_tokens": known_output,
                    "cache_read_input_tokens": sum(row["cache_read_input_tokens"] or 0 for row in rows),
                    "cache_creation_input_tokens": sum(row["cache_creation_input_tokens"] or 0 for row in rows),
                    "reserved_output_tokens": sum(row["ceiling"] for row in rows),
                    "charged_output_tokens": snapshot["output_tokens"],
                    "max_output_tokens": snapshot["max_output_tokens"],
                    "usage_complete": complete, "unknown_requests": sum(
                        row["usage_complete"] != 1 or row["reservation_status"] != "COMPLETED" for row in rows),
                    "unresolved_requests": snapshot["active"]}

    def snapshot(self, account: str) -> dict:
        # SQLite cursor metadata must not race a different statement on this
        # shared connection, including the read preceding account creation.
        with self._lock:
            row = self.connection.execute("SELECT * FROM inference_budgets WHERE account=?", (account,)).fetchone()
            if row is None:
                raise BudgetExceeded("Unknown account")
            return dict(row)

    def close(self):
        self.connection.close()
