"""SQLite storage and batch validation for the exposure ledger.

Data model
----------
Each imported CSV row keeps its original cell values (``raw_json``, keyed by
the exact header names from the uploaded file) and its raw start token
(``start_raw``).  A start token is either UTC text - which may use
``23:59:60`` - or an integer count of real seconds since
2000-01-01 00:00:00 UTC.

``start_real`` is the canonical instant used for range queries.  Text rows
derive it with the leap table active at import; real-second rows use the
submitted integer directly.  Both are re-derived or re-tagged when the leap
table changes.

Leap tables are immutable, versioned rows.  Replacement validates the whole
candidate table with the whole stored set in one transaction.  If any stored
UTC text would become invalid, the swap is rejected, the offending exposure
ids are returned, and the old table stays active.

Writers serialise with an in-process lock and ``BEGIN IMMEDIATE``.  Readers
use a WAL snapshot and load exactly one leap-table version per request, so a
day fetch always uses one consistent table even while a replacement or import
is running.

Coverage summaries
------------------
``day_summaries`` stores one group-independent coverage map per UTC day: the
maximal ranges of that day on which the set of exposing telescopes is
constant.  Within a telescope its exposures are unioned first, so overlapping
exposures never count one telescope twice.  Any telescope group is then a
cheap fold over the segments, without reading exposure rows again.

An import deletes the summaries for exactly the days its rows intersect and
bumps those days' generations in the same transaction.  A leap-table swap
deletes all summaries.  Rebuilds validate their captured generation and leap
version before writing; a change committed in between raises
:class:`SummaryStale`, so the numbers are never written over newer data.
"""

from __future__ import annotations

import csv
import datetime as _dt
import io
import json
import re
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path

from .timecode import (
    DEFAULT_LEAP_TABLE,
    EPOCH,
    LeapTable,
    TimecodeError,
    day_range_real,
    fmt_utc,
    format_leap_table,
    parse_leap_table,
    parse_utc,
    real_to_utc,
    utc_to_real,
)

CSV_FIELDS = ("telescope", "exposure_id", "start", "duration_s")
MIN_DURATION = 1
MAX_DURATION = 3600
MAX_SWAP_REPORT = 500
EARLIEST_DAY = EPOCH.date()
LATEST_DAY = _dt.date(9999, 12, 30)
INTEGER_RE = re.compile(r"^[+-]?\d+$")


def _iso_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class LedgerError(Exception):
    """Base class for user-facing ledger errors."""


class ValidationError(LedgerError):
    """A submitted batch is invalid; nothing was written."""

    def __init__(self, message: str, details: list[str] | None = None):
        super().__init__(message)
        self.details = list(details or [])


class LeapSwapRejected(LedgerError):
    """The candidate leap table would invalidate stored UTC text."""

    def __init__(self, offending: list[dict]):
        super().__init__(
            "the new leap table would invalidate stored UTC times; "
            "the previous table remains active"
        )
        self.offending = offending


@dataclass(frozen=True)
class ParsedBatch:
    headers: tuple[str, ...]
    rows: list[dict]


@dataclass(frozen=True)
class DayReport:
    day: _dt.date
    leap_version: int
    leap_day: bool
    day_start_utc: str
    day_end_utc: str
    day_last_utc: str
    day_start_real: int
    day_end_real: int
    day_seconds: int
    rows: list[dict]
    total_rows: int
    total_seconds: int


@dataclass(frozen=True)
class DayCoverage:
    """Group-independent coverage of one UTC day.

    ``segments`` is a sorted, non-overlapping tuple of
    ``(start_offset, end_offset, telescope_indexes)`` with offsets measured
    from the day's first real second.  ``telescope_indexes`` refers to
    :attr:`telescopes`.  Any second not covered by a segment has zero
    telescopes exposing.
    """

    day: _dt.date
    leap_version: int
    epoch: int
    day_start_real: int
    day_end_real: int
    leap_day: bool
    day_start_utc: str
    day_end_utc: str
    day_last_utc: str
    telescopes: tuple[str, ...]
    segments: tuple[tuple[int, int, tuple[int, ...]], ...]
    segment_times: tuple[tuple[str, str], ...]
    built_at: str
    source: str  # "cache" or "rebuilt"

    @property
    def day_seconds(self) -> int:
        return self.day_end_real - self.day_start_real

    @property
    def covered_seconds(self) -> int:
        return sum(end - start for start, end, _mask in self.segments)

    @property
    def segment_count(self) -> int:
        return len(self.segments)

    @property
    def telescope_count(self) -> int:
        return len(self.telescopes)


@dataclass(frozen=True)
class GroupStats:
    """Simultaneous-exposure histogram for one telescope group."""

    telescopes: tuple[str, ...]
    day_seconds: int
    levels: tuple[dict, ...]
    observed_seconds: int
    joint_seconds: int

    @property
    def max_simultaneous(self) -> int:
        return len(self.telescopes)


class SummaryStale(LedgerError):
    """A committed change touched the day before its summary was written."""


def _connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    # WAL lets day reads proceed on a snapshot while an import or leap-table
    # replacement writes.  The mode persists in the database file.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS leap_tables (
    version    INTEGER PRIMARY KEY,
    body       TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS import_batches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at   TEXT NOT NULL,
    source_name  TEXT NOT NULL,
    row_count    INTEGER NOT NULL,
    headers_json TEXT NOT NULL,
    leap_version INTEGER NOT NULL REFERENCES leap_tables(version)
);

CREATE TABLE IF NOT EXISTS exposures (
    exposure_id  TEXT PRIMARY KEY,
    telescope    TEXT NOT NULL,
    start_source TEXT NOT NULL CHECK (start_source IN ('utc', 'real')),
    start_raw    TEXT NOT NULL,
    start_real   INTEGER NOT NULL,
    duration_s   INTEGER NOT NULL CHECK (duration_s BETWEEN 1 AND 3600),
    raw_json     TEXT NOT NULL,
    leap_version INTEGER NOT NULL REFERENCES leap_tables(version),
    batch_id     INTEGER NOT NULL REFERENCES import_batches(id)
);

CREATE INDEX IF NOT EXISTS exposures_start_real ON exposures(start_real);
CREATE INDEX IF NOT EXISTS exposures_start_real_id ON exposures(start_real, exposure_id);
CREATE INDEX IF NOT EXISTS exposures_batch ON exposures(batch_id);

-- One group-independent coverage summary per UTC day.  ``leap_version`` ties
-- the summary to the leap table that produced its day boundaries; a new table
-- version makes every older summary unusable without touching these rows.
CREATE TABLE IF NOT EXISTS day_summaries (
    day            TEXT PRIMARY KEY,
    leap_version   INTEGER NOT NULL,
    day_start_real INTEGER NOT NULL,
    day_end_real   INTEGER NOT NULL,
    telescopes_json TEXT NOT NULL,
    segments_json  TEXT NOT NULL,
    built_at       TEXT NOT NULL
);

-- Bumped inside the same transaction as every import for each day the batch
-- actually intersects.  A rebuild compares the generation captured with its
-- snapshot against this value before writing, so a change committed mid-build
-- is detected instead of being silently overwritten with stale numbers.
CREATE TABLE IF NOT EXISTS day_generations (
    day        TEXT PRIMARY KEY,
    generation INTEGER NOT NULL
);
"""


# ---------------------------------------------------------------------------
# CSV parsing and row validation
# ---------------------------------------------------------------------------


def _blank(text: str | None) -> bool:
    return text is None or not text.strip()


def _too_many_digits(text: str) -> bool:
    """Guard ``int()`` against pathological digit strings.

    Python 3.11 refuses to parse very long integer strings; the supported
    instant range needs at most 12 digits, so anything past 19 is out of range
    regardless and can be rejected without converting.
    """

    return len(text.lstrip("+-")) > 19


def _sniff_header(fieldnames: list[str]) -> dict[str, str]:
    """Map exact original header names to canonical field names.

    The whole header is validated at once: unknown, duplicate or missing
    columns reject the batch before any row is looked at.
    """

    cleaned = [(name or "").strip() for name in fieldnames]
    if len(set(cleaned)) != len(cleaned):
        raise ValidationError(
            "CSV rejected", ["the header contains duplicate column names"]
        )
    if any(not name for name in cleaned):
        raise ValidationError(
            "CSV rejected", ["the header contains an empty column name"]
        )

    if set(cleaned) != set(CSV_FIELDS):
        unknown = [name for name in cleaned if name not in CSV_FIELDS]
        missing = [name for name in CSV_FIELDS if name not in cleaned]
        details = []
        if missing:
            details.append(f"missing column(s): {', '.join(missing)}")
        if unknown:
            details.append(f"unexpected column(s): {', '.join(unknown)}")
        details.append(f"expected header: {','.join(CSV_FIELDS)}")
        raise ValidationError("CSV rejected", details)
    return {name: name for name in cleaned}


def parse_csv(text: str) -> ParsedBatch:
    """Parse a CSV batch into raw rows without validating start times."""

    if _blank(text):
        raise ValidationError("CSV rejected", ["no CSV text was supplied"])
    text = text.lstrip("﻿")
    reader = csv.DictReader(io.StringIO(text, newline=""))
    if reader.fieldnames is None:
        raise ValidationError("CSV rejected", ["the CSV is empty"])
    mapping = _sniff_header(reader.fieldnames)
    headers = tuple((name or "").strip() for name in reader.fieldnames)

    rows: list[dict] = []
    for record in reader:
        if None in record or len(record) != len(reader.fieldnames):
            raise ValidationError(
                "CSV rejected",
                [f"line {reader.line_num}: wrong number of columns"],
            )
        original = {
            (key or "").strip(): "" if value is None else value
            for key, value in record.items()
        }
        canonical = {mapping[key]: value for key, value in original.items()}
        if not any(value.strip() for value in canonical.values()):
            continue  # blank line
        rows.append(
            {
                "line": reader.line_num,
                "raw": canonical,
                "raw_original": original,
            }
        )
    if not rows:
        raise ValidationError("CSV rejected", ["the CSV has a header but no data rows"])
    return ParsedBatch(headers=headers, rows=rows)


def _validate_row(row: dict, table: LeapTable) -> list[str]:
    """Validate one raw row against the leap table selected at submit time."""

    line = row["line"]
    raw = row["raw"]
    errors: list[str] = []
    telescope = raw["telescope"].strip()
    exposure_id = raw["exposure_id"].strip()
    start_text = raw["start"].strip()
    duration_text = raw["duration_s"].strip()

    if not telescope:
        errors.append(f"line {line}: telescope is required")
    elif len(telescope) > 64:
        errors.append(f"line {line}: telescope is longer than 64 characters")
    if not exposure_id:
        errors.append(f"line {line}: exposure_id is required")
    elif len(exposure_id) > 128:
        errors.append(f"line {line}: exposure_id is longer than 128 characters")

    start_real: int | None = None
    start_source: str | None = None
    if not start_text:
        errors.append(f"line {line}: start is required")
    elif INTEGER_RE.match(start_text):
        if _too_many_digits(start_text):
            errors.append(f"line {line}: start real seconds are too large")
        else:
            value = int(start_text)
            if value < 0:
                errors.append(
                    f"line {line}: real seconds must be at or after "
                    "2000-01-01 00:00:00 UTC (>= 0)"
                )
            else:
                try:
                    start_day = real_to_utc(value, table)[0].date()
                except (TimecodeError, OverflowError):
                    start_day = _dt.date.max
                if start_day > LATEST_DAY:
                    errors.append(
                        f"line {line}: start is outside the supported range "
                        f"(latest day is {LATEST_DAY.isoformat()})"
                    )
                else:
                    start_real = value
                    start_source = "real"
    else:
        try:
            parse_utc(start_text, table)
            start_real = utc_to_real(start_text, table)
        except TimecodeError as exc:
            errors.append(f"line {line}: {exc}")
        else:
            if start_real < 0:
                errors.append(f"line {line}: start is before 2000-01-01 00:00:00 UTC")
            elif parse_utc(start_text, table).date() > LATEST_DAY:
                errors.append(
                    f"line {line}: start is outside the supported range "
                    f"(latest day is {LATEST_DAY.isoformat()})"
                )
            else:
                start_source = "utc"

    duration: int | None = None
    if not duration_text:
        errors.append(f"line {line}: duration_s is required")
    elif not duration_text.isascii() or not duration_text.isdigit():
        errors.append(
            f"line {line}: duration_s must be a whole number of seconds, "
            f"got {duration_text!r}"
        )
    elif _too_many_digits(duration_text):
        errors.append(
            f"line {line}: duration_s must be between {MIN_DURATION} and "
            f"{MAX_DURATION}, got {duration_text[:20]}…"
        )
    else:
        value = int(duration_text)
        if not (MIN_DURATION <= value <= MAX_DURATION):
            errors.append(
                f"line {line}: duration_s must be between {MIN_DURATION} and "
                f"{MAX_DURATION}, got {value}"
            )
        else:
            duration = value

    row.update(
        {
            "telescope": telescope,
            "exposure_id": exposure_id,
            "start_source": start_source,
            "start_raw": start_text,
            "start_real": start_real,
            "duration_s": duration,
            "raw_json": json.dumps(
                row["raw_original"], ensure_ascii=False, sort_keys=True
            ),
        }
    )
    return errors


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------


class Ledger:
    """One SQLite database, safe for Flask's threaded request handling."""

    def __init__(self, path: str | Path, initial_table: LeapTable | None = None):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        with self._write_lock:
            conn = _connect(self.path)
            try:
                conn.executescript(SCHEMA)
                conn.execute("BEGIN IMMEDIATE")
                try:
                    count = conn.execute("SELECT COUNT(*) FROM leap_tables").fetchone()[
                        0
                    ]
                    if count == 0:
                        table = initial_table or DEFAULT_LEAP_TABLE
                        conn.execute(
                            "INSERT OR IGNORE INTO leap_tables(version, body, created_at) "
                            "VALUES(1, ?, ?)",
                            (format_leap_table(table), _iso_now()),
                        )
                    conn.execute("COMMIT")
                except Exception:
                    if conn.in_transaction:
                        conn.execute("ROLLBACK")
                    raise
            finally:
                conn.close()

    # -- read side ---------------------------------------------------------

    def active_table(self) -> tuple[int, LeapTable]:
        conn = _connect(self.path)
        try:
            return self._active_table_tx(conn)
        finally:
            conn.close()

    @staticmethod
    def _active_table_tx(conn: sqlite3.Connection) -> tuple[int, LeapTable]:
        row = conn.execute(
            "SELECT version, body FROM leap_tables ORDER BY version DESC LIMIT 1"
        ).fetchone()
        return int(row["version"]), parse_leap_table(row["body"])

    def leap_history(self) -> list[dict]:
        conn = _connect(self.path)
        try:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT version, body, created_at FROM leap_tables ORDER BY version"
                )
            ]
        finally:
            conn.close()

    def stats(self) -> dict:
        conn = _connect(self.path)
        try:
            exposures = conn.execute("SELECT COUNT(*) FROM exposures").fetchone()[0]
            batches = conn.execute("SELECT COUNT(*) FROM import_batches").fetchone()[0]
            version = conn.execute("SELECT MAX(version) FROM leap_tables").fetchone()[0]
            return {
                "exposures": int(exposures),
                "batches": int(batches),
                "leap_version": int(version),
            }
        finally:
            conn.close()

    def day_report(self, day: _dt.date) -> DayReport:
        """Fetch exposures intersecting one UTC civil day.

        One read transaction supplies both the leap table and the rows, so the
        day boundaries, SQL filter, clipping and formatting all use the same
        table version even if a replacement commits concurrently.  Only rows
        whose start lies within ``[day_start - 3600, day_end)`` are read.
        """

        conn = _connect(self.path)
        try:
            conn.execute("BEGIN")
            version, table, day_start, day_end, rows = self._load_day(conn, day)
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        return self._report_from(day, version, table, day_start, day_end, rows)

    def _load_day(
        self, conn: sqlite3.Connection, day: _dt.date
    ) -> tuple[int, LeapTable, int, int, list[sqlite3.Row]]:
        if not isinstance(day, _dt.date):
            raise ValidationError("date rejected", ["choose a UTC date"])
        if day < EARLIEST_DAY:
            raise ValidationError(
                "date rejected", ["the ledger starts at 2000-01-01 UTC"]
            )
        if day > LATEST_DAY:
            raise ValidationError(
                "date rejected",
                [f"the latest supported day is {LATEST_DAY.isoformat()}"],
            )
        version, table = self._active_table_tx(conn)
        day_start, day_end = day_range_real(day, table)
        rows = conn.execute(
            "SELECT exposure_id, telescope, start_source, start_raw, start_real, duration_s "
            "FROM exposures "
            "WHERE start_real >= ? AND start_real < ? "
            "AND start_real + duration_s > ? "
            "ORDER BY start_real, exposure_id",
            (day_start - MAX_DURATION, day_end, day_start),
        ).fetchall()
        return version, table, day_start, day_end, rows

    @staticmethod
    def _report_from(
        day: _dt.date,
        version: int,
        table: LeapTable,
        day_start: int,
        day_end: int,
        rows: list[sqlite3.Row],
    ) -> DayReport:
        clipped = [_clip_row(row, day_start, day_end, table) for row in rows]
        start_dt, start_leap = real_to_utc(day_start, table)
        last_dt, last_leap = real_to_utc(day_end - 1, table)
        return DayReport(
            day=day,
            leap_version=version,
            leap_day=day in table,
            day_start_utc=fmt_utc(start_dt, start_leap),
            day_end_utc=fmt_utc(*real_to_utc(day_end, table)),
            day_last_utc=fmt_utc(last_dt, last_leap),
            day_start_real=day_start,
            day_end_real=day_end,
            day_seconds=day_end - day_start,
            rows=clipped,
            total_rows=len(clipped),
            total_seconds=sum(row["window_seconds"] for row in clipped),
        )

    def all_rows(self) -> list[dict]:
        conn = _connect(self.path)
        try:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT exposure_id, telescope, start_source, start_raw, start_real, "
                    "duration_s, leap_version, batch_id FROM exposures ORDER BY exposure_id"
                )
            ]
        finally:
            conn.close()

    # -- day coverage summaries -------------------------------------------

    @staticmethod
    def _generation(conn: sqlite3.Connection, day: _dt.date) -> int:
        row = conn.execute(
            "SELECT generation FROM day_generations WHERE day = ?",
            (day.isoformat(),),
        ).fetchone()
        return int(row["generation"]) if row else 0

    @staticmethod
    def _check_day(day) -> _dt.date:
        if not isinstance(day, _dt.date):
            raise ValidationError("date rejected", ["choose a UTC date"])
        if day < EARLIEST_DAY:
            raise ValidationError(
                "date rejected", ["the ledger starts at 2000-01-01 UTC"]
            )
        if day > LATEST_DAY:
            raise ValidationError(
                "date rejected",
                [f"the latest supported day is {LATEST_DAY.isoformat()}"],
            )
        return day

    def coverage(self, day: _dt.date) -> DayCoverage:
        """Return the coverage summary for one UTC day, rebuilding if needed.

        A cached summary is reused only when it was produced by the active leap
        table version.  A rebuild snapshots the day generations and recomputes
        from the exposure rows in the same read transaction, then re-checks the
        generations in a short write transaction before storing.  If an import
        touching this day committed in between, the result is discarded and
        :class:`SummaryStale` is raised; a retry starts from the new committed
        state.
        """

        day = self._check_day(day)

        conn = _connect(self.path)
        try:
            conn.execute("BEGIN")
            version, table = self._active_table_tx(conn)
            cached = conn.execute(
                "SELECT * FROM day_summaries WHERE day = ?", (day.isoformat(),)
            ).fetchone()
            if cached is not None and int(cached["leap_version"]) == version:
                result = _coverage_from_row(cached, table, day, "cache")
                conn.execute("COMMIT")
                return result

            day_start, day_end = day_range_real(day, table)
            generation = self._generation(conn, day)
            rows = conn.execute(
                "SELECT exposure_id, telescope, start_real, duration_s FROM exposures "
                "WHERE start_real >= ? AND start_real < ? "
                "AND start_real + duration_s > ? ORDER BY start_real, exposure_id",
                (day_start - MAX_DURATION, day_end, day_start),
            ).fetchall()
            # The table was read in the same snapshot as the rows; its version
            # is re-checked before the summary is stored.
            connection_version = conn.execute(
                "SELECT MAX(version) FROM leap_tables"
            ).fetchone()[0]
            if int(connection_version) != version:  # pragma: no cover - defensive
                raise SummaryStale()
            built_at = _iso_now()
            coverage = _build_coverage(
                day, version, table, rows, day_start, day_end, built_at
            )
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        with self._write_lock:
            conn = _connect(self.path)
            try:
                conn.execute("BEGIN IMMEDIATE")
                if self._generation(conn, day) != generation:
                    raise SummaryStale()
                # A leap-table swap invalidates summaries wholesale; if one
                # committed after our snapshot, this result used stale day
                # boundaries and must not be stored.
                active_version, _active_table = self._active_table_tx(conn)
                if active_version != version:
                    raise SummaryStale()
                conn.execute(
                    "INSERT INTO day_summaries(day, leap_version, day_start_real, "
                    "day_end_real, telescopes_json, segments_json, built_at) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(day) DO UPDATE SET "
                    "leap_version = excluded.leap_version, "
                    "day_start_real = excluded.day_start_real, "
                    "day_end_real = excluded.day_end_real, "
                    "telescopes_json = excluded.telescopes_json, "
                    "segments_json = excluded.segments_json, "
                    "built_at = excluded.built_at",
                    (
                        day.isoformat(),
                        version,
                        day_start,
                        day_end,
                        json.dumps(list(coverage.telescopes), ensure_ascii=False),
                        json.dumps(
                            [
                                [start, end, list(mask)]
                                for start, end, mask in coverage.segments
                            ],
                            ensure_ascii=False,
                        ),
                        built_at,
                    ),
                )
                conn.execute("COMMIT")
            except Exception:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
        return coverage

    def group_stats(
        self, day: _dt.date, telescopes: list[str]
    ) -> tuple[DayCoverage, GroupStats]:
        """Simultaneous-exposure histogram for one telescope group.

        Only the day's coverage summary is read.  Overlapping exposures of the
        same telescope were already unioned when the summary was built, so a
        telescope contributes at most one to the count at any second.
        """

        coverage = self.coverage(day)
        return coverage, compute_group_stats(coverage, telescopes)

    # -- write side --------------------------------------------------------

    def import_csv(self, text: str, source_name: str = "upload.csv") -> dict:
        """Validate and insert one CSV batch under the active leap table.

        The batch is all-or-nothing.  Duplicate ids inside the batch and ids
        already stored reject the whole batch.
        """

        batch = parse_csv(text)
        with self._write_lock:
            conn = _connect(self.path)
            try:
                conn.execute("BEGIN IMMEDIATE")
                version, table = self._active_table_tx(conn)
                errors: list[str] = []
                for row in batch.rows:
                    errors.extend(_validate_row(row, table))
                seen: dict[str, int] = {}
                for row in batch.rows:
                    prior = seen.setdefault(row["exposure_id"], row["line"])
                    if prior != row["line"]:
                        errors.append(
                            f"line {row['line']}: exposure id {row['exposure_id']!r} "
                            f"duplicates line {prior}"
                        )
                if errors:
                    raise ValidationError(
                        "import rejected; nothing was written", errors
                    )

                cursor = conn.execute(
                    "INSERT INTO import_batches(created_at, source_name, row_count, "
                    "headers_json, leap_version) VALUES(?, ?, ?, ?, ?)",
                    (
                        _iso_now(),
                        source_name or "upload.csv",
                        len(batch.rows),
                        json.dumps(list(batch.headers), ensure_ascii=False),
                        version,
                    ),
                )
                batch_id = int(cursor.lastrowid)
                try:
                    conn.executemany(
                        "INSERT INTO exposures(exposure_id, telescope, start_source, start_raw, "
                        "start_real, duration_s, raw_json, leap_version, batch_id) "
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            (
                                row["exposure_id"],
                                row["telescope"],
                                row["start_source"],
                                row["start_raw"],
                                row["start_real"],
                                row["duration_s"],
                                row["raw_json"],
                                version,
                                batch_id,
                            )
                            for row in batch.rows
                        ],
                    )
                except sqlite3.IntegrityError as exc:
                    if "exposures.exposure_id" in str(exc):
                        raise ValidationError(
                            "import rejected; nothing was written",
                            [
                                "an exposure id in this batch already exists in the ledger; "
                                "the whole batch was rolled back"
                            ],
                        ) from exc
                    raise ValidationError(
                        "import rejected; nothing was written", [str(exc)]
                    ) from exc

                # Invalidate only the summaries of days this batch actually
                # touches.  This deletion and the generation bump commit with
                # the batch, so a rolled-back import leaves summaries intact
                # and an in-flight rebuild notices the change.
                affected: set[str] = set()
                for row in batch.rows:
                    for day in _intersected_days(
                        row["start_real"], row["duration_s"], table
                    ):
                        affected.add(day.isoformat())
                for day_text in sorted(affected):
                    conn.execute(
                        "DELETE FROM day_summaries WHERE day = ?", (day_text,)
                    )
                    conn.execute(
                        "INSERT INTO day_generations(day, generation) VALUES(?, 1) "
                        "ON CONFLICT(day) DO UPDATE SET generation = generation + 1",
                        (day_text,),
                    )
                conn.execute("COMMIT")
                return {
                    "batch_id": batch_id,
                    "row_count": len(batch.rows),
                    "leap_version": version,
                    "leap_days": len(table.dates),
                    "affected_days": sorted(affected),
                }
            except Exception:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def replace_leap_table(self, text: str) -> dict:
        """Validate and activate a complete positive-leap date table.

        The candidate is applied to every stored text-sourced start time in one
        transaction.  A single invalid stored time aborts the swap, reports the
        offending exposure ids, and leaves the previous table in force.  Rows
        submitted as absolute real seconds are never invalidated, but their
        formatting uses the new table afterwards.
        """

        try:
            new_table = parse_leap_table(text)
        except TimecodeError as exc:
            raise ValidationError(
                "leap table rejected; nothing was changed", [str(exc)]
            ) from exc

        with self._write_lock:
            conn = _connect(self.path)
            try:
                conn.execute("BEGIN IMMEDIATE")
                old_version, old_table = self._active_table_tx(conn)

                invalid: list[dict] = []
                text_updates: list[tuple[int, int, str]] = []
                for row in conn.execute(
                    "SELECT exposure_id, telescope, start_raw FROM exposures "
                    "WHERE start_source = 'utc' ORDER BY exposure_id"
                ):
                    try:
                        parse_utc(row["start_raw"], new_table)
                        real = utc_to_real(row["start_raw"], new_table)
                        if real < 0:
                            raise TimecodeError("stored time is before 2000-01-01")
                        real_to_utc(real, new_table)  # ensure it is representable
                    except (TimecodeError, OverflowError) as exc:
                        invalid.append(
                            {
                                "exposure_id": row["exposure_id"],
                                "telescope": row["telescope"],
                                "start": row["start_raw"],
                                "error": str(exc),
                            }
                        )
                    else:
                        text_updates.append((real, 0, row["exposure_id"]))
                    if len(invalid) >= MAX_SWAP_REPORT:
                        break
                if invalid:
                    raise LeapSwapRejected(invalid)

                if new_table == old_table:
                    conn.execute("COMMIT")
                    return {
                        "version": old_version,
                        "previous_version": old_version,
                        "updated_text_rows": 0,
                        "unchanged": True,
                        "leap_days": len(new_table.dates),
                    }

                new_version = old_version + 1
                conn.execute(
                    "INSERT INTO leap_tables(version, body, created_at) VALUES(?, ?, ?)",
                    (new_version, format_leap_table(new_table), _iso_now()),
                )
                conn.executemany(
                    "UPDATE exposures SET start_real = ?, leap_version = ? "
                    "WHERE exposure_id = ?",
                    [(real, new_version, eid) for real, _old, eid in text_updates],
                )
                # Real-second rows keep their absolute instant; their
                # provenance moves to the active table for future fetches.
                conn.execute(
                    "UPDATE exposures SET leap_version = ? WHERE start_source = 'real'",
                    (new_version,),
                )
                # Summaries are tagged with the table that produced their day
                # boundaries and cannot be trusted after a swap; the next view
                # rebuilds them from the same committed state.
                summaries_invalidated = conn.execute(
                    "DELETE FROM day_summaries"
                ).rowcount
                conn.execute("COMMIT")
                return {
                    "version": new_version,
                    "previous_version": old_version,
                    "updated_text_rows": len(text_updates),
                    "unchanged": False,
                    "leap_days": len(new_table.dates),
                    "summaries_invalidated": summaries_invalidated,
                }
            except Exception:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()


def _clip_row(row: sqlite3.Row, day_start: int, day_end: int, table: LeapTable) -> dict:
    real_start = int(row["start_real"])
    real_end = real_start + int(row["duration_s"])
    clip_start = max(real_start, day_start)
    clip_end = min(real_end, day_end)

    source_dt, source_leap = real_to_utc(real_start, table)
    start_dt, start_leap = real_to_utc(clip_start, table)
    end_dt, end_leap = real_to_utc(clip_end, table)
    return {
        "exposure_id": row["exposure_id"],
        "telescope": row["telescope"],
        "start_source": row["start_source"],
        "source_start": row["start_raw"],
        "source_start_utc": fmt_utc(source_dt, source_leap),
        "source_duration_s": int(row["duration_s"]),
        "window_start_utc": fmt_utc(start_dt, start_leap),
        "window_end_utc": fmt_utc(end_dt, end_leap),
        "window_start_real": clip_start,
        "window_end_real": clip_end,
        "window_seconds": clip_end - clip_start,
        "whole_day": clip_start == day_start and clip_end == day_end,
        "starts_before_day": real_start < day_start,
        "ends_after_day": real_end > day_end,
    }


def _merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Union of half-open intervals, keeping disjoint ascending ranges."""

    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if start >= end:
            continue
        if merged and start <= merged[-1][1]:
            prev_start, prev_end = merged[-1]
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))
    return merged


def compute_group_stats(coverage: DayCoverage, telescopes: list[str]) -> GroupStats:
    """Histogram of simultaneous exposure seconds for a telescope group.

    Every second of the day falls in exactly one bucket: 0 telescopes, 1,
    or ... up to the group size.  Overlaps of the same telescope were unioned
    into the coverage beforehand, so one telescope never counts twice.
    """

    wanted = tuple(sorted({name.strip() for name in telescopes if name.strip()}))
    if not wanted:
        raise ValidationError(
            "telescope group rejected", ["choose at least one telescope"]
        )
    index = {name: position for position, name in enumerate(coverage.telescopes)}
    selected = {index[name] for name in wanted if name in index}

    counts = [0] * (len(wanted) + 1)
    covered_seconds = 0
    observed_seconds = 0
    for start, end, mask in coverage.segments:
        active = sum(1 for telescope_index in mask if telescope_index in selected)
        segment_seconds = end - start
        counts[active] += segment_seconds
        covered_seconds += segment_seconds
        if active:
            observed_seconds += segment_seconds

    day_seconds = coverage.day_seconds
    # Seconds outside every segment have no telescope exposing at all.
    counts[0] += max(0, day_seconds - covered_seconds)
    levels = tuple(
        {
            "simultaneous": simultaneous,
            "seconds": counts[simultaneous],
            "ratio": counts[simultaneous] / day_seconds if day_seconds else 0.0,
            "available": counts[simultaneous] > 0,
        }
        for simultaneous in range(len(wanted) + 1)
    )
    return GroupStats(
        telescopes=wanted,
        day_seconds=day_seconds,
        levels=levels,
        observed_seconds=observed_seconds,
        joint_seconds=counts[len(wanted)],
    )


def _build_coverage(
    day: _dt.date,
    version: int,
    table: LeapTable,
    raw_rows: list[sqlite3.Row],
    day_start: int,
    day_end: int,
    built_at: str,
) -> DayCoverage:
    """Fold clipped exposures into a group-independent coverage summary.

    Exposures of the same telescope are unioned first, so overlapping
    exposures of one telescope still count that telescope once.  The sweep then
    records maximal ranges with an unchanged set of exposing telescopes.  The
    result therefore serves every possible telescope group without touching the
    exposure table again.
    """

    per_telescope: dict[str, list[tuple[int, int]]] = {}
    for row in raw_rows:
        real_start = int(row["start_real"])
        clip_start = max(real_start, day_start)
        clip_end = min(real_start + int(row["duration_s"]), day_end)
        if clip_end > clip_start:
            per_telescope.setdefault(row["telescope"], []).append(
                (clip_start - day_start, clip_end - day_start)
            )

    telescopes = tuple(sorted(per_telescope))
    index = {name: position for position, name in enumerate(telescopes)}
    events: dict[int, list[tuple[int, bool]]] = {}
    for name, intervals in per_telescope.items():
        for start, end in _merge_intervals(intervals):
            events.setdefault(start, []).append((index[name], True))
            events.setdefault(end, []).append((index[name], False))

    segments: list[tuple[int, int, tuple[int, ...]]] = []
    active: set[int] = set()
    previous: int | None = None
    for offset in sorted(events):
        if previous is not None and offset > previous and active:
            mask = tuple(sorted(active))
            if segments and segments[-1][1] == previous and segments[-1][2] == mask:
                merged_start, _merged_end, _mask = segments[-1]
                segments[-1] = (merged_start, offset, mask)
            else:
                segments.append((previous, offset, mask))
        for telescope_index, is_start in events[offset]:
            if is_start:
                active.add(telescope_index)
            else:
                active.discard(telescope_index)
        previous = offset

    start_dt, start_leap = real_to_utc(day_start, table)
    end_dt, end_leap = real_to_utc(day_end, table)
    last_dt, last_leap = real_to_utc(day_end - 1, table)
    segment_times = tuple(
        (
            fmt_utc(*real_to_utc(day_start + start, table)),
            fmt_utc(*real_to_utc(day_start + end, table)),
        )
        for start, end, _mask in segments
    )
    return DayCoverage(
        day=day,
        leap_version=version,
        epoch=day_start,
        day_start_real=day_start,
        day_end_real=day_end,
        leap_day=day in table,
        day_start_utc=fmt_utc(start_dt, start_leap),
        day_end_utc=fmt_utc(end_dt, end_leap),
        day_last_utc=fmt_utc(last_dt, last_leap),
        telescopes=telescopes,
        segments=tuple(segments),
        segment_times=segment_times,
        built_at=built_at,
        source="rebuilt",
    )


def _coverage_from_row(
    row: sqlite3.Row, table: LeapTable, day: _dt.date, source: str
) -> DayCoverage:
    day_start = int(row["day_start_real"])
    day_end = int(row["day_end_real"])
    telescopes = tuple(json.loads(row["telescopes_json"]))
    raw_segments = json.loads(row["segments_json"])
    segments = tuple(
        (int(start), int(end), tuple(int(i) for i in mask))
        for start, end, mask in raw_segments
    )
    start_dt, start_leap = real_to_utc(day_start, table)
    end_dt, end_leap = real_to_utc(day_end, table)
    last_dt, last_leap = real_to_utc(day_end - 1, table)
    segment_times = tuple(
        (
            fmt_utc(*real_to_utc(day_start + int(start), table)),
            fmt_utc(*real_to_utc(day_start + int(end), table)),
        )
        for start, end, _mask in raw_segments
    )
    return DayCoverage(
        day=day,
        leap_version=int(row["leap_version"]),
        epoch=day_start,
        day_start_real=day_start,
        day_end_real=day_end,
        leap_day=day in table,
        day_start_utc=fmt_utc(start_dt, start_leap),
        day_end_utc=fmt_utc(end_dt, end_leap),
        day_last_utc=fmt_utc(last_dt, last_leap),
        telescopes=telescopes,
        segments=segments,
        segment_times=segment_times,
        built_at=row["built_at"],
        source=source,
    )


def _intersected_days(
    start_real: int, duration_s: int, table: LeapTable
) -> list[_dt.date]:
    """UTC civil days a half-open exposure actually touches."""

    first = real_to_utc(start_real, table)[0].date()
    last = real_to_utc(start_real + duration_s - 1, table)[0].date()
    days = [first]
    cursor = first
    while cursor < last:
        cursor += _dt.timedelta(days=1)
        days.append(cursor)
    return days

