"""Flask web app for the exposure ledger."""

from __future__ import annotations

import csv
import datetime as _dt
import io
import os
import re
from pathlib import Path

from flask import (
    Flask,
    Response,
    redirect,
    render_template,
    request,
    url_for,
)

from .store import (
    CSV_FIELDS,
    DayCoverage,
    Ledger,
    LedgerError,
    LeapSwapRejected,
    SummaryStale,
    ValidationError,
    compute_group_stats,
)
from .timecode import (
    TimecodeError,
    format_leap_table,
    parse_date,
)

MAX_UPLOAD_BYTES = 5 * 1024 * 1024
STALE_SUMMARY_MESSAGE = (
    "计算期间有影响该日的导入已提交，本次结果已丢弃；请重新查看（摘要将从最新已提交状态重建）。"
)


def _split_telescopes(raw: str) -> list[str]:
    """Accept a comma/whitespace separated telescope list."""

    return [part for part in re.split(r"[\s,，、]+", raw or "") if part]


def _normalise_group(telescopes: list[str]) -> tuple[str, ...]:
    return tuple(sorted({name for name in telescopes if name}))


def _group_stats_for(day, telescopes: list[str], coverage: DayCoverage):
    """Return ``(requested_stats, all_telescope_stats or None)``.

    The "all telescopes" histogram comes from the same summary, so comparing a
    group against the whole night never costs another read.
    """

    requested = compute_group_stats(coverage, telescopes)
    if not coverage.telescopes:
        return requested, None
    every = list(coverage.telescopes)
    if _normalise_group(telescopes) == _normalise_group(every):
        return requested, requested
    return requested, compute_group_stats(coverage, every)


def _csv_report(report) -> str:
    """Render one day report as a CSV attachment (raw values preserved)."""

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        [
            "telescope",
            "exposure_id",
            "start_utc",
            "end_utc_exclusive",
            "seconds",
            "source_start",
            "source_duration_s",
        ]
    )
    for row in report.rows:
        writer.writerow(
            [
                row["telescope"],
                row["exposure_id"],
                row["window_start_utc"],
                row["window_end_utc"],
                row["window_seconds"],
                row["source_start"],
                row["source_duration_s"],
            ]
        )
    return buffer.getvalue()


def create_app(config: dict | None = None) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = os.environ.get("LEDGER_SECRET", "dev-only-change-me")
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES
    app.config["LEDGER_DB"] = os.environ.get(
        "LEDGER_DB", str(Path(app.root_path).parent / "var" / "ledger.sqlite3")
    )
    if config:
        app.config.update(config)
    app.ledger = Ledger(app.config["LEDGER_DB"])

    @app.get("/")
    def index():
        return redirect(url_for("day_view"))

    @app.get("/day")
    def day_view():
        raw_day = request.args.get("date", "").strip()
        raw_group = request.args.get("group", "").strip()
        group = _split_telescopes(raw_group)
        day = None
        report = None
        coverage = None
        stats_group = None
        all_group = None
        error = None
        stale = request.args.get("stale") == "1"
        if raw_day:
            try:
                day = parse_date(raw_day)
                report = app.ledger.day_report(day)
                if group:
                    coverage = app.ledger.coverage(day)
                    stats_group, all_group = _group_stats_for(day, group, coverage)
            except SummaryStale:
                return redirect(
                    url_for("day_view", date=raw_day, group=raw_group, stale="1")
                )
            except (TimecodeError, ValidationError) as exc:
                error = str(exc)
        else:
            today = _dt.datetime.now(_dt.timezone.utc).date()
            raw_day = today.isoformat()
        return render_template(
            "day.html",
            active="day",
            raw_day=raw_day,
            raw_group=raw_group,
            error=error,
            report=report,
            coverage=coverage,
            group_stats=stats_group,
            all_group=all_group,
            stale=stale,
            stale_message=STALE_SUMMARY_MESSAGE,
            stats=app.ledger.stats(),
            leap_version=app.ledger.active_table()[0],
        )

    @app.get("/coverage.csv")
    def coverage_csv():
        """Coverage segments for a day plus the histogram for a telescope group."""

        raw_day = request.args.get("date", "").strip()
        group = _split_telescopes(request.args.get("group", ""))
        try:
            day = parse_date(raw_day)
            if group:
                coverage, group_stats = app.ledger.group_stats(day, group)
            else:
                coverage, group_stats = app.ledger.coverage(day), None
        except SummaryStale:
            return redirect(
                url_for("day_view", date=raw_day, group=",".join(group), stale="1")
            )
        except (TimecodeError, ValidationError) as exc:
            return Response(str(exc), status=400, mimetype="text/plain")

        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(
            [
                "start_utc",
                "end_utc_exclusive",
                "seconds",
                "telescopes_exposing",
                "telescope_count",
            ]
        )
        for (start_text, end_text), (start, end, mask) in zip(
            coverage.segment_times, coverage.segments
        ):
            writer.writerow(
                [
                    start_text,
                    end_text,
                    end - start,
                    " ".join(coverage.telescopes[i] for i in mask),
                    len(mask),
                ]
            )
        if group_stats is not None:
            writer.writerow([])
            writer.writerow(["simultaneous_telescopes", "seconds", "of_day"])
            for level in group_stats.levels:
                writer.writerow(
                    [
                        level["simultaneous"],
                        level["seconds"],
                        f"{level['ratio']:.6f}",
                    ]
                )
        filename = f"coverage_{coverage.day.isoformat()}.csv"
        return Response(
            buffer.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.get("/day.csv")
    def day_csv():
        raw_day = request.args.get("date", "").strip()
        try:
            day = parse_date(raw_day)
            report = app.ledger.day_report(day)
        except (TimecodeError, ValidationError) as exc:
            return Response(str(exc), status=400, mimetype="text/plain")
        body = _csv_report(report)
        filename = f"exposures_{report.day.isoformat()}.csv"
        return Response(
            body,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.route("/import", methods=("GET", "POST"))
    def import_view():
        result = None
        errors: list[str] = []
        message = None
        if request.method == "POST":
            uploaded = request.files.get("csv_file")
            pasted = request.form.get("csv_text", "")
            source_name = "pasted.csv"
            text = pasted
            if uploaded is not None and uploaded.filename:
                source_name = os.path.basename(uploaded.filename)
                raw = uploaded.read()
                try:
                    text = raw.decode("utf-8-sig")
                except UnicodeDecodeError:
                    errors.append("the uploaded file is not valid UTF-8")
                if len(raw) > MAX_UPLOAD_BYTES:
                    errors.append("the uploaded file is larger than 5 MiB")
            if not errors:
                try:
                    result = app.ledger.import_csv(text, source_name=source_name)
                except ValidationError as exc:
                    message = str(exc)
                    errors = exc.details
                except LedgerError as exc:
                    message = str(exc)
        return render_template(
            "import.html",
            active="import",
            result=result,
            errors=errors,
            message=message,
            stats=app.ledger.stats(),
            leap_version=app.ledger.active_table()[0],
            csv_fields=",".join(CSV_FIELDS),
        )

    @app.get("/template.csv")
    def template_csv():
        sample = (
            "telescope,exposure_id,start,duration_s\n"
            "T1,EXP-0001,2026-11-24 00:00:00,600\n"
            "T2,EXP-0002,2026-11-24 12:30:00,1200\n"
            "T3,EXP-0003,848793605,900\n"
        )
        return Response(
            sample,
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": 'attachment; filename="exposures_template.csv"'
            },
        )

    @app.route("/leap", methods=("GET", "POST"))
    def leap_view():
        offending: list[dict] | None = None
        result = None
        message = None
        if request.method == "POST":
            text = request.form.get("leap_text", "")
            try:
                result = app.ledger.replace_leap_table(text)
            except LeapSwapRejected as exc:
                offending = exc.offending
                message = str(exc)
            except ValidationError as exc:
                message = str(exc)
                offending = [{"error": detail} for detail in exc.details]
            except LedgerError as exc:
                message = str(exc)
        version, table = app.ledger.active_table()
        return render_template(
            "leap.html",
            active="leap",
            version=version,
            table=table,
            table_text=format_leap_table(table),
            history=app.ledger.leap_history(),
            offending=offending,
            result=result,
            message=message,
            stats=app.ledger.stats(),
        )

    @app.errorhandler(413)
    def too_large(_exc):
        return (
            render_template(
                "import.html",
                active="import",
                result=None,
                errors=["the upload is larger than 5 MiB"],
                message="upload rejected",
                stats=app.ledger.stats(),
                leap_version=app.ledger.active_table()[0],
                csv_fields=",".join(CSV_FIELDS),
            ),
            413,
        )

    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "ledger": app.ledger.stats()}

    return app
