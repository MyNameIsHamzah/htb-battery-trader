"""Submit Flink SQL to the SQL Gateway REST API (SPEC-streaming.md §7.3).

    python scripts/submit_sql.py deploy/flink/wind_demo_query.sql
    python scripts/submit_sql.py deploy/flink/wind_demo_query.sql --dry-run
    python scripts/submit_sql.py x.sql --gateway http://sql-gateway:8083

Self-contained (stdlib only, like scripts/peek.py). Splits the file into
statements (respecting quotes, backticks, and `--` / `/* */` comments), opens a
streaming session on the gateway, POSTs each statement, and polls until it
FINISHES or — for an INSERT — is RUNNING (job submitted). Exit code 0 = success.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_GATEWAY = os.environ.get("FLINK_SQL_GATEWAY", "http://localhost:8083")
TERMINAL_OK = {"FINISHED", "CANCELED", "CLOSED", "RUNNING"}


def split_statements(text: str) -> list[str]:
    """Split a SQL script into statements on ';' outside strings/backticks/comments."""
    stmts: list[str] = []
    buf: list[str] = []
    i, n = 0, len(text)
    in_single = in_double = in_backtick = False
    while i < n:
        c = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if not (in_single or in_double or in_backtick):
            # line comment
            if c == "-" and nxt == "-":
                j = text.find("\n", i)
                if j == -1:
                    break
                i = j + 1
                continue
            # block comment
            if c == "/" and nxt == "*":
                j = text.find("*/", i + 2)
                if j == -1:
                    break
                i = j + 2
                continue
        if c == "'" and not (in_double or in_backtick):
            in_single = not in_single
        elif c == '"' and not (in_single or in_backtick):
            in_double = not in_double
        elif c == "`" and not (in_single or in_double):
            in_backtick = not in_backtick
        if c == ";" and not (in_single or in_double or in_backtick):
            s = "".join(buf).strip()
            if s:
                stmts.append(s)
            buf = []
        else:
            buf.append(c)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def _call(method: str, url: str, body: dict | None = None, timeout: int = 30) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method, headers={"content-type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode() or "{}"
    return json.loads(raw)


def _poll(base: str, handle: str, op: str, stmt: str, timeout: int) -> str:
    start = time.time()
    is_insert = stmt.lstrip().lower().startswith("insert")
    while time.time() - start < timeout:
        res = _call("GET", f"{base}/v1/sessions/{handle}/statements/{op}")
        status = res.get("status", "")
        if status in ("FINISHED", "CANCELED", "CLOSED"):
            return status
        if status == "ERROR":
            detail = res.get("results", res.get("operationStatus", {}))
            print("  error:", json.dumps(detail)[:400])
            return "ERROR"
        if status == "RUNNING" and is_insert:
            # an INSERT starts a streaming job; RUNNING == submitted successfully
            return "RUNNING"
        time.sleep(1)
    return "TIMEOUT"


def submit(gateway: str, statements: list[str], poll_timeout: int = 120) -> int:
    base = gateway.rstrip("/")
    try:
        sess = _call(
            "POST",
            f"{base}/v1/sessions",
            {"properties": {"execution.runtime-mode": "streaming"}},
        )
    except urllib.error.URLError as e:
        print(f"cannot reach SQL Gateway at {base}: {e}", file=sys.stderr)
        return 2
    handle = sess["sessionHandle"]
    print(f"session {handle}")
    rc = 0
    for i, stmt in enumerate(statements, 1):
        preview = " ".join(stmt.split())[:72]
        print(f"[{i}/{len(statements)}] {preview}{'...' if len(stmt) > 72 else ''}")
        try:
            op = _call(
                "POST",
                f"{base}/v1/sessions/{handle}/statements",
                {"statement": stmt},
            )
        except urllib.error.HTTPError as e:
            print(f"  submit failed: {e.read().decode(errors='replace')[:400]}")
            rc = 1
            break
        op_handle = op.get("operationHandle", "")
        state = _poll(base, handle, op_handle, stmt, poll_timeout)
        print(f"  -> {state}")
        if state in ("ERROR", "TIMEOUT"):
            rc = 1
            break
    return rc


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Submit Flink SQL to the SQL Gateway.")
    p.add_argument("sql_file", help="path to a .sql file")
    p.add_argument(
        "--gateway", default=DEFAULT_GATEWAY, help="SQL Gateway base URL (:8083)"
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="parse + list statements, do not contact the gateway",
    )
    p.add_argument("--poll-timeout", type=int, default=120, help="seconds per statement")
    args = p.parse_args(argv)

    path = Path(args.sql_file)
    if not path.exists():
        print(f"no such file: {path}", file=sys.stderr)
        return 2
    statements = split_statements(path.read_text())
    if not statements:
        print(f"no SQL statements found in {path}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(f"{len(statements)} statement(s) in {path}:")
        for i, stmt in enumerate(statements, 1):
            preview = " ".join(stmt.split())
            print(f"  [{i}] {preview[:90]}{'...' if len(preview) > 90 else ''}")
        return 0

    return submit(args.gateway, statements, args.poll_timeout)


if __name__ == "__main__":
    sys.exit(main())
