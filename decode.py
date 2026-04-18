#!/usr/bin/env python3
"""Runs `cargo flux`, extracts KVAR constraints per source function, and decodes them into JSON."""

import re
import json
import subprocess
import sys

SORT_TAGS = {0: "String", 1: "Number", 2: "Bool", 3: "Unknown"}


def decode_sort(n: int) -> str:
    return SORT_TAGS.get(n, "Unknown")


def parse_constraint(constraint: str) -> dict:
    body = re.sub(r"^\d+:\s*for<\?b0>\s*", "", constraint)
    conjuncts = [s.strip() for s in body.split("∧")]

    columns: dict[str, dict] = {}
    table = ""

    for c in conjuncts:
        m = re.match(r'^b0\.0\s*=\s*"([^"]+)"$', c)
        if m:
            table = m.group(1)
            continue

        m = re.match(
            r'^MySort::(\d+)\s*\{[^}]*\}\s*=\s*map_select\(b0\.1\)\("([^"]+)"\)\.0$', c
        )
        if m:
            col = m.group(2)
            sort_type = decode_sort(int(m.group(1)))
            columns.setdefault(col, {})["type"] = sort_type
            continue

        m = re.match(r'^map_select\(b0\.1\)\("([^"]+)"\)\.1\s*=\s*"([^"]*)"$', c)
        if m:
            col, val = m.group(1), m.group(2)
            columns.setdefault(col, {})["str_val"] = val
            continue

        m = re.match(r'^(true|false)\s*=\s*map_select\(b0\.1\)\("([^"]+)"\)\.2$', c)
        if m:
            col = m.group(2)
            bool_val = m.group(1) == "true"
            columns.setdefault(col, {})["bool_val"] = bool_val
            continue

    resolved_columns = {}
    for col, entry in columns.items():
        sort_type = entry.get("type", "Unknown")
        if sort_type in ("String", "Number"):
            value = entry.get("str_val", None)
        elif sort_type == "Bool":
            value = entry.get("bool_val", None)
        else:
            value = None
        resolved_columns[col] = {"type": sort_type, "value": value}

    return {"table": table, "items": resolved_columns}


def run_cargo_flux() -> str:
    """Run `cargo flux` and return combined stdout+stderr."""
    try:
        proc = subprocess.run(
            ["cargo", "flux"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        return proc.stdout
    except FileNotFoundError:
        print("Error: `cargo` not found in PATH", file=sys.stderr)
        sys.exit(1)


def parse_source_name(def_id_line: str) -> str:
    """Extract function name from DefId line."""
    m = re.match(r"^SOLUTION FOR DefId\(\d+:\d+ ~ .+::(.+)\)$", def_id_line)
    if m:
        return m.group(1)
    return def_id_line


def extract_solutions(output: str) -> list[dict]:
    """
    Parse output into per-source-function solutions.
    Each SOLUTION FOR block contains a sink type and constraint lines.
    """
    solutions = []
    lines = output.splitlines()
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        if line.startswith("SOLUTION FOR "):
            source_fn = parse_source_name(line)
            i += 1

            # Next line is the sink type (e.g., DynamoPut)
            sink_type = ""
            if i < len(lines):
                sink_type = lines[i].strip()
                i += 1

            # Collect constraint lines until next SOLUTION FOR or end
            paths = []
            while i < len(lines):
                cline = lines[i].strip()
                if cline.startswith("SOLUTION FOR "):
                    break
                if re.match(r"^\d+:\s*for<\?b0>", cline):
                    paths.append(parse_constraint(cline))
                i += 1

            solutions.append(
                {
                    "source": source_fn,
                    "sink_type": sink_type,
                    "paths": paths,
                }
            )
        else:
            i += 1

    return solutions


if __name__ == "__main__":
    raw_output = run_cargo_flux()
    solutions = extract_solutions(raw_output)

    if not solutions:
        print("No solutions found.", file=sys.stderr)
        sys.exit(1)

    print(json.dumps(solutions, indent=2))
