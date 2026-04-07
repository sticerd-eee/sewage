#!/usr/bin/env python3
"""Convert article-oriented tabularray tables into Beamer-friendly tabular files."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


TABLE_ENV_RE = re.compile(r"\\begin\{(talltblr|longtblr|tblr)\}")
CELL_SPAN_RE = re.compile(r"cell\{(\d+)\}\{(\d+)\}=\{([^}]*)\}(?:\{([^}]*)\})?")
SETCELL_RE = re.compile(
    r"^\\SetCell\[(?P<options>[^\]]+)\]\{(?P<style>[^}]*)\}\s*(?P<content>.*)$"
)
COMMENT_RE = re.compile(r"(?<!\\)%.*$")

ROW_LABEL_REPLACEMENTS = {
    "Adj. R-squared": r"Adj. $R^2$",
    "Adj. Pseudo R2": r"Adj. Pseudo $R^2$",
}


@dataclass(frozen=True)
class TableSpec:
    env_name: str
    inner_options: str
    body: str


@dataclass(frozen=True)
class SpanSpec:
    colspan: int
    alignment: str


@dataclass(frozen=True)
class RowInfo:
    raw_line: str
    raw_cells: list[str]


@dataclass(frozen=True)
class SetCellSpec:
    colspan: int
    alignment: str
    content: str


def find_matching(text: str, start: int, open_char: str, close_char: str) -> int:
    depth = 0
    index = start
    while index < len(text):
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == open_char:
            depth += 1
        elif char == close_char:
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise ValueError(f"Unmatched {open_char}{close_char} block")


def skip_ignored(text: str, position: int) -> int:
    while position < len(text):
        if text[position].isspace():
            position += 1
            continue
        if text[position] == "%":
            while position < len(text) and text[position] != "\n":
                position += 1
            continue
        break
    return position


def extract_table_spec(text: str) -> TableSpec:
    match = TABLE_ENV_RE.search(text)
    if match is None:
        raise ValueError("No tabularray environment found")

    env_name = match.group(1)
    position = skip_ignored(text, match.end())

    if position < len(text) and text[position] == "[":
        outer_end = find_matching(text, position, "[", "]")
        position = skip_ignored(text, outer_end + 1)

    if position >= len(text) or text[position] != "{":
        raise ValueError("Missing inner tabularray options block")

    inner_end = find_matching(text, position, "{", "}")
    inner_options = text[position + 1 : inner_end]
    body_start = inner_end + 1

    end_match = re.search(rf"\\end\{{{env_name}\}}", text[body_start:])
    if end_match is None:
        raise ValueError(f"Missing end tag for {env_name}")

    body_end = body_start + end_match.start()
    body = text[body_start:body_end]

    return TableSpec(env_name=env_name, inner_options=inner_options, body=body)


def strip_comments(text: str) -> str:
    stripped_lines = []
    for line in text.splitlines():
        stripped_lines.append(COMMENT_RE.sub("", line).rstrip())
    return "\n".join(stripped_lines)


def normalize_body(text: str) -> str:
    text = strip_comments(text)
    text = re.sub(
        r"\\\\\s*(\\cmidrule\[[^\]]+\]\{[^}]+\}(?:\\cmidrule\[[^\]]+\]\{[^}]+\})*)",
        r"\\\\\n\1",
        text,
    )
    text = re.sub(r"\\\\\s*(\\midrule\b)", r"\\\\\n\1", text)
    text = re.sub(r"\\cmidrule\[([^\]]+)\]\{([^}]+)\}", r"\\cmidrule(\1){\2}", text)
    return text.strip()


def split_top_level(text: str, separator: str = "&") -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    index = 0
    while index < len(text):
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth = max(0, depth - 1)
        elif char == separator and depth == 0:
            parts.append(text[start:index].strip())
            start = index + 1
        index += 1
    parts.append(text[start:].strip())
    return parts


def unwrap_full_braces(text: str) -> str | None:
    candidate = text.strip()
    if not candidate.startswith("{") or not candidate.endswith("}"):
        return None
    end_index = find_matching(candidate, 0, "{", "}")
    if end_index != len(candidate) - 1:
        return None
    return candidate[1:-1]


def normalize_cell_content(text: str) -> str:
    content = text.strip()
    while True:
        unwrapped = unwrap_full_braces(content)
        if unwrapped is None:
            break
        content = unwrapped.strip()

    content = ROW_LABEL_REPLACEMENTS.get(content, content)

    if r"\\" in content:
        return rf"\shortstack[l]{{{content}}}"
    return content


def parse_int_option(text: str, key: str) -> int | None:
    match = re.search(rf"(?:^|,)\s*{re.escape(key)}\s*=\s*(\d+)", text)
    if match is None:
        return None
    return int(match.group(1))


def parse_alignment(text: str) -> str | None:
    stripped = text.strip()
    if stripped in {"l", "c", "r"}:
        return stripped
    match = re.search(r"halign\s*=\s*([lcr])", text)
    if match is None:
        return None
    return match.group(1)


def parse_span_map(inner_options: str) -> dict[tuple[int, int], SpanSpec]:
    span_map: dict[tuple[int, int], SpanSpec] = {}
    for match in CELL_SPAN_RE.finditer(inner_options):
        row_number = int(match.group(1))
        col_number = int(match.group(2))
        options_a = match.group(3) or ""
        options_b = match.group(4) or ""
        combined_options = ",".join(filter(None, [options_a, options_b]))
        colspan = parse_int_option(combined_options, "c")
        if colspan is None or colspan <= 1:
            continue
        alignment = parse_alignment(combined_options)
        if alignment is None:
            alignment = "l" if col_number == 1 else "c"
        span_map[(row_number, col_number)] = SpanSpec(colspan=colspan, alignment=alignment)
    return span_map


def parse_setcell(cell_text: str) -> SetCellSpec | None:
    match = SETCELL_RE.match(cell_text.strip())
    if match is None:
        return None

    options = match.group("options")
    style = match.group("style")
    content = normalize_cell_content(match.group("content"))
    colspan = parse_int_option(options, "c") or 1
    alignment = parse_alignment(style) or parse_alignment(options) or "c"

    return SetCellSpec(colspan=colspan, alignment=alignment, content=content)


def parse_rows(body: str) -> list[RowInfo]:
    data_rows: list[RowInfo] = []

    for raw_line in normalize_body(body).splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.endswith(r"\\"):
            row_text = line[:-2].rstrip()
            data_rows.append(RowInfo(raw_line=line, raw_cells=split_top_level(row_text)))
    return data_rows


def is_numericish(cell: str) -> bool:
    candidate = normalize_cell_content(cell)
    if not candidate:
        return True
    if candidate.startswith(r"\multicolumn"):
        return False
    if candidate.startswith("(") and candidate.endswith(")"):
        return is_numericish(candidate[1:-1].strip())
    if re.fullmatch(r"\\num\{[^}]+\}(?:\*+)?(?:\\%)?", candidate):
        return True
    normalized = candidate.replace(r"\%", "").replace(",", "")
    return re.fullmatch(r"[-+<>]?\d+(?:\.\d+)?(?:\*+)?", normalized) is not None


def is_descriptive_numeric_table(rows: list[RowInfo], has_group_headers: bool, ncols: int) -> bool:
    if has_group_headers or ncols > 7 or len(rows) <= 1:
        return False

    numeric_cells = 0
    checked_cells = 0

    for row in rows[1:]:
        for cell in row.raw_cells[1:]:
            stripped = cell.strip()
            if not stripped:
                continue
            checked_cells += 1
            if is_numericish(stripped):
                numeric_cells += 1

    if checked_cells == 0:
        return False
    return numeric_cells / checked_cells >= 0.9


def format_alignment(ncols: int, right_align_numeric: bool) -> str:
    if ncols <= 0:
        raise ValueError("Table must have at least one column")
    fill = "r" if right_align_numeric else "c"
    return "l" + fill * (ncols - 1)


def is_data_row(row: RowInfo) -> bool:
    if not row.raw_cells:
        return False
    first_cell = normalize_cell_content(row.raw_cells[0])
    if not first_cell:
        return False

    numeric_cells = sum(
        1 for cell in row.raw_cells[1:] if cell.strip() and is_numericish(cell.strip())
    )
    return numeric_cells > 0


def header_row_count(rows: list[RowInfo]) -> int:
    for index, row in enumerate(rows, start=1):
        if is_data_row(row):
            return index - 1
    return 0


def row_span_segments(
    row: RowInfo, row_number: int, span_map: dict[tuple[int, int], SpanSpec]
) -> list[tuple[int, int]]:
    segments: list[tuple[int, int]] = []
    skip_cells = 0

    for col_number, raw_cell in enumerate(row.raw_cells, start=1):
        if skip_cells > 0:
            skip_cells -= 1
            continue

        setcell = parse_setcell(raw_cell)
        if setcell is not None and setcell.colspan > 1:
            colspan = setcell.colspan
        else:
            span_spec = span_map.get((row_number, col_number))
            colspan = span_spec.colspan if span_spec is not None else 1

        if colspan > 1:
            segments.append((col_number, col_number + colspan - 1))
            skip_cells = colspan - 1

    return segments


def convert_row(row: RowInfo, row_number: int, span_map: dict[tuple[int, int], SpanSpec]) -> str:
    converted_cells: list[str] = []
    skip_cells = 0

    for col_number, raw_cell in enumerate(row.raw_cells, start=1):
        if skip_cells > 0:
            skip_cells -= 1
            continue

        setcell = parse_setcell(raw_cell)
        if setcell is not None:
            colspan = setcell.colspan
            alignment = setcell.alignment
            content = setcell.content
        else:
            span_spec = span_map.get((row_number, col_number))
            if span_spec is not None:
                colspan = span_spec.colspan
                alignment = span_spec.alignment
            else:
                colspan = 1
                alignment = "l" if col_number == 1 else "c"
            content = normalize_cell_content(raw_cell)

        if colspan > 1:
            converted_cells.append(
                rf"\multicolumn{{{colspan}}}{{{alignment}}}{{{content}}}"
            )
            skip_cells = colspan - 1
        else:
            converted_cells.append(content)

    return " & ".join(converted_cells) + r" \\"


def convert_table(text: str) -> str:
    table_spec = extract_table_spec(text)
    span_map = parse_span_map(table_spec.inner_options)
    rows = parse_rows(table_spec.body)

    if not rows:
        raise ValueError("No table rows found")

    ncols = max(len(row.raw_cells) for row in rows)
    has_group_headers = bool(span_map) or any(parse_setcell(cell) for row in rows for cell in row.raw_cells)
    right_align_numeric = is_descriptive_numeric_table(rows, has_group_headers, ncols)
    alignment = format_alignment(ncols, right_align_numeric)
    use_resizebox = has_group_headers or ncols >= 8

    converted_rows = [
        convert_row(row=row, row_number=index, span_map=span_map)
        for index, row in enumerate(rows, start=1)
    ]
    header_rows = header_row_count(rows)

    output_lines = [rf"\begin{{tabular}}{{{alignment}}}"]
    output_lines.append(r"\toprule")

    for row_number, converted_row in enumerate(converted_rows, start=1):
        row = rows[row_number - 1]
        if row_number > header_rows:
            first_cell = normalize_cell_content(row.raw_cells[0]) if row.raw_cells else ""
            if first_cell.startswith(r"\textbf{Panel") and row_number != header_rows + 1:
                output_lines.append(r"\midrule")

        output_lines.append(converted_row)

        if row_number <= header_rows:
            segments = row_span_segments(row=row, row_number=row_number, span_map=span_map)
            if segments:
                cmidrules = "".join(
                    rf"\cmidrule(lr){{{start}-{end}}}" for start, end in segments
                )
                output_lines.append(cmidrules)

        if row_number == header_rows and header_rows > 0:
            output_lines.append(r"\midrule")

    output_lines.append(r"\bottomrule")
    output_lines.append(r"\end{tabular}")

    if use_resizebox:
        wrapped_lines = [r"\resizebox{\linewidth}{!}{%"]
        wrapped_lines.extend(output_lines[:-1])
        wrapped_lines.append(output_lines[-1] + "%")
        wrapped_lines.append("}")
        return "\n".join(wrapped_lines) + "\n"

    return "\n".join(output_lines) + "\n"


def convert_directory(source_dir: Path, target_dir: Path) -> int:
    errors: list[tuple[Path, str]] = []
    target_dir.mkdir(parents=True, exist_ok=True)

    for source_path in sorted(source_dir.glob("*.tex")):
        try:
            converted = convert_table(source_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            errors.append((source_path, str(exc)))
            continue

        destination = target_dir / source_path.name
        header = (
            "% Generated by scripts/python/convert_paper_tables_to_beamer.py\n"
            f"% Source: output/tables/{source_path.name}\n"
        )
        destination.write_text(header + converted, encoding="utf-8")

    print(f"Converted {len(list(source_dir.glob('*.tex'))) - len(errors)} tables.")
    print(f"Wrote slide tables to {target_dir}.")

    if errors:
        print("\nConversion errors:", file=sys.stderr)
        for path, message in errors:
            print(f"  - {path.name}: {message}", file=sys.stderr)
        return 1

    return 0


def build_parser() -> argparse.ArgumentParser:
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(
        description="Convert output/tables tabularray files into Beamer-friendly tabular files."
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=repo_root / "output" / "tables",
        help="Directory containing paper LaTeX tables.",
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=repo_root / "docs" / "overleaf" / "slides" / "tables",
        help="Directory to receive converted Beamer tables.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return convert_directory(source_dir=args.source_dir, target_dir=args.target_dir)


if __name__ == "__main__":
    raise SystemExit(main())
