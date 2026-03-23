from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import subprocess
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from markdown_it import MarkdownIt
except Exception:  # pragma: no cover - optional dependency
    MarkdownIt = None

try:
    from pylatexenc.latexwalker import LatexWalker
except Exception:  # pragma: no cover - optional dependency
    LatexWalker = None


LATEX_MAIN_NAMES = ("_main.tex", "main.tex", "paper.tex", "manuscript.tex")
MARKDOWN_EXTENSIONS = {".md", ".markdown"}
LATEX_EXTENSIONS = {".tex", ".latex"}
PDF_EXTENSIONS = {".pdf"}
PREFERRED_PDF_NAMES = (
    "manuscript.pdf",
    "paper.pdf",
    "main.pdf",
    "_main.pdf",
    "draft.pdf",
    "submission.pdf",
)
SECTION_COMMANDS = ("part", "chapter", "section", "subsection", "subsubsection", "paragraph")
THEOREM_ENV_NAMES = (
    "theorem",
    "lemma",
    "proposition",
    "corollary",
    "claim",
    "remark",
    "definition",
    "assumption",
    "example",
    "proof",
)
REF_COMMANDS = ("ref", "pageref", "autoref", "eqref", "cref", "Cref", "fref", "Fref")
GLOBAL_REVIEW_ROLES = (
    "CentralClaimAndPositioning",
    "ExpositionAndClarity",
    "EmpiricalConsistency",
    "IdentificationAndCausalLanguage",
    "MathematicalAndLogicalReasoning",
    "NotationAndDefinitions",
    "InternalReferencesAndBuild",
    "NumbersUnitsAndSigns",
    "CrossDocumentConsistency",
)


@dataclass
class SourceRef:
    path: str
    line_start: int
    line_end: int
    role: str | None = None


@dataclass
class SectionRecord:
    id: str
    title: str
    level: int
    path: str
    line_start: int
    line_end: int
    token_estimate: int


@dataclass
class ChunkRecord:
    id: str
    title: str
    path: str
    line_start: int
    line_end: int
    token_estimate: int
    section_ids: list[str] = field(default_factory=list)
    risk_hints: list[str] = field(default_factory=list)
    adjacent_chunk_ids: list[str] = field(default_factory=list)
    coverage_priority: str = "standard"


@dataclass
class ReviewBrief:
    central_claim_hint: str
    outline: list[str]
    likely_hotspots: list[str]
    summary: str
    coverage_strategy: str


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def estimate_tokens(text: str) -> int:
    words = len(re.findall(r"\S+", text))
    return max(1, math.ceil(words * 1.33))


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path.resolve())


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "section"


def choose_pdf_candidate(target: Path) -> Path | None:
    pdf_candidates = sorted(path for path in target.iterdir() if path.is_file() and path.suffix.lower() in PDF_EXTENSIONS)
    if not pdf_candidates:
        return None

    prioritized = [target / name for name in PREFERRED_PDF_NAMES if (target / name).exists()]
    if prioritized:
        return prioritized[0]

    def score(path: Path) -> tuple[int, int, str]:
        name = path.name.lower()
        score_value = 0
        if any(token in name for token in ("manuscript", "paper", "draft", "submission", "main")):
            score_value += 30
        if any(token in name for token in ("figure", "fig", "table", "appendix", "slides", "poster", "cover", "letter")):
            score_value -= 20
        return (score_value, -len(name), name)

    best = max(pdf_candidates, key=score)
    if score(best)[0] > 0 or len(pdf_candidates) == 1:
        return best
    return None


def choose_entrypoint(target: Path) -> tuple[Path, Path]:
    target = target.resolve()
    if target.is_file():
        return target, target.parent

    if not target.is_dir():
        raise FileNotFoundError(f"Target does not exist: {target}")

    pdf_candidate = choose_pdf_candidate(target)
    if pdf_candidate is not None:
        return pdf_candidate, target

    for name in LATEX_MAIN_NAMES:
        candidate = target / name
        if candidate.exists():
            return candidate, target

    markdown_candidates = sorted(
        path for path in target.iterdir() if path.is_file() and path.suffix.lower() in MARKDOWN_EXTENSIONS
    )
    if len(markdown_candidates) == 1:
        return markdown_candidates[0], target

    latex_candidates = sorted(
        path for path in target.iterdir() if path.is_file() and path.suffix.lower() in LATEX_EXTENSIONS
    )
    if len(latex_candidates) == 1:
        return latex_candidates[0], target

    raise FileNotFoundError(f"Could not detect manuscript entrypoint in {target}")


def parse_existing_logs(source_root: Path, main_file: Path) -> dict[str, Any]:
    log_paths: list[Path] = []
    preferred = source_root / f"{main_file.stem}.log"
    if preferred.exists():
        log_paths.append(preferred.resolve())

    for path in sorted(source_root.rglob("*.log")):
        resolved = path.resolve()
        if resolved not in log_paths:
            log_paths.append(resolved)

    overfull_hbox = []
    undefined_control = []
    for path in log_paths:
        lines = read_text(path).splitlines()
        for idx, line in enumerate(lines, start=1):
            if "Overfull \\hbox" in line:
                overfull_hbox.append({"path": relative_path(path, source_root), "line": idx, "message": line.strip()})
            if "Undefined control sequence" in line:
                context = " | ".join(lines[idx - 1 : min(len(lines), idx + 2)])
                undefined_control.append(
                    {"path": relative_path(path, source_root), "line": idx, "message": normalize_whitespace(context)}
                )

    return {
        "log_files": [relative_path(path, source_root) for path in log_paths],
        "overfull_hbox": overfull_hbox,
        "undefined_control_sequence": undefined_control,
    }


def resolve_include(base_dir: Path, raw_spec: str) -> Path:
    candidate = (base_dir / raw_spec).resolve()
    if candidate.suffix:
        return candidate
    return candidate.with_suffix(".tex")


def find_includes(path: Path, lines: list[str], source_root: Path) -> list[dict[str, Any]]:
    includes = []
    patterns = [
        re.compile(r"\\(input|include|subfile)\s*\{([^}]+)\}"),
        re.compile(r"\\(import|subimport)\s*\{([^}]+)\}\s*\{([^}]+)\}"),
    ]

    for line_number, line in enumerate(lines, start=1):
        for match in patterns[0].finditer(line):
            raw_target = match.group(2)
            resolved = resolve_include(path.parent, raw_target)
            includes.append(
                {
                    "source": relative_path(path, source_root),
                    "line": line_number,
                    "type": match.group(1),
                    "raw_target": raw_target,
                    "resolved_target": resolved,
                }
            )
        for match in patterns[1].finditer(line):
            prefix = match.group(2)
            raw_target = match.group(3)
            resolved = resolve_include(path.parent / prefix, raw_target)
            includes.append(
                {
                    "source": relative_path(path, source_root),
                    "line": line_number,
                    "type": match.group(1),
                    "raw_target": f"{prefix}{raw_target}",
                    "resolved_target": resolved,
                }
            )
    return includes


def read_pdf_text(path: Path) -> tuple[str, list[dict[str, Any]], list[str], str]:
    warnings: list[str] = []
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(path), "-"],
            check=True,
            capture_output=True,
            text=True,
        )
        backend = "pdftotext"
    except FileNotFoundError as exc:  # pragma: no cover - environment specific
        raise RuntimeError("PDF review requires `pdftotext` to be installed and available on PATH.") from exc
    except subprocess.CalledProcessError:
        result = subprocess.run(
            ["pdftotext", str(path), "-"],
            check=True,
            capture_output=True,
            text=True,
        )
        backend = "pdftotext"
        warnings.append("Fell back to plain pdftotext extraction without layout preservation.")

    raw_pages = result.stdout.replace("\x0c", "\f").split("\f")
    pages: list[dict[str, Any]] = []
    linear_lines: list[str] = []
    current_line = 1
    for page_index, raw_page in enumerate(raw_pages, start=1):
        page_text = raw_page.strip("\n")
        page_lines = page_text.splitlines()
        if not page_lines and page_index == len(raw_pages):
            continue
        if not page_lines:
            page_lines = [""]
        line_start = current_line
        line_end = current_line + len(page_lines) - 1
        linear_lines.extend(page_lines)
        pages.append(
            {
                "page_number": page_index,
                "line_start": line_start,
                "line_end": line_end,
                "line_count": len(page_lines),
                "word_count": len(re.findall(r"\S+", page_text)),
                "token_estimate": estimate_tokens(page_text or " "),
                "text": page_text,
            }
        )
        current_line = line_end + 1

    linear_text = "\n".join(linear_lines).strip("\n")
    if not linear_text:
        warnings.append("PDF text extraction returned no visible text.")
    return linear_text, pages, warnings, backend


def infer_chunk_hints(title: str, text: str, token_estimate: int) -> tuple[list[str], str]:
    risk_hints: list[str] = []
    lowered = f"{title} {text[:500]}".lower()
    if any(keyword in lowered for keyword in ("abstract", "introduction", "conclusion", "appendix", "proof", "result")):
        risk_hints.append("high-signal-section")
    if any(keyword in lowered for keyword in ("table", "figure", "estimate", "regression", "effect", "specification")):
        risk_hints.append("quantitative-claims")
    if any(keyword in lowered for keyword in ("theorem", "lemma", "proof", "proposition", "assumption", "equation")):
        risk_hints.append("symbolic-content")
    if token_estimate > 2500:
        risk_hints.append("long-section")

    if "high-signal-section" in risk_hints or "symbolic-content" in risk_hints:
        priority = "high"
    elif "quantitative-claims" in risk_hints or token_estimate > 1400:
        priority = "elevated"
    else:
        priority = "standard"
    return risk_hints, priority


def annotate_chunks(chunks: list[ChunkRecord]) -> list[ChunkRecord]:
    ordered = sorted(chunks, key=lambda chunk: (chunk.path, chunk.line_start, chunk.line_end, chunk.id))
    for index, chunk in enumerate(ordered):
        adjacent: list[str] = []
        if index > 0:
            adjacent.append(ordered[index - 1].id)
        if index + 1 < len(ordered):
            adjacent.append(ordered[index + 1].id)
        chunk.adjacent_chunk_ids = adjacent
    return ordered


def walk_tex_graph(entrypoint: Path, source_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    files: list[dict[str, Any]] = []
    include_edges: list[dict[str, Any]] = []
    unresolved_includes: list[dict[str, Any]] = []
    circular_includes: list[dict[str, Any]] = []
    seen: set[Path] = set()
    stack: list[Path] = []
    stack_set: set[Path] = set()

    def visit(path: Path) -> None:
        resolved = path.resolve()
        if resolved in stack_set:
            circular_includes.append(
                {
                    "path": relative_path(resolved, source_root),
                    "cycle": [relative_path(item, source_root) for item in stack + [resolved]],
                }
            )
            return
        if resolved in seen:
            return
        if not resolved.exists():
            unresolved_includes.append({"path": relative_path(resolved, source_root)})
            return

        seen.add(resolved)
        stack.append(resolved)
        stack_set.add(resolved)

        text = read_text(resolved)
        lines = text.splitlines()
        file_record = {
            "path": relative_path(resolved, source_root),
            "absolute_path": str(resolved),
            "role": "entrypoint" if resolved == entrypoint.resolve() else "include",
            "line_count": len(lines),
            "word_count": len(re.findall(r"\S+", text)),
            "file_hash": sha256_file(resolved),
        }
        files.append(file_record)

        includes = find_includes(resolved, lines, source_root)
        for include in includes:
            edge = {
                "source": include["source"],
                "line": include["line"],
                "type": include["type"],
                "raw_target": include["raw_target"],
                "resolved_target": relative_path(include["resolved_target"], source_root),
            }
            include_edges.append(edge)
            if not include["resolved_target"].exists():
                unresolved_includes.append(edge)
            else:
                visit(include["resolved_target"])

        stack.pop()
        stack_set.remove(resolved)

    visit(entrypoint)
    return files, include_edges, unresolved_includes, circular_includes


def extract_tex_sections(text: str, path: str) -> list[SectionRecord]:
    lines = text.splitlines()
    pattern = re.compile(r"\\(part|chapter|section|subsection|subsubsection|paragraph)\*?\{([^}]+)\}")
    sections: list[SectionRecord] = []
    matches = []
    for line_number, line in enumerate(lines, start=1):
        match = pattern.search(line)
        if match:
            level = SECTION_COMMANDS.index(match.group(1)) + 1
            title = normalize_whitespace(match.group(2))
            matches.append((line_number, level, title))

    for index, (line_number, level, title) in enumerate(matches):
        line_end = matches[index + 1][0] - 1 if index + 1 < len(matches) else len(lines)
        chunk_text = "\n".join(lines[line_number - 1 : line_end])
        sections.append(
            SectionRecord(
                id=f"{path}:{slugify(title)}:{line_number}",
                title=title,
                level=level,
                path=path,
                line_start=line_number,
                line_end=line_end,
                token_estimate=estimate_tokens(chunk_text),
            )
        )
    return sections


def extract_markdown_sections(text: str, path: str) -> list[SectionRecord]:
    lines = text.splitlines()
    sections: list[SectionRecord] = []

    if MarkdownIt is not None:
        md = MarkdownIt("commonmark")
        tokens = md.parse(text)
        heading_entries: list[tuple[int, int, str]] = []
        for index, token in enumerate(tokens):
            if token.type == "heading_open" and token.map:
                inline = tokens[index + 1] if index + 1 < len(tokens) else None
                title = normalize_whitespace(getattr(inline, "content", "")) or "Untitled section"
                level = int(token.tag[1]) if token.tag.startswith("h") else 1
                heading_entries.append((token.map[0] + 1, level, title))
        for idx, (line_start, level, title) in enumerate(heading_entries):
            line_end = heading_entries[idx + 1][0] - 1 if idx + 1 < len(heading_entries) else len(lines)
            section_text = "\n".join(lines[line_start - 1 : line_end])
            sections.append(
                SectionRecord(
                    id=f"{path}:{slugify(title)}:{line_start}",
                    title=title,
                    level=level,
                    path=path,
                    line_start=line_start,
                    line_end=line_end,
                    token_estimate=estimate_tokens(section_text),
                )
            )
        if sections:
            return sections

    pattern = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
    matches = []
    for line_number, line in enumerate(lines, start=1):
        match = pattern.match(line)
        if match:
            matches.append((line_number, len(match.group(1)), normalize_whitespace(match.group(2))))
    for idx, (line_number, level, title) in enumerate(matches):
        line_end = matches[idx + 1][0] - 1 if idx + 1 < len(matches) else len(lines)
        section_text = "\n".join(lines[line_number - 1 : line_end])
        sections.append(
            SectionRecord(
                id=f"{path}:{slugify(title)}:{line_number}",
                title=title,
                level=level,
                path=path,
                line_start=line_number,
                line_end=line_end,
                token_estimate=estimate_tokens(section_text),
            )
        )
    return sections


def extract_tex_inventory(text: str, path: str) -> dict[str, Any]:
    lines = text.splitlines()
    inventory: dict[str, Any] = {
        "labels": [],
        "refs": [],
        "citations": [],
        "equations": [],
        "theorem_like_blocks": [],
        "figures": [],
        "tables": [],
        "footnotes": [],
        "hardcoded_refs": [],
    }

    for line_number, line in enumerate(lines, start=1):
        for match in re.finditer(r"\\label\{([^}]+)\}", line):
            inventory["labels"].append({"path": path, "line": line_number, "label": match.group(1)})
        for match in re.finditer(r"\\(" + "|".join(REF_COMMANDS) + r")\{([^}]+)\}", line):
            inventory["refs"].append(
                {"path": path, "line": line_number, "command": match.group(1), "label": match.group(2)}
            )
        for match in re.finditer(
            r"\\([A-Za-z]*cite[a-zA-Z]*|textcite|parencite|autocite|footcite)\s*(?:\[[^\]]*\]\s*){0,2}\{([^}]+)\}",
            line,
        ):
            keys = [key.strip() for key in match.group(2).split(",") if key.strip()]
            inventory["citations"].append(
                {"path": path, "line": line_number, "command": match.group(1), "keys": keys}
            )
        for match in re.finditer(r"\\footnote\{", line):
            inventory["footnotes"].append({"path": path, "line": line_number})
        if re.search(r"(Table|Figure|Equation|Eq\.)\s+[0-9A-Z]", line) and "\\ref{" not in line and "\\eqref{" not in line:
            inventory["hardcoded_refs"].append({"path": path, "line": line_number, "text": normalize_whitespace(line)})

    env_pattern = re.compile(r"\\begin\{([A-Za-z*]+)\}")
    for line_number, line in enumerate(lines, start=1):
        match = env_pattern.search(line)
        if not match:
            continue
        env = match.group(1)
        lower_env = env.lower().rstrip("*")
        if lower_env in {"equation", "align", "multline", "gather"}:
            inventory["equations"].append({"path": path, "line": line_number, "environment": env})
        if lower_env in {"figure"}:
            inventory["figures"].append({"path": path, "line": line_number, "environment": env})
        if lower_env in {"table"}:
            inventory["tables"].append({"path": path, "line": line_number, "environment": env})
        if lower_env in THEOREM_ENV_NAMES:
            inventory["theorem_like_blocks"].append({"path": path, "line": line_number, "environment": env})

    if LatexWalker is not None:
        try:
            walker = LatexWalker(text)
            walker.get_latex_nodes()  # type: ignore[attr-defined]
            inventory["parser_backend"] = "pylatexenc"
        except Exception as exc:  # pragma: no cover - best effort
            inventory["parser_backend"] = f"regex-fallback ({exc.__class__.__name__})"
    else:
        inventory["parser_backend"] = "regex-fallback"

    return inventory


def extract_markdown_inventory(text: str, path: str) -> dict[str, Any]:
    lines = text.splitlines()
    inventory: dict[str, Any] = {
        "labels": [],
        "refs": [],
        "citations": [],
        "equations": [],
        "theorem_like_blocks": [],
        "figures": [],
        "tables": [],
        "footnotes": [],
        "hardcoded_refs": [],
        "broken_link_candidates": [],
        "parser_backend": "regex-fallback",
    }

    if MarkdownIt is not None:
        md = MarkdownIt("commonmark")
        tokens = md.parse(text)
        inventory["parser_backend"] = "markdown-it-py"
        for token in tokens:
            if token.type == "inline" and getattr(token, "children", None):
                line_start = token.map[0] + 1 if token.map else 1
                for child in token.children or []:
                    if child.type == "link_open":
                        href = dict(child.attrs or {}).get("href", "")
                        inventory["refs"].append({"path": path, "line": line_start, "command": "markdown_link", "label": href})
                        if href and not href.startswith(("http://", "https://", "#")):
                            inventory["broken_link_candidates"].append({"path": path, "line": line_start, "target": href})
                    if child.type == "image":
                        inventory["figures"].append({"path": path, "line": line_start, "target": child.attrGet("src") or ""})

    for line_number, line in enumerate(lines, start=1):
        if re.search(r"\[[^\]]+\]\(([^)]+)\)", line):
            for match in re.finditer(r"\[[^\]]+\]\(([^)]+)\)", line):
                target = match.group(1).strip()
                if not target.startswith(("http://", "https://", "#")):
                    inventory["broken_link_candidates"].append({"path": path, "line": line_number, "target": target})
        if re.search(r"^\|.+\|$", line):
            inventory["tables"].append({"path": path, "line": line_number, "environment": "pipe-table"})
        if "$$" in line or re.search(r"\$[^$]+\$", line):
            inventory["equations"].append({"path": path, "line": line_number, "environment": "markdown-math"})
        if re.match(r"^\[\^[^\]]+\]:", line):
            inventory["footnotes"].append({"path": path, "line": line_number})

    return inventory


def extract_plaintext_inventory(text: str, path: str, parser_backend: str) -> dict[str, Any]:
    lines = text.splitlines()
    inventory: dict[str, Any] = {
        "labels": [],
        "refs": [],
        "citations": [],
        "equations": [],
        "theorem_like_blocks": [],
        "figures": [],
        "tables": [],
        "footnotes": [],
        "hardcoded_refs": [],
        "broken_link_candidates": [],
        "parser_backend": parser_backend,
    }

    for line_number, line in enumerate(lines, start=1):
        lowered = line.lower()
        if re.search(r"(https?://|www\.)", line):
            inventory["refs"].append({"path": path, "line": line_number, "command": "plaintext_link", "label": line.strip()})
        if re.search(r"(=|\\sum|\\int|\\frac|\bargmax\b|\bargmin\b)", line):
            inventory["equations"].append({"path": path, "line": line_number, "environment": "plaintext-math"})
        if re.match(r"^(theorem|lemma|proposition|corollary|claim|remark|definition|assumption|proof)\b", lowered):
            env = lowered.split()[0]
            inventory["theorem_like_blocks"].append({"path": path, "line": line_number, "environment": env})
        if re.search(r"\bfigure\s+[0-9A-Z]", line, re.IGNORECASE):
            inventory["figures"].append({"path": path, "line": line_number, "environment": "plaintext-figure"})
        if re.search(r"\btable\s+[0-9A-Z]", line, re.IGNORECASE):
            inventory["tables"].append({"path": path, "line": line_number, "environment": "plaintext-table"})
        if re.search(r"\b(eq\.|equation|figure|table)\s+[0-9A-Z]", line, re.IGNORECASE):
            inventory["hardcoded_refs"].append({"path": path, "line": line_number, "text": normalize_whitespace(line)})
    return inventory


def build_pdf_chunks(pages: list[dict[str, Any]], path: str) -> list[ChunkRecord]:
    chunks: list[ChunkRecord] = []
    for page in pages:
        title = f"PDF page {page['page_number']}"
        risk_hints, coverage_priority = infer_chunk_hints(title, page["text"], int(page["token_estimate"]))
        chunks.append(
            ChunkRecord(
                id=f"{path}:page:{page['page_number']}",
                title=title,
                path=path,
                line_start=int(page["line_start"]),
                line_end=int(page["line_end"]),
                token_estimate=int(page["token_estimate"]),
                section_ids=[],
                risk_hints=risk_hints,
                coverage_priority=coverage_priority,
            )
        )
    return annotate_chunks(chunks)


def build_chunks(sections: list[SectionRecord], fallback_path: str, text: str) -> list[ChunkRecord]:
    if not sections:
        risk_hints, coverage_priority = infer_chunk_hints("Full document", text, estimate_tokens(text))
        return annotate_chunks(
            [
            ChunkRecord(
                id=f"{fallback_path}:full:1",
                title="Full document",
                path=fallback_path,
                line_start=1,
                line_end=len(text.splitlines()),
                token_estimate=estimate_tokens(text),
                risk_hints=risk_hints,
                coverage_priority=coverage_priority,
            )
        ]
        )

    chunks: list[ChunkRecord] = []
    for section in sections:
        risk_hints, coverage_priority = infer_chunk_hints(section.title, section.title, section.token_estimate)
        chunks.append(
            ChunkRecord(
                id=section.id,
                title=section.title,
                path=section.path,
                line_start=section.line_start,
                line_end=section.line_end,
                token_estimate=section.token_estimate,
                section_ids=[section.id],
                risk_hints=risk_hints,
                coverage_priority=coverage_priority,
            )
        )
    return annotate_chunks(chunks)


def derive_review_brief(entrypoint: Path, sections: list[SectionRecord], chunks: list[ChunkRecord]) -> ReviewBrief:
    outline = [section.title for section in sections[:6]]
    likely_hotspots = [chunk.title for chunk in chunks if chunk.risk_hints][:6]
    central_claim_hint = f"Review manuscript rooted at {entrypoint.name} for central contribution, evidence alignment, and internal consistency."
    summary = (
        f"{entrypoint.name} contains {len(sections)} indexed sections and {len(chunks)} review chunks. "
        f"Run the global passes, then a local chunk pass on every chunk, then verifier passes for high-risk or disputed findings."
    )
    return ReviewBrief(
        central_claim_hint=central_claim_hint,
        outline=outline,
        likely_hotspots=likely_hotspots,
        summary=summary,
        coverage_strategy="full-coverage-parallel",
    )


def build_document_profile(inventory: dict[str, Any], files: list[dict[str, Any]], chunks: list[ChunkRecord]) -> dict[str, Any]:
    total_words = sum(file["word_count"] for file in files)
    total_lines = sum(file["line_count"] for file in files)
    theorem_count = len(inventory["theorem_like_blocks"])
    equation_count = len(inventory["equations"])
    figure_count = len(inventory["figures"])
    table_count = len(inventory["tables"])
    avg_chunk_tokens = round(sum(chunk.token_estimate for chunk in chunks) / len(chunks), 2) if chunks else 0.0
    return {
        "file_count": len(files),
        "total_lines": total_lines,
        "total_words": total_words,
        "estimated_tokens": max(1, math.ceil(total_words * 1.33)),
        "chunk_count": len(chunks),
        "avg_chunk_tokens": avg_chunk_tokens,
        "equation_count": equation_count,
        "theorem_like_count": theorem_count,
        "figure_count": figure_count,
        "table_count": table_count,
        "needs_theory_notation_reviewer": theorem_count > 0 or equation_count >= 8,
        "likely_empirical_manuscript": table_count > 0 or figure_count > 0,
    }


def collect_bibliography(entrypoint_text: str, source_root: Path) -> list[dict[str, Any]]:
    bibs = []
    for match in re.finditer(r"\\(?:bibliography|addbibresource)\{([^}]+)\}", entrypoint_text):
        raw = match.group(1)
        for piece in raw.split(","):
            candidate = source_root / piece.strip()
            if not candidate.suffix:
                candidate = candidate.with_suffix(".bib")
            bibs.append({"path": relative_path(candidate, source_root), "exists": candidate.exists()})
    return bibs


def consolidate_inventory(items: list[dict[str, Any]]) -> dict[str, Any]:
    merged = {
        "labels": [],
        "refs": [],
        "citations": [],
        "equations": [],
        "theorem_like_blocks": [],
        "figures": [],
        "tables": [],
        "footnotes": [],
        "hardcoded_refs": [],
        "broken_link_candidates": [],
        "parser_backends": [],
    }
    for item in items:
        for key in merged:
            if key == "parser_backends":
                backend = item.get("parser_backend")
                if backend:
                    merged["parser_backends"].append(backend)
                continue
            merged[key].extend(item.get(key, []))
    merged["parser_backends"] = sorted(set(merged["parser_backends"]))
    return merged


def select_chunk_ids(chunks: list[ChunkRecord], keywords: tuple[str, ...], fallback_count: int = 0) -> list[str]:
    selected = [chunk.id for chunk in chunks if any(keyword in chunk.title.lower() for keyword in keywords)]
    if selected:
        return selected
    if fallback_count > 0:
        return [chunk.id for chunk in chunks[:fallback_count]]
    return []


def build_review_packets(
    chunks: list[ChunkRecord],
    inventory: dict[str, Any],
    profile: dict[str, Any],
) -> list[dict[str, Any]]:
    packets: list[dict[str, Any]] = []

    central_chunks = select_chunk_ids(chunks, ("abstract", "introduction", "conclusion", "discussion", "summary"), 2)
    exposition_chunks = [chunk.id for chunk in chunks if chunk.coverage_priority in {"high", "elevated"}][: max(3, min(len(chunks), 8))]
    empirical_chunks = select_chunk_ids(
        chunks,
        ("data", "result", "results", "empirical", "table", "figure", "appendix", "robustness", "heterogeneity"),
        0,
    )
    identification_chunks = select_chunk_ids(
        chunks,
        ("method", "methods", "identification", "strategy", "design", "result", "results", "appendix", "estimation"),
        0,
    )
    math_chunks = select_chunk_ids(chunks, ("model", "theorem", "lemma", "proof", "proposition", "assumption", "equation"), 0)
    notation_chunks = select_chunk_ids(chunks, ("notation", "definition", "setup", "model", "proof", "appendix"), 0)
    numbers_chunks = select_chunk_ids(chunks, ("result", "results", "table", "figure", "estimate", "effect", "appendix"), 0)
    cross_doc_chunks = select_chunk_ids(chunks, ("abstract", "introduction", "result", "results", "conclusion", "appendix"), 3)

    packets.extend(
        [
            {
                "packet_id": "packet-central-claim-and-positioning",
                "role": "CentralClaimAndPositioning",
                "kind": "global",
                "required": True,
                "chunk_ids": central_chunks,
                "boundary_chunk_ids": [],
                "notes": "Restate the paper's contribution, novelty, and scope as readers would understand it.",
            },
            {
                "packet_id": "packet-exposition-and-clarity",
                "role": "ExpositionAndClarity",
                "kind": "global",
                "required": True,
                "chunk_ids": exposition_chunks,
                "boundary_chunk_ids": [],
                "notes": "Check whether the exposition is legible, staged well, and clear on what is known versus newly shown.",
            },
            {
                "packet_id": "packet-empirical-consistency",
                "role": "EmpiricalConsistency",
                "kind": "global",
                "required": bool(profile.get("likely_empirical_manuscript")),
                "chunk_ids": empirical_chunks,
                "boundary_chunk_ids": [],
                "notes": "Compare quantitative claims to visible tables, figures, appendices, and specification language.",
            },
            {
                "packet_id": "packet-identification-and-causal-language",
                "role": "IdentificationAndCausalLanguage",
                "kind": "global",
                "required": bool(profile.get("likely_empirical_manuscript")),
                "chunk_ids": identification_chunks,
                "boundary_chunk_ids": [],
                "notes": "Check estimands, timing, identifying assumptions, and whether causal language outruns the design.",
            },
            {
                "packet_id": "packet-mathematical-and-logical-reasoning",
                "role": "MathematicalAndLogicalReasoning",
                "kind": "global",
                "required": bool(profile.get("needs_theory_notation_reviewer")),
                "chunk_ids": math_chunks,
                "boundary_chunk_ids": [],
                "notes": "Look for logical gaps, unstated cases, and proof steps that do not follow from visible assumptions.",
            },
            {
                "packet_id": "packet-notation-and-definitions",
                "role": "NotationAndDefinitions",
                "kind": "global",
                "required": bool(profile.get("needs_theory_notation_reviewer")),
                "chunk_ids": notation_chunks,
                "boundary_chunk_ids": [],
                "notes": "Track symbol definitions, reuse, collisions, and assumptions used before statement.",
            },
            {
                "packet_id": "packet-internal-references-and-build",
                "role": "InternalReferencesAndBuild",
                "kind": "global",
                "required": True,
                "chunk_ids": [chunk.id for chunk in chunks[: min(4, len(chunks))]],
                "boundary_chunk_ids": [],
                "notes": "Use inventories, include graphs, and existing logs to check labels, refs, citations, and buildability.",
                "inventory_targets": {
                    "labels": len(inventory.get("labels", [])),
                    "refs": len(inventory.get("refs", [])),
                    "citations": len(inventory.get("citations", [])),
                },
            },
            {
                "packet_id": "packet-numbers-units-and-signs",
                "role": "NumbersUnitsAndSigns",
                "kind": "global",
                "required": True,
                "chunk_ids": numbers_chunks,
                "boundary_chunk_ids": [],
                "notes": "Check magnitudes, signs, units, denominators, and percentage versus percentage-point wording.",
            },
            {
                "packet_id": "packet-cross-document-consistency",
                "role": "CrossDocumentConsistency",
                "kind": "global",
                "required": True,
                "chunk_ids": cross_doc_chunks,
                "boundary_chunk_ids": [],
                "notes": "Look for drift across abstract, introduction, results, conclusion, and appendices.",
            },
        ]
    )

    for chunk in chunks:
        packets.append(
            {
                "packet_id": f"packet-{slugify(chunk.id)}",
                "role": "ChunkReviewer",
                "kind": "chunk",
                "required": True,
                "chunk_ids": [chunk.id],
                "boundary_chunk_ids": chunk.adjacent_chunk_ids,
                "notes": "Review this chunk line-by-line, using adjacent chunks only for local continuity and definitions.",
                "coverage_priority": chunk.coverage_priority,
            }
        )

    packets.append(
        {
            "packet_id": "packet-finding-verifier",
            "role": "FindingVerifier",
            "kind": "verification",
            "required": True,
            "chunk_ids": [],
            "boundary_chunk_ids": [],
            "selection_rules": [
                "Verify every High or Critical finding before final ranking.",
                "Verify any finding with conflicting evidence or low-confidence support.",
                "Reject findings that collapse under direct re-reading of the cited text.",
            ],
            "notes": "The verifier pass runs after candidate findings exist and can confirm, narrow, or reject them.",
        }
    )

    return packets


def build_coverage_units(chunks: list[ChunkRecord]) -> list[dict[str, Any]]:
    return [
        {
            "unit_id": chunk.id,
            "title": chunk.title,
            "path": chunk.path,
            "line_start": chunk.line_start,
            "line_end": chunk.line_end,
            "token_estimate": chunk.token_estimate,
            "coverage_priority": chunk.coverage_priority,
            "adjacent_chunk_ids": chunk.adjacent_chunk_ids,
        }
        for chunk in chunks
    ]


def build_coverage_status(chunks: list[ChunkRecord], review_packets: list[dict[str, Any]]) -> dict[str, Any]:
    chunk_packet_ids = [packet["packet_id"] for packet in review_packets if packet["role"] == "ChunkReviewer"]
    return {
        "mode": "full-coverage-parallel",
        "status": "planned",
        "required_chunk_ids": [chunk.id for chunk in chunks],
        "planned_chunk_packet_ids": chunk_packet_ids,
        "required_chunk_review_count": len(chunk_packet_ids),
        "verification_required_for": ["High", "Critical", "conflicted", "low-confidence"],
        "uncovered_chunk_ids": [],
    }


def build_manifest(target: Path) -> dict[str, Any]:
    entrypoint, source_root = choose_entrypoint(target)
    extension = entrypoint.suffix.lower()

    if extension in LATEX_EXTENSIONS:
        files, include_edges, unresolved_includes, circular_includes = walk_tex_graph(entrypoint, source_root)
        inventories = []
        sections = []
        for file_record in files:
            file_path = Path(file_record["absolute_path"])
            text = read_text(file_path)
            inventories.append(extract_tex_inventory(text, file_record["path"]))
            sections.extend(extract_tex_sections(text, file_record["path"]))
        inventory = consolidate_inventory(inventories)
        entry_text = read_text(entrypoint)
        bibliography_files = collect_bibliography(entry_text, source_root)
        logs = parse_existing_logs(source_root, entrypoint)
        coverage_notes = []
        if unresolved_includes:
            coverage_notes.append("Some LaTeX includes could not be resolved.")
        if circular_includes:
            coverage_notes.append("Circular LaTeX includes were detected.")
        chunks = build_chunks(sections, relative_path(entrypoint, source_root), entry_text)
        review_brief = derive_review_brief(entrypoint, sections, chunks)
        profile = build_document_profile(inventory, files, chunks)
        profile["estimated_tokens"] = sum(chunk.token_estimate for chunk in chunks)
        review_packets = build_review_packets(chunks, inventory, profile)
        manifest = {
            "schema_version": "1",
            "extraction_version": "2026-03-23",
            "generated_at": utc_now(),
            "target": str(target.resolve()),
            "source_root": str(source_root.resolve()),
            "entrypoint": relative_path(entrypoint, source_root),
            "input_kind": "latex",
            "resolved_files": files,
            "include_edges": include_edges,
            "unresolved_includes": unresolved_includes,
            "circular_includes": circular_includes,
            "sections": [asdict(section) for section in sections],
            "chunks": [asdict(chunk) for chunk in chunks],
            "inventory": inventory,
            "bibliography_files": bibliography_files,
            "diagnostics": {"existing_logs": logs},
            "document_profile": profile,
            "review_brief": asdict(review_brief),
            "coverage_units": build_coverage_units(chunks),
            "coverage_status": build_coverage_status(chunks, review_packets),
            "review_packets": review_packets,
            "coverage_notes": coverage_notes,
        }
    elif extension in PDF_EXTENSIONS:
        pdf_text, pdf_pages, pdf_warnings, parser_backend = read_pdf_text(entrypoint)
        file_record = {
            "path": relative_path(entrypoint, source_root),
            "absolute_path": str(entrypoint.resolve()),
            "role": "entrypoint",
            "line_count": len(pdf_text.splitlines()),
            "word_count": len(re.findall(r"\S+", pdf_text)),
            "file_hash": sha256_file(entrypoint),
            "page_count": len(pdf_pages),
        }
        sections: list[SectionRecord] = []
        chunks = build_pdf_chunks(pdf_pages, file_record["path"])
        inventory = extract_plaintext_inventory(pdf_text, file_record["path"], parser_backend)
        review_brief = derive_review_brief(entrypoint, sections, chunks)
        profile = build_document_profile(inventory, [file_record], chunks)
        profile["estimated_tokens"] = estimate_tokens(pdf_text or " ")
        review_packets = build_review_packets(chunks, inventory, profile)
        coverage_notes = [
            "PDF extraction is lossy relative to source LaTeX. Treat line references as extracted-text positions.",
        ]
        coverage_notes.extend(pdf_warnings)
        manifest = {
            "schema_version": "1",
            "extraction_version": "2026-03-23",
            "generated_at": utc_now(),
            "target": str(target.resolve()),
            "source_root": str(source_root.resolve()),
            "entrypoint": relative_path(entrypoint, source_root),
            "input_kind": "pdf",
            "resolved_files": [file_record],
            "include_edges": [],
            "unresolved_includes": [],
            "circular_includes": [],
            "sections": [],
            "chunks": [asdict(chunk) for chunk in chunks],
            "inventory": inventory,
            "bibliography_files": [],
            "diagnostics": {
                "existing_logs": {"log_files": [], "overfull_hbox": [], "undefined_control_sequence": []},
                "pdf_pages": pdf_pages,
            },
            "document_profile": profile,
            "review_brief": asdict(review_brief),
            "coverage_units": build_coverage_units(chunks),
            "coverage_status": build_coverage_status(chunks, review_packets),
            "review_packets": review_packets,
            "coverage_notes": coverage_notes,
        }
    elif extension in MARKDOWN_EXTENSIONS:
        text = read_text(entrypoint)
        file_record = {
            "path": relative_path(entrypoint, source_root),
            "absolute_path": str(entrypoint.resolve()),
            "role": "entrypoint",
            "line_count": len(text.splitlines()),
            "word_count": len(re.findall(r"\S+", text)),
            "file_hash": sha256_file(entrypoint),
        }
        sections = extract_markdown_sections(text, file_record["path"])
        chunks = build_chunks(sections, file_record["path"], text)
        inventory = extract_markdown_inventory(text, file_record["path"])
        review_brief = derive_review_brief(entrypoint, sections, chunks)
        profile = build_document_profile(inventory, [file_record], chunks)
        profile["estimated_tokens"] = estimate_tokens(text)
        review_packets = build_review_packets(chunks, inventory, profile)
        manifest = {
            "schema_version": "1",
            "extraction_version": "2026-03-23",
            "generated_at": utc_now(),
            "target": str(target.resolve()),
            "source_root": str(source_root.resolve()),
            "entrypoint": relative_path(entrypoint, source_root),
            "input_kind": "markdown",
            "resolved_files": [file_record],
            "include_edges": [],
            "unresolved_includes": [],
            "circular_includes": [],
            "sections": [asdict(section) for section in sections],
            "chunks": [asdict(chunk) for chunk in chunks],
            "inventory": inventory,
            "bibliography_files": [],
            "diagnostics": {"existing_logs": {"log_files": [], "overfull_hbox": [], "undefined_control_sequence": []}},
            "document_profile": profile,
            "review_brief": asdict(review_brief),
            "coverage_units": build_coverage_units(chunks),
            "coverage_status": build_coverage_status(chunks, review_packets),
            "review_packets": review_packets,
            "coverage_notes": [],
        }
    else:
        raise ValueError(f"Unsupported manuscript type: {entrypoint.suffix}")

    manifest["manifest_hash"] = sha256_text(json.dumps(manifest, sort_keys=True))
    return manifest


def cli() -> None:
    parser = argparse.ArgumentParser(description="Build a manuscript manifest for Refine-style review.")
    parser.add_argument("target", help="Markdown file, LaTeX root file, or manuscript directory")
    parser.add_argument("--output", help="Write manifest JSON to this path")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    args = parser.parse_args()

    manifest = build_manifest(Path(args.target))
    output = json.dumps(manifest, indent=2 if args.pretty or args.output else None, sort_keys=True)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output + "\n", encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    cli()
