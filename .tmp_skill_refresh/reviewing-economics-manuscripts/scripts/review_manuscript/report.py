from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any


OVERLEAF_PAPER_ROOT = Path("/Users/jacopoolivieri/Library/CloudStorage/Dropbox/Apps/Overleaf/Sewage in Our Waters").resolve()


def default_output_dir(source_root: Path) -> Path:
    resolved = source_root.resolve()
    if resolved == OVERLEAF_PAPER_ROOT:
        return resolved / "review_reports"
    return resolved / "review_reports"


def timestamp_slug(generated_at: str) -> str:
    parsed = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    return parsed.strftime("%Y-%m-%d__%H%M%S")


def derive_output_paths(manifest: dict[str, Any], explicit_output: str | None) -> tuple[Path, Path]:
    if explicit_output:
        report_path = Path(explicit_output)
        if report_path.suffix != ".md":
            report_path = report_path.with_suffix(".md")
        sidecar = report_path.with_suffix(".findings.json")
        return report_path, sidecar

    source_root = Path(manifest["source_root"])
    output_dir = default_output_dir(source_root)
    stem = Path(manifest["entrypoint"]).stem
    slug = timestamp_slug(manifest["generated_at"])
    report_path = output_dir / f"{stem}__{slug}__refine_review.md"
    sidecar = output_dir / f"{stem}__{slug}__refine_review.findings.json"
    return report_path, sidecar


def location_label(location: dict[str, Any]) -> str:
    return f"{location['path']}:{location['line_start']}-{location['line_end']}"


def fallback_outline(manifest: dict[str, Any]) -> str:
    outline = manifest["review_brief"]["outline"]
    if not outline:
        return "The manuscript structure could not be summarized from the available source map."
    if len(outline) == 1:
        return f"The manuscript centers on {outline[0]}."
    joined = ", ".join(outline[:-1]) + f", and {outline[-1]}"
    return f"The manuscript proceeds through {joined}."


def fallback_overall_feedback(merged: dict[str, Any]) -> str:
    finding_count = merged["finding_count"]
    if finding_count == 0:
        return "No retained findings survived verification from the available review inputs."
    top = merged["findings"][0]
    return (
        f"The review retained {finding_count} substantive findings after merge and verification. "
        f"The most consequential issues cluster around {top['category'].replace('_', ' ')}."
    )


def overall_feedback_text(manifest: dict[str, Any], merged: dict[str, Any], synthesis: dict[str, Any]) -> str:
    base = synthesis.get("overall_feedback", fallback_overall_feedback(merged))
    central_claim = synthesis.get("central_claim")
    if not central_claim:
        return base
    return f"{base}\n\nCentral claim as read in review: {central_claim}"


def render_high_level_concerns(merged: dict[str, Any], synthesis: dict[str, Any]) -> list[str]:
    concerns = synthesis.get("high_level_concerns")
    if concerns:
        lines = []
        for concern in concerns[:6]:
            refs = ", ".join(concern.get("related_finding_ids", []))
            label = f" Related findings: {refs}." if refs else ""
            lines.append(f"- **{concern['title']}**: {concern['summary']}{label}")
        return lines

    lines = []
    for theme in merged.get("reflection_themes", [])[:6]:
        refs = ", ".join(theme["related_finding_ids"])
        lines.append(f"- **{theme['title']}**: {theme['summary']} Related findings: {refs}.")
    if not lines:
        lines.append("- No major concerns were derived from the retained finding set.")
    return lines


def render_finding_block(index: int, finding: dict[str, Any]) -> list[str]:
    lines = [
        f"### #{index} {finding['title']}",
        "",
        f"- `ID`: `{finding['id']}`",
        f"- `Category`: `{finding['category']}` / `{finding['subcategory']}`",
        f"- `Severity`: `{finding['severity']}`",
        f"- `Status`: `{finding['status']}`",
        f"- `Verification`: `{finding['verification_state']}`",
        f"- `Confidence`: `{finding['confidence']}`",
        f"- `Claim threat`: `{finding['claim_threat_score']}`",
        f"- `Chunk`: `{finding['chunk_id']}`",
        f"- `Location`: `{location_label(finding['location'])}`",
        f"- `Quote`: {finding['evidence'].get('quote', '').strip() or 'No short quote provided.'}",
        f"- `Issue`: {finding['detail']}",
        f"- `Why it matters`: {finding['why_it_matters']}",
        f"- `Suggested action`: {finding['recommendation']}",
    ]
    supporting_refs = finding["evidence"].get("supporting_refs", [])
    if supporting_refs:
        refs = ", ".join(location_label(ref) for ref in supporting_refs)
        lines.append(f"- `Supporting refs`: {refs}")
    counterevidence = finding.get("counterevidence_refs", [])
    if counterevidence:
        refs = ", ".join(location_label(ref) for ref in counterevidence)
        lines.append(f"- `Counterevidence refs`: {refs}")
    lines.append("")
    return lines


def render_detailed_findings_by_relevance(merged: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for index, finding in enumerate(merged["findings"], start=1):
        lines.extend(render_finding_block(index, finding))
    if not lines:
        return ["No retained issues found.", ""]
    return lines


def render_detailed_findings_by_position(merged: dict[str, Any]) -> list[str]:
    findings = sorted(
        merged["findings"],
        key=lambda item: (
            item["location"]["path"],
            item["location"]["line_start"],
            item["location"]["line_end"],
            item["id"],
        ),
    )
    lines: list[str] = []
    for index, finding in enumerate(findings, start=1):
        lines.extend(render_finding_block(index, finding))
    if not lines:
        return ["No retained issues found.", ""]
    return lines


def render_coverage_ledger(manifest: dict[str, Any], synthesis: dict[str, Any]) -> list[str]:
    coverage_summary = synthesis.get("coverage_summary", {})
    completed_chunk_ids = set(coverage_summary.get("completed_chunk_ids", []))
    completed_packet_ids = set(coverage_summary.get("completed_packet_ids", []))
    lines = [
        f"- `Mode`: `{manifest['coverage_status']['mode']}`",
        f"- `Planned chunk reviews`: `{manifest['coverage_status']['required_chunk_review_count']}`",
    ]
    if completed_chunk_ids:
        lines.append(f"- `Completed chunk reviews`: `{len(completed_chunk_ids)}`")
    else:
        lines.append("- `Execution status`: No completed chunk list was provided; ledger reflects planned coverage.")
    if completed_packet_ids:
        lines.append(f"- `Completed packets`: `{len(completed_packet_ids)}`")

    for unit in manifest.get("coverage_units", []):
        if completed_chunk_ids:
            status = "completed" if unit["unit_id"] in completed_chunk_ids else "missing"
        else:
            status = "planned"
        lines.append(
            f"- `{unit['unit_id']}` [{status}/{unit['coverage_priority']}] "
            f"`{unit['path']}:{unit['line_start']}-{unit['line_end']}` {unit['title']}"
        )
    return lines


def render_source_notes(manifest: dict[str, Any]) -> list[str]:
    lines = []
    if manifest["unresolved_includes"]:
        lines.append("- Missing includes were detected:")
        for item in manifest["unresolved_includes"]:
            source = item.get("source", item.get("path", "unknown"))
            lines.append(f"  - `{source}` -> `{item.get('resolved_target', item.get('path', 'unknown'))}`")
    if manifest["circular_includes"]:
        lines.append("- Circular includes were detected:")
        for item in manifest["circular_includes"]:
            lines.append(f"  - `{' -> '.join(item['cycle'])}`")
    backends = manifest["inventory"].get("parser_backends")
    if isinstance(backends, list) and backends:
        lines.append(f"- Parser backends used: {', '.join(backends)}")
    elif manifest["inventory"].get("parser_backend"):
        lines.append(f"- Parser backend used: {manifest['inventory']['parser_backend']}")
    inaccessible_bibs = [item["path"] for item in manifest.get("bibliography_files", []) if not item.get("exists", False)]
    if inaccessible_bibs:
        lines.append(f"- Inaccessible bibliography files: {', '.join(inaccessible_bibs)}")
    if manifest.get("input_kind") == "pdf":
        lines.append("- PDF review uses extracted text positions rather than author-side source line numbers.")
    for note in manifest.get("coverage_notes", []):
        lowered = note.lower()
        if "lossy" in lowered or "pdf" in lowered:
            lines.append(f"- {note}")
    if not lines:
        lines.append("- No source resolution issues were recorded.")
    return lines


def render_coverage_gaps(manifest: dict[str, Any], merged: dict[str, Any]) -> list[str]:
    notes = list(manifest.get("coverage_notes") or [])
    rejected = int(merged.get("rejected_finding_count", 0))
    if rejected:
        notes.append(f"{rejected} candidate findings were rejected during verification and excluded from the final ranking.")
    if not notes:
        return ["- No explicit coverage gaps were recorded during manifest construction or merge."]
    return [f"- {note}" for note in notes]


def render_markdown(manifest: dict[str, Any], merged: dict[str, Any], synthesis: dict[str, Any] | None) -> str:
    synthesis = synthesis or {}
    worker_count = synthesis.get("worker_count", len({finding["pass_name"] for finding in merged["findings"]}))

    lines = [
        f"# Refine-Like Review: {Path(manifest['entrypoint']).name}",
        "",
        f"- `Generated`: {manifest['generated_at']}",
        f"- `Source root`: `{manifest['source_root']}`",
        f"- `Entrypoint`: `{manifest['entrypoint']}`",
        f"- `Input kind`: `{manifest['input_kind']}`",
        f"- `Manifest hash`: `{manifest['manifest_hash']}`",
        f"- `Worker count`: `{worker_count}`",
        f"- `Skill version`: `{manifest['extraction_version']}`",
        "",
        "## Overall Feedback",
        "",
        overall_feedback_text(manifest, merged, synthesis),
        "",
        "## Outline",
        "",
        synthesis.get("outline", fallback_outline(manifest)),
        "",
        "## High-Level Concerns",
        "",
    ]
    lines.extend(render_high_level_concerns(merged, synthesis))
    lines.extend(["", "## Detailed Feedback By Relevance", ""])
    lines.extend(render_detailed_findings_by_relevance(merged))
    lines.extend(["## Detailed Feedback By Position", ""])
    lines.extend(render_detailed_findings_by_position(merged))
    lines.extend(["## Coverage Ledger", ""])
    lines.extend(render_coverage_ledger(manifest, synthesis))
    lines.extend(["", "## Source Resolution Notes", ""])
    lines.extend(render_source_notes(manifest))
    lines.extend(["", "## Coverage Gaps", ""])
    lines.extend(render_coverage_gaps(manifest, merged))
    lines.append("")
    return "\n".join(lines)


def render_report(manifest_path: Path, findings_path: Path, output_path: str | None, synthesis_path: str | None) -> tuple[Path, Path]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    merged = json.loads(findings_path.read_text(encoding="utf-8"))
    synthesis = json.loads(Path(synthesis_path).read_text(encoding="utf-8")) if synthesis_path else None

    report_path, sidecar_path = derive_output_paths(manifest, output_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_markdown(manifest, merged, synthesis), encoding="utf-8")
    sidecar_path.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report_path, sidecar_path


def cli() -> None:
    parser = argparse.ArgumentParser(description="Render a Refine-like manuscript review report.")
    parser.add_argument("manifest", help="Manifest JSON path")
    parser.add_argument("findings", help="Merged findings JSON path")
    parser.add_argument("--output", help="Markdown output path")
    parser.add_argument("--synthesis", help="Optional JSON file with overall_feedback, outline, high_level_concerns, coverage_summary, and worker_count")
    args = parser.parse_args()
    report_path, sidecar_path = render_report(Path(args.manifest), Path(args.findings), args.output, args.synthesis)
    print(json.dumps({"report_path": str(report_path), "findings_sidecar": str(sidecar_path)}, indent=2))


if __name__ == "__main__":
    cli()
