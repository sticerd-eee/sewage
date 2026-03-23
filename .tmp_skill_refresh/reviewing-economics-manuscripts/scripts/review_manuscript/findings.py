from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


SEVERITY_ORDER = {"Critical": 4, "High": 3, "Medium": 2, "Low": 1}
STATUS_ORDER = {"Confirmed": 3, "Likely": 2, "Unverified": 1}
CONFIDENCE_ORDER = {"High": 3, "Medium": 2, "Low": 1}
VERIFICATION_ORDER = {"Verified": 4, "NeedsAuthorCheck": 3, "NeedsVerification": 2, "Rejected": 1}
CATEGORY_WEIGHTS = {
    "identification_and_causal_language": 36,
    "empirical_consistency": 34,
    "mathematical_and_logical_reasoning": 34,
    "notation_and_definitions": 28,
    "cross_document_consistency": 26,
    "central_claim_and_positioning": 24,
    "numbers_units_and_signs": 22,
    "exposition_and_clarity": 18,
    "internal_references_and_build": 14,
}
THEME_TITLES = {
    "central_claim_and_positioning": "Central claim and positioning",
    "exposition_and_clarity": "Exposition and clarity",
    "empirical_consistency": "Empirical consistency",
    "identification_and_causal_language": "Identification and causal language",
    "mathematical_and_logical_reasoning": "Mathematical and logical reasoning",
    "notation_and_definitions": "Notation and definitions",
    "internal_references_and_build": "Internal references and buildability",
    "numbers_units_and_signs": "Numbers, units, and signs",
    "cross_document_consistency": "Cross-document consistency",
}
LEGACY_CATEGORY_MAP = {
    "structure_and_spine": "central_claim_and_positioning",
    "claims_evidence_alignment": "empirical_consistency",
    "economics_identification": "identification_and_causal_language",
    "theory_and_notation": "notation_and_definitions",
    "numbers_signs_and_units": "numbers_units_and_signs",
    "markup_and_references": "internal_references_and_build",
    "cross_document_consistency": "cross_document_consistency",
}
REQUIRED_FIELDS = {
    "schema_version",
    "pass_name",
    "category",
    "subcategory",
    "severity",
    "status",
    "title",
    "summary",
    "detail",
    "why_it_matters",
    "location",
    "chunk_id",
    "evidence",
    "recommendation",
    "confidence",
    "verification_state",
    "claim_threat_score",
}


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def location_key(location: dict[str, Any]) -> tuple[str, int, int]:
    return (
        location.get("path", ""),
        int(location.get("line_start", 0) or 0),
        int(location.get("line_end", 0) or 0),
    )


def default_claim_threat(severity: str) -> int:
    return {"Critical": 95, "High": 80, "Medium": 55, "Low": 25}[severity]


def normalize_category(category: str) -> str:
    normalized = LEGACY_CATEGORY_MAP.get(category, category)
    if normalized not in CATEGORY_WEIGHTS:
        raise ValueError(f"Invalid category: {category}")
    return normalized


def upgrade_legacy_finding(raw: dict[str, Any]) -> dict[str, Any]:
    upgraded = dict(raw)
    upgraded["schema_version"] = "2"
    upgraded["category"] = normalize_category(upgraded["category"])
    upgraded.setdefault("subcategory", upgraded["category"])
    upgraded.setdefault("why_it_matters", upgraded.get("summary", "").strip())
    upgraded.setdefault("verification_state", "NeedsVerification")
    upgraded.setdefault("claim_threat_score", default_claim_threat(upgraded["severity"]))
    location = upgraded.get("location") or {}
    upgraded.setdefault(
        "chunk_id",
        f"{location.get('path', 'unknown')}:{location.get('line_start', 0)}-{location.get('line_end', 0)}",
    )
    upgraded.setdefault("counterevidence_refs", [])
    upgraded.setdefault("related_finding_ids", [])
    return upgraded


def validate_finding(raw: dict[str, Any]) -> dict[str, Any]:
    if str(raw.get("schema_version", "")) == "1":
        raw = upgrade_legacy_finding(raw)

    raw["schema_version"] = str(raw.get("schema_version", ""))
    raw["category"] = normalize_category(str(raw.get("category", "")))
    raw.setdefault("counterevidence_refs", [])
    raw.setdefault("related_finding_ids", [])
    raw.setdefault("evidence", {})
    raw["evidence"].setdefault("quote", "")
    raw["evidence"].setdefault("supporting_refs", [])

    missing = sorted(REQUIRED_FIELDS - raw.keys())
    if missing:
        raise ValueError(f"Finding missing required fields: {', '.join(missing)}")

    severity = raw["severity"]
    status = raw["status"]
    confidence = raw["confidence"]
    verification_state = raw["verification_state"]
    if severity not in SEVERITY_ORDER:
        raise ValueError(f"Invalid severity: {severity}")
    if status not in STATUS_ORDER:
        raise ValueError(f"Invalid status: {status}")
    if confidence not in CONFIDENCE_ORDER:
        raise ValueError(f"Invalid confidence: {confidence}")
    if verification_state not in VERIFICATION_ORDER:
        raise ValueError(f"Invalid verification_state: {verification_state}")

    location = raw.get("location") or {}
    if not location.get("path"):
        raise ValueError("Finding location.path is required")

    claim_threat_score = int(raw["claim_threat_score"])
    if claim_threat_score < 0 or claim_threat_score > 100:
        raise ValueError("claim_threat_score must be between 0 and 100")
    raw["claim_threat_score"] = claim_threat_score

    raw["title"] = raw["title"].strip()
    raw["summary"] = raw["summary"].strip()
    raw["detail"] = raw["detail"].strip()
    raw["why_it_matters"] = raw["why_it_matters"].strip()
    raw["recommendation"] = raw["recommendation"].strip()
    raw["subcategory"] = raw["subcategory"].strip()
    raw["chunk_id"] = raw["chunk_id"].strip()
    return raw


def finding_hash_basis(finding: dict[str, Any]) -> str:
    location = finding["location"]
    return "||".join(
        [
            finding["category"],
            finding["subcategory"],
            normalize_text(finding["title"]),
            normalize_text(finding["summary"]),
            location.get("path", ""),
            str(location.get("line_start", "")),
            str(location.get("line_end", "")),
        ]
    )


def assign_stable_id(finding: dict[str, Any]) -> str:
    digest = hashlib.sha1(finding_hash_basis(finding).encode("utf-8")).hexdigest()[:10]
    return f"F-{digest}"


def jaccard_similarity(left: str, right: str) -> float:
    left_tokens = set(re.findall(r"[a-z0-9]+", left.lower()))
    right_tokens = set(re.findall(r"[a-z0-9]+", right.lower()))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def should_merge(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if left["category"] != right["category"]:
        return False
    if left["location"]["path"] != right["location"]["path"]:
        return False
    left_start = int(left["location"]["line_start"])
    left_end = int(left["location"]["line_end"])
    right_start = int(right["location"]["line_start"])
    right_end = int(right["location"]["line_end"])
    overlaps = max(left_start, right_start) <= min(left_end, right_end)
    if not overlaps:
        return False
    title_similarity = jaccard_similarity(left["title"], right["title"])
    summary_similarity = jaccard_similarity(left["summary"], right["summary"])
    return title_similarity >= 0.5 or summary_similarity >= 0.6


def unique_refs(refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, int, int]] = set()
    deduped: list[dict[str, Any]] = []
    for ref in refs:
        key = (
            ref.get("path", ""),
            int(ref.get("line_start", 0) or 0),
            int(ref.get("line_end", 0) or 0),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(ref)
    return deduped


def merge_pair(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    merged = dict(left)
    merged["severity"] = max([left["severity"], right["severity"]], key=lambda value: SEVERITY_ORDER[value])
    merged["status"] = max([left["status"], right["status"]], key=lambda value: STATUS_ORDER[value])
    merged["confidence"] = max([left["confidence"], right["confidence"]], key=lambda value: CONFIDENCE_ORDER[value])
    merged["verification_state"] = max(
        [left["verification_state"], right["verification_state"]],
        key=lambda value: VERIFICATION_ORDER[value],
    )
    merged["claim_threat_score"] = max(int(left["claim_threat_score"]), int(right["claim_threat_score"]))
    merged["pass_name"] = "+".join(sorted(set((left["pass_name"] + "+" + right["pass_name"]).split("+"))))
    merged["related_finding_ids"] = sorted(set(left.get("related_finding_ids", []) + right.get("related_finding_ids", [])))
    merged["evidence"] = {
        "quote": left["evidence"].get("quote") or right["evidence"].get("quote") or "",
        "supporting_refs": unique_refs(left["evidence"].get("supporting_refs", []) + right["evidence"].get("supporting_refs", [])),
    }
    merged["counterevidence_refs"] = unique_refs(
        left.get("counterevidence_refs", []) + right.get("counterevidence_refs", [])
    )

    for key in ("detail", "summary", "recommendation", "why_it_matters"):
        if len(right[key]) > len(left[key]):
            merged[key] = right[key]
    if len(right.get("subcategory", "")) > len(left.get("subcategory", "")):
        merged["subcategory"] = right["subcategory"]
    if right.get("chunk_id") and not left.get("chunk_id"):
        merged["chunk_id"] = right["chunk_id"]
    return merged


def evidence_strength(finding: dict[str, Any]) -> int:
    quote_points = 4 if finding["evidence"].get("quote", "").strip() else 0
    supporting_points = min(6, len(finding["evidence"].get("supporting_refs", [])) * 2)
    counter_penalty = min(4, len(finding.get("counterevidence_refs", [])))
    return quote_points + supporting_points - counter_penalty


def rank_score(finding: dict[str, Any]) -> int:
    return (
        VERIFICATION_ORDER[finding["verification_state"]] * 200
        + SEVERITY_ORDER[finding["severity"]] * 120
        + STATUS_ORDER[finding["status"]] * 25
        + CONFIDENCE_ORDER[finding["confidence"]] * 15
        + CATEGORY_WEIGHTS.get(finding["category"], 0)
        + int(finding["claim_threat_score"])
        + evidence_strength(finding)
    )


def derive_themes(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for finding in findings:
        grouped[finding["category"]].append(finding)

    themes = []
    for category, items in grouped.items():
        ordered = sorted(items, key=rank_score, reverse=True)
        lead = ordered[0]
        themes.append(
            {
                "title": THEME_TITLES.get(category, category.replace("_", " ").title()),
                "category": category,
                "related_finding_ids": [item["id"] for item in ordered[:4]],
                "summary": lead["why_it_matters"] or lead["summary"],
                "finding_count": len(items),
                "top_severity": ordered[0]["severity"],
                "verification_state": ordered[0]["verification_state"],
            }
        )
    themes.sort(
        key=lambda item: (
            SEVERITY_ORDER[item["top_severity"]],
            VERIFICATION_ORDER[item["verification_state"]],
            item["finding_count"],
        ),
        reverse=True,
    )
    return themes[:6]


def load_findings(paths: list[Path]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for path in paths:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            payloads = data
        elif isinstance(data, dict) and isinstance(data.get("findings"), list):
            payloads = data["findings"]
        else:
            raise ValueError(f"Unsupported findings file shape: {path}")
        for payload in payloads:
            finding = validate_finding(dict(payload))
            finding["id"] = payload.get("id") or assign_stable_id(finding)
            findings.append(finding)
    return findings


def dedupe_findings(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for finding in findings:
        buckets[(finding["category"], finding["location"]["path"])].append(finding)

    merged: list[dict[str, Any]] = []
    for _, bucket in buckets.items():
        ordered = sorted(bucket, key=lambda item: (location_key(item["location"]), item["id"]))
        consumed = [False] * len(ordered)
        for index, candidate in enumerate(ordered):
            if consumed[index]:
                continue
            current = candidate
            for other_index in range(index + 1, len(ordered)):
                if consumed[other_index]:
                    continue
                if should_merge(current, ordered[other_index]):
                    current = merge_pair(current, ordered[other_index])
                    current["id"] = assign_stable_id(current)
                    consumed[other_index] = True
            merged.append(current)
    return merged


def merge_findings(paths: list[Path]) -> dict[str, Any]:
    loaded = load_findings(paths)
    deduped = dedupe_findings(loaded)
    rejected = [finding for finding in deduped if finding["verification_state"] == "Rejected"]
    retained = [finding for finding in deduped if finding["verification_state"] != "Rejected"]
    ranked = sorted(
        retained,
        key=lambda item: (
            rank_score(item),
            item["location"]["path"],
            item["location"]["line_start"],
            item["id"],
        ),
        reverse=True,
    )
    return {
        "schema_version": "2",
        "input_files": [str(path) for path in paths],
        "finding_count": len(ranked),
        "rejected_finding_count": len(rejected),
        "findings": ranked,
        "rejected_findings": rejected,
        "reflection_themes": derive_themes(ranked),
    }


def cli() -> None:
    parser = argparse.ArgumentParser(description="Merge structured manuscript review findings.")
    parser.add_argument("paths", nargs="+", help="JSON files containing review findings")
    parser.add_argument("--output", required=True, help="Write merged findings JSON to this path")
    args = parser.parse_args()

    merged = merge_findings([Path(path) for path in args.paths])
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    cli()
