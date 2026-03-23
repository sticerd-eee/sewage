from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from review_manuscript.findings import merge_findings
from review_manuscript.manifest import build_manifest
from review_manuscript.report import render_report


class ManifestBuilderTests(unittest.TestCase):
    def test_build_markdown_manifest_has_full_coverage_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manuscript = root / "draft.md"
            manuscript.write_text(
                "# Title\n\n## Introduction\nText with [local](figure.png) and $x+y$.\n\n## Results\nA result.\n",
                encoding="utf-8",
            )

            manifest = build_manifest(manuscript)
            self.assertEqual(manifest["input_kind"], "markdown")
            self.assertEqual(manifest["entrypoint"], "draft.md")
            self.assertGreaterEqual(len(manifest["sections"]), 2)
            self.assertTrue(manifest["inventory"]["broken_link_candidates"])
            self.assertEqual(manifest["coverage_status"]["mode"], "full-coverage-parallel")
            chunk_packets = [packet for packet in manifest["review_packets"] if packet["role"] == "ChunkReviewer"]
            self.assertEqual(len(chunk_packets), len(manifest["chunks"]))

    @patch("review_manuscript.manifest.read_pdf_text")
    def test_directory_prefers_pdf_by_default(self, mock_read_pdf_text: unittest.mock.Mock) -> None:
        mock_read_pdf_text.return_value = (
            "Title\nResults",
            [
                {
                    "page_number": 1,
                    "line_start": 1,
                    "line_end": 2,
                    "line_count": 2,
                    "word_count": 2,
                    "token_estimate": 3,
                    "text": "Title\nResults",
                }
            ],
            [],
            "pdftotext",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "manuscript.pdf").write_bytes(b"%PDF-1.4\n% fake\n")
            (root / "_main.tex").write_text("\\documentclass{article}\n\\begin{document}\nHello\n\\end{document}\n", encoding="utf-8")

            manifest = build_manifest(root)
            self.assertEqual(manifest["input_kind"], "pdf")
            self.assertEqual(manifest["entrypoint"], "manuscript.pdf")
            self.assertEqual(len(manifest["coverage_units"]), 1)

    def test_explicit_tex_path_bypasses_pdf_preference(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "manuscript.pdf").write_bytes(b"%PDF-1.4\n% fake\n")
            main = root / "_main.tex"
            included = root / "section_one.tex"
            main.write_text(
                "\\documentclass{article}\n\\begin{document}\n\\input{section_one}\n\\input{missing_section}\n\\end{document}\n",
                encoding="utf-8",
            )
            included.write_text("\\section{Intro}\n\\label{sec:intro}\nSee \\ref{sec:intro}.\n", encoding="utf-8")

            manifest = build_manifest(main)
            self.assertEqual(manifest["input_kind"], "latex")
            self.assertEqual(len(manifest["resolved_files"]), 2)
            self.assertEqual(len(manifest["unresolved_includes"]), 1)
            self.assertEqual(len(manifest["inventory"]["labels"]), 1)


class FindingPipelineTests(unittest.TestCase):
    def test_merge_and_render_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manuscript = root / "draft.md"
            manuscript.write_text("# Title\n\n## Results\nClaim text.\n", encoding="utf-8")
            manifest = build_manifest(manuscript)

            findings_a = root / "findings_a.json"
            findings_b = root / "findings_b.json"
            payload_a = [
                {
                    "schema_version": "2",
                    "pass_name": "EmpiricalConsistency",
                    "category": "empirical_consistency",
                    "subcategory": "claim_support",
                    "severity": "High",
                    "status": "Confirmed",
                    "title": "Claim overstates support",
                    "summary": "The claim is broader than the visible support.",
                    "detail": "The only visible results text does not justify the broad statement.",
                    "why_it_matters": "Readers may infer stronger support than the document actually provides.",
                    "location": {"path": "draft.md", "line_start": 3, "line_end": 3},
                    "chunk_id": manifest["chunks"][0]["id"],
                    "evidence": {"quote": "Claim text.", "supporting_refs": []},
                    "counterevidence_refs": [],
                    "recommendation": "Narrow the claim or add the missing support.",
                    "confidence": "High",
                    "verification_state": "Verified",
                    "claim_threat_score": 74,
                    "related_finding_ids": [],
                }
            ]
            payload_b = [
                {
                    "schema_version": "2",
                    "pass_name": "FindingVerifier",
                    "category": "exposition_and_clarity",
                    "subcategory": "false_positive",
                    "severity": "Medium",
                    "status": "Likely",
                    "title": "Rejected issue",
                    "summary": "This candidate should not survive.",
                    "detail": "Verifier rejected the candidate.",
                    "why_it_matters": "It should not appear in the final report.",
                    "location": {"path": "draft.md", "line_start": 3, "line_end": 3},
                    "chunk_id": manifest["chunks"][0]["id"],
                    "evidence": {"quote": "Claim text.", "supporting_refs": []},
                    "counterevidence_refs": [],
                    "recommendation": "Drop the issue.",
                    "confidence": "Medium",
                    "verification_state": "Rejected",
                    "claim_threat_score": 20,
                    "related_finding_ids": [],
                }
            ]
            findings_a.write_text(json.dumps(payload_a), encoding="utf-8")
            findings_b.write_text(json.dumps(payload_b), encoding="utf-8")

            merged = merge_findings([findings_a, findings_b])
            self.assertEqual(merged["finding_count"], 1)
            self.assertEqual(merged["rejected_finding_count"], 1)

            manifest_path = root / "manifest.json"
            merged_path = root / "merged.json"
            synthesis_path = root / "synthesis.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            merged_path.write_text(json.dumps(merged), encoding="utf-8")
            synthesis_path.write_text(
                json.dumps(
                    {
                        "overall_feedback": "One retained issue matters most.",
                        "outline": "The manuscript states a claim and then reports one result.",
                        "high_level_concerns": [
                            {
                                "title": "Support is overstated",
                                "summary": "The results prose runs ahead of visible support.",
                                "related_finding_ids": [merged["findings"][0]["id"]],
                            }
                        ],
                        "coverage_summary": {
                            "completed_chunk_ids": [unit["unit_id"] for unit in manifest["coverage_units"]],
                            "completed_packet_ids": [packet["packet_id"] for packet in manifest["review_packets"]],
                        },
                        "worker_count": 4,
                    }
                ),
                encoding="utf-8",
            )

            report_path, sidecar_path = render_report(manifest_path, merged_path, None, str(synthesis_path))
            self.assertTrue(report_path.exists())
            self.assertTrue(sidecar_path.exists())
            content = report_path.read_text(encoding="utf-8")
            self.assertIn("## High-Level Concerns", content)
            self.assertIn("## Detailed Feedback By Position", content)
            self.assertIn("## Coverage Ledger", content)
            self.assertIn("Claim overstates support", content)


if __name__ == "__main__":
    unittest.main()
