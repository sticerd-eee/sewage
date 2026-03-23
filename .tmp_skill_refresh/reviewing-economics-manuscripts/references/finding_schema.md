# Finding Schema

Every reviewer must emit JSON objects with these fields.

```json
{
  "schema_version": "2",
  "pass_name": "EmpiricalConsistency",
  "category": "empirical_consistency",
  "subcategory": "table_text_mismatch",
  "severity": "High",
  "status": "Confirmed",
  "title": "Text describes the wrong column in Table 2",
  "summary": "The paragraph attributes the significant estimate to column 2, but the visible effect appears in column 3.",
  "detail": "Explain the issue in 2-5 sentences.",
  "why_it_matters": "Readers will misread the main empirical takeaway if the text points them to the wrong specification.",
  "location": {
    "path": "04_results.tex",
    "line_start": 88,
    "line_end": 93
  },
  "chunk_id": "04_results.tex:main-results:80",
  "evidence": {
    "quote": "As shown in column 2 of Table 2...",
    "supporting_refs": [
      {
        "path": "tables/table_2.tex",
        "line_start": 1,
        "line_end": 20
      }
    ]
  },
  "counterevidence_refs": [],
  "recommendation": "Align the prose with the table or explain why the text should focus on a different column.",
  "confidence": "High",
  "verification_state": "NeedsVerification",
  "claim_threat_score": 76,
  "related_finding_ids": []
}
```

## Allowed Categories

- `central_claim_and_positioning`
- `exposition_and_clarity`
- `empirical_consistency`
- `identification_and_causal_language`
- `mathematical_and_logical_reasoning`
- `notation_and_definitions`
- `internal_references_and_build`
- `numbers_units_and_signs`
- `cross_document_consistency`

## Required Rules

- Use exact enum values for `severity`, `status`, `confidence`, and `verification_state`.
- Keep `evidence.quote` short and verbatim.
- Use one primary `location` per finding.
- Put secondary corroborating locations under `evidence.supporting_refs`.
- Put contradictory source spans under `counterevidence_refs`.
- Set `claim_threat_score` on a `0-100` scale.
- Use `chunk_id` from the manifest.
- Do not emit Markdown. Emit JSON only.

## Verification State Enum

- `Verified`
- `NeedsAuthorCheck`
- `NeedsVerification`
- `Rejected`
