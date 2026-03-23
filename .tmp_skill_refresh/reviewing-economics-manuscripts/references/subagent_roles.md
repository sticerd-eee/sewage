# Subagent Roles

Use these roles for full Refine-like mode. The manifest's `review_packets` field is the source of truth for concrete routing.

## Global Reviewers

### CentralClaimAndPositioning

Input:
- manifest
- review brief
- abstract, introduction, conclusion, and framing chunks

Output:
- concise restatement of the paper's central contribution
- findings on scope drift, novelty overstatement, and manuscript spine

### ExpositionAndClarity

Input:
- manifest
- review brief
- high-priority narrative chunks

Output:
- findings on exposition, staging, transitions, and reader legibility

### EmpiricalConsistency

Input:
- manifest
- tables, figures, appendices, and claim-heavy chunks

Output:
- findings on mismatches between text and visible empirical support

### IdentificationAndCausalLanguage

Input:
- manifest
- methods, results, specification-heavy chunks, and appendices

Output:
- findings on estimands, timing, identification language, and causal overreach

### MathematicalAndLogicalReasoning

Input:
- manifest
- theorem, proof, equation, and model chunks

Output:
- findings on logical gaps, missing cases, and invalid inferential steps

### NotationAndDefinitions

Input:
- manifest
- notation-heavy chunks

Output:
- findings on undefined objects, drift, numbering, and assumption ordering

### InternalReferencesAndBuild

Input:
- manifest
- diagnostics
- label, ref, citation, and include inventories

Output:
- findings on internal references, broken links, missing includes, build warnings, and source mechanics

### NumbersUnitsAndSigns

Input:
- manifest
- quantitative or claim-heavy chunks

Output:
- findings on signs, magnitudes, denominators, units, and percentage language

### CrossDocumentConsistency

Input:
- manifest
- abstract, introduction, results, conclusion, and appendix summaries

Output:
- findings on drift or contradiction across sections

## Exhaustive Local Reviewer

### ChunkReviewer

Spawn one worker per chunk listed in `coverage_units`.

Input:
- one target chunk
- adjacent chunks listed in `boundary_chunk_ids`
- any relevant global findings for local context

Output:
- new local findings
- confirmations or refinements of global findings
- explicit note when the chunk yields no issues worth keeping

Do not skip chunks because they look low-risk.

## Verification Reviewer

### FindingVerifier

Run after candidate findings exist.

Input:
- candidate findings
- cited source spans
- any supporting or contradictory refs

Output:
- `Verified`, `NeedsAuthorCheck`, or `Rejected` judgment
- narrowed wording when the original finding was directionally right but overstated
- verifier-updated `why_it_matters`, `recommendation`, and `claim_threat_score` when needed
