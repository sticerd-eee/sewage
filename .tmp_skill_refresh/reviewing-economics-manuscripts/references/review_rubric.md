# Review Rubric

Use this rubric to keep reviewer outputs aligned and to keep the review closer to Refine's public emphasis on substantive, evidence-backed comments.

## Review Domains

### 1. Central Claim and Positioning

Check:
- whether the core contribution is legible and stable across abstract, introduction, results, conclusion, and appendix
- whether novelty, scope, and generality are stated at the right level
- whether the framing outruns what the visible argument or evidence can support

### 2. Exposition and Clarity

Check:
- whether the manuscript gives the reader enough staging to follow the argument
- whether definitions and transitions arrive before they are used
- whether the paper distinguishes synthesis of existing work from its own contribution

### 3. Empirical Consistency

Check:
- whether quantitative claims match visible tables, figures, appendices, and specifications
- whether robustness, heterogeneity, and significance claims are supported by visible source
- whether text descriptions match the sign, magnitude, and column or panel actually shown

### 4. Identification and Causal Language

Check:
- estimand language
- treatment timing
- sample restrictions
- fixed effects, clustering, standard errors, weights, and inference wording
- whether causal language outruns the design

### 5. Mathematical and Logical Reasoning

Check:
- hidden cases
- proof steps that do not follow from visible assumptions
- claims that require lemmas or conditions not yet established
- mismatches between displayed statements and surrounding interpretation

### 6. Notation and Definitions

Check:
- notation reuse or drift
- undefined symbols
- theorem, lemma, and assumption numbering
- assumptions or objects used before they are defined

### 7. Numbers, Units, and Signs

Check:
- sign interpretation
- percentages versus percentage points
- units, denominators, and sample sizes
- nominal versus real values where relevant

### 8. Internal References and Buildability

Check:
- labels and refs
- citation commands and accessible bibliography files
- broken Markdown links or image paths
- hard-coded figure or table numbers
- existing LaTeX log warnings

### 9. Cross-Document Consistency

Check:
- whether the same result or claim is described differently across sections
- whether the abstract, introduction, conclusion, and appendix stay mutually consistent
- whether caveats appear in one section but disappear in another

## Severity

- `Critical`: likely to change the paper's substantive claim, identification logic, proof validity, reproducibility, or buildability
- `High`: likely to mislead a careful reader or materially weaken the paper if left unfixed
- `Medium`: important clarity, consistency, or methodological issue
- `Low`: polish or minor style issue with limited substantive risk

## Status

- `Confirmed`: directly supported by visible source evidence
- `Likely`: strongly suggested by visible evidence but still worth manual confirmation
- `Unverified`: could not be fully checked because required artifacts were missing or extraction was lossy

## Verification State

- `Verified`: a separate verifier pass re-read the relevant text and upheld the finding
- `NeedsAuthorCheck`: the issue is useful and plausible, but the verifier still needs author-side context or artifacts
- `NeedsVerification`: candidate finding not yet verifier-checked
- `Rejected`: verifier determined the candidate finding should not appear in the final report

## Confidence

- `High`: evidence is direct and specific
- `Medium`: evidence is good but partial
- `Low`: weak signal; keep only if still useful
