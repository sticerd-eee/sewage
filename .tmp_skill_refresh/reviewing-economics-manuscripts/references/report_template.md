# Report Template

Use this fixed section order.

## Header

- manuscript name
- generated time
- source root
- entrypoint
- input kind
- manifest hash
- worker count
- skill version

## Overall Feedback

One short synthesis paragraph. Add a second paragraph only when the synthesis includes an explicit central-claim restatement.

## Outline

One short staged summary of how the manuscript proceeds.

## High-Level Concerns

List `3-6` concerns. Each concern should have:
- title
- one short paragraph
- related finding IDs

## Detailed Feedback By Relevance

Order findings by:
1. verification state
2. severity
3. claim-threat score
4. confidence
5. evidence strength

For each finding include:
- finding ID and title
- category and subcategory
- severity, status, verification state, and confidence
- claim-threat score
- chunk ID
- source path and line span
- short quote
- explanation
- why it matters
- suggested action

## Detailed Feedback By Position

List the same retained findings in source order.

## Coverage Ledger

List:
- review mode
- planned chunk review count
- completed chunk review count when provided
- one line per coverage unit with coverage status and source span

## Source Resolution Notes

List:
- missing includes
- circular includes
- parser backends used
- PDF extraction warnings
- inaccessible bibliography files
- lossy extraction warnings

## Coverage Gaps

List checks that could not be completed from visible source files and note any verifier-rejected findings that were excluded from the final ranking.
