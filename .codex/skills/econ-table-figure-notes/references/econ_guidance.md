# External Economics Guidance

Use these sources to sharpen note-writing choices without overriding the repository's established style.

## 1. David McKenzie On Self-Contained Regression Output

Source:

- David McKenzie, `A crowd-sourced checklist of the top 10 little things that drive us crazy with regression output`
- https://blogs.worldbank.org/en/impactevaluations/crowd-sourced-checklist-top-10-little-things-drive-us-crazy-regression-output

Useful takeaways:

- Make tables as self-contained as possible.
- Explain non-obvious variables, especially indexes or composite outcomes.
- Clarify how standard errors are calculated.
- Explain when sample sizes differ across columns or outcomes.
- Surface whether outcomes are conditional or unconditional, weighted or unweighted, winsorised, trimmed, logged, or otherwise transformed.

Implication for this skill:

- Default to notes that explain variable construction, transformations, and sample changes directly.

## 2. Keith Head Guidance Reproduced In Coding For Economists

Source:

- Arthur Turrell, `Writing Papers`, reproducing Keith Head's guidance on regression tables
- https://aeturrell.github.io/coding-for-economists/craft-writing-papers.html

Useful takeaways:

- Report the dependent variable and estimation method in the caption when common across specifications.
- Use self-explanatory labels for regressors.
- Report standard errors in the same column as coefficients.
- Include core fit statistics such as observations and r-squared.
- Make tables readable and usable inside the paper rather than as appendix clutter.

Implication for this skill:

- Use the caption and note together so the table can be decoded without cross-referencing model notation in the text.

## 3. Arthur Turrell On Self-Contained Figures

Source:

- Arthur Turrell, `Writing Papers`
- https://aeturrell.github.io/coding-for-economists/craft-writing-papers.html

Useful takeaways:

- Make figures as self-contained as possible.
- Use informative labels rather than forcing readers to decode panels from a separate caption.
- Aim for figures whose mechanics can be understood without constant back-and-forth with the text.

Implication for this skill:

- Use figure notes to define panels, axes, and encodings whenever the figure would otherwise require too much inference from the reader.

## 4. Eric Zwick On Graph Design

Source:

- Eric Zwick, `A Graph is Worth a Thousand Citations`
- https://www.ericzwick.com/public_goods/1000_citations.pdf

Useful takeaways:

- Identify the message of the graph.
- Label axes precisely and include units.
- Put clarifying text on graphs when helpful and avoid unnecessary legend friction.
- Use colour and shape deliberately.
- Iterate on figures instead of treating them as an afterthought.

Implication for this skill:

- When writing notes for figures, explain the graph's mechanics clearly enough that the reader can recover the intended message without ambiguity.

## 5. AEA Style Rules

Source:

- AER: Insights Style Guide
- https://www.aeaweb.org/journals/aeri/style-guide

Useful takeaways:

- Use Panel A, Panel B, and so on for sections of tables.
- Do not abbreviate column headings.
- Report standard errors in parentheses.
- Place a source note after other notes related to the table or figure.
- AEA guidance does not use asterisks for statistical significance.

Implication for this skill:

- Separate `Notes:` from `Source:` cleanly.
- Treat significance stars as a house-style choice rather than a universal journal rule.

## How To Resolve Conflicts Between Sources

Use this order:

1. Repository examples and current manuscript style.
2. Economist guidance on self-containedness and readability.
3. Journal-specific rules when the user explicitly asks for a journal-specific format.

Practical consequence:

- Preserve the repository's current regression-note convention, including star boilerplate, unless the user asks for strict AEA-style compliance.
- Still adopt the stronger parts of the external guidance: self-contained notes, explicit transformations, precise labels, and clean separation of caption, notes, and source.
