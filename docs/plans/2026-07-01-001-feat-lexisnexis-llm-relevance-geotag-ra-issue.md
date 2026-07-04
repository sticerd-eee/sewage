---
title: "Refine the LexisNexis public-attention measure (relevance filtering + local geo-tagging)"
type: feat
date: 2026-07-01
status: draft-issue
note: "Draft GitHub issue for a summer RA — not yet posted. Body below (from ## Context) is paste-ready; title above is the issue title."
---

## Context
We are estimating the causal effect of sewage spills on property transactions, covering both house sales and rentals. One part of the analysis uses media coverage as an information-based quasi-experiment, in the spirit of Davis (2004).

The identifying idea is that media coverage increases public awareness of sewage spill risk. If sewage spills are a local disamenity, then the price or rent discount for properties near spill sites should be larger when public attention is higher.

At present, public attention is measured using the volume of UK news coverage about sewage spills, collected from LexisNexis. LexisNexis is one of the largest searchable archives of licensed news content, so it is a plausible source for constructing a media-attention time series.

The current LexisNexis search uses the following criteria:

* Headline or leading paragraph: “sewage” and either “leak” or “overflow”
* Article body: “incident”
* Geography: UK-sourced articles only

The resulting articles are exported from LexisNexis as PDFs, converted into article-level rows, and aggregated into a single UK-wide monthly coverage series. This monthly series is then merged onto property transactions, meaning every property transacting in the same month receives the same public-attention value.

The measure has three known limitations:

1. **Off-topic articles.**
   * The query is broad, so the results include articles that are not actually about sewage spills. (e.g. *"Queen legend Brian May left struggling to sleep after house ruined by London flash floods"*).
2. **Unreliable article extraction from PDFs.**
   * The current conversion of LexisNexis PDFs into article-level rows does not reliably identify where each article starts and ends. Sometimes, separate articles are incorrectly merged into a single row (e.g. on page 68 of `data/raw/lexis_nexis/search_1/search_1_3.PDF`, two distinct articles are incorrectly merged into a single article).
   * When this happens, headlines, publication names, dates, and body text may be incorrectly associated with one another. We therefore need a more reliable way to recover article boundaries and attach the correct metadata to each article.
3. **Geographic aggregation.**
   * Coverage is currently measured as a single UK-wide monthly series, even though some articles contain more precise geographic information, such as the names of specific water companies, rivers, towns, regions, or local authorities.
   * Aggregating all coverage to the UK-month level discards this geographic variation and may assign the same attention value to properties facing different local information environments.

## Objective
The goal is to turn the national series $A_t$ into a local space × time panel $A_{g,t}$, so that identification can come from within-period (e.g. within-month) variation in local coverage. This would be a more credible information shock than the current national monthly series, because national salience can be held fixed with month fixed effects while local coverage still varies across places.

To get there, we need to address the three issues above:

1. remove off-topic articles;
2. rebuild a reliable article-level dataset;
3. extract and use geographic information from the articles.

## Tasks
1. **Read the project materials**
   * Read the preliminary paper draft and slides (links below).
   * Use them to understand the research question, empirical setting, modelling approach, and policy counterfactuals.
2. **Set up the workspace.**
   * Access the project Dropbox folder (link below).
   * Clone the GitHub repo in your local files
   * Start a task branch (`initials/short-task`, e.g. `xy/lexisnexis-relabel`).
   * Because `data/` and `output/` are not tracked in git, you need to create them locally as symlinks to the project’s Dropbox folders:
     ```bash
     ln -s "<your Dropbox>/01_projects/sewage/data" data
     ln -s "<your Dropbox>/01_projects/sewage/output" output
     ```
   * Use R 4.6.0 and initialise the project environment by running: `rv sync`
3. **Rebuild a clean article-level dataset.**
   * Before improving the PDF parser, check whether LexisNexis can export article-level text and metadata directly.
   * In an ideal world, the export should contain one row per article, with data on: date, source, heading, and body.
   * Choose the input source based on the check above:
      * if LexisNexis can export article-level text and metadata directly, import that export;
      * otherwise, improve the existing PDF segmenter — `scripts/R/02_data_cleaning/nexis_pdf_conversion.R` (defines `nexis_pdf_to_table()`, sourced by `clean_lexis_nexis_search1.R`) — so it recovers individual articles from the current PDF exports.
   * Clean and standardise the article records so that each row corresponds to one distinct article:
      * recover article boundaries, so separate articles are not merged and single articles are not split across rows;
      * remove any text incorrectly carried over from preceding or following articles;
      * attach the correct `date`, `source`, `heading`, and `body` to each article;
      * remove duplicated text within each article `body`;
   * Save the interim output as `articles_clean.parquet`.
      * Do not overwrite the existing dataset.
4. **Write the approach/design plan.**
   * Write a brief memo proposing how to turn the cleaned article-level dataset into a local monthly media-attention panel $A_{g,t}$.
   * For relevance filtering, choose and justify an approach for identifying articles that are about sewage spills.
      * Some possible approaches include (but not exhaustive!):
         * LLM classification (see for example https://causalinf.substack.com/p/claude-code-15-the-results-are-in and https://causalinf.substack.com/p/claude-code-16-the-memory-foam-mattress)
         * Supervised NLP classification
         * Rules-based keyword filtering
   * For geographic tagging, choose and justify an approach for assigning articles to local geographies.

## Reference material
* **Overleaf:** [link to Overleaf](https://www.overleaf.com/read/cxdgwrmmntxr#b23224)
   * Paper draft: compile `_main.tex`
   * Slides: compile `slides/short_pres.tex` and `slides/public_attention.tex`
* **Dropbox folder:** [link to Dropbox](https://www.dropbox.com/scl/fo/uk3ejmreeoimbiomrero1/ABy-jvdJ2CjzVUfJxtJQAHE?rlkey=3f80pqt7bv0o0pjs46x2rih51&st=11awsdn4&dl=0)
   * Cleaning script: `scripts/R/02_data_cleaning/clean_lexis_nexis_search1.R`
   * PDF→article converter (the segmenter to improve): `scripts/R/02_data_cleaning/nexis_pdf_conversion.R`
