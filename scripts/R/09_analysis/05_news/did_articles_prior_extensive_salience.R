# ==============================================================================
# Extensive-Margin Public Attention by Local Salience (Articles)
# Purpose: Saturated sales/rental estimates by coast/bathing and intensity strata.
# Inputs: The parent's published transaction, proximity and attention data;
#   Site Group characteristics and the 500m property-radius companions.
# Outputs: did_articles_prior_extensive_salience_{coast_bathing,intensity}.tex,
#   _robust_{coast10km,london,dropunknown}.tex, model bundle and audit CSVs.
# Run from the repository root with plain Rscript (rv activates via .Rprofile).
# --reproduce checks both unrestricted markets with London retained.
# ==============================================================================

source(here::here("scripts", "R", "09_analysis", "05_news",
                  "extensive_margin_salience_utils.R"), local = TRUE)

main <- function(reproduce_only = FALSE) {
  run_salience_extensive("articles", reproduce_only)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(setdiff(args, "--reproduce"))) stop("Only --reproduce is supported.", call. = FALSE)
  main(reproduce_only = "--reproduce" %in% args)
}
