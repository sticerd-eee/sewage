# ==============================================================================
# Intensive-Margin Public Attention by Local Salience (Post)
# Purpose: Saturated sales/rental estimates by coast/bathing and intensity strata.
# Inputs: Published prior exposure, transactions and attention data; 250m
#   property-radius companions, Site Group characteristics and property lookups.
# Outputs: did_trends_prior_salience_{coast_bathing,intensity}.tex,
#   _robust_{coast10km,london,dropunknown}.tex, model bundle and audit CSVs.
# Run from the repository root with plain Rscript (rv activates via .Rprofile).
# --reproduce checks both unrestricted markets with London retained.
# ==============================================================================

source(here::here("scripts", "R", "09_analysis", "05_news",
                  "intensive_margin_salience_utils.R"), local = TRUE)

main <- function(reproduce_only = FALSE) {
  run_salience_intensive("trends", reproduce_only)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(setdiff(args, "--reproduce"))) stop("Only --reproduce is supported.", call. = FALSE)
  main(reproduce_only = "--reproduce" %in% args)
}
