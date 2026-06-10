# ==============================================================================
# RF Matching Evaluation Harness
# ==============================================================================
#
# Pre-enablement gate for the annual-return RF matching path. Re-runs the
# 2026-06-10 in-session evaluation design against the current code and data:
#
#   1. Fixed-label ground truth: the 2023<->2024 deterministic unique_id
#      links (the same labels the production trainer uses).
#   2. 80/20 holdout split; the downsampled trainer sees only the 80%.
#   3. Recovery test: present the held-out rows without their labels and
#      measure precision/recall of re-linking at candidate thresholds.
#   4. No-partner decoy test: present held-out left rows against a right
#      pool that does NOT contain their true partners; every proposal is a
#      false link. Reports the false-link rate at the legacy 0.05 and the
#      default 0.90 thresholds.
#
# Exit status is non-zero when recovery precision at the default 0.90
# threshold falls below 0.99 - do not enable RF in production on a red run.
#
# Runs standalone: Rscript scripts/R/testing/evaluate_rf_matching.R
# Memory stays well under 4 GB (per-company chunked pair comparison).
#
# ==============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(tidyr)
  library(stringr)
  library(purrr)
})

logger::log_threshold(logger::WARN)

env <- new.env(parent = globalenv())
source(
  here::here("scripts", "R", "03_data_enrichment", "create_annual_return_lookup.R"),
  local = env
)
source(
  here::here("scripts", "R", "utils", "annual_return_lookup_rf_matching.R"),
  local = env
)

DEFAULT_THRESHOLD <- 0.90
LEGACY_THRESHOLD <- 0.05
RECOVERY_THRESHOLDS <- c(0.50, 0.70, 0.90, 0.95)
PRECISION_GATE <- 0.99
HOLDOUT_SHARE <- 0.20
SEED <- 20260610

cat("== RF matching evaluation ==\n")
t0 <- Sys.time()

data_list <- env$prepare_data_list()

# Fixed-label ground truth: deterministic unique_id links.
known_matches <- env$perform_windfall_matching(
  "2023_2024",
  matching_levels = list(c("unique_id_2023")),
  data_list = data_list
)$matches %>%
  filter(join_keys_2023_2024 == "water_company|unique_id_2023") %>%
  select(site_id_2023, site_id_2024)
cat(sprintf("Ground-truth links (unique_id 2023<->2024): %d\n", nrow(known_matches)))

set.seed(SEED)
holdout_idx <- sample(nrow(known_matches), ceiling(HOLDOUT_SHARE * nrow(known_matches)))
known_train <- known_matches[-holdout_idx, ]
known_holdout <- known_matches[holdout_idx, ]
cat(sprintf(
  "Split: %d training links, %d holdout links\n",
  nrow(known_train), nrow(known_holdout)
))

cat("Training downsampled RF model on the training split...\n")
t_train <- system.time(
  rf_model <- env$train_rf_linkage_model(
    df_left = data_list$df2023,
    df_right = data_list$df2024,
    known_matches = known_train,
    model_file = NULL
  )
)
cat(sprintf(
  "Trained in %.1f s (OOB error %.5f)\n",
  t_train[["elapsed"]], rf_model$prediction.error
))

# ------------------------------------------------------------------------------
# Recovery test: re-link the held-out pairs from their rows alone.
# ------------------------------------------------------------------------------

holdout_left <- data_list$df2023 %>%
  filter(site_id_2023 %in% known_holdout$site_id_2023)
holdout_right <- data_list$df2024 %>%
  filter(site_id_2024 %in% known_holdout$site_id_2024)
truth_keys <- paste(known_holdout$site_id_2023, known_holdout$site_id_2024)

cat("\n-- Recovery test (holdout rows, true partner present) --\n")
cat(sprintf("%9s %10s %10s %10s\n", "threshold", "proposals", "precision", "recall"))
recovery_precision_default <- NA_real_
for (th in RECOVERY_THRESHOLDS) {
  proposals <- env$perform_rf_matching(
    holdout_left, holdout_right, rf_model,
    match_threshold = th
  )$matches
  if (nrow(proposals) == 0) {
    cat(sprintf("%9.2f %10d %10s %10s\n", th, 0L, "-", "0.000"))
    next
  }
  proposal_keys <- paste(proposals$site_id_2023, proposals$site_id_2024)
  n_correct <- sum(proposal_keys %in% truth_keys)
  precision <- n_correct / nrow(proposals)
  recall <- n_correct / nrow(known_holdout)
  cat(sprintf("%9.2f %10d %10.4f %10.4f\n", th, nrow(proposals), precision, recall))
  if (th == DEFAULT_THRESHOLD) {
    recovery_precision_default <- precision
  }
}

# ------------------------------------------------------------------------------
# No-partner decoy test: true partners removed; every proposal is false.
# ------------------------------------------------------------------------------

decoy_right <- data_list$df2024 %>%
  filter(!site_id_2024 %in% known_matches$site_id_2024)
cat(sprintf(
  "\n-- No-partner decoy test (%d left rows vs %d partner-less right rows) --\n",
  nrow(holdout_left), nrow(decoy_right)
))
cat(sprintf("%9s %12s %16s\n", "threshold", "false links", "false-link rate"))
for (th in c(LEGACY_THRESHOLD, DEFAULT_THRESHOLD)) {
  false_links <- env$perform_rf_matching(
    holdout_left, decoy_right, rf_model,
    match_threshold = th
  )$matches
  rate <- nrow(false_links) / nrow(holdout_left)
  cat(sprintf("%9.2f %12d %15.4f%%\n", th, nrow(false_links), 100 * rate))
}

gc_stats <- gc()
cat(sprintf(
  "\nPeak memory (R heap, max used): %.2f GB\n",
  sum(gc_stats[, "max used"] * c(56, 8)) / 1e9
))
cat(sprintf("Total runtime: %.1f s\n", as.numeric(Sys.time() - t0, units = "secs")))

if (is.na(recovery_precision_default) ||
    recovery_precision_default < PRECISION_GATE) {
  stop(sprintf(
    "Recovery precision at the %.2f threshold is %s (gate: >= %.2f). Do not enable RF.",
    DEFAULT_THRESHOLD,
    ifelse(is.na(recovery_precision_default), "undefined (no proposals)",
      sprintf("%.4f", recovery_precision_default)
    ),
    PRECISION_GATE
  ))
}
cat(sprintf(
  "\nPASS: recovery precision %.4f at threshold %.2f (gate >= %.2f).\n",
  recovery_precision_default, DEFAULT_THRESHOLD, PRECISION_GATE
))
