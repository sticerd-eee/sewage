#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run all analysis R scripts in a controlled order.

Usage:
  scripts/R/09_analysis/run_all_analysis.sh [--dry-run] [--keep-going]

Options:
  --dry-run     Print the run order without executing.
  --keep-going  Continue if a script fails (default is fail-fast).

Environment:
  RSCRIPT_BIN   Path to Rscript binary (default: Rscript).
EOF
}

DRY_RUN=0
KEEP_GOING=0
RSCRIPT_BIN="${RSCRIPT_BIN:-Rscript}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --keep-going)
      KEEP_GOING=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

# Edit this list to add/remove or comment out scripts.
SCRIPT_LIST=$(cat <<'EOF'
# 00_data_load
# scripts/R/09_analysis/00_data_load/load_data_sewage.R

# 01_descriptive
# scripts/R/09_analysis/01_descriptive/google_trends_article_counts_combined.R
# scripts/R/09_analysis/01_descriptive/google_trends_sewage_spill.R
# scripts/R/09_analysis/01_descriptive/cross_sectional_plots.R
# scripts/R/09_analysis/01_descriptive/spill_maps.R
# scripts/R/09_analysis/01_descriptive/spill_maps_inset.R
# scripts/R/09_analysis/01_descriptive/spill_phase_diagrams.R

# 02_hedonic
scripts/R/09_analysis/02_hedonic/hedonic_continuous_prior.R
scripts/R/09_analysis/02_hedonic/hedonic_bins_prior.R
scripts/R/09_analysis/02_hedonic/hedonic_continuous_full.R
scripts/R/09_analysis/02_hedonic/hedonic_bins_full.R

# 03_repeat_sales
scripts/R/09_analysis/03_repeat_sales/repeat_sales.R

# 04_long_difference
scripts/R/09_analysis/04_long_difference/longdiff_unweighted_all.R
scripts/R/09_analysis/04_long_difference/longdiff_unweighted_exposed.R
scripts/R/09_analysis/04_long_difference/longdiff_weighted_all.R
scripts/R/09_analysis/04_long_difference/longdiff_weighted_exposed.R

# 05_news
scripts/R/09_analysis/05_news/did_trends_full.R
scripts/R/09_analysis/05_news/did_trends_prior.R
scripts/R/09_analysis/05_news/es_trends_prior.R
scripts/R/09_analysis/05_news/did_articles_prior.R
scripts/R/09_analysis/05_news/did_articles_lag4_prior.R

# 06_upstream_downstream
#scripts/R/09_analysis/06_upstream_downstream/upstream_downstream.R
EOF
)

exit_code=0
while IFS= read -r line; do
  # Strip leading/trailing whitespace.
  script="${line#"${line%%[![:space:]]*}"}"
  script="${script%"${script##*[![:space:]]}"}"

  # Skip empty lines and comments.
  [[ -z "$script" ]] && continue
  [[ "$script" == \#* ]] && continue

  if [[ ! -f "$script" ]]; then
    echo "Missing script: $script" >&2
    if [[ $KEEP_GOING -eq 1 ]]; then
      exit_code=1
      continue
    fi
    exit 1
  fi

  echo "==> Running $script"
  if [[ $DRY_RUN -eq 1 ]]; then
    continue
  fi

  if ! "$RSCRIPT_BIN" "$script"; then
    echo "FAILED: $script" >&2
    if [[ $KEEP_GOING -eq 1 ]]; then
      exit_code=1
      continue
    fi
    exit 1
  fi
done <<<"$SCRIPT_LIST"

exit "$exit_code"
