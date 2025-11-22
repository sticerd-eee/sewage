suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(here)
})

# ------------------------------------------------
# Load the tags
# ------------------------------------------------
path_tags <- here::here("data", "processed", "house_spill_tags.parquet")

stopifnot("house_spill_tags.parquet not found" = file.exists(path_tags))

house_spill_tags <- arrow::read_parquet(path_tags)

cat("Loaded house_spill_tags from:\n  ", path_tags, "\n\n", sep = "")

# Basic structure
cat("=== BASIC STRUCTURE ===\n")
print(house_spill_tags %>% head(10))
cat("\nColumns:\n")
print(str(house_spill_tags))

# ------------------------------------------------
# Summary stats – River_Len and Dist_River
# ------------------------------------------------
cat("\n=== SUMMARY: River_Len and Dist_River ===\n")

sign_summary <- house_spill_tags %>%
  mutate(
    sign = case_when(
      River_Len < 0  ~ "upstream",
      River_Len > 0  ~ "downstream",
      TRUE           ~ "zero"
    )
  ) %>%
  count(sign) %>%
  mutate(pct = 100 * n / sum(n))

cat("\nCounts and percentages by sign of River_Len:\n")
print(sign_summary, n = Inf)

cat("\nSummary of River_Len (m):\n")
print(summary(house_spill_tags$River_Len))

cat("\nSummary of |River_Len| (m):\n")
print(summary(abs(house_spill_tags$River_Len)))

cat("\nSummary of Dist_River (m):\n")
print(summary(house_spill_tags$Dist_River))

# Dist_River conditional on zero vs non-zero River_Len
cat("\nDist_River by River_Len == 0 vs != 0:\n")
dist_by_zero <- house_spill_tags %>%
  mutate(is_zero = River_Len == 0) %>%
  group_by(is_zero) %>%
  summarise(
    n           = n(),
    mean_dist   = mean(Dist_River, na.rm = TRUE),
    median_dist = median(Dist_River, na.rm = TRUE),
    p90_dist    = quantile(Dist_River, 0.9, na.rm = TRUE),
    p99_dist    = quantile(Dist_River, 0.99, na.rm = TRUE),
    .groups = "drop"
  )

print(dist_by_zero, n = Inf)

# ------------------------------------------------
# How many houses and spills, and degree distributions
# ------------------------------------------------
cat("\nDistinct houses and spills in tags:\n")
n_hh    <- n_distinct(house_spill_tags$HH_ID)
n_spill <- n_distinct(house_spill_tags$Sewage_ID)
cat("  Houses :", n_hh, "\n")
cat("  Spills :", n_spill, "\n")

cat("\nDistribution of number of spills per house:\n")
house_deg <- house_spill_tags %>%
  count(HH_ID, name = "n_spills")

print(summary(house_deg$n_spills))

cat("\nTop 10 houses by number of connected spills:\n")
print(house_deg %>% arrange(desc(n_spills)) %>% head(10))

cat("\nDistribution of number of houses per spill:\n")
spill_deg <- house_spill_tags %>%
  count(Sewage_ID, name = "n_houses")

print(summary(spill_deg$n_houses))

cat("\nTop 10 spills by number of connected houses:\n")
print(spill_deg %>% arrange(desc(n_houses)) %>% head(10))
