# ==============================================================================
# News/Information Event Study (Google Trends Peak) - Prior to Sale/Rental
# ==============================================================================
#
# Purpose: Estimate dynamic spill-count capitalization around the Google Trends
#          peak quarter using a quarter-level event study. Treatment is the
#          daily average spill count (Jan 2021 to transaction date) within 250m.
#
# Author: Jacopo Olivieri
# Date: 2025-01-08
#
# Inputs:
#   - data/raw/google_trends/google_trends_uk.xlsx - Google Trends search data
#   - data/processed/cross_section/sales/prior_to_sale/ - Cross-sectional sales
#   - data/processed/cross_section/rentals/prior_to_rental/ - Cross-sectional rentals
#   - data/processed/house_price.parquet - House sales transactions
#   - data/processed/zoopla/zoopla_rentals.parquet - Rental transactions
#
# Outputs:
#   - output/tables/event_study_google_trends_prior.tex - LaTeX regression table
#   - output/figures/event_study_google_trends_prior_sales.pdf - Sales plot
#   - output/figures/event_study_google_trends_prior_rentals.pdf - Rentals plot
#
# ==============================================================================


# ==============================================================================
# 1. Configuration
# ==============================================================================
RAD <- 250L
CONLEY_CUTOFF <- 0.5  # Conley SE cutoff in km (500m)
EVENT_MIN <- -4L
EVENT_MAX <- 5L
EVENT_REF <- -1L

PLOT_WIDTH <- 23  # cm
PLOT_HEIGHT <- 14 # cm
PLOT_DPI <- 300


# ==============================================================================
# 2. Package Management
# ==============================================================================
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

required_packages <- c(
  "arrow",
  "rio",
  "tidyverse",
  "here",
  "readxl",
  "modelsummary",
  "fixest",
  "ggfixest",
  "viridis",
  "stringr"
)

install_if_missing <- function(packages) {
  new_packages <- packages[!sapply(packages, requireNamespace, quietly = TRUE)]
  if (length(new_packages) > 0) {
    install.packages(new_packages)
  }
  invisible(sapply(packages, library, character.only = TRUE))
}
install_if_missing(required_packages)


# ==============================================================================
# 3. Setup
# ==============================================================================
output_dir <- here::here("output", "tables")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

figure_dir <- here::here("output", "figures")
if (!dir.exists(figure_dir)) {
  dir.create(figure_dir, recursive = TRUE)
}

theme_pref <- theme_minimal() +
  theme(
    text = element_text(size = 12),
    plot.title = element_text(
      face = "bold",
      size = 12,
      margin = ggplot2::margin(b = 10, unit = "pt")
    ),
    axis.title = element_text(face = "bold", size = 10),
    axis.text = element_text(angle = 45, hjust = 1, vjust = 1, size = 8),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_line(color = "gray95"),
    panel.grid.major.y = element_line(color = "gray95"),
    legend.position = "bottom",
    legend.title = element_text(face = "bold", size = 10),
    legend.text = element_text(size = 8),
    plot.margin = ggplot2::margin(t = 10, r = 10, b = 10, l = 10, unit = "pt")
  )


# ==============================================================================
# 4. Data Preparation
# ==============================================================================

# 4.1 Load Google Trends and find peak month ----------------------------------
cat("Loading Google Trends data...\n")

google_trends <- readxl::read_excel(
  here::here("data", "raw", "google_trends", "google_trends_uk.xlsx"),
  sheet = "united_kingdom"
) |>
  filter(Year >= 2021, Year <= 2023)

# Find peak month (earliest if ties)
peak_row <- google_trends |>
  slice_max(`'Sewage Spill' Google Searches`, n = 1, with_ties = FALSE)

# Convert YYYY-MM to month_id (Jan 2021 = 1)
peak_year <- peak_row$Year
peak_month <- as.integer(substr(peak_row$Date, 6, 7))
PEAK_MONTH_ID <- (peak_year - 2021) * 12 + peak_month
PEAK_QTR_ID <- (peak_year - 2021) * 4 + ceiling(peak_month / 3)

cat(sprintf("  Google Trends peak: %s (month_id = %d, qtr_id = %d)\n",
            peak_row$Date, PEAK_MONTH_ID, PEAK_QTR_ID))

# 4.2 Load cross-section sales data (prior to sale) ---------------------------
cat("Loading cross-section sales data...\n")

dat_cs_sales <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "sales", "prior_to_sale")
) |>
  filter(radius == RAD) |>
  collect()

cat(sprintf("  Found %d sales records within %dm\n", nrow(dat_cs_sales), RAD))

# 4.3 Load house price data for property characteristics ----------------------
cat("Loading house price data...\n")

sales <- import(
  here::here("data", "processed", "house_price.parquet"),
  trust = TRUE
) |>
  mutate(
    property_type = forcats::as_factor(property_type),
    old_new = forcats::as_factor(old_new),
    duration = forcats::as_factor(duration)
  )

cat(sprintf("  Loaded %d transactions\n", nrow(sales)))

# 4.4 Merge and create sales analysis variables -------------------------------
cat("Creating sales analysis dataset...\n")

dat <- dat_cs_sales |>
  inner_join(sales, by = "house_id") |>
  mutate(
    log_price = log(price.y),
    event_qtr = as.integer(qtr_id - PEAK_QTR_ID)
  ) |>
  filter(
    !is.na(spill_count_daily_avg),
    !is.na(lsoa),
    !is.na(latitude),
    !is.na(longitude),
    dplyr::between(event_qtr, EVENT_MIN, EVENT_MAX)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    property_type = forcats::fct_drop(property_type),
    old_new = forcats::fct_drop(old_new),
    duration = forcats::fct_drop(duration)
  )

cat(sprintf("  Final sales dataset: %d transactions\n", nrow(dat)))
cat(sprintf("  Event window [%d, %d] quarters from peak\n", EVENT_MIN, EVENT_MAX))
cat(sprintf("  Spill count daily avg: mean=%.4f, sd=%.4f, min=%.4f, max=%.4f\n",
            mean(dat$spill_count_daily_avg, na.rm = TRUE),
            sd(dat$spill_count_daily_avg, na.rm = TRUE),
            min(dat$spill_count_daily_avg, na.rm = TRUE),
            max(dat$spill_count_daily_avg, na.rm = TRUE)))

# 4.5 Load cross-section rental data (prior to rental) ------------------------
cat("Loading cross-section rental data...\n")

dat_cs_rentals <- arrow::open_dataset(
  here::here("data", "processed", "cross_section", "rentals", "prior_to_rental")
) |>
  filter(radius == RAD) |>
  collect()

cat(sprintf("  Found %d rental records within %dm\n", nrow(dat_cs_rentals), RAD))

# 4.6 Load rental data for property characteristics ---------------------------
cat("Loading rental transactions...\n")

rentals <- import(
  here::here("data", "processed", "zoopla", "zoopla_rentals.parquet"),
  trust = TRUE
) |>
  mutate(
    property_type = forcats::as_factor(property_type)
  )

cat(sprintf("  Loaded %d rental transactions\n", nrow(rentals)))

# 4.7 Merge and create rental analysis variables ------------------------------
cat("Creating rental analysis dataset...\n")

dat_rental <- dat_cs_rentals |>
  inner_join(rentals, by = "rental_id") |>
  mutate(
    log_price = log(listing_price.y),
    event_qtr = as.integer(qtr_id - PEAK_QTR_ID)
  ) |>
  filter(
    !is.na(spill_count_daily_avg),
    !is.na(lsoa),
    !is.na(latitude),
    !is.na(longitude),
    dplyr::between(event_qtr, EVENT_MIN, EVENT_MAX)
  ) |>
  mutate(
    lsoa = forcats::fct_drop(forcats::as_factor(lsoa)),
    property_type = forcats::fct_drop(property_type)
  )

cat(sprintf("  Final rental dataset: %d transactions\n", nrow(dat_rental)))
cat(sprintf("  Event window [%d, %d] quarters from peak\n", EVENT_MIN, EVENT_MAX))
cat(sprintf("  Spill count daily avg: mean=%.4f, sd=%.4f, min=%.4f, max=%.4f\n",
            mean(dat_rental$spill_count_daily_avg, na.rm = TRUE),
            sd(dat_rental$spill_count_daily_avg, na.rm = TRUE),
            min(dat_rental$spill_count_daily_avg, na.rm = TRUE),
            max(dat_rental$spill_count_daily_avg, na.rm = TRUE)))


# ==============================================================================
# 5. Estimation
# ==============================================================================
cat("\nEstimating regression models...\n")

event_term <- paste0("i(event_qtr, spill_count_daily_avg, ref = ", EVENT_REF, ")")

# 5.1 Sales models ------------------------------------------------------------

# Model 1: No controls, no FE
model_sale_1 <- fixest::feols(
  as.formula(paste0("log_price ~ ", event_term)),
  data = dat,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Sales Model 1 (no controls, no FE) estimated\n")

# Model 2: LSOA + Quarter FE only
model_sale_2 <- fixest::feols(
  as.formula(paste0("log_price ~ ", event_term, " | lsoa + qtr_id")),
  data = dat,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Sales Model 2 (FE only) estimated\n")

# Model 3: LSOA + Quarter FE + property controls
model_sale_3 <- fixest::feols(
  as.formula(paste0(
    "log_price ~ ", event_term,
    " + property_type + old_new + duration | lsoa + qtr_id"
  )),
  data = dat,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Sales Model 3 (FE + controls) estimated\n")

# 5.2 Rental models -----------------------------------------------------------

# Model 4: No controls, no FE
model_rent_1 <- fixest::feols(
  as.formula(paste0("log_price ~ ", event_term)),
  data = dat_rental,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Rental Model 1 (no controls, no FE) estimated\n")

# Model 5: LSOA + Quarter FE only
model_rent_2 <- fixest::feols(
  as.formula(paste0("log_price ~ ", event_term, " | lsoa + qtr_id")),
  data = dat_rental,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Rental Model 2 (FE only) estimated\n")

# Model 6: LSOA + Quarter FE + property controls
model_rent_3 <- fixest::feols(
  as.formula(paste0(
    "log_price ~ ", event_term,
    " + property_type + bedrooms + bathrooms | lsoa + qtr_id"
  )),
  data = dat_rental,
  vcov = conley(cutoff = CONLEY_CUTOFF)
)
cat("  Rental Model 3 (FE + controls) estimated\n")

cat(sprintf("  Using Conley SEs with %.0fm cutoff\n", CONLEY_CUTOFF * 1000))


# ==============================================================================
# 6. Export Table
# ==============================================================================
cat("\nExporting regression table...\n")

build_event_coef_map <- function(models, event_var = "event_qtr") {
  # Get all coefficient names from all models
  all_coefs <- unique(unlist(lapply(models, function(m) names(coef(m)))))

  # Match coefficients containing event_qtr (fixest i() format)
  event_coefs <- all_coefs[grepl(event_var, all_coefs)]

  if (length(event_coefs) == 0) {
    warning("No event-time coefficients found. Using default labels.")
    return(NULL)
  }

  # Extract event times - handle format: var::event_qtr::time or event_qtr::time:var
  event_times <- as.integer(stringr::str_extract(event_coefs, "-?\\d+"))

  # Remove NAs and sort
  valid_idx <- !is.na(event_times)
  event_coefs <- event_coefs[valid_idx]
  event_times <- event_times[valid_idx]

  # Sort by event time
  ord <- order(event_times)
  event_coefs <- event_coefs[ord]
  event_times <- event_times[ord]

  stats::setNames(
    paste0("Event time ", event_times),
    event_coefs
  )
}

all_models <- list(
  model_sale_1, model_sale_2, model_sale_3,
  model_rent_1, model_rent_2, model_rent_3
)
coef_labels <- build_event_coef_map(all_models, event_var = "event_qtr")

# Goodness of fit map
gof_map <- tibble::tribble(
  ~raw, ~clean, ~fmt,
  "nobs", "Observations", 0,
  "adj.r.squared", "Adj. R-squared", 3
)

# Add rows for fixed effects
add_rows <- tibble::tribble(
  ~term, ~`(1)`, ~`(2)`, ~`(3)`, ~`(4)`, ~`(5)`, ~`(6)`,
  "Property controls", "No", "No", "Yes", "No", "No", "Yes",
  "LSOA FE", "No", "Yes", "Yes", "No", "Yes", "Yes",
  "Quarter FE", "No", "Yes", "Yes", "No", "Yes", "Yes"
)
attr(add_rows, "position") <- "coef_end"

# Notes
custom_notes <- paste0(
  "note{}={\\\\footnotesize{\\\\textbf{Notes:} Dependent variables are log house price (cols 1-3) and log rental price (cols 4-6). Event time is measured in quarters relative to the Google Trends peak quarter. Estimates report the interaction between daily average spill count and event-time dummies, with the quarter immediately before the peak (t = -1) as the reference period. ",
  sprintf("The event window is [%d, %d] quarters. ", EVENT_MIN, EVENT_MAX),
  "Daily avg. spill count measures the average total number of spill events per day (12/24 count) recorded across all nearby overflows from January 2021 to the transaction date. Conley spatial standard errors (500m cutoff) in parentheses. Property controls include type (flat, semi-detached, terraced, other), new build status, and tenure for sales; and type (bungalow, detached, semi-detached, terraced), bedrooms, and bathrooms for rentals. *** p<0.01, ** p<0.05, * p<0.1.}},"
)

# Set option to avoid siunitx wrapping
options("modelsummary_format_numeric_latex" = "plain")

# Structure models into panels
panels <- list(
  "House Sales" = list(
    "(1)" = model_sale_1,
    "(2)" = model_sale_2,
    "(3)" = model_sale_3
  ),
  "House Rentals" = list(
    "(4)" = model_rent_1,
    "(5)" = model_rent_2,
    "(6)" = model_rent_3
  )
)

# Generate table
table_latex <- modelsummary::modelsummary(
  panels,
  shape = "cbind",
  output = "latex",
  escape = FALSE,
  estimate = "{estimate}{stars}",
  statistic = "({std.error})",
  stars = c("*" = 0.1, "**" = 0.05, "***" = 0.01),
  fmt = 3,
  coef_map = coef_labels,
  gof_map = gof_map,
  add_rows = add_rows,
  notes = " ",
  title = "Event Study of Spill Exposure Around Google Trends Peak (Prior to Transaction)"
)

# Force table environment to [H]
table_latex <- sub("\\\\begin\\{table\\}", "\\\\begin{table}[H]", table_latex)

# Add label in tabularray format
table_latex <- sub(
  "caption=\\{([^}]*)\\},",
  "caption={\\1},\nlabel={tbl:event-study-google-trends-prior},",
  table_latex
)

# Add colsep and font size for tighter column spacing
table_latex <- sub(
  "(\\{\\s*%% tabularray inner open\\n)",
  "\\1width=0.9\\\\linewidth,\ncolsep=3pt,\ncells   = {font = \\\\fontsize{11pt}{12pt}\\\\selectfont},\n",
  table_latex
)

# Replace empty note with custom notes (tabularray format)
table_latex <- sub(
  "note\\{\\}=\\{\\s*\\},",
  custom_notes,
  table_latex
)

# Distribute available width among columns (X[] instead of Q[])
table_latex <- gsub("Q\\[\\]", "X[c] ", table_latex)
table_latex <- sub("colspec=\\{X\\[c\\] ", "colspec={l ", table_latex)

# Write to file
output_path <- file.path(output_dir, "event_study_google_trends_prior.tex")
writeLines(table_latex, output_path)

cat(sprintf("LaTeX table exported to: %s\n", output_path))


# ==============================================================================
# 7. Event Study Plots
# ==============================================================================
cat("\nGenerating event-study plots...\n")

build_event_plot <- function(model, subtitle, caption) {
  plot_data <- ggfixest::iplot_data(model, .ci_level = 0.95) |>
    mutate(
      event_time = as.integer(estimate_names),
      group = factor(
        ifelse(event_time < 0, "Pre", "Post"),
        levels = c("Pre", "Post")
      )
    ) |>
    filter(
      !is.na(event_time),
      dplyr::between(event_time, EVENT_MIN, EVENT_MAX),
      ci_level == 0.95
    )

  ggplot(plot_data, aes(x = event_time, y = estimate, color = group)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
    geom_vline(xintercept = EVENT_REF, linetype = "dashed", color = "gray50") +
    geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0.2) +
    geom_point(size = 1.2) +
    scale_color_manual(
      values = c(
        "Pre" = viridis::viridis(1, option = "magma"),
        "Post" = viridis::viridis(3, option = "magma")[2]
      ),
      breaks = c("Pre", "Post")
    ) +
    theme_minimal() +
    labs(
      x = "Time (quarters from peak)",
      y = "Estimate",
      title = "",
      subtitle = subtitle,
      caption = caption
    ) +
    theme_pref +
    theme(plot.caption = element_text(hjust = 0))
}

sales_caption <- paste0(
  "log_price ~ i(event_qtr, spill_count_daily_avg, ref = ", EVENT_REF,
  ") + property controls | lsoa + qtr_id"
)
rentals_caption <- paste0(
  "log_price ~ i(event_qtr, spill_count_daily_avg, ref = ", EVENT_REF,
  ") + property controls | lsoa + qtr_id"
)

sales_plot <- build_event_plot(
  model = model_sale_3,
  subtitle = "Sales: LSOA + Quarter FE with property controls",
  caption = sales_caption
)
rentals_plot <- build_event_plot(
  model = model_rent_3,
  subtitle = "Rentals: LSOA + Quarter FE with property controls",
  caption = rentals_caption
)

ggsave(
  filename = file.path(figure_dir, "event_study_google_trends_prior_sales.pdf"),
  plot = sales_plot,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  units = "cm"
)

ggsave(
  filename = file.path(figure_dir, "event_study_google_trends_prior_rentals.pdf"),
  plot = rentals_plot,
  width = PLOT_WIDTH,
  height = PLOT_HEIGHT,
  dpi = PLOT_DPI,
  units = "cm"
)

cat("Event-study plots exported to output/figures\n")
cat("\nScript completed successfully.\n")
