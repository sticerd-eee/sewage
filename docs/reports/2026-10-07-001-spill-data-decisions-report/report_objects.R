# Objects for docs/reports/2026-10-07-001-spill-data-decisions-report.qmd.
# The report sources this file once and prints the figures and tables it
# defines; each object is named after its chunk label. Inline numbers in the
# prose come from the list `n` at the end. Most counts are recorded snapshots from the
# source named in their comments; Section 3 reads the current group crosswalk.

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(tidyr)
  library(scales)
  library(knitr)
  library(tinytable)
})

base_family <- if (requireNamespace("systemfonts", quietly = TRUE) &&
  nrow(systemfonts::match_fonts("Avenir Next")) > 0) {
  "Avenir Next"
} else {
  "sans"
}

# One palette for the whole report. Statuses keep the same colour everywhere.
pal <- c(
  events     = "#3B6EA8", # the event-level recordswh
  returns    = "#2F6F5E", # the annual returns
  verified   = "#2E7D4F", # positive year with matched events
  zero       = "#9DB0C2", # explicit zero in the returns
  absent     = "#C0392B", # no return filed
  na         = "#E0963C", # return filed, metrics missing
  pos_noev   = "#7D4EA6", # return says spills, no events matched
  ink        = "#1F2A37",
  muted      = "#6B7785",
  rule       = "#CBD3DC",
  paper      = "#FFFFFF"
)

theme_report <- function(base_size = 11) {
  theme_minimal(base_size = base_size, base_family = base_family) +
    theme(
      plot.title = element_text(face = "bold", colour = pal["ink"], size = base_size + 1.5),
      plot.subtitle = element_text(colour = pal["muted"], size = base_size - 0.5),
      plot.title.position = "plot",
      panel.grid.minor = element_blank(),
      panel.grid.major = element_line(colour = pal["rule"], linewidth = 0.3),
      axis.title = element_text(colour = pal["muted"], size = base_size - 1),
      axis.text = element_text(colour = pal["ink"]),
      legend.position = "bottom",
      legend.title = element_blank(),
      legend.text = element_text(size = base_size - 1),
      strip.text = element_text(face = "bold", colour = pal["ink"], hjust = 0),
      plot.margin = margin(6, 10, 4, 6)
    )
}

theme_schematic <- function(base_size = 11) {
  theme_void(base_size = base_size, base_family = base_family) +
    theme(
      plot.title = element_text(face = "bold", colour = pal["ink"], size = base_size + 1.5, hjust = 0),
      plot.subtitle = element_text(colour = pal["muted"], size = base_size - 0.5, hjust = 0),
      plot.title.position = "plot",
      legend.position = "bottom",
      legend.title = element_blank(),
      legend.text = element_text(size = base_size - 1),
      plot.margin = margin(6, 10, 4, 6)
    )
}

fmt <- function(x) format(x, big.mark = ",", trim = TRUE)
pct <- function(x, d = 1) paste0(formatC(100 * x, format = "f", digits = d), "%")

# --- Recorded pipeline figures ------------------------------------------------
# Sources: output/log/05_merge_individ_annual_location.log (canonical run,
# 2026-08-11); data/processed/matched_events_annual_data/site_group_crosswalk
# .parquet; data/processed/cross_section/{sales,rentals}/* (read 2026-10-07).

n_events <- 7383972
n_annual_rows <- 57865
n_lookup_clusters <- 15430
n_site_groups <- 13990
n_site_years <- 55960

# Storm overflows listed in each year's annual return, 2021-2024. These match
# the Environment Agency's published totals.
# Source: data/processed/annual_return_edm.parquet (read 2026-10-10).
rows_by_year <- c(`2021` = 14470, `2022` = 14580, `2023` = 14530, `2024` = 14285)
stopifnot(sum(rows_by_year) == n_annual_rows)

# Stage 2 results: the years in which each spill site is observed.
# Source: data/processed/annual_return_lookup.parquet (read 2026-10-10).
# Each pattern lists the years in which a spill site has a spill site-year.
site_patterns <- tribble(
  ~pattern, ~n,
  "1234", 13606,
  "123-", 184, "12-4", 2, "1-34", 22, "-234", 359,
  "12--", 137, "1--4", 4, "-23-", 111, "-2-4", 3, "--34", 228,
  "1---", 515, "-2--", 178, "--3-", 20, "---4", 61
) |>
  mutate(
    n_years = nchar(gsub("-", "", pattern, fixed = TRUE)),
    group = c("Single year", "Two years", "Three years", "Four years")[n_years]
  ) |>
  arrange(desc(n_years), desc(pattern))
stopifnot(sum(site_patterns$n) == n_lookup_clusters)
n_sites_by_years <- tapply(site_patterns$n, site_patterns$n_years, sum)
# A gap year is a missing year between a spill site's first and last year.
n_sites_gap <- sum(site_patterns$n[grepl("[0-9]-+[0-9]", site_patterns$pattern)])
single_year <- site_patterns |>
  filter(n_years == 1) |>
  slice_max(n, n = 1)
single_year_modal <- 2020 + regexpr("[0-9]", single_year$pattern)
share_sites <- function(n) pct(n / n_lookup_clusters)
# Single-year spill sites that Stage 3 places in a group with other spill sites.
# Source: annual_return_lookup.parquet and unique_spill_sites.parquet (read 2026-10-10).
n_single_regrouped <- 331

# Stage 3 cost: group-years in which one spill site is reported without spill
# figures (and not as no longer operational) while another spill site in the
# same group is reported with figures. The group-year then counts as reported.
# Excludes Site Group 12250, which merges unrelated Yorkshire Water sites under
# placeholder names. Recompute once that grouping is fixed.
# Source: annual_return_edm, annual_return_lookup, unique_spill_sites,
# site_group_crosswalk and spill_house_lookup parquet files (read 2026-10-10).
masked_by_year <- c(`2021` = 141, `2022` = 87, `2023` = 15, `2024` = 8)
n_masked_groups <- 178
n_masked_zero <- 34 # of which classed as zero spills
# Silent spill sites imputed from their nearest year with figures (198 of 270).
masked_missing_share_median <- 0.222 # of the group's spill count, 176 group-years
n_masked_zero_checkable <- 22
n_masked_false_zero <- 15 # silent spill site reported positive spills in that year
masked_hours_share <- c(`2021` = 0.0077, `2022` = 0.0092, `2023` = 0.0008, `2024` = 0.0001)
# Houses with any Site Group within the radius, and those with a masked group.
masked_houses <- tribble(
  ~radius, ~n_any, ~n_masked,
  250, 233818, 3316,
  500, 769554, 14390,
  1000, 1849570, 58257
)
n_masked <- sum(masked_by_year)
stopifnot(n_masked == 251)

# Stage 1 spill sites by their weakest cross-year link (identifier strongest).
# Source: annual_return_lookup.parquet and annual_return_lookup_edges.parquet
# (read 2026-10-09). Unmatched sites are single-year spill sites with no links.
n_unmatched_sites <- 774
sites_weak <- 210 + 333 # weakest link rests on 1 or 2 fields
sites_strong <- n_lookup_clusters - n_unmatched_sites - sites_weak
stopifnot(sites_strong == 226 + 6110 + 4406 + 2606 + 765)

# Event matching: matched discharge events by year and by what they were
# matched to. A single spill site is a site group with one spill site.
# Source: matched_events_annual_data.parquet joined to n_outlets in
# site_group_crosswalk.parquet (read 2026-10-10).
event_matches <- tribble(
  ~year, ~outcome, ~unique_id, ~events,
  2021, "single", FALSE, 1747128, 2021, "group", FALSE, 333418,
  2022, "single", FALSE, 1272661, 2022, "group", FALSE, 259920,
  2023, "single", TRUE, 1503525, 2023, "single", FALSE, 9394,
  2023, "group", TRUE, 294765, 2023, "group", FALSE, 153,
  2024, "single", TRUE, 1552094, 2024, "single", FALSE, 45,
  2024, "group", TRUE, 298608
)
# Unmatched discharge events by year and reason.
# Source: events_unmatched.parquet (read 2026-10-10). "other" combines
# name_spans_site_groups and agreement_uninformative.
event_unmatched <- tribble(
  ~year, ~reason, ~events,
  2021, "no_field", 12898, 2021, "hours", 1478, 2021, "other", 213,
  2022, "no_field", 74135, 2022, "hours", 21989, 2022, "other", 3,
  2023, "no_field", 461,
  2024, "no_field", 1084
)
n_matched <- sum(event_matches$events)
n_unmatched <- sum(event_unmatched$events)
stopifnot(n_matched + n_unmatched == n_events)
n_matched_group <- sum(event_matches$events[event_matches$outcome == "group"])
n_unmatched_by_reason <- tapply(event_unmatched$events, event_unmatched$reason, sum)
n_unmatched_2022 <- sum(event_unmatched$events[event_unmatched$year == 2022])

# Section 3: derive reporting and matching status from the current crosswalk.
status_crosswalk <- arrow::read_parquet(here::here(
  "data", "processed", "matched_events_annual_data", "site_group_crosswalk.parquet"
)) |>
  filter(year %in% 2021:2024) |>
  mutate(
    status = case_when(
      annual_status == "reported_positive" & coalesce(matched_event_count, 0L) > 0L ~
        "Positive, events matched",
      annual_status == "reported_positive" ~ "Positive, no events matched",
      annual_status == "reported_zero" ~ "Reported zero",
      annual_status == "reported_na" ~ "Reported NA",
      annual_status == "absent" ~ "No return filed"
    ),
    non_missing = status %in% c("Positive, events matched", "Reported zero")
  )
stopifnot(!anyNA(status_crosswalk$status))
status_by_year <- status_crosswalk |> count(year, status)
status_summary <- status_crosswalk |>
  group_by(year) |>
  summarise(groups = n(), non_missing_share = mean(non_missing), .groups = "drop")
status_group_count <- n_distinct(status_crosswalk$site_id)
stopifnot(all(status_summary$groups == status_group_count))
status_non_missing_share <- setNames(status_summary$non_missing_share, status_summary$year)
status_levels <- c(
  "Positive, events matched", "Reported zero", "Reported NA",
  "No return filed", "Positive, no events matched"
)
status_cols <- c(pal[["verified"]], pal[["zero"]], pal[["na"]], pal[["absent"]], pal[["pos_noev"]])
names(status_cols) <- status_levels

sg_verified_all <- status_crosswalk |>
  group_by(site_id) |>
  summarise(non_missing_all = n_distinct(year) == 4L & all(non_missing), .groups = "drop") |>
  summarise(total = sum(non_missing_all)) |>
  pull(total)
site_years_verified <- sum(status_crosswalk$non_missing)

outlets_per_group <- tribble(
  ~outlets, ~n,
  "1", 13104, "2", 689, "3 to 5", 74 + 62 + 14, "6 or more", 18 + 9 + 4 + 7 + 3 + 1 + 3 + 1 + 1
)

# Spread of multi-site groups, using each spill site's most recent parsed grid
# reference (clean_ngr, parse_bng_coordinates) and the largest distance between
# any two spill sites in the group.
# Source: annual_return_edm, annual_return_lookup and unique_spill_sites parquet
# files (read 2026-10-11).
n_multi_groups <- 886
n_multi_same_ngr <- 576
n_multi_within_250 <- 739
n_multi_within_1000 <- 801 # one group has no parsed grid reference

# Exposure samples: transactions with at least one Site Group within the radius,
# prior-to-transaction datasets (the hedonic samples). Reasons are not exclusive;
# "two or more" collects properties whose nearby sites fail for different reasons.
cost <- tribble(
  ~market, ~radius, ~reason, ~n,
  "Sales", 250, "Verified", 166192, "Sales", 250, "Reported NA only", 32028,
  "Sales", 250, "Positive, no events only", 9414, "Sales", 250, "No return filed only", 7382,
  "Sales", 250, "Two or more gaps", 14236,
  "Sales", 500, "Verified", 523626, "Sales", 500, "Reported NA only", 113538,
  "Sales", 500, "Positive, no events only", 31239, "Sales", 500, "No return filed only", 27293,
  "Sales", 500, "Two or more gaps", 58541,
  "Sales", 1000, "Verified", 1109285, "Sales", 1000, "Reported NA only", 323957,
  "Sales", 1000, "Positive, no events only", 77244, "Sales", 1000, "No return filed only", 84512,
  "Sales", 1000, "Two or more gaps", 217473,
  "Rentals", 250, "Verified", 58354, "Rentals", 250, "Reported NA only", 12074,
  "Rentals", 250, "Positive, no events only", 2974, "Rentals", 250, "No return filed only", 3835,
  "Rentals", 250, "Two or more gaps", 6489,
  "Rentals", 500, "Verified", 171967, "Rentals", 500, "Reported NA only", 41742,
  "Rentals", 500, "Positive, no events only", 9301, "Rentals", 500, "No return filed only", 13207,
  "Rentals", 500, "Two or more gaps", 27295,
  "Rentals", 1000, "Verified", 344086, "Rentals", 1000, "Reported NA only", 111685,
  "Rentals", 1000, "Positive, no events only", 23847, "Rentals", 1000, "No return filed only", 41939,
  "Rentals", 1000, "Two or more gaps", 102940
) |>
  group_by(market, radius) |>
  mutate(total = sum(n), share = n / total) |>
  ungroup()
cost_levels <- c(
  "Verified", "Reported NA only", "No return filed only",
  "Positive, no events only", "Two or more gaps"
)
cost_cols <- c(pal[["verified"]], pal[["na"]], pal[["absent"]], pal[["pos_noev"]], "#8A8F99")
names(cost_cols) <- cost_levels

# Small helpers for schematic drawings
box <- function(x, y, w, h, fill, colour = NA, r = 0.015) {
  annotate("rect",
    xmin = x - w / 2, xmax = x + w / 2, ymin = y - h / 2, ymax = y + h / 2,
    fill = fill, colour = colour, linewidth = 0.4
  )
}
txt <- function(x, y, label, size = 3.4, colour = pal[["ink"]], ...) {
  annotate("text",
    x = x, y = y, label = label, size = size, colour = colour,
    family = base_family, ...
  )
}
arrow_seg <- function(x, y, xend, yend, colour = pal[["muted"]]) {
  annotate("segment",
    x = x, y = y, xend = xend, yend = yend, colour = colour,
    linewidth = 0.5, arrow = arrow(length = unit(5, "pt"), type = "closed")
  )
}

# --- tbl-sources --------------------------------------------------------------
sources <- tribble(
  ~` `, ~`EDM event-level records (EA, via EIR)`, ~`EDM annual returns (EA)`,
  # Row 1: Data provenance
  "Data provenance",
  "Requested annually by the EA from water companies to check for dry-day spills, and obtained by us from the EA under the EIR",
  "Compiled by water companies and submitted annually to the EA, which publishes the returns on data.gov.uk",
  # Row 2: Unit of observation
  "Unit of observation",
  "Discharge event, with start and end times recorded to the minute",
  "Storm overflow–year, with annual spill count and total spill hours",
  # Row 3: Size
  "Observations, 2021–24",
  paste0(fmt(n_events), " individual discharge events"),
  paste0(fmt(n_annual_rows), " overflow-years, from ≈14,500 spill sites in each year's annual returns"),
  # Row 4: Timing of spills
  "Timing of spills",
  "Spills can be counted over any period (e.g. the months preceding a property sale)",
  "Annual totals only",
  # Row 5: Zeros and missing values
  "Zeros and missing values (see @fig-sources)",
  "Not distinguished. An absence of records may indicate either:\n\n1. zero spills, or\n2. missing event records",
  "Explicitly distinguished:\n\n1. zero indicates no spills\n2. NA indicates missing spill figures\n3. a missing row indicates that no annual-return record is linked to that spill site in that year",
  # Row 6: Site characteristics
  "Site characteristics",
  "None",
  "Location, asset and treatment type, receiving waterbody, bathing and shellfish water designations, monitor commissioning date, annual monitoring coverage (and reasons for gaps), and high-spill review fields (see the #link(\"https://www.dropbox.com/scl/fi/dsfhx0xpvlruyn5rhnyah/2024_annual_return_edm_dataset_readme.pdf?rlkey=6si2yony20edfva4wm4jwc94q&dl=0\")[data README] for details)",
  # Row 7: Site identifiers
  "Site identifiers",
  "- 2021–22: Site names, permit references and activity codes, which can be missing or inconsistent across years.\n- 2023–24: Unique site IDs, consistent across years and shared by both datasets.",
  ""
)
tbl_sources <- tt(sources, width = c(0.19, 0.405, 0.405)) |>
  style_tt(align = "l") |>
  style_tt(j = 1, bold = TRUE, color = "#1F2A37") |>
  style_tt(i = 0, bold = TRUE, background = "#EEF2F5", color = "#1F2A37") |>
  style_tt(i = nrow(sources), j = 2, colspan = 2) |>
  style_tt(i = seq_len(nrow(sources)), line = "b", line_color = "#DFE5E8", line_width = 0.05) |>
  style_tt(fontsize = 0.9)

# --- fig-sources --------------------------------------------------------------
set.seed(11)
yrs <- as.Date(c("2021-01-01", "2022-01-01", "2023-01-01", "2024-01-01", "2025-01-01"))
mk_events <- function(n, from, to, mean_h) {
  st <- sort(sample(seq(as.Date(from), as.Date(to), by = "day"), n))
  tibble(
    start = as.POSIXct(st) + runif(n, 0, 86400),
    dur_h = pmax(0.5, rexp(n, 1 / mean_h))
  )
}
ev <- bind_rows(
  mk_events(55, "2021-01-05", "2024-12-20", 18) |> mutate(site = "Site A"),
  mk_events(3, "2024-02-01", "2024-11-30", 6) |> mutate(site = "Site B"),
  mk_events(9, "2022-01-10", "2023-12-15", 9) |> mutate(site = "Site C")
) |> mutate(end = start + dur_h * 3600, site = factor(site, levels = c("Site C", "Site B", "Site A")))
gaps <- tibble(
  xmin = as.POSIXct(c("2021-01-01", "2021-01-01", "2024-01-01")),
  xmax = as.POSIXct(c("2023-12-31", "2021-12-31", "2024-12-31")),
  y = c(2, 1, 1),
  status = c("Zero reported", "NA: missing spill figures", "No annual return")
)
gap_cols <- c("Zero reported" = pal[["zero"]], "NA: missing spill figures" = pal[["na"]], "No annual return" = pal[["absent"]])
fig_sources <- list()
fig_sources$events <- ggplot(ev) +
  geom_vline(xintercept = as.POSIXct(yrs), colour = pal[["rule"]], linewidth = 0.4) +
  geom_rect(
    data = gaps, aes(xmin = xmin, xmax = xmax, ymin = y - 0.36, ymax = y + 0.36, colour = status),
    fill = NA, linewidth = 0.7, show.legend = FALSE
  ) +
  scale_colour_manual(values = gap_cols) +
  geom_rect(aes(
    xmin = start, xmax = end + 3 * 86400, ymin = as.numeric(site) - 0.28,
    ymax = as.numeric(site) + 0.28
  ), fill = pal[["events"]], colour = pal[["events"]], linewidth = 0.3) +
  scale_y_continuous(breaks = 1:3, labels = c("Site C", "Site B", "Site A"), limits = c(0.5, 3.6)) +
  scale_x_datetime(breaks = as.POSIXct(yrs[1:4]), labels = 2021:2024, expand = expansion(mult = 0.01)) +
  labs(x = NULL, y = NULL) +
  theme_report() +
  theme(panel.grid.major.y = element_blank(), panel.grid.major.x = element_blank())

ret <- tribble(
  ~site, ~year, ~label, ~status,
  "Site A", 2021, "12 spills\n96 h", "Spills reported", "Site A", 2022, "9 spills\n54 h", "Spills reported",
  "Site A", 2023, "41 spills\n312 h", "Spills reported", "Site A", 2024, "7 spills\n40 h", "Spills reported",
  "Site B", 2021, "0 spills\n0 h", "Zero reported", "Site B", 2022, "0 spills\n0 h", "Zero reported",
  "Site B", 2023, "0 spills\n0 h", "Zero reported", "Site B", 2024, "3 spills\n11 h", "Spills reported",
  "Site C", 2021, "NA", "NA: missing spill figures", "Site C", 2022, "5 spills\n22 h", "Spills reported",
  "Site C", 2023, "4 spills\n19 h", "Spills reported", "Site C", 2024, "no return", "No annual return"
) |> mutate(
  site = factor(site, levels = c("Site C", "Site B", "Site A")),
  status = factor(status, levels = c("Spills reported", "Zero reported", "NA: missing spill figures", "No annual return"))
)
ret_cols <- c(
  "Spills reported" = pal[["returns"]], "Zero reported" = pal[["zero"]],
  "NA: missing spill figures" = pal[["na"]], "No annual return" = pal[["absent"]]
)
fig_sources$returns <- ggplot(ret, aes(x = factor(year), y = site)) +
  geom_tile(aes(fill = status), colour = "white", linewidth = 1.2, width = 0.96, height = 0.9) +
  geom_text(aes(label = label, colour = status == "Zero reported"), size = 3.0, family = base_family, lineheight = 0.9) +
  scale_colour_manual(values = c(`TRUE` = pal[["ink"]], `FALSE` = "white"), guide = "none") +
  scale_fill_manual(values = ret_cols, drop = FALSE) +
  labs(x = NULL, y = NULL) +
  theme_report() +
  theme(panel.grid = element_blank(), legend.key.size = unit(10, "pt")) +
  guides(fill = guide_legend(nrow = 2))

# --- fig-pipeline -------------------------------------------------------------
steps <- tibble(
  x = 1:4,
  head = c("Define sites", "Attach events", "Classify years", "Sum near property"),
  body = c(
    "Follow one monitored\npoint across years;\ncollapse outlets\n250 m apart",
    "Best exact key per\nrecord; unmatched\nrows carry a\nreason code",
    "Verified zero,\nverified positive,\nor unknown, decided\nby the returns",
    "Hours and counts near\neach property; unknown\nif any site-year\nis unknown"
  ),
  sec = c("Section 3", "Section 4", "Section 5", "Sections 5 and 6")
)
fig_pipeline <- ggplot() +
  xlim(-0.5, 5.25) +
  ylim(0.45, 2.0) +
  box(0.0, 1.5, 0.92, 0.46, fill = alpha(pal[["events"]], 0.15), colour = pal[["events"]]) +
  txt(0.0, 1.575, "Event records", size = 3.8, fontface = "bold", colour = pal[["events"]]) +
  txt(0.0, 1.42, "7.4 M events", size = 3.1, colour = pal[["events"]]) +
  box(0.0, 0.98, 0.92, 0.46, fill = alpha(pal[["returns"]], 0.15), colour = pal[["returns"]]) +
  txt(0.0, 1.055, "Annual returns", size = 3.8, fontface = "bold", colour = pal[["returns"]]) +
  txt(0.0, 0.90, "58 k site-years", size = 3.1, colour = pal[["returns"]]) +
  arrow_seg(0.47, 1.5, 0.53, 1.3) +
  arrow_seg(0.47, 0.98, 0.53, 1.18)
for (i in seq_len(nrow(steps))) {
  fig_pipeline <- fig_pipeline +
    box(steps$x[i], 1.24, 0.9, 1.08, fill = "#F3F6F9", colour = pal[["rule"]]) +
    txt(steps$x[i], 1.63, steps$head[i], size = 3.7, fontface = "bold") +
    txt(steps$x[i], 1.19, steps$body[i], size = 3.15, colour = pal[["ink"]], lineheight = 0.92) +
    txt(steps$x[i], 0.58, steps$sec[i], size = 3.1, colour = pal[["muted"]], fontface = "italic")
  if (i < nrow(steps)) fig_pipeline <- fig_pipeline + arrow_seg(steps$x[i] + 0.46, 1.24, steps$x[i] + 0.54, 1.24)
}
fig_pipeline <- fig_pipeline +
  arrow_seg(4.46, 1.24, 4.54, 1.24) +
  box(4.9, 1.24, 0.62, 1.08, fill = alpha(pal[["verified"]], 0.12), colour = pal[["verified"]]) +
  txt(4.9, 1.24, "Regression\nsamples", size = 3.7, fontface = "bold", colour = pal[["verified"]], lineheight = 0.95) +
  theme_schematic()

# --- fig-problems -------------------------------------------------------------
# Source: data/processed/annual_return_edm.parquet (read 2026-10-09).
# (a) Lechlade STW, Thames Water, 2023 unique ID ThW0422. (b) Newbiggin, United
# Utilities, 2021: the two rows also match on the company's own name and permit
# and on the activity code (ST, storm tank).
match_fill <- c(same = "#DCEBE2", changed = "#F6E3B4", none = pal[["paper"]])

compare_panel <- function(cells, headers, fields) {
  cells <- cells |>
    mutate(
      y = length(fields) + 1 - match(field, fields),
      size = if_else(nchar(value) > 16, 3.1, 3.7)
    )
  ggplot(cells) +
    coord_cartesian(xlim = c(-1.3, 2.35), ylim = c(0.45, length(fields) + 1.05), expand = FALSE) +
    geom_tile(aes(x = x, y = y, fill = status),
      width = 1.12, height = 0.82, colour = pal[["rule"]], linewidth = 0.4
    ) +
    geom_text(
      aes(x = x, y = y, label = value, size = size, fontface = if_else(status == "changed", "bold", "plain")),
      family = base_family, colour = pal[["ink"]], lineheight = 0.88
    ) +
    txt(-0.1, length(fields):1, fields, hjust = 1, size = 3.7) +
    txt(c(0.55, 1.7), length(fields) + 0.72, headers, size = 3.9, fontface = "bold") +
    scale_fill_manual(values = match_fill, guide = "none") +
    scale_size_identity() +
    theme_schematic() +
    theme(plot.margin = margin(2, 4, 2, 2))
}

fields_a <- c("Site name", "Permit reference", "Grid reference", "Unique ID (2023)")
cells_a <- tribble(
  ~field, ~x, ~value, ~status,
  "Site name", 0.55, "LECHLADE STW", "changed",
  "Site name", 1.7, "LECHLADE WWTW", "changed",
  "Permit reference", 0.55, "CTCR.1797", "same",
  "Permit reference", 1.7, "CTCR.1797", "same",
  "Grid reference", 0.55, "SU2271099330", "same",
  "Grid reference", 1.7, "SU2271099330", "same",
  "Unique ID (2023)", 0.55, "ThW0422", "same",
  "Unique ID (2023)", 1.7, "ThW0422", "same"
)
fig_problems <- list()
fig_problems$ids_change <- compare_panel(cells_a, c("2023", "2024"), fields_a)

fields_b <- c("Site name", "Permit reference", "Grid reference", "Spill hours, 2021")
cells_b <- tribble(
  ~field, ~x, ~value, ~status,
  "Site name", 0.55, "NEWBIGGIN (LEVEN)\nWWTW NEWBG", "same",
  "Site name", 1.7, "NEWBIGGIN (LEVEN)\nWWTW NEWBG", "same",
  "Permit reference", 0.55, "017370051", "same",
  "Permit reference", 1.7, "017370051", "same",
  "Grid reference", 0.55, "SD2675168941", "same",
  "Grid reference", 1.7, "SD2675168941", "same",
  "Spill hours, 2021", 0.55, "3,878", "none",
  "Spill hours, 2021", 1.7, "1.9", "none"
)
fig_problems$not_unique <- compare_panel(cells_b, c("Spill site 1", "Spill site 2"), fields_b)

# --- tbl-links ----------------------------------------------------------------
# Source: data/processed/annual_return_lookup_edges.parquet (read 2026-10-09).
# These are the links spill sites are built from: spill site-years minus spill
# sites. Spill-site counts use annual_return_lookup.parquet: each spill site is
# classified by its weakest link (identifier counted as strongest). Unmatched
# spill site-years are single-year spill sites with no links.
links <- tribble(
  ~evidence, ~n,
  "Unique identifier (2023–2024)", 14086,
  "Identifying fields, all 6 agree", 15027,
  "Identifying fields, 5 agree", 8748,
  "Identifying fields, 4 agree", 2983,
  "Identifying fields, 3 agree", 1022,
  "Identifying fields, 2 agree", 358,
  "Identifying fields, 1 agrees", 211
)
stopifnot(sum(links$n) == n_annual_rows - n_lookup_clusters)
links_tbl <- links |>
  bind_rows(tibble(evidence = "Total", n = sum(links$n))) |>
  transmute(`Matched on` = evidence, Links = fmt(n), `Share of links` = pct(n / sum(links$n))) |>
  bind_rows(tibble(`Matched on` = "Unmatched spill site-years", Links = fmt(n_unmatched_sites), `Share of links` = "–"))
tbl_links <- tt(links_tbl, width = c(0.5, 0.25, 0.25)) |>
  style_tt(align = "lrr") |>
  style_tt(j = 1, color = "#1F2A37") |>
  style_tt(i = 0, bold = TRUE, background = "#EEF2F5", color = "#1F2A37") |>
  style_tt(i = nrow(links_tbl) - 1, bold = TRUE) |>
  style_tt(i = nrow(links_tbl), italic = TRUE) |>
  style_tt(i = seq_len(nrow(links_tbl)), line = "b", line_color = "#DFE5E8", line_width = 0.05) |>
  style_tt(fontsize = 0.9)

# --- fig-chaining -------------------------------------------------------------
# Stylised: each row is one case. Dots are spill site-years, lines are matches.
# Row 5 has two 2022 spill site-years (B above, D below): A-B, A-C and C-D are
# each valid within their own pair of years, but chained they put B and D in
# one spill site, so the weakest match (C-D) is dropped.
conflict_dy <- 0.62
chain_cases <- tibble(
  row = 1:5,
  y = 5:1,
  label = c(
    "Matched in\nevery year", "Matched only to\nadjacent years", "Not reported\nin 2022",
    "No match\nfound", "Conflicting\nmatches"
  )
)
chain_sites <- tibble(
  site = 1:6,
  colour = c(pal[["events"]], pal[["pos_noev"]], "#B5651D", pal[["muted"]], "#A23B72", pal[["muted"]])
)
chain_dots <- tribble(
  ~row, ~x, ~dy, ~site,
  1, 1, 0, 1, 1, 2, 0, 1, 1, 3, 0, 1, 1, 4, 0, 1,
  2, 1, 0, 2, 2, 2, 0, 2, 2, 3, 0, 2, 2, 4, 0, 2,
  3, 1, 0, 3, 3, 3, 0, 3, 3, 4, 0, 3,
  4, 1, 0, 4,
  5, 1, 0, 5, 5, 2, 0, 5, 5, 3, 0, 5, 5, 2, -conflict_dy, 6
) |>
  left_join(select(chain_cases, row, y), by = "row") |>
  left_join(chain_sites, by = "site") |>
  mutate(y = y + dy)
chain_links <- tribble(
  ~row, ~x, ~xend, ~dy_end, ~dropped,
  1, 1, 2, 0, FALSE, 1, 2, 3, 0, FALSE, 1, 3, 4, 0, FALSE,
  1, 1, 3, 0, FALSE, 1, 2, 4, 0, FALSE, 1, 1, 4, 0, FALSE,
  2, 1, 2, 0, FALSE, 2, 2, 3, 0, FALSE, 2, 3, 4, 0, FALSE,
  3, 1, 3, 0, FALSE, 3, 3, 4, 0, FALSE, 3, 1, 4, 0, FALSE,
  5, 1, 2, 0, FALSE, 5, 1, 3, 0, FALSE, 5, 3, 2, -conflict_dy, TRUE
) |>
  left_join(select(chain_cases, row, y), by = "row") |>
  mutate(
    adjacent = abs(xend - x) == 1,
    # Trim each straight link so it stops short of the dots.
    len = sqrt((xend - x)^2 + dy_end^2),
    x0 = x + 0.13 * (xend - x) / len, y0 = y + 0.13 * dy_end / len,
    x1 = xend - 0.13 * (xend - x) / len, y1 = y + dy_end - 0.13 * dy_end / len
  )

# Matches between non-adjacent years are drawn as arcs whose height grows with
# the gap between the years.
chain_arcs <- chain_links |>
  filter(!adjacent) |>
  mutate(link = row_number(), h = 0.13 * (xend - x)) |>
  tidyr::crossing(u = seq(0, 1, length.out = 40)) |>
  # Trim 0.16 x-units off each end so arcs stop short of the dots.
  mutate(
    d = 0.16 / (xend - x),
    t = d + u * (1 - 2 * d),
    ax = x + t * (xend - x),
    # Row 1's 2021-2024 match is drawn below the dots to reduce clutter.
    side = if_else(row == 1 & xend - x == 3, -1, 1),
    ay = y + side * 4 * h * t * (1 - t)
  )

chain_base <- function() {
  ggplot() +
    coord_cartesian(xlim = c(-0.85, 5.45), ylim = c(0.1, 5.95), expand = FALSE) +
    txt(1:4, 5.8, 2021:2024, size = 3.7, fontface = "bold") +
    theme_schematic() +
    theme(plot.margin = margin(2, 4, 2, 2))
}
match_arrow <- arrow(length = unit(3.5, "pt"), ends = "both", type = "closed")

# Panel (a): every pairwise match as its own two-headed line, stopping short of
# the dots so each reads as a separate link.
fig_chaining <- list()
fig_chaining$matches <- chain_base() +
  geom_text(
    data = chain_cases, aes(x = 0.6, y = y + 0.14, label = label), hjust = 1,
    size = 3.4, family = base_family, colour = pal[["ink"]], lineheight = 0.9
  ) +
  geom_segment(
    data = filter(chain_links, adjacent), aes(x = x0, xend = x1, y = y0, yend = y1),
    colour = pal[["muted"]], linewidth = 0.6, arrow = match_arrow
  ) +
  geom_path(
    data = chain_arcs, aes(x = ax, y = ay, group = link),
    colour = pal[["muted"]], linewidth = 0.5, arrow = match_arrow
  ) +
  geom_point(data = chain_dots, aes(x = x, y = y), size = 3.2, colour = pal[["ink"]])

# Panel (b): each spill site as one bar spanning its years. The open circle is
# the year with no spill site-year. The dropped match is dashed and crossed out.
chain_bars <- chain_dots |>
  group_by(site, colour) |>
  summarise(y = first(y), xmin = min(x), xmax = max(x), .groups = "drop") |>
  # Spill site 6 is labelled on its left, clear of the dropped match.
  mutate(
    label_x = if_else(site == 6, xmin - 0.32, xmax + 0.32),
    label_hjust = if_else(site == 6, 1, 0)
  )
chain_gaps <- tibble(site = 3, x = 2, y = chain_cases$y[3]) |> left_join(chain_sites, by = "site")
dropped <- filter(chain_links, dropped)
fig_chaining$sites <- chain_base() +
  geom_segment(
    data = chain_bars, aes(x = xmin - 0.12, xend = xmax + 0.12, y = y, yend = y),
    colour = alpha(chain_bars$colour, 0.35), linewidth = 7, lineend = "round"
  ) +
  geom_segment(
    data = dropped, aes(x = x0, xend = x1, y = y0, yend = y1),
    colour = pal[["muted"]], linewidth = 0.5, linetype = "22"
  ) +
  geom_text(
    data = dropped, aes(x = (x0 + x1) / 2, y = (y0 + y1) / 2, label = "×"),
    size = 5, colour = pal[["absent"]], family = base_family, fontface = "bold"
  ) +
  geom_point(data = chain_dots, aes(x = x, y = y), size = 3.2, colour = chain_dots$colour) +
  geom_point(
    data = chain_gaps, aes(x = x, y = y), size = 3.2, shape = 21,
    fill = pal[["paper"]], colour = chain_gaps$colour, stroke = 0.9
  ) +
  geom_text(
    data = chain_bars, aes(x = label_x, y = y, label = paste("Spill site", site), hjust = label_hjust),
    size = 3.5, family = base_family, colour = chain_bars$colour, fontface = "bold"
  )

# --- tbl-grouping -------------------------------------------------------------
# Source: distinct site_id / n_outlets pairs in site_group_crosswalk.parquet,
# read 2026-10-11. Counts are recorded snapshots, as elsewhere in this report.
grouping_counts <- tribble(
  ~sites_per_group, ~groups,
  1L, 13104L, 2L, 689L, 3L, 74L, 4L, 62L, 5L, 14L, 6L, 18L,
  7L, 9L, 8L, 4L, 9L, 7L, 10L, 3L, 11L, 1L, 14L, 3L, 15L, 1L, 44L, 1L
) |>
  mutate(sites = sites_per_group * groups)
stopifnot(
  sum(grouping_counts$groups) == n_site_groups,
  sum(grouping_counts$sites) == n_lookup_clusters
)
grouping_detail <- grouping_counts |>
  mutate(composition = if_else(sites_per_group >= 5, "5+", as.character(sites_per_group))) |>
  group_by(composition) |>
  summarise(groups = sum(groups), sites = sum(sites), .groups = "drop")
grouping_multi <- grouping_counts |>
  filter(sites_per_group > 1) |>
  summarise(composition = "All groups with 2+ sites", groups = sum(groups), sites = sum(sites))
grouping_tbl <- bind_rows(
  grouping_detail,
  grouping_multi,
  tibble(composition = "Total", groups = n_site_groups, sites = n_lookup_clusters)
) |>
  transmute(
    `Spill sites per group` = composition,
    `Number of groups` = fmt(groups),
    `Share of groups` = pct(groups / n_site_groups),
    `Number of spill sites` = fmt(sites)
  )
tbl_grouping <- tt(grouping_tbl, width = c(0.37, 0.21, 0.21, 0.21)) |>
  style_tt(align = "lrrr") |>
  style_tt(j = 1, color = "#1F2A37") |>
  style_tt(i = 0, bold = TRUE, background = "#EEF2F5", color = "#1F2A37") |>
  style_tt(i = seq_len(nrow(grouping_tbl)), line = "b", line_color = "#DFE5E8", line_width = 0.05) |>
  style_tt(i = 1, line = "b", line_color = "#CBD3DC", line_width = 0.1) |>
  style_tt(i = 6, bold = TRUE, line = "t", line_color = "#CBD3DC", line_width = 0.1) |>
  style_tt(i = 7, bold = TRUE) |>
  style_tt(fontsize = 0.9)

# --- fig-site-patterns --------------------------------------------------------
pattern_rows <- site_patterns |>
  mutate(y = rev(seq_len(n())) + cumsum(group != lag(group, default = first(group))) * -0.6)
pattern_rows$y <- pattern_rows$y - min(pattern_rows$y) + 1
pattern_dots <- pattern_rows |>
  tidyr::crossing(x = 1:4) |>
  mutate(observed = substr(pattern, x, x) != "-")
pattern_spans <- pattern_dots |>
  filter(observed) |>
  group_by(pattern, y) |>
  summarise(xmin = min(x), xmax = max(x), .groups = "drop")
# Open circles only for gaps inside a spill site's span.
pattern_gaps <- pattern_dots |>
  filter(!observed) |>
  inner_join(pattern_spans, by = c("pattern", "y")) |>
  filter(x > xmin, x < xmax)
group_labels <- pattern_rows |>
  group_by(group) |>
  summarise(ymin = min(y), ymax = max(y), y = (ymin + ymax) / 2, n = sum(n), .groups = "drop") |>
  mutate(share = n / sum(n))

bar_x0 <- 5.0
bar_w <- 3.1
pattern_rows <- mutate(pattern_rows, bar_len = bar_w * n / max(n))
site_col <- pal[["events"]]

fig_site_patterns <- ggplot() +
  coord_cartesian(xlim = c(-1.6, 11.9), ylim = c(0.4, max(pattern_rows$y) + 1.45), expand = FALSE) +
  geom_text(
    data = group_labels, aes(x = 0.45, y = y, label = group), hjust = 1,
    size = 3.4, family = base_family, colour = pal[["ink"]]
  ) +
  geom_segment(
    data = pattern_spans, aes(x = xmin - 0.12, xend = xmax + 0.12, y = y, yend = y),
    colour = alpha(site_col, 0.3), linewidth = 4.5, lineend = "round"
  ) +
  geom_point(
    data = filter(pattern_dots, observed), aes(x = x, y = y),
    size = 2.1, colour = site_col
  ) +
  geom_point(
    data = pattern_gaps, aes(x = x, y = y), size = 2.1, shape = 21,
    fill = pal[["paper"]], colour = site_col, stroke = 0.7
  ) +
  geom_rect(
    data = pattern_rows, aes(xmin = bar_x0, xmax = bar_x0 + bar_len, ymin = y - 0.32, ymax = y + 0.32),
    fill = pal[["muted"]]
  ) +
  geom_text(
    data = pattern_rows, aes(x = bar_x0 + bar_len + 0.08, y = y, label = fmt(n)),
    hjust = 0, size = 3.2, family = base_family, colour = pal[["ink"]]
  ) +
  txt(1:4, max(pattern_rows$y) + 1.05, 2021:2024, size = 3.5, fontface = "bold") +
  txt(bar_x0, max(pattern_rows$y) + 1.05, "Spill sites", size = 3.5, fontface = "bold", hjust = 0) +
  # Category totals: a bracket spanning each group's rows, with the share of all spill sites.
  geom_segment(
    data = group_labels, aes(x = 9.45, xend = 9.45, y = ymin - 0.3, yend = ymax + 0.3),
    colour = pal[["muted"]], linewidth = 0.6
  ) +
  geom_text(
    data = group_labels, aes(x = 9.6, y = y, label = paste0(fmt(n), " (", pct(share), ")")),
    hjust = 0, size = 3.2, family = base_family, colour = pal[["ink"]]
  ) +
  txt(9.45, max(pattern_rows$y) + 1.05, "Total (share)", size = 3.5, fontface = "bold", hjust = 0) +
  theme_schematic()

# --- fig-ladder ---------------------------------------------------------------
# One row per year plus an all-years row. Left: what the discharge events were
# matched to, as shares of the row. Right: the unmatched discharge events, in
# counts on their own scale, by reason.
outcome_cols <- c(single = pal[["events"]], group = pal[["returns"]], unmatched = pal[["absent"]])
reason_cols <- c(no_field = pal[["absent"]], hours = "#E59A8F", other = "#F3CFC9")
ladder_rows <- tibble(row = c("2021", "2022", "2023", "2024", "All years"), y = c(5, 4, 3, 2, 0.7))
bar_w <- 5.2   # width of the 100% bar
zoom_x0 <- 6.75 # start of the unmatched count bars
zoom_w <- 2.0  # width of the all-years unmatched bar

by_row <- function(d) {
  bind_rows(mutate(d, row = as.character(year)), mutate(d, row = "All years"))
}
ladder_outcomes <- bind_rows(
  event_matches |> select(year, outcome, events),
  event_unmatched |> mutate(outcome = "unmatched") |> select(year, outcome, events)
) |>
  by_row() |>
  group_by(row, outcome) |>
  summarise(events = sum(events), .groups = "drop") |>
  mutate(outcome = factor(outcome, levels = names(outcome_cols))) |>
  arrange(row, outcome) |>
  group_by(row) |>
  mutate(
    share = events / sum(events),
    xmax = cumsum(share) * bar_w,
    xmin = xmax - share * bar_w,
    lab = if_else(share >= 0.07, pct(share), NA_character_),
    match_lab = paste0(pct(1 - share[outcome == "unmatched"], 2), " matched"),
    total = sum(events)
  ) |>
  ungroup() |>
  left_join(ladder_rows, by = "row")
ladder_unmatched <- event_unmatched |>
  by_row() |>
  group_by(row, reason) |>
  summarise(events = sum(events), .groups = "drop") |>
  mutate(reason = factor(reason, levels = names(reason_cols))) |>
  arrange(row, reason) |>
  group_by(row) |>
  mutate(xmax = zoom_x0 + cumsum(events) / n_unmatched * zoom_w, xmin = xmax - events / n_unmatched * zoom_w) |>
  ungroup() |>
  left_join(ladder_rows, by = "row")
ladder_unmatched_lab <- ladder_unmatched |>
  group_by(row, y) |>
  summarise(events = sum(events), xmax = max(xmax), .groups = "drop") |>
  left_join(distinct(ladder_outcomes, row, total), by = "row") |>
  mutate(lab = paste0(fmt(events), " (", pct(events / total, 2), if_else(row == "All years", " of all)", " of the year)")))
ladder_key <- tibble(
  x = c(0, 2.75, 5.25, zoom_x0, zoom_x0),
  y = c(-0.3, -0.3, -0.3, -0.3, -0.7),
  key = c(names(outcome_cols), "no_field", "hours"),
  lab = c(
    "Spill site group with\none spill site", "Spill site group with\nseveral spill sites", "Unmatched",
    "No identifying field matches any spill site-year",
    "Site name shared by several site groups whose\nspill hours do not single one out"
  )
)

fig_ladder <- ggplot() +
  coord_cartesian(xlim = c(-1.25, 10.75), ylim = c(-1.0, 5.75), expand = FALSE) +
  txt(0, 5.6, "Share of discharge events, by what they were matched to", hjust = 0, size = 3.3, fontface = "bold") +
  txt(zoom_x0, 5.6, "Unmatched discharge events, by reason", hjust = 0, size = 3.3, fontface = "bold") +
  geom_text(data = ladder_rows, aes(x = -0.12, y = y, label = row), hjust = 1, size = 3.5,
    family = base_family, colour = pal[["ink"]], fontface = "bold") +
  geom_rect(data = ladder_outcomes, aes(xmin = xmin, xmax = xmax, ymin = y - 0.31, ymax = y + 0.31, fill = outcome),
    colour = pal[["paper"]], linewidth = 0.4) +
  geom_text(data = filter(ladder_outcomes, !is.na(lab)), aes(x = (xmin + xmax) / 2, y = y, label = lab),
    size = 3.0, family = base_family, colour = pal[["paper"]]) +
  geom_text(data = distinct(ladder_outcomes, row, y, match_lab), aes(x = bar_w + 0.1, y = y, label = match_lab),
    hjust = 0, size = 3.0, family = base_family, colour = pal[["ink"]]) +
  geom_rect(data = ladder_unmatched, aes(xmin = xmin, xmax = xmax, ymin = y - 0.31, ymax = y + 0.31, fill = reason),
    colour = pal[["paper"]], linewidth = 0.3) +
  geom_text(data = ladder_unmatched_lab, aes(x = xmax + 0.08, y = y, label = lab), hjust = 0, size = 3.0,
    family = base_family, colour = pal[["ink"]]) +
  annotate("segment", x = -1.25, xend = 10.75, y = 1.35, yend = 1.35, colour = pal[["rule"]], linewidth = 0.4) +
  geom_tile(data = ladder_key, aes(x = x + 0.11, y = y, fill = key), width = 0.22, height = 0.24) +
  geom_text(data = ladder_key, aes(x = x + 0.32, y = y, label = lab), hjust = 0, size = 2.9,
    family = base_family, colour = pal[["ink"]], lineheight = 0.85) +
  scale_fill_manual(values = c(outcome_cols, reason_cols), guide = "none") +
  theme_schematic()

# --- fig-verified -------------------------------------------------------------
status_overview <- status_crosswalk |>
  mutate(category = case_when(
    status == "Positive, events matched" ~ ">0 spills",
    status == "Reported zero" ~ "0 spills",
    TRUE ~ "NA"
  )) |>
  count(year, category) |>
  mutate(category = factor(category, levels = c(">0 spills", "0 spills", "NA")))
status_missing <- status_by_year |>
  filter(!status %in% status_levels[1:2]) |>
  mutate(category = factor(status, levels = status_levels[3:5]))
missing_legend <- c(
  "Reported NA" = "Spill figures are NA in EDM annual returns",
  "No return filed" = "Site group absent from EDM annual returns",
  "Positive, no events matched" = paste0(
    "Spills reported in EDM annual returns, but no\n",
    "matched EDM event-level records"
  )
)

# Match the schematic bars and directly placed legends used in Figure 5.
# A common canvas keeps the year rows aligned despite different legend lengths.
status_panel <- function(data, colours, labels, label_min, compact_legend = FALSE) {
  data <- data |>
    arrange(year, category) |>
    group_by(year) |>
    mutate(xmax = cumsum(n), xmin = xmax - n) |>
    ungroup()
  totals <- data |> group_by(year) |> summarise(total = sum(n), .groups = "drop")
  max_total <- max(totals$total)
  data <- data |> mutate(xmin = xmin / max_total, xmax = xmax / max_total)
  key <- tibble(
    category = levels(data$category),
    x = if (compact_legend) c(0, 0.36, 0.70) else rep(0, 3),
    y = if (compact_legend) rep(-0.15, 3) else c(-0.15, -0.65, -1.3),
    lab = if (compact_legend) labels else vapply(labels, function(label) {
      paste(strwrap(gsub("\n", " ", label), width = 46), collapse = "\n")
    }, character(1))
  )
  ggplot(data) +
    geom_rect(aes(xmin = xmin, xmax = xmax, ymin = 2025 - year - 0.3,
      ymax = 2025 - year + 0.3, fill = category),
      colour = pal[["paper"]], linewidth = 0.4) +
    geom_text(
      data = filter(data, n >= label_min),
      aes(x = (xmin + xmax) / 2, y = 2025 - year, label = fmt(n),
        colour = category == "0 spills"),
      size = 3.2, family = base_family
    ) +
    geom_text(data = totals, aes(x = total / max_total + 0.025,
      y = 2025 - year, label = fmt(total)),
      hjust = 0, size = 3.2, family = base_family, colour = pal[["ink"]]) +
    geom_text(data = totals, aes(x = -0.035, y = 2025 - year, label = year),
      hjust = 1, size = 3.5, fontface = "bold", family = base_family, colour = pal[["ink"]]) +
    geom_tile(data = key, aes(x = x + 0.015, y = y, fill = category),
      width = 0.03, height = 0.2) +
    geom_text(data = key, aes(x = x + 0.055, y = y, label = lab),
      hjust = 0, size = 3.1, lineheight = 0.95,
      family = base_family, colour = pal[["ink"]]) +
    scale_fill_manual(values = colours, guide = "none") +
    scale_colour_manual(values = c(`TRUE` = pal[["ink"]], `FALSE` = pal[["paper"]]), guide = "none") +
    coord_cartesian(xlim = c(-0.16, 1.19), ylim = c(-1.9, 4.5), expand = FALSE) +
    theme_schematic()
}
fig_verified <- list(
  status = status_panel(
    status_overview,
    c(">0 spills" = pal[["returns"]], "0 spills" = pal[["zero"]], "NA" = pal[["na"]]),
    c(">0 spills", "0 spills", "NA"), label_min = 700, compact_legend = TRUE
  ),
  missing = status_panel(
    status_missing, status_cols[status_levels[3:5]], missing_legend,
    label_min = 100
  )
)

# --- tbl-cost -----------------------------------------------------------------
ct <- cost |>
  select(market, radius, reason, share, total) |>
  pivot_wider(names_from = reason, values_from = share) |>
  mutate(Dropped = 1 - Verified) |>
  transmute(
    Market = market, `Radius` = paste(radius, "m"), `N` = fmt(total),
    `Dropped` = pct(Dropped),
    `Return NA` = pct(`Reported NA only`),
    `No return` = pct(`No return filed only`),
    `Spills, no events` = pct(`Positive, no events only`),
    `Two or more` = pct(`Two or more gaps`)
  )
tbl_cost <- kable(ct, align = "llrrrrrr")

# --- fig-cost -----------------------------------------------------------------
cst <- cost |> mutate(
  reason = factor(reason, levels = rev(cost_levels)),
  radius = factor(paste0(radius, " m"), levels = c("1000 m", "500 m", "250 m")),
  market = factor(market, levels = c("Sales", "Rentals"))
)
fig_cost <- ggplot(cst, aes(x = share, y = radius, fill = reason)) +
  geom_col(width = 0.68) +
  geom_text(
    data = cst |> filter(share > 0.06), aes(label = pct(share, 0)),
    position = position_stack(vjust = 0.5), size = 2.9, family = base_family, colour = "white"
  ) +
  facet_wrap(~market, ncol = 2) +
  scale_fill_manual(values = cost_cols, breaks = cost_levels) +
  scale_x_continuous(labels = label_percent(), breaks = c(0, 0.25, 0.5, 0.75, 1), expand = expansion(mult = c(0, 0.02))) +
  labs(title = "Share of transactions kept and dropped, by radius", x = NULL, y = NULL) +
  theme_report() +
  theme(
    panel.grid.major.y = element_blank(), legend.key.size = unit(9, "pt"),
    panel.spacing.x = unit(2.2, "lines")
  ) +
  guides(fill = guide_legend(nrow = 1))

# --- Inline numbers -----------------------------------------------------------
# Ready-formatted strings for the prose, in the order they appear.
drop_share <- function(m, r) {
  pct(1 - cost$share[cost$market == m & cost$radius == r & cost$reason == "Verified"])
}
n <- list(
  # Stage 1
  share_strong = pct(sites_strong / n_lookup_clusters),
  share_weak = pct(sites_weak / n_lookup_clusters),
  unmatched_sites = fmt(n_unmatched_sites),
  # Stage 2
  spill_sites = fmt(n_lookup_clusters),
  overflows_year_min = fmt(min(rows_by_year)),
  overflows_year_max = fmt(max(rows_by_year)),
  share_4_years = share_sites(n_sites_by_years[["4"]]),
  share_3_years = share_sites(n_sites_by_years[["3"]]),
  share_2_years = share_sites(n_sites_by_years[["2"]]),
  single_year = fmt(n_sites_by_years[["1"]]),
  share_1_year = share_sites(n_sites_by_years[["1"]]),
  single_year_modal = single_year_modal,
  single_regrouped = fmt(n_single_regrouped),
  # Stage 3
  site_groups = fmt(n_site_groups),
  outlets_1 = fmt(outlets_per_group$n[1]),
  outlets_2 = fmt(outlets_per_group$n[2]),
  outlets_3_plus = fmt(sum(outlets_per_group$n[3:4])),
  share_multi_same_ngr = pct(n_multi_same_ngr / n_multi_groups, 0),
  share_multi_within_250 = pct(n_multi_within_250 / n_multi_groups, 0),
  share_multi_over_1000 = pct(1 - n_multi_within_1000 / n_multi_groups, 0),
  masked_group_years = fmt(n_masked),
  masked_groups = fmt(n_masked_groups),
  masked_zero = fmt(n_masked_zero),
  masked_missing_share = pct(masked_missing_share_median, 0),
  masked_false_zero = fmt(n_masked_false_zero),
  masked_hours_2021 = pct(masked_hours_share[["2021"]]),
  masked_hours_2022 = pct(masked_hours_share[["2022"]]),
  masked_houses_250 = pct(masked_houses$n_masked[1] / masked_houses$n_any[1]),
  masked_houses_1000 = pct(masked_houses$n_masked[3] / masked_houses$n_any[3]),
  # Event matching
  events_matched = fmt(n_matched),
  events = fmt(n_events),
  share_events_matched = pct(n_matched / n_events, 2),
  events_unmatched = fmt(n_unmatched),
  share_events_group = pct(n_matched_group / n_events),
  unmatched_no_key = fmt(n_unmatched_by_reason[["no_field"]]),
  unmatched_hours_disagree = fmt(n_unmatched_by_reason[["hours"]]),
  unmatched_other = fmt(n_unmatched_by_reason[["other"]]),
  share_unmatched_2022 = pct(n_unmatched_2022 / n_unmatched, 0),
  # Verified site-years
  site_years_verified = fmt(site_years_verified),
  site_years = fmt(n_site_years),
  share_site_years_verified = pct(site_years_verified / n_site_years),
  sites_verified_all = fmt(sg_verified_all),
  # Dropped transactions
  drop_sales_250 = drop_share("Sales", 250),
  drop_rentals_250 = drop_share("Rentals", 250)
)
