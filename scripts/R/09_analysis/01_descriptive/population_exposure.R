# ==============================================================================
# Population Exposure to Sewage Spill Sites
# ==============================================================================
#
# Purpose: Compute the residential population living within distance buffers
#          (50m, 100m, 250m, 500m, 1000m) of sewage spill sites, classified
#          into three categories: Any Spill, Dry Spills, and Zero Spills.
#          Outputs a tabularray LaTeX table.
#
# Author: Jacopo Olivieri
# Date: 2026-03-18
#
# Inputs:
#   - data/processed/agg_spill_stats/agg_spill_mo.parquet
#   - data/processed/agg_spill_stats/agg_spill_dry_mo.parquet
#   - data/raw/population_grid/data/uk_residential_population_2021.tif
#
# Outputs:
#   - output/tables/population_exposure.tex
#
# ==============================================================================

if (!requireNamespace("here", quietly = TRUE)) {
  stop(
    "Package `here` is required to run this script. ",
    "Install project dependencies first, e.g. `renv::restore()`.",
    call. = FALSE
  )
}

source(here::here("scripts", "R", "utils", "script_setup.R"), local = TRUE)

REQUIRED_PACKAGES <- c(
  "dplyr",
  "tidyr",
  "sf",
  "terra",
  "arrow",
  "here",
  "rnrfa"
)


# ==============================================================================
# 1. Configuration
# ==============================================================================
TARGET_YEARS <- 2021:2023
BASE_YEAR <- 2021L
DRY_SPILL_COUNT_COL <- "dry_spill_count_mo_r1_d0123_strict"
DISTANCES_M <- c(50, 100, 250, 500, 1000)

PATH_ALL_MO <- here::here("data", "processed", "agg_spill_stats", "agg_spill_mo.parquet")
PATH_DRY_MO <- here::here("data", "processed", "agg_spill_stats", "agg_spill_dry_mo.parquet")
PATH_POP_RASTER <- here::here(
  "data", "raw", "population_grid", "data", "uk_residential_population_2021.tif"
)
PATH_OUTPUT <- here::here("output", "tables", "population_exposure.tex")


# ==============================================================================
# 2. Package Management
# ==============================================================================
check_required_packages(REQUIRED_PACKAGES)

initialise_environment <- function() {
  invisible(
    lapply(
      REQUIRED_PACKAGES,
      function(pkg) suppressPackageStartupMessages(
        library(pkg, character.only = TRUE)
      )
    )
  )
}

initialise_environment()


# ==============================================================================
# 3. Helper Functions
# ==============================================================================
fmt_int <- function(x) {
  formatC(as.numeric(x), format = "d", big.mark = ",")
}

fmt_thousands <- function(x) {
  formatC(round(as.numeric(x) / 1000), format = "d", big.mark = ",")
}

#' Compute population within dissolved buffers (no double-counting)
pop_within_union_buffers <- function(pop_raster, pts_vect, dist_m) {
  if (length(pts_vect) == 0) return(0)
  buf <- terra::buffer(pts_vect, width = dist_m)
  buf_u <- terra::aggregate(buf, dissolve = TRUE)
  pop_crop <- terra::crop(pop_raster, buf_u)
  vals <- terra::extract(pop_crop, buf_u, fun = sum, na.rm = TRUE)
  sums <- vals[, -1, drop = FALSE]
  sum(as.numeric(unlist(sums)), na.rm = TRUE)
}


# ==============================================================================
# 4. Data Loading
# ==============================================================================
cat("Loading monthly spill aggregates...\n")

all_mo <- arrow::read_parquet(PATH_ALL_MO) |>
  dplyr::mutate(
    site_id = as.integer(site_id),
    year = BASE_YEAR + floor((month_id - 1L) / 12)
  )

dry_mo <- arrow::read_parquet(PATH_DRY_MO) |>
  dplyr::mutate(
    site_id = as.integer(site_id),
    year = BASE_YEAR + floor((month_id - 1L) / 12)
  )

stopifnot(DRY_SPILL_COUNT_COL %in% names(dry_mo))

cat("Loading population raster...\n")
pop_r <- terra::rast(PATH_POP_RASTER)


# ==============================================================================
# 5. Site Classification
# ==============================================================================
cat("Classifying sites over ", min(TARGET_YEARS), "-", max(TARGET_YEARS), "...\n",
    sep = "")

all_win <- all_mo |> dplyr::filter(year >= min(TARGET_YEARS), year <= max(TARGET_YEARS))
dry_win <- dry_mo |> dplyr::filter(year >= min(TARGET_YEARS), year <= max(TARGET_YEARS))

site_any <- all_win |>
  dplyr::group_by(site_id, water_company) |>
  dplyr::summarise(total_spills = sum(spill_count_mo, na.rm = TRUE), .groups = "drop")

site_dry <- dry_win |>
  dplyr::group_by(site_id, water_company) |>
  dplyr::summarise(
    total_dry = sum(.data[[DRY_SPILL_COUNT_COL]], na.rm = TRUE),
    .groups = "drop"
  )

site_status <- site_any |>
  dplyr::left_join(site_dry, by = c("site_id", "water_company")) |>
  dplyr::mutate(
    total_dry = tidyr::replace_na(total_dry, 0),
    any_spill = total_spills > 0,
    has_dry = total_dry > 0
  )


# ==============================================================================
# 6. Geocoding (NGR -> BNG via rnrfa)
# ==============================================================================
cat("Geocoding sites from NGR...\n")

ngr_df <- dry_mo |>
  dplyr::transmute(
    site_id = as.integer(site_id),
    water_company,
    ngr = dplyr::coalesce(dplyr::na_if(ngr, ""), dplyr::na_if(ngr_og, ""))
  ) |>
  dplyr::distinct(site_id, water_company, .keep_all = TRUE) |>
  dplyr::mutate(ngr = toupper(gsub("\\s+", "", ngr))) |>
  dplyr::filter(
    !is.na(ngr),
    grepl("^[A-Z]{2}[0-9]{2,10}$", ngr),
    nchar(sub("^[A-Z]{2}", "", ngr)) %% 2 == 0
  )

ll <- tryCatch(
  {
    parsed <- rnrfa::osg_parse(ngr_df$ngr, coord_system = "WGS84")
    cbind(lon = as.numeric(parsed[[1]]), lat = as.numeric(parsed[[2]]))
  },
  error = function(e) {
    warning("Vectorised osg_parse failed: ", conditionMessage(e))
    matrix(NA_real_, nrow = nrow(ngr_df), ncol = 2, dimnames = list(NULL, c("lon", "lat")))
  }
)

loc_df <- dplyr::bind_cols(ngr_df, as.data.frame(ll)) |>
  dplyr::filter(!is.na(lat), !is.na(lon))

sites_sf <- sf::st_as_sf(loc_df, coords = c("lon", "lat"), crs = 4326) |>
  dplyr::left_join(site_status, by = c("site_id", "water_company")) |>
  dplyr::filter(!is.na(any_spill))

cat("Geocoded ", nrow(sites_sf), " sites.\n", sep = "")


# ==============================================================================
# 7. Prepare Raster and Project Sites to BNG
# ==============================================================================
if (terra::is.lonlat(pop_r)) {
  cat("Projecting population raster to EPSG:27700...\n")
  pop_r_m <- terra::project(pop_r, "EPSG:27700", method = "near")
  sites_proj <- sf::st_transform(sites_sf, 27700)
} else {
  pop_r_m <- pop_r
  sites_proj <- sf::st_transform(sites_sf, terra::crs(pop_r_m))
}

sites_v <- terra::vect(sites_proj)


# ==============================================================================
# 8. Buffer Computation
# ==============================================================================
groups <- list(
  list(label = "Any Spill",  filter_expr = quote(any_spill == TRUE)),
  list(label = "Dry Spills", filter_expr = quote(has_dry == TRUE)),
  list(label = "Zero Spills", filter_expr = quote(any_spill == FALSE))
)

res_rows <- list()
for (g in groups) {
  cat("Computing buffers for ", g$label, "...\n", sep = "")
  idx <- which(eval(g$filter_expr, as.data.frame(sites_v)))
  pts_g <- sites_v[idx, ]
  totals <- vapply(
    DISTANCES_M,
    function(d) pop_within_union_buffers(pop_r_m, pts_g, d),
    numeric(1)
  )
  names(totals) <- paste0("within_", DISTANCES_M, "m")
  res_rows[[g$label]] <- c(n_sites = length(pts_g), totals)
}

pop_table <- tibble::as_tibble(do.call(rbind, res_rows)) |>
  dplyr::mutate(
    group = names(res_rows),
    dplyr::across(-group, ~ round(as.numeric(.x)))
  ) |>
  dplyr::relocate(group, n_sites)

cat("\nResults:\n")
print(pop_table)


# ==============================================================================
# 9. LaTeX Table Export
# ==============================================================================
cat("\nWriting LaTeX table...\n")

dir.create(dirname(PATH_OUTPUT), recursive = TRUE, showWarnings = FALSE)

make_row <- function(i) {
  row <- pop_table[i, ]
  paste0(
    row$group, " & ",
    fmt_int(row$n_sites), " & ",
    fmt_thousands(row$within_50m), " & ",
    fmt_thousands(row$within_100m), " & ",
    fmt_thousands(row$within_250m), " & ",
    fmt_thousands(row$within_500m), " & ",
    fmt_thousands(row$within_1000m),
    " \\\\"
  )
}

data_rows <- vapply(seq_len(nrow(pop_table)), make_row, character(1))

tex_lines <- c(
  "\\begin{table}[H]",
  "\\centering",
  "\\begin{talltblr}[",
  "caption={Residential Population (Thousands) Within Distance of Spill Sites (2021--2023)},",
  "label={tab:pop_exposure},",
  "note{a}={Population (in thousands) is computed from the UK residential population 2021 raster",
  "  (100\\,m grid). Where buffers from multiple sites overlap, the overlapping",
  "  area is counted only once.},",
  "]{",
  "colsep=3pt,",
  "rowsep=0.5pt,",
  "cells={font=\\fontsize{11pt}{12pt}\\selectfont},",
  "colspec={Q[l]Q[c]Q[c]Q[c]Q[c]Q[c]Q[c]},",
  "hline{1}={1-7}{solid, black, 0.1em},",
  "hline{2}={1-7}{solid, black, 0.05em},",
  "hline{5}={1-7}{solid, black, 0.1em},",
  "column{1}={}{halign=l},",
  "column{2-7}={}{halign=c}",
  "}",
  "{Storm Overflow \\\\ Category} & Sites & Within 50\\,m & Within 100\\,m & Within 250\\,m & Within 500\\,m & Within 1000\\,m \\\\",
  data_rows,
  "\\end{talltblr}",
  "\\end{table}"
)

writeLines(tex_lines, PATH_OUTPUT)
cat("Wrote: ", PATH_OUTPUT, "\n", sep = "")
