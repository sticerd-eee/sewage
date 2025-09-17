# =================== CONFIG ===================
years_of_interest <- 2021:2024
parquet_path <- "/Users/odran/Dropbox/sewage/data/processed/agg_spill_stats/agg_spill_mo.parquet"
out_dir <- "/Users/odran/Dropbox/sewage/data/processed/never_spilled_sites"

# ================= LIBRARIES ==================
pkgs <- c("dplyr", "arrow", "readr", "tibble", "purrr", "stringr")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) install.packages(p)

library(dplyr)
library(arrow)
library(readr)
library(tibble)
library(purrr)
library(stringr)

# ========== HELPER: NGR -> EASTING/NORTHING ==========
# Converts OS National Grid Reference (e.g., "SP6419046470") to Easting/Northing (EPSG:27700)
# Supports 2/4/6/8/10-digit precisions (1km/100m/10m/1m).
ngr_to_easting_northing <- function(ngr_vec) {
  # mapping letters excluding I: A..Z without I
  letters_no_i <- strsplit("ABCDEFGHJKLMNOPQRSTUVWXYZ", "")[[1]]
  letter_index <- function(ch) {
    idx <- match(ch, letters_no_i) - 1L  # 0-based index
    ifelse(is.na(idx), NA_integer_, idx)
  }

  v <- as.character(ngr_vec)
  res <- map(v, function(gr) {
    if (is.na(gr) || !nzchar(gr)) {
      return(c(NA_real_, NA_real_))
    }
    # normalize: remove spaces, uppercase
    g <- toupper(gsub("\\s+", "", gr))

    # must start with two letters then digits (even count)
    if (!grepl("^[A-Z]{2}[0-9]*$", g)) return(c(NA_real_, NA_real_))

    L1 <- substr(g, 1, 1)
    L2 <- substr(g, 2, 2)
    d  <- substr(g, 3, nchar(g))

    l1 <- letter_index(L1)
    l2 <- letter_index(L2)
    if (is.na(l1) || is.na(l2)) return(c(NA_real_, NA_real_))

    # Compute 100km grid indices per OS scheme (Chris Veness algorithm)
    # e100km & n100km are in 100 km units
    e100km <- (((l1 - 2L) %% 5L) * 5L) + (l2 %% 5L)
    n100km <- 19L - (l1 %/% 5L) * 5L - (l2 %/% 5L)

    # digits: split evenly into e/n parts and pad to 5
    if (nchar(d) == 0) {
      e_rel <- 0L; n_rel <- 0L; pad <- 5
    } else {
      if ((nchar(d) %% 2L) != 0L) return(c(NA_real_, NA_real_)) # must be even
      half <- nchar(d) / 2L
      e_rel <- as.integer(substr(d, 1, half))
      n_rel <- as.integer(substr(d, half + 1, nchar(d)))
      # scale up to metres (pad to 5 digits)
      pad <- 5 - half
      e_rel <- as.integer(e_rel * (10^pad))
      n_rel <- as.integer(n_rel * (10^pad))
    }

    e <- as.numeric(e100km) * 100000 + as.numeric(e_rel)
    n <- as.numeric(n100km) * 100000 + as.numeric(n_rel)

    # sanity: valid OSGB ranges roughly 0<=e<=700000, 0<=n<=1300000
    if (is.na(e) || is.na(n) || e < 0 || n < 0 || e > 800000 || n > 1400000) {
      return(c(NA_real_, NA_real_))
    }
    c(e, n)
  })

  mat <- do.call(rbind, res)
  tibble(easting = as.numeric(mat[,1]), northing = as.numeric(mat[,2]))
}

# ================ LOAD DATA ===================
df <- arrow::read_parquet(parquet_path)

required_cols <- c("site_id", "water_company", "year", "month", "spill_count_mo",
                   "ngr", "ngr_og", "site_name_ea", "site_name_wa_sc",
                   "permit_reference_ea", "permit_reference_wa_sc",
                   "asset_type", "wfd_waterbody_id_cycle_2", "receiving_water_name")
missing_cols <- setdiff(required_cols, names(df))
if (length(missing_cols) > 0) {
  stop("Missing required columns in parquet: ", paste(missing_cols, collapse = ", "))
}

dfw <- df %>% filter(year %in% years_of_interest)
if (nrow(dfw) == 0) stop("No rows in selected years: ", paste(years_of_interest, collapse = ", "))

# ======= SITE METADATA (one row per site) =====
site_meta <- dfw %>%
  arrange(site_id) %>%
  group_by(site_id) %>%
  summarise(
    water_company            = first(na.omit(water_company)),
    site_name_ea             = first(na.omit(site_name_ea)),
    site_name_wa_sc          = first(na.omit(site_name_wa_sc)),
    permit_reference_ea      = first(na.omit(permit_reference_ea)),
    permit_reference_wa_sc   = first(na.omit(permit_reference_wa_sc)),
    asset_type               = first(na.omit(asset_type)),
    ngr                      = first(na.omit(ngr)),
    ngr_og                   = first(na.omit(ngr_og)),
    wfd_waterbody_id_cycle_2 = first(na.omit(wfd_waterbody_id_cycle_2)),
    receiving_water_name     = first(na.omit(receiving_water_name)),
    .groups = "drop"
  ) %>%
  mutate(ngr_use = dplyr::coalesce(ngr, ngr_og))

# ======= ADD EASTING/NORTHING FROM NGR ========
coords_tbl <- ngr_to_easting_northing(site_meta$ngr_use)
site_meta_coords <- bind_cols(site_meta, coords_tbl)

# ========= BUILD NO-SPILL SITE LISTS ==========
# STRICT: zero total AND no NA months in the window
strict_no_spill_sites <- dfw %>%
  group_by(site_id) %>%
  summarise(
    total_spills_mo = sum(spill_count_mo, na.rm = TRUE),
    any_na_mo       = any(is.na(spill_count_mo)),
    n_months_obs    = sum(!is.na(spill_count_mo)),
    .groups = "drop"
  ) %>%
  filter(total_spills_mo == 0, !any_na_mo, n_months_obs > 0) %>%
  inner_join(site_meta_coords, by = "site_id") %>%
  arrange(water_company, site_id)

# LOOSE: zero total over observed months (NAs allowed)
loose_no_spill_sites <- dfw %>%
  group_by(site_id) %>%
  summarise(total_spills_mo = sum(spill_count_mo, na.rm = TRUE), .groups = "drop") %>%
  filter(total_spills_mo == 0) %>%
  inner_join(site_meta_coords, by = "site_id") %>%
  arrange(water_company, site_id)

# ============== OUTPUT & PRINT ===============
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

strict_path_csv <- file.path(out_dir, sprintf("never_spilled_sites_STRICT_%s_%s.csv",
                                              min(years_of_interest), max(years_of_interest)))
loose_path_csv  <- file.path(out_dir, sprintf("never_spilled_sites_LOOSE_%s_%s.csv",
                                              min(years_of_interest), max(years_of_interest)))

readr::write_csv(strict_no_spill_sites, strict_path_csv)
readr::write_csv(loose_no_spill_sites,  loose_path_csv)

message("Years: ", min(years_of_interest), "–", max(years_of_interest))
message("Strict no-spill sites: ", nrow(strict_no_spill_sites), " -> ", strict_path_csv)
message("Loose  no-spill sites: ", nrow(loose_no_spill_sites),  " -> ", loose_path_csv)

# Quick preview
message("\nHead (strict):")
print(utils::head(strict_no_spill_sites, 10), n = 10)
message("\nHead (loose):")
print(utils::head(loose_no_spill_sites, 10), n = 10)
