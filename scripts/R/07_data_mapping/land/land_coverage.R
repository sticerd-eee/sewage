# ---- Packages ----
# install.packages(c("terra","sf","exactextractr","dplyr","readr","stringr")) # if needed
library(terra)
library(sf)
library(exactextractr)
library(dplyr)
library(readr)
library(stringr)

# ---- Inputs ----
pop_path   <- "/Users/odran/Dropbox/sewage/data/raw/population_grid/data/uk_residential_population_2021.tif"
spill_path <- "/Users/odran/Dropbox/sewage/data/processed/merged_edm_1224_dry_spill_data.RData"

# No-spill site lists (from your earlier export step with easting/northing)
strict_csv <- "/Users/odran/Dropbox/sewage/data/processed/never_spilled_sites/never_spilled_sites_STRICT_2021_2024.csv"
loose_csv  <- "/Users/odran/Dropbox/sewage/data/processed/never_spilled_sites/never_spilled_sites_LOOSE_2021_2024.csv"

# Outputs
out_dir                 <- "/Users/odran/Dropbox/sewage/data/processed/pop_stats"
merged_loose_join_csv   <- file.path(out_dir, "spill_events_annotated_with_loose_no_spill.csv")
summary_results_csv     <- file.path(out_dir, "population_by_group_and_radius.csv")

# Radii
buffer_radii_m <- c(50, 100, 250, 500, 1000)

# ---- Helper (logging) ----
section <- function(msg) cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), msg))

# Turn a df with easting/northing into sf (27700 -> raster CRS), de-duplicate exact same coords
df_to_points_sf <- function(df, ras_crs, group_label) {
  nm <- tolower(names(df))
  e_idx <- which(nm %in% c("easting","eastings","x","bng_e","e"))
  n_idx <- which(nm %in% c("northing","northings","y","bng_n","n"))
  if (length(e_idx) == 0L || length(n_idx) == 0L) {
    stop(sprintf("[%s] Could not find easting/northing columns.", group_label))
  }
  e_col <- names(df)[e_idx[1]]; n_col <- names(df)[n_idx[1]]

  pts <- st_as_sf(df, coords = c(e_col, n_col), crs = 27700)
  if (st_crs(pts)$wkt != ras_crs) pts <- st_transform(pts, crs = ras_crs)

  pts %>%
    mutate(.wkt = st_as_text(geometry)) %>%
    distinct(.wkt, .keep_all = TRUE) %>%
    select(-.wkt)
}

# Unique population within dissolved buffers for a point set
compute_unique_pop <- function(points_sf, pop_raster, radii, group_label) {
  section(sprintf("Computing unique pop for '%s'...", group_label))
  pb <- txtProgressBar(min = 0, max = length(radii), style = 3)
  out <- vector("list", length(radii))
  for (i in seq_along(radii)) {
    r <- radii[i]
    buf_union <- st_union(st_buffer(points_sf, r))
    total_unique <- exactextractr::exact_extract(pop_raster, buf_union, "sum")
    out[[i]] <- data.frame(
      group            = group_label,
      radius_m         = r,
      unique_population = as.numeric(total_unique)
    )
    setTxtProgressBar(pb, i)
  }
  close(pb)
  bind_rows(out)
}

# ---- Load population raster ----
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
section("Loading population raster...")
pop_raster <- rast(pop_path)
ras_crs <- crs(pop_raster, proj = TRUE)
section(paste("Raster loaded. CRS =", ras_crs, "| res =", paste(res(pop_raster), collapse=" x ")))

# ---- Load spill events (.RData) ----
section("Loading spill events (.RData)...")
loaded_names <- load(spill_path)
section(paste("Objects found:", paste(loaded_names, collapse=", ")))
spill_raw <- get(loaded_names[1])
section(paste("Using object:", loaded_names[1], "| class =", paste(class(spill_raw), collapse=",")))

stopifnot("Expect spill events as data.frame or sf" = is.data.frame(spill_raw) || inherits(spill_raw, "sf"))

# Ensure consistent lower-case 'yes'/'no' in dry flags if present
norm_yesno <- function(x) {
  if (is.null(x)) return(NULL)
  y <- as.character(x)
  y <- tolower(trimws(y))
  y[y %in% c("y","true","t","1")] <- "yes"
  y[y %in% c("n","false","f","0")] <- "no"
  y
}

# ---- Prepare ALL-SPILLS sites as points (unique coordinates) ----
section("Preparing ALL-SPILLS unique site coordinates...")
if (inherits(spill_raw, "sf")) {
  spill_events_df <- st_drop_geometry(spill_raw)
  spills_pts <- spill_raw
  if (st_crs(spills_pts)$wkt != ras_crs) spills_pts <- st_transform(spills_pts, ras_crs)
  spills_pts <- spills_pts %>%
    mutate(.wkt = st_as_text(geometry)) %>%
    distinct(.wkt, .keep_all = TRUE) %>%
    select(-.wkt)
} else {
  spill_events_df <- spill_raw
  spills_pts <- df_to_points_sf(spill_raw, ras_crs, "ALL-SPILLS")
}
section(paste("ALL-SPILLS unique sites:", nrow(spills_pts)))

# Try to carry site identifiers if present
site_id_col <- intersect(tolower(names(spill_events_df)), c("site_id","sitecode","site_code","id"))
if (length(site_id_col) == 1) {
  site_id_col <- names(spill_events_df)[tolower(names(spill_events_df)) == site_id_col]
} else {
  site_id_col <- NULL
}

# ---- Dry-spill subsets (4 flags): dry_day_1, dry_day_2, ea_dry_spill, bbc_dry_spill ----
dry_cols <- c("dry_day_1","dry_day_2","ea_dry_spill","bbc_dry_spill")
present_dry_cols <- dry_cols[dry_cols %in% names(spill_events_df)]
for (dc in present_dry_cols) spill_events_df[[dc]] <- norm_yesno(spill_events_df[[dc]])

make_dry_subset_points <- function(events_df, ras_crs, flag_col, label) {
  stopifnot(flag_col %in% names(events_df))
  sub <- events_df %>% filter(.data[[flag_col]] == "yes")
  if (nrow(sub) == 0) return(NULL)
  df_to_points_sf(sub, ras_crs, paste0("DRY subset: ", label))
}

dry_points <- list(
  Dry_day_1   = if ("dry_day_1"   %in% present_dry_cols) make_dry_subset_points(spill_events_df, ras_crs, "dry_day_1",   "dry_day_1")   else NULL,
  Dry_day_2   = if ("dry_day_2"   %in% present_dry_cols) make_dry_subset_points(spill_events_df, ras_crs, "dry_day_2",   "dry_day_2")   else NULL,
  EA_dry      = if ("ea_dry_spill"%in% present_dry_cols) make_dry_subset_points(spill_events_df, ras_crs, "ea_dry_spill","ea_dry_spill") else NULL,
  BBC_dry     = if ("bbc_dry_spill"%in%present_dry_cols) make_dry_subset_points(spill_events_df, ras_crs, "bbc_dry_spill","bbc_dry_spill") else NULL
)

# ---- Load STRICT/LOOSE no-spill site lists (if present) ----
strict_pts <- NULL; loose_pts <- NULL
strict_df  <- NULL; loose_df  <- NULL

if (!is.null(strict_csv) && file.exists(strict_csv)) {
  section("Loading STRICT no-spill sites CSV...")
  strict_df <- readr::read_csv(strict_csv, show_col_types = FALSE) %>%
    filter(!is.na(easting), !is.na(northing))
  if (nrow(strict_df) > 0) {
    strict_pts <- df_to_points_sf(strict_df, ras_crs, "No_spill_STRICT")
    section(paste("STRICT no-spill unique coords:", nrow(strict_pts)))
  } else section("STRICT CSV has no valid easting/northing rows; skipping.")
} else section("STRICT CSV not found; skipping.")

if (!is.null(loose_csv) && file.exists(loose_csv)) {
  section("Loading LOOSE no-spill sites CSV...")
  loose_df <- readr::read_csv(loose_csv, show_col_types = FALSE) %>%
    filter(!is.na(easting), !is.na(northing))
  if (nrow(loose_df) > 0) {
    loose_pts <- df_to_points_sf(loose_df, ras_crs, "No_spill_LOOSE")
    section(paste("LOOSE no-spill unique coords:", nrow(loose_pts)))
  } else section("LOOSE CSV has no valid easting/northing rows; skipping.")
} else section("LOOSE CSV not found; skipping.")

# ---- Merge loose no-spill with spill observations & save ----
# (Annotates event rows with a flag if their site is in the loose no-spill list)
if (!is.null(loose_df)) {
  loose_ids <- NULL
  if (!is.null(site_id_col) && (site_id_col %in% names(loose_df))) {
    loose_ids <- unique(loose_df[[site_id_col]])
  } else if ("site_id" %in% names(loose_df) && !is.null(site_id_col)) {
    loose_ids <- unique(loose_df$site_id)
  }
  if (!is.null(loose_ids)) {
    spill_events_df <- spill_events_df %>%
      mutate(in_loose_no_spill = ifelse(.data[[site_id_col]] %in% loose_ids, TRUE, FALSE))
  } else {
    # best-effort coord match if no ids
    if (all(c("easting","northing") %in% names(spill_events_df)) &&
        all(c("easting","northing") %in% names(loose_df))) {
      key_spill <- paste(round(spill_events_df$easting,3), round(spill_events_df$northing,3))
      key_loose <- paste(round(loose_df$easting,3),      round(loose_df$northing,3))
      spill_events_df$in_loose_no_spill <- key_spill %in% unique(key_loose)
    } else {
      spill_events_df$in_loose_no_spill <- NA
    }
  }
  readr::write_csv(spill_events_df, merged_loose_join_csv)
  section(paste("Saved spill events annotated with loose no-spill flag ->", merged_loose_join_csv))
}

# ---- Build SPILLERS-ONLY complements (All-spills minus No-spill) ----
spillers_only_strict_pts <- NULL
spillers_only_loose_pts  <- NULL

if (!is.null(strict_df)) {
  if (!is.null(site_id_col) && (site_id_col %in% names(strict_df)) && (site_id_col %in% names(spill_events_df))) {
    strict_ids <- unique(strict_df[[site_id_col]])
    spillers_only_df <- spill_events_df %>% filter(!(.data[[site_id_col]] %in% strict_ids))
    if (nrow(spillers_only_df) > 0) {
      spillers_only_strict_pts <- df_to_points_sf(spillers_only_df, ras_crs, "SPILLERS_ONLY_STRICT")
    }
  } else if (all(c("easting","northing") %in% names(strict_df)) && all(c("easting","northing") %in% names(spill_events_df))) {
    key_strict <- paste(round(strict_df$easting,3), round(strict_df$northing,3))
    key_spill  <- paste(round(spill_events_df$easting,3), round(spill_events_df$northing,3))
    spillers_only_df <- spill_events_df[!(key_spill %in% unique(key_strict)), ]
    if (nrow(spillers_only_df) > 0) {
      spillers_only_strict_pts <- df_to_points_sf(spillers_only_df, ras_crs, "SPILLERS_ONLY_STRICT")
    }
  }
}

if (!is.null(loose_df)) {
  if (!is.null(site_id_col) && (site_id_col %in% names(loose_df)) && (site_id_col %in% names(spill_events_df))) {
    loose_ids <- unique(loose_df[[site_id_col]])
    spillers_only_df <- spill_events_df %>% filter(!(.data[[site_id_col]] %in% loose_ids))
    if (nrow(spillers_only_df) > 0) {
      spillers_only_loose_pts <- df_to_points_sf(spillers_only_df, ras_crs, "SPILLERS_ONLY_LOOSE")
    }
  } else if (all(c("easting","northing") %in% names(loose_df)) && all(c("easting","northing") %in% names(spill_events_df))) {
    key_loose <- paste(round(loose_df$easting,3), round(loose_df$northing,3))
    key_spill <- paste(round(spill_events_df$easting,3), round(spill_events_df$northing,3))
    spillers_only_df <- spill_events_df[!(key_spill %in% unique(key_loose)), ]
    if (nrow(spillers_only_df) > 0) {
      spillers_only_loose_pts <- df_to_points_sf(spillers_only_df, ras_crs, "SPILLERS_ONLY_LOOSE")
    }
  }
}

# ---- Compute populations for each group ----
results_list <- list()

# 1) All-spills (unique sites with any spill event)
results_list[["All_spills"]] <- compute_unique_pop(spills_pts, pop_raster, buffer_radii_m, "All_spills")

# 2) Four dry definitions (if present)
if (!is.null(dry_points$Dry_day_1)) results_list[["Dry_day_1"]] <- compute_unique_pop(dry_points$Dry_day_1, pop_raster, buffer_radii_m, "Dry_day_1")
if (!is.null(dry_points$Dry_day_2)) results_list[["Dry_day_2"]] <- compute_unique_pop(dry_points$Dry_day_2, pop_raster, buffer_radii_m, "Dry_day_2")
if (!is.null(dry_points$EA_dry))    results_list[["EA_dry"]]    <- compute_unique_pop(dry_points$EA_dry,    pop_raster, buffer_radii_m, "EA_dry_spill")
if (!is.null(dry_points$BBC_dry))   results_list[["BBC_dry"]]   <- compute_unique_pop(dry_points$BBC_dry,   pop_raster, buffer_radii_m, "BBC_dry_spill")

# 3) No-spill strict/loose (if present)
if (!is.null(strict_pts)) results_list[["No_spill_STRICT"]] <- compute_unique_pop(strict_pts, pop_raster, buffer_radii_m, "No_spill_STRICT")
if (!is.null(loose_pts))  results_list[["No_spill_LOOSE"]]  <- compute_unique_pop(loose_pts,  pop_raster, buffer_radii_m, "No_spill_LOOSE")

# 4) Spillers-only complements (if computable)
if (!is.null(spillers_only_strict_pts)) results_list[["Spillers_only_STRICT"]] <- compute_unique_pop(spillers_only_strict_pts, pop_raster, buffer_radii_m, "Spillers_only_STRICT")
if (!is.null(spillers_only_loose_pts))  results_list[["Spillers_only_LOOSE"]]  <- compute_unique_pop(spillers_only_loose_pts,  pop_raster, buffer_radii_m, "Spillers_only_LOOSE")

# ---- Combine & print ----
results <- bind_rows(results_list) %>% arrange(group, radius_m)

section("Final totals (unique residents within radius):")
print(results)

# Pretty lines per group
for (g in unique(results$group)) {
  cat("\n== ", g, " ==\n", sep = "")
  sub <- results %>% filter(group == g) %>% arrange(radius_m)
  apply(sub, 1, function(row) {
    cat(sprintf("Within %4sm: %s people\n",
                as.integer(row["radius_m"]),
                format(round(as.numeric(row["unique_population"])),
                       big.mark = ",", trim = TRUE, scientific = FALSE)))
  })
}

# Save summary
readr::write_csv(results, summary_results_csv)
section(paste("Saved summary results ->", summary_results_csv))
