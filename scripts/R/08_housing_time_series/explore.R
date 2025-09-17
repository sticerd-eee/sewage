# ==== Explore general_panel sales/rentals datasets: print columns & heads ====

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fs)
})

# ----- Paths -----
rent_paths <- c(
  "/Users/odran/Dropbox/sewage/data/processed/general_panel/rentals/radius=250/part-0.parquet",
  "/Users/odran/Dropbox/sewage/data/processed/general_panel/rentals/radius=500/part-0.parquet",
  "/Users/odran/Dropbox/sewage/data/processed/general_panel/rentals/radius=1000/part-0.parquet"
)

sale_paths <- c(
  "/Users/odran/Dropbox/sewage/data/processed/general_panel/sales/radius=250/part-0.parquet",
  "/Users/odran/Dropbox/sewage/data/processed/general_panel/sales/radius=500/part-0.parquet",
  "/Users/odran/Dropbox/sewage/data/processed/general_panel/sales/radius=1000/part-0.parquet"
)

# ----- Helper to explore a single path -----
explore_path <- function(p) {
  cat("\n================================================================================\n")
  cat("Path:", p, "\n")
  if (!file.exists(p)) {
    cat("!! Does not exist.\n")
    return(invisible(NULL))
  }
  kind <- if (fs::is_dir(p)) "directory (dataset)" else "file (single parquet)"
  cat("Type:", kind, "\n")

  # Try dataset for directories; single file read for files
  if (fs::is_dir(p)) {
    # Directory of parquet parts
    ok <- TRUE
    ds <- tryCatch(arrow::open_dataset(p, format = "parquet"),
                   error = function(e) { cat("!! open_dataset error:", e$message, "\n"); ok <<- FALSE; NULL })
    if (!ok) return(invisible(NULL))

    cat("\n--- Schema ---\n")
    print(ds$schema)

    cat("\n--- Column names ---\n")
    print(names(ds))

    # Head (first 5 rows)
    cat("\n--- Preview (first 5 rows) ---\n")
    head_df <- tryCatch(
      ds %>% select(dplyr::everything()) %>% collect(n = 5),
      error = function(e) { cat("!! collect error:", e$message, "\n"); NULL }
    )
    if (!is.null(head_df)) print(head_df)

  } else {
    # Single parquet file
    ok <- TRUE
    tab <- tryCatch(arrow::read_parquet(p, as_data_frame = FALSE),
                    error = function(e) { cat("!! read_parquet error:", e$message, "\n"); ok <<- FALSE; NULL })
    if (!ok) return(invisible(NULL))

    cat("\n--- Schema ---\n")
    print(tab$schema)

    cat("\n--- Column names ---\n")
    cols <- names(as.data.frame(tab))
    print(cols)

    cat("\n--- Preview (first 5 rows) ---\n")
    df <- as.data.frame(tab)
    print(utils::head(df, 5))
  }
}

cat("\n=============================== RENTALS ===============================\n")
invisible(lapply(rent_paths, explore_path))

cat("\n=============================== SALES ================================\n")
invisible(lapply(sale_paths, explore_path))

cat("\nDone.\n")
