suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
})

path <- "/Users/odran/Dropbox/sewage/data/processed/zoopla/zoopla_rentals.parquet"

inspect_zoopla <- function(path, n = 10) {
  cat("\n==============================\n")
  cat("FILE:", path, "\n")
  cat("==============================\n")

  if (!file.exists(path)) {
    cat("Status: MISSING\n"); return(invisible(NULL))
  }
  fi <- file.info(path)
  if (is.na(fi$size) || fi$size == 0) {
    cat("Status: EMPTY (0 bytes)\n"); return(invisible(NULL))
  }

  # Read parquet
  df <- tryCatch(arrow::read_parquet(path), error = function(e) e)
  if (inherits(df, "error")) { cat("Error reading parquet:\n"); print(df); return(invisible(NULL)) }

  # Basics
  cat("Rows:", nrow(df), " Cols:", ncol(df), "\n\n")
  cat("Column names:\n"); print(names(df)); cat("\n")

  # Types (compact)
  cat("Column classes (first 30):\n")
  cls <- sapply(df, function(x) paste(class(x), collapse="|"))
  print(utils::head(cls, 30)); cat(if (length(cls) > 30) "\n... (more columns)\n\n" else "\n")

  # Heuristics
  id_cols <- intersect(c("rental_id","listing_id","property_id","site_id"), names(df))
  time_cols <- intersect(c("qtr_id","month_id","date","listed_date","year","quarter"), names(df))
  # price-like columns (avoid 'rental_id' false-positive)
  price_cols <- names(df)[
    grepl("(^price$|price_|_price$|^rent$|rent_|_rent$|pcm|per_month|perweek|pw|monthly_rent)", 
          names(df), ignore.case = TRUE) &
    !grepl("^rental_id$", names(df), ignore.case = TRUE)
  ]
  unit_cols <- names(df)[grepl("period|per_|pcm|pw|unit", names(df), ignore.case = TRUE)]

  cat("Detected ID cols:      ", if (length(id_cols)) paste(id_cols, collapse=", ") else "none", "\n")
  cat("Detected time cols:    ", if (length(time_cols)) paste(time_cols, collapse=", ") else "none", "\n")
  cat("Detected price cols:   ", if (length(price_cols)) paste(price_cols, collapse=", ") else "none", "\n")
  cat("Detected price units:  ", if (length(unit_cols)) paste(unit_cols, collapse=", ") else "none", "\n\n")

  # Price summaries (numeric coercion)
  if (length(price_cols)) {
    coerce_price <- function(x) {
      if (is.numeric(x)) return(as.numeric(x))
      as.numeric(gsub("[^0-9.]+", "", as.character(x)))
    }
    cat("Price column summaries:\n")
    sumry <- df |>
      summarise(across(all_of(price_cols),
                       list(n = ~sum(!is.na(.)),
                            min = ~suppressWarnings(min(coerce_price(.), na.rm=TRUE)),
                            p50 = ~suppressWarnings(median(coerce_price(.), na.rm=TRUE)),
                            max = ~suppressWarnings(max(coerce_price(.), na.rm=TRUE))),
                       .names = "{.col}.{.fn}"))
    print(sumry); cat("\n")
  }

  # If keys exist, check duplicates by (rental_id, qtr_id) or (rental_id, month_id)
  if (all(c("rental_id","qtr_id") %in% names(df))) {
    dup_q <- df |> count(rental_id, qtr_id, name="n") |> filter(n > 1) |> nrow()
    cat("Duplicate (rental_id, qtr_id) groups:", dup_q, "\n")
  } else if (all(c("rental_id","month_id") %in% names(df))) {
    dup_m <- df |> count(rental_id, month_id, name="n") |> filter(n > 1) |> nrow()
    cat("Duplicate (rental_id, month_id) groups:", dup_m, "\n")
  }
  cat("\n")

  # Show first n rows (focus on likely-useful columns)
  sample_cols <- unique(c(id_cols, time_cols, price_cols, unit_cols))
  if (!length(sample_cols)) sample_cols <- names(df)
  cat(sprintf("First %d rows (%s):\n", n, if (length(sample_cols) < ncol(df)) "selected columns" else "all columns"))
  print(slice_head(select(df, any_of(sample_cols)), n = n))

  invisible(df)
}

zoopla_df <- inspect_zoopla(path)
# If you’re in RStudio and want a spreadsheet view, uncomment:
# View(zoopla_df)
