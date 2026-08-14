# ==============================================================================
# Content-Stable Hash Utilities
# ==============================================================================

# ASCII record/unit separators are reserved so field boundaries and missing
# values cannot be confused with source text. Callers fail before hashing if
# either character appears in a non-missing source field.
HASH_NA_TOKEN <- "\u001e"
HASH_FIELD_SEPARATOR <- "\u001f"

assert_hash_dependencies <- function() {
  if (!requireNamespace("digest", quietly = TRUE)) {
    stop(
      "Package `digest` is required. Install project dependencies with `rv sync`.",
      call. = FALSE
    )
  }
}

format_hash_field <- function(x) {
  missing <- is.na(x)

  if (inherits(x, "Date") || inherits(x, "POSIXt")) {
    value <- format(x, "%Y-%m-%d")
  } else if (is.numeric(x)) {
    value <- format(x, scientific = FALSE, trim = TRUE)
  } else {
    value <- as.character(x)
  }

  value <- enc2utf8(value)
  nonmissing <- value[!missing]
  has_reserved <- vapply(
    c(HASH_NA_TOKEN, HASH_FIELD_SEPARATOR),
    function(token) any(grepl(token, nonmissing, fixed = TRUE)),
    logical(1)
  )
  if (any(has_reserved)) {
    stop("Hash input contains a reserved serialization token.", call. = FALSE)
  }

  value[missing] <- HASH_NA_TOKEN
  value
}

#' Serialize ordered fields for stable hashing
#'
#' Field order is supplied explicitly. Dates use YYYY-MM-DD, numerics avoid
#' scientific notation, strings are UTF-8, and missing values use a reserved
#' token distinct from an empty string.
serialize_hash_fields <- function(data, fields) {
  if (!is.data.frame(data)) {
    stop("`data` must be a data frame.", call. = FALSE)
  }
  if (!is.character(fields) || length(fields) == 0L || anyDuplicated(fields)) {
    stop("`fields` must be a non-empty unique character vector.", call. = FALSE)
  }

  missing_fields <- setdiff(fields, names(data))
  if (length(missing_fields) > 0L) {
    stop(
      "Missing hash fields: ", paste(missing_fields, collapse = ", "),
      call. = FALSE
    )
  }

  serialized <- lapply(as.data.frame(data)[fields], format_hash_field)
  do.call(paste, c(serialized, sep = HASH_FIELD_SEPARATOR))
}

#' Hash already-serialized UTF-8 values with xxhash64
hash_serialized_values <- function(x) {
  assert_hash_dependencies()
  if (!is.character(x) || anyNA(x)) {
    stop("Serialized hash input must be a non-missing character vector.", call. = FALSE)
  }
  digest::getVDigest("xxhash64")(enc2utf8(x), serialize = FALSE)
}

#' Hash ordered fields from a data frame
hash_fields <- function(data, fields) {
  hash_serialized_values(serialize_hash_fields(data, fields))
}

#' Hash Land Registry transaction identifiers
hash_transaction_id <- function(transaction_id) {
  if (anyNA(transaction_id) || any(!nzchar(as.character(transaction_id)))) {
    stop("Land Registry transaction ids must be non-missing and non-empty.", call. = FALSE)
  }
  hash_fields(data.frame(transaction_id = transaction_id), "transaction_id")
}

#' Hash the locked seven-field Zoopla rental identity composite
hash_rental_identity <- function(data) {
  hash_fields(
    data,
    c(
      "postcode", "address_line_01", "address_line_02", "address_line_03",
      "listing_price", "latest_to_rent", "rented"
    )
  )
}
