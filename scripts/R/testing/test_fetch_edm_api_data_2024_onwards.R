assert_true <- function(condition, message) {
  if (!isTRUE(condition)) {
    stop(message, call. = FALSE)
  }
}

assert_error_contains <- function(expr, pattern) {
  error_message <- tryCatch(
    {
      force(expr)
      NULL
    },
    error = function(e) e$message
  )

  if (is.null(error_message)) {
    stop("Expected an error but the expression succeeded.", call. = FALSE)
  }

  if (!grepl(pattern, error_message, fixed = TRUE)) {
    stop(
      paste0(
        "Expected error containing `", pattern, "` but got `", error_message, "`."
      ),
      call. = FALSE
    )
  }
}

source("scripts/R/01_data_ingestion/fetch_edm_api_data_2024_onwards.R", local = TRUE)

# URL normalization should accept both service-root and query-endpoint inputs.
northumbrian_query_url <- paste0(
  "https://services-eu1.arcgis.com/MSNNjkZ51iVh8yBj/arcgis/rest/services/",
  "Northumbrian_Water_Storm_Overflow_Activity_2_view/FeatureServer/0/query",
  "?outFields=*&where=1%3D1"
)
expected_northumbrian_root <- paste0(
  "https://services-eu1.arcgis.com/MSNNjkZ51iVh8yBj/arcgis/rest/services/",
  "Northumbrian_Water_Storm_Overflow_Activity_2_view/FeatureServer/0"
)

assert_true(
  identical(
    normalise_arcgis_service_url(northumbrian_query_url),
    expected_northumbrian_root
  ),
  "ArcGIS service URL normalization should strip `/query` and query parameters."
)

assert_true(
  identical(
    build_query_url(northumbrian_query_url),
    paste0(expected_northumbrian_root, "/query")
  ),
  "ArcGIS query URL construction should always end with a single `/query`."
)

# Empty but valid ArcGIS responses should be accepted.
empty_response <- jsonlite::fromJSON(
  '{"objectIdFieldName":"OBJECTID","geometryType":"esriGeometryPoint","features":[]}',
  flatten = TRUE
)

validate_arcgis_response(empty_response, "empty-test", 0)
empty_features <- coerce_arcgis_features(empty_response$features, "empty-test", 0)
assert_true(
  is.data.frame(empty_features) && nrow(empty_features) == 0,
  "Valid empty ArcGIS responses should coerce to a 0-row data frame."
)

# ArcGIS error payloads should fail fast even if HTTP status was 200.
error_response <- jsonlite::fromJSON(
  paste0(
    '{"error":{"code":400,"message":"Invalid query parameters",',
    '"details":["where clause is invalid"]}}'
  ),
  flatten = TRUE
)

assert_error_contains(
  validate_arcgis_response(error_response, "error-test", 0),
  "ArcGIS returned an error"
)

# Missing `features` should also fail fast instead of being treated as 0 rows.
missing_features_response <- list(objectIdFieldName = "OBJECTID")
assert_error_contains(
  validate_arcgis_response(missing_features_response, "missing-features-test", 0),
  "did not contain a `features` element"
)

if (identical(Sys.getenv("RUN_LIVE_API_SMOKE_TEST"), "1")) {
  source("scripts/config/api_config.R", local = TRUE)

  smoke_config <- api_config_list$anglian_water
  query_url <- build_query_url(smoke_config$base_url)
  query_params <- c(
    smoke_config$query_params,
    list(
      resultOffset = 0,
      resultRecordCount = 1
    )
  )

  response <- httr::GET(query_url, query = query_params, httr::timeout(120))
  httr::stop_for_status(response)

  response_text <- httr::content(response, "text", encoding = "UTF-8")
  response_data <- jsonlite::fromJSON(response_text, flatten = TRUE)

  validate_arcgis_response(response_data, smoke_config$name, 0)
  live_features <- coerce_arcgis_features(response_data$features, smoke_config$name, 0)

  assert_true(
    is.data.frame(live_features),
    "The live smoke test should return a feature data frame."
  )
  assert_true(
    nrow(live_features) <= 1,
    "The live smoke test should respect `resultRecordCount = 1`."
  )
}

cat("All fetch_edm_api_data_2024_onwards tests passed.\n")
