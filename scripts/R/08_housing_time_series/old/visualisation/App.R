#!/usr/bin/env Rscript
# Rivers: read GeoParquet with sfarrow::st_read_parquet() or fall back to edges; draw after bounds exist

suppressPackageStartupMessages({
  pkgs <- c("shiny","leaflet","sf","arrow")
  need <- pkgs[!sapply(pkgs, requireNamespace, quietly = TRUE)]
  if (length(need)) install.packages(need)
  lapply(pkgs, library, character.only = TRUE)
  if (!requireNamespace("sfarrow", quietly = TRUE)) {
    message("NOTE: install.packages('sfarrow') to read GeoParquet directly.")
  }
})
sf::sf_use_s2(FALSE); options(arrow.use_threads = TRUE)

ROOT <- "/Users/odran/Dropbox/sewage"
LINES_GPQ <- file.path(ROOT, "data/processed/Rivers/River_Lines.parquet")  # GeoParquet
FLOW_PARQ <- file.path(ROOT, "data/processed/Rivers/River_Flow.parquet")   # 4-col edges
CRS_BNG <- 27700
CRS_WGS <- 4326

# --- helpers ---
wgs_bounds_to_bng_bbox <- function(b) {
  # b is a list with west/east/south/north from Leaflet
  bb_wgs <- sf::st_bbox(c(xmin = b$west, ymin = b$south, xmax = b$east, ymax = b$north), crs = sf::st_crs(CRS_WGS))
  poly_b <- sf::st_transform(sf::st_as_sfc(bb_wgs), CRS_BNG)
  sf::st_bbox(poly_b)
}

make_lines_sf_bng <- function(df_subset) {
  if (!nrow(df_subset)) return(sf::st_sf(geometry = sf::st_sfc(), crs = CRS_BNG))
  lines <- Map(function(x1,y1,x2,y2){
    sf::st_linestring(matrix(c(x1,y1, x2,y2), ncol = 2, byrow = TRUE))
  }, df_subset$easting, df_subset$northing, df_subset$flow_easting, df_subset$flow_northing)
  sf::st_as_sf(data.frame(id = seq_along(lines)), geometry = sf::st_sfc(lines, crs = CRS_BNG))
}

subset_edges_by_bbox <- function(df, bbox_bng) {
  xmn <- bbox_bng["xmin"]; xmx <- bbox_bng["xmax"]; ymn <- bbox_bng["ymin"]; ymx <- bbox_bng["ymax"]
  keep <- (df$easting >= xmn & df$easting <= xmx & df$northing >= ymn & df$northing <= ymx) |
          (df$flow_easting >= xmn & df$flow_easting <= xmx &
           df$flow_northing >= ymn & df$flow_northing <= ymx)
  df[keep, , drop = FALSE]
}

# --- load once ---
g_lines_bng <- NULL
if (file.exists(LINES_GPQ) && requireNamespace("sfarrow", quietly = TRUE)) {
  g_lines_bng <- tryCatch(sfarrow::st_read_parquet(LINES_GPQ), error = function(e) e)
  if (inherits(g_lines_bng, "error")) {
    warning("GeoParquet read failed via sfarrow: ", conditionMessage(g_lines_bng), " — falling back to edges.")
    g_lines_bng <- NULL
  } else if (is.na(sf::st_crs(g_lines_bng))) {
    sf::st_crs(g_lines_bng) <- CRS_BNG
  }
}

edges_df <- NULL
if (is.null(g_lines_bng)) {
  stopifnot(file.exists(FLOW_PARQ))
  edges_df <- arrow::read_parquet(FLOW_PARQ,
                                  col_select = c("easting","northing","flow_easting","flow_northing"))
  for (nm in names(edges_df)) edges_df[[nm]] <- suppressWarnings(as.numeric(edges_df[[nm]]))
  edges_df <- edges_df[stats::complete.cases(edges_df), , drop = FALSE]
  edges_df <- edges_df[!(edges_df$easting == edges_df$flow_easting &
                         edges_df$northing == edges_df$flow_northing), , drop = FALSE]
}

# --- UI ---
ui <- fluidPage(
  tags$head(tags$title("River Lines")),
  fluidRow(
    column(
      width = 3,
      h4("Display"),
      numericInput("pad_m", "BBox padding (m)", value = 1000, min = 0, step = 500),
      sliderInput("ln_w", "Line width", min = 0.5, max = 3, value = 1.2, step = 0.1),
      selectInput("ln_col", "Line colour",
                  choices = c("#2b6cb0","#444444","#1f78b4","#0d6efd","#2ca02c"),
                  selected = "#2b6cb0"),
      actionButton("refresh", "Refresh")
    ),
    column(width = 9, leafletOutput("map", height = "92vh"))
  )
)

# --- server ---
server <- function(input, output, session) {

  # Initial map with a static UK-ish view; we’ll draw lines only after bounds exist
  output$map <- renderLeaflet({
    leaflet(options = leafletOptions(preferCanvas = TRUE)) |>
      addProviderTiles(providers$CartoDB.Positron) |>
      setView(lng = -2.5, lat = 53.6, zoom = 6)
  })

  # Debounce map movements to avoid redraw storms
  deb_bounds <- debounce(reactive(input$map_bounds), 300)

  draw_view <- function(bounds) {
    req(bounds)  # do nothing until bounds are available
    bb_bng <- wgs_bounds_to_bng_bbox(bounds)
    pad <- as.numeric(input$pad_m %||% 0)
    bb_bng["xmin"] <- bb_bng["xmin"] - pad
    bb_bng["xmax"] <- bb_bng["xmax"] + pad
    bb_bng["ymin"] <- bb_bng["ymin"] - pad
    bb_bng["ymax"] <- bb_bng["ymax"] + pad

    proxy <- leafletProxy("map") %>% clearGroup("Rivers")

    if (!is.null(g_lines_bng)) {
      sub <- suppressWarnings(sf::st_crop(g_lines_bng, bb_bng))
      if (nrow(sub)) {
        sub_wgs <- sf::st_transform(sub, CRS_WGS)
        proxy %>% addPolylines(data = sub_wgs, group = "Rivers",
                               weight = input$ln_w, opacity = 0.9, color = input$ln_col)
      }
    } else {
      df_view <- subset_edges_by_bbox(edges_df, bb_bng)
      if (nrow(df_view)) {
        segs_bng <- make_lines_sf_bng(df_view)
        segs_wgs <- sf::st_transform(segs_bng, CRS_WGS)
        proxy %>% addPolylines(data = segs_wgs, group = "Rivers",
                               weight = input$ln_w, opacity = 0.9, color = input$ln_col)
      }
    }
  }

  # Draw only after bounds are present (ignoreInit = TRUE avoids the NULL crash)
  observeEvent(deb_bounds(), { draw_view(deb_bounds()) }, ignoreInit = TRUE)

  # Redraw on controls
  observeEvent(list(input$pad_m, input$ln_w, input$ln_col, input$refresh), {
    if (!is.null(input$map_bounds)) draw_view(input$map_bounds)
  }, ignoreInit = TRUE)
}

shinyApp(ui, server)
