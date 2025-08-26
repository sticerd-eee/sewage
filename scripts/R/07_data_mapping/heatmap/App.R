# app.R — Choropleth Heatmap of English Subdivisions (Dry/Wet/Total + "Never" counts)
# Uses local ONS CED shapefile at:
# /Users/odran/Dropbox/sewage/data/raw/shapefiles/Counties/CED_MAY_2024_EN_BFC_V3.shp

# ---- Packages ----
suppressPackageStartupMessages({
  library(shiny)
  library(dplyr)
  library(lubridate)
  library(sf)
  library(here)
  library(fs)
  library(scales)
  library(leaflet)
  library(ggplot2)     # for PNG snapshot export
  library(readr)
  library(stringr)
  library(RColorBrewer)
})

options(shiny.launch.browser = TRUE)
`%||%` <- function(x, y) if (is.null(x)) y else x

# ---- Paths ----
dp_path       <- here::here("data", "processed")
never_dir     <- fs::path(dp_path, "never_spilled_sites")
output_path   <- here::here("output", "maps")
fs::dir_create(output_path)

data_rdata <- fs::path(dp_path, "merged_edm_1224_dry_spill_data.RData")
data_rds   <- fs::path(dp_path, "merged_edm_1224_dry_spill_data.rds")

# Helper: latest file matching pattern (returns NULL if none)
latest_csv <- function(dir, pattern) {
  if (!fs::dir_exists(dir)) return(NULL)
  f <- fs::dir_ls(dir, regexp = pattern, type = "file", fail = FALSE)
  if (length(f) == 0) return(NULL)
  f[order(fs::file_info(f)$modification_time, decreasing = TRUE)][1]
}

# Try to detect latest strict/loose CSVs automatically
strict_csv_auto <- latest_csv(never_dir, "STRICT_\\d{4}_\\d{4}\\.csv$")
loose_csv_auto  <- latest_csv(never_dir, "LOOSE_\\d{4}_\\d{4}\\.csv$")

# ---- Load data (robust; don't stop app if missing) ----
loaded <- FALSE
dry_spills_defined <- NULL

if (fs::file_exists(data_rdata) && fs::file_info(data_rdata)$size > 0) {
  load(data_rdata) # should define dry_spills_defined
  loaded <- exists("dry_spills_defined") && nrow(dry_spills_defined) > 0
} else if (fs::file_exists(data_rds) && fs::file_info(data_rds)$size > 0) {
  dry_spills_defined <- readRDS(data_rds)
  loaded <- is.data.frame(dry_spills_defined) && nrow(dry_spills_defined) > 0
}

no_data_msg <- NULL
if (!loaded) {
  dry_spills_defined <- tibble(
    site_id = character(),
    water_company = character(),
    easting = numeric(),
    northing = numeric(),
    block_start = as.POSIXct(character())
  )
  no_data_msg <- paste0(
    "Could not load spill data. Expected either:\n• ",
    data_rdata, "\n• ", data_rds,
    "\n\nCheck your working directory/project root so here::here('data','processed') resolves correctly."
  )
}

# ---- Flags & time fields ----
to_yes <- function(x) tolower(as.character(x)) == "yes"

spill_data <- dry_spills_defined %>%
  mutate(
    year    = suppressWarnings(lubridate::year(block_start)),
    month   = suppressWarnings(lubridate::month(block_start)),
    quarter = suppressWarnings(lubridate::quarter(block_start)),
    dry_d1  = to_yes(dry_day_1 %||% NA),
    dry_d2  = to_yes(dry_day_2 %||% NA),
    dry_ea  = to_yes(ea_dry_spill %||% NA),
    dry_bbc = to_yes(bbc_dry_spill %||% NA),
    any_dry = (dry_d1 %||% FALSE) | (dry_d2 %||% FALSE) | (dry_ea %||% FALSE) | (dry_bbc %||% FALSE),
    wet_spill = !any_dry,
    all_spills = TRUE
  )

# ---- Never-spilled CSVs (optional) ----
read_never_csv <- function(path) {
  if (is.null(path) || !fs::file_exists(path)) return(NULL)
  df <- suppressMessages(readr::read_csv(path, show_col_types = FALSE))
  nm <- tolower(names(df))
  if (!all(c("easting","northing") %in% nm)) return(NULL)
  df %>%
    rename_with(~tolower(.x)) %>%
    filter(!is.na(easting), !is.na(northing)) %>%
    mutate(spill_count = 0L) %>%
    distinct(easting, northing, .keep_all = TRUE)
}

never_strict_df <- read_never_csv(strict_csv_auto)
never_loose_df  <- read_never_csv(loose_csv_auto)

# ---- Geometry helpers ----
df_to_sf4326 <- function(df27700) {
  st_as_sf(df27700, coords = c("easting","northing"), crs = 27700) |>
    st_transform(4326)
}

# ---- Load polygons from your local ONS shapefile ----
load_eng_subdivisions <- function() {
  shp <- "/Users/odran/Dropbox/sewage/data/raw/shapefiles/Counties_and_Unitary_Authorities/CTYUA_DEC_2024_UK_BGC.shp"

  if (!fs::file_exists(shp)) {
    warning("Shapefile not found at: ", shp)
    return(NULL)
  }

  g <- suppressWarnings(try(sf::st_read(shp, quiet = TRUE), silent = TRUE))
  if (!inherits(g, "sf")) return(NULL)

  g <- sf::st_make_valid(g)
  g <- sf::st_transform(g, 4326)

  # Choose a sensible name column (common fields in ONS CED/County layers)
  name_candidates <- c("CED24NM","CED23NM","CED22NM","CTY24NM","CTYUA24NM","LAD24NM","NAME","Name","name")
  name_col <- {
    hits <- name_candidates[name_candidates %in% names(g)]
    if (length(hits)) hits[1] else {
      # last resort: first character/factor column
      chrs <- names(g)[vapply(g, function(x) is.character(x) || is.factor(x), logical(1))]
      if (length(chrs)) chrs[1] else names(g)[1]
    }
  }

  g$subdiv_name <- as.character(g[[name_col]])
  dplyr::select(g, subdiv_name, geometry)
}

eng_sf <- load_eng_subdivisions()

# ---- UI ----
ui <- fluidPage(
  tags$head(tags$style(HTML("
    .control-panel { background: #ffffff; padding: 10px; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,.1); }
    .statbox { display:flex; gap:18px; margin-top:8px; flex-wrap:wrap; }
    .stat { background:#f7f7f7; padding:10px 12px; border-radius:8px; }
    .notice { background:#fff3cd; border:1px solid #ffeeba; color:#856404; padding:10px; border-radius:8px; margin-bottom:10px; }
  "))),
  titlePanel("Dry / Wet / Total — English Subdivision Heatmap"),
  sidebarLayout(
    sidebarPanel(
      if (!is.null(no_data_msg))
        div(class = "notice", pre(no_data_msg)),
      if (is.null(eng_sf))
        div(class = "notice", "Could not load polygons; check the CED shapefile path in load_eng_subdivisions()."),
      div(class="control-panel",
          selectInput("spill_flag", "Spill type",
                      choices = c("Dry Day 1"             = "dry_d1",
                                  "Dry Day 2"             = "dry_d2",
                                  "EA Dry Spill"          = "dry_ea",
                                  "BBC Dry Spill"         = "dry_bbc",
                                  "Wet spills"            = "wet_spill",
                                  "Total spills"          = "all_spills",
                                  "Never Spilled (Loose)" = "never_loose",
                                  "Never Spilled (Strict)"= "never_strict"),
                      selected = "dry_d1"),
          uiOutput("year_ui"),
          uiOutput("company_ui"),
          checkboxInput("rate_mode", "Show per-site rate (events per unique site) where applicable", FALSE),
          hr(),
          downloadButton("dl_gpkg", "Download Polygons (GPKG)"),
          downloadButton("dl_png",  "Download Map Snapshot (PNG)")
      ),
      width = 4
    ),
    mainPanel(
      leafletOutput("map", height = 700),
      div(class="statbox",
          div(class="stat", strong("Areas displayed: "), textOutput("area_n", inline = TRUE)),
          div(class="stat", strong("Total events: "), textOutput("event_sum", inline = TRUE)),
          div(class="stat", strong("Metric shown: "), textOutput("metric_label", inline = TRUE))
      ),
      width = 8
    )
  )
)

# ---- Server ----
server <- function(input, output, session) {

  # Year control fixed to 2021–2024
  output$year_ui <- renderUI({
    sliderInput(
      "year_range", "Year range",
      min = 2021, max = 2024, value = c(2021, 2024), step = 1, sep = ""
    )
  })

  output$company_ui <- renderUI({
    comps <- sort(unique(spill_data$water_company))
    selectizeInput("companies", "Water company (optional filter)",
                   choices = comps, multiple = TRUE, options = list(placeholder = "All companies"))
  })

  # Row-level filters (events table)
  filtered_rows <- reactive({
    d <- spill_data
    if (!is.null(input$year_range)) {
      d <- d %>% filter(dplyr::between(year, input$year_range[1], input$year_range[2]))
    }
    if (!is.null(input$companies) && length(input$companies) > 0) {
      d <- d %>% filter(water_company %in% input$companies)
    }
    d
  })

  # Points for chosen flag (including never-data branches)
  flag_points_sf <- reactive({
    req(input$spill_flag)
    flag <- input$spill_flag

    if (flag %in% c("never_loose","never_strict")) {
      df <- switch(flag, never_loose = never_loose_df, never_strict = never_strict_df)
      if (is.null(df) || nrow(df) == 0) {
        return(st_as_sf(tibble(easting=numeric(), northing=numeric()),
                        coords=c("easting","northing"), crs=27700) %>% st_transform(4326))
      }
      if (!is.null(input$companies) && length(input$companies) > 0 && "water_company" %in% names(df)) {
        df <- df %>% filter(water_company %in% input$companies)
      }
      s <- df_to_sf4326(df)
      if (!"site_id" %in% names(s)) s$site_id <- NA_character_
      s$spill_count <- 0L
      return(s)
    }

    d <- filtered_rows() %>%
      filter(!is.na(easting), !is.na(northing))
    if (nrow(d) == 0) {
      return(st_as_sf(tibble(easting=numeric(), northing=numeric()),
                      coords=c("easting","northing"), crs=27700) %>% st_transform(4326))
    }
    # aggregate at site-event level for spill flag
    d <- d %>%
      mutate(flag_val = .data[[flag]] %||% FALSE) %>%
      filter(flag_val) %>%
      group_by(site_id, water_company, easting, northing) %>%
      summarise(spill_count = n(), .groups = "drop")
    if (nrow(d) == 0) {
      return(st_as_sf(tibble(easting=numeric(), northing=numeric()),
                      coords=c("easting","northing"), crs=27700) %>% st_transform(4326))
    }
    st_as_sf(d, coords=c("easting","northing"), crs=27700) %>% st_transform(4326)
  })

  # Aggregate to English subdivisions
  agg_subdivisions <- reactive({
    if (is.null(eng_sf)) return(NULL)
    pts <- flag_points_sf()
    if (nrow(pts) == 0) {
      return(eng_sf %>% mutate(value = NA_real_, sites = 0L, events = 0L))
    }
    # join points to polygons
    joined <- st_join(pts, eng_sf, join = st_within, left = FALSE)
    if (nrow(joined) == 0) {
      return(eng_sf %>% mutate(value = NA_real_, sites = 0L, events = 0L))
    }
    # metrics: sites = unique site_id; events = sum(spill_count)
    out <- joined %>%
      st_drop_geometry() %>%
      group_by(subdiv_name) %>%
      summarise(
        sites  = n_distinct(site_id %||% row_number()),
        events = sum(spill_count %||% 0L, na.rm = TRUE),
        .groups = "drop"
      )
    # per-site rate toggle (never_* will be 0 events)
    out <- out %>%
      mutate(value = if (isTRUE(input$rate_mode) & !input$spill_flag %in% c("never_loose","never_strict")) {
        ifelse(sites > 0, events / sites, NA_real_)
      } else {
        if (input$spill_flag %in% c("never_loose","never_strict")) sites else events
      })

    eng_sf %>% left_join(out, by = "subdiv_name")
  })

  # Stats
  output$area_n <- renderText({
    a <- agg_subdivisions()
    if (is.null(a)) "0" else sum(!is.na(a$value))
  })
  output$event_sum <- renderText({
    a <- agg_subdivisions()
    if (is.null(a)) "0" else {
      if (input$spill_flag %in% c("never_loose","never_strict")) {
        comma(sum(a$sites %||% 0L, na.rm = TRUE))
      } else {
        comma(sum(a$events %||% 0L, na.rm = TRUE))
      }
    }
  })
  output$metric_label <- renderText({
    if (input$spill_flag %in% c("never_loose","never_strict")) "Count of never-spilled sites"
    else if (isTRUE(input$rate_mode)) "Events per unique site"
    else "Total spill events"
  })

  # Map
  output$map <- renderLeaflet({
    leaflet(options = leafletOptions(minZoom = 5, zoomControl = TRUE)) %>%
      addProviderTiles(providers$CartoDB.Positron) %>%
      setView(lng = -1.5, lat = 52.8, zoom = 6)
  })

  observe({
    a <- agg_subdivisions()
    if (is.null(a)) {
      leafletProxy("map") %>% clearShapes() %>% clearControls()
      return(invisible())
    }

    # palette + legend title
    vals <- a$value
    if (all(is.na(vals))) {
      leafletProxy("map") %>% clearShapes() %>% clearControls()
      return(invisible())
    }

    # choose palette domain (avoid single-valued domain issues)
    rng <- range(vals, na.rm = TRUE)
    if (!is.finite(rng[1]) || !is.finite(rng[2]) || diff(rng) == 0) {
      rng <- c(rng[1] - 0.5, rng[2] + 0.5)  # tiny epsilon
    }
    pal <- colorNumeric("YlOrRd", domain = rng, na.color = "#EEEEEE")

    leg_title <- if (input$spill_flag %in% c("never_loose","never_strict")) {
      "Never-spilled sites"
    } else if (isTRUE(input$rate_mode)) "Events per site" else "Spill events"

    leafletProxy("map") %>%
      clearShapes() %>% clearControls() %>%
      addPolygons(
        data = a,
        fillColor = ~pal(value),
        weight = 0.7, color = "#666", fillOpacity = 0.8,
        label = ~htmltools::HTML(sprintf(
          "<b>%s</b><br>%s: %s<br>Sites: %s<br>Events: %s",
          subdiv_name,
          leg_title, ifelse(is.na(value), "NA", scales::comma(value, accuracy = 0.01)),
          ifelse(is.na(sites), "0", scales::comma(sites)),
          ifelse(is.na(events), "0", scales::comma(events)
        ))),
        highlightOptions = highlightOptions(weight = 2, color = "#000", fillOpacity = 0.9, bringToFront = TRUE),
        group = "areas"
      ) %>%
      addLegend("bottomright", pal = pal, values = vals, title = leg_title, opacity = 1)
  })

  # ---- Downloads ----
  output$dl_gpkg <- downloadHandler(
    filename = function() {
      paste0("eng_heatmap_", input$spill_flag, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".gpkg")
    },
    content = function(file) {
      a <- agg_subdivisions()
      if (is.null(a)) {
        st_write(st_sf(subdiv_name=character(), geometry = st_sfc(crs = 4326), value = numeric()), file, quiet = TRUE)
      } else {
        tmp <- file.path(tempdir(), "eng_heatmap_tmp.gpkg")
        if (fs::file_exists(tmp)) fs::file_delete(tmp)
        st_write(a, tmp, quiet = TRUE, delete_dsn = TRUE)
        fs::file_copy(tmp, file, overwrite = TRUE)
      }
    }
  )

  output$dl_png <- downloadHandler(
    filename = function() {
      paste0("eng_heatmap_", input$spill_flag, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")
    },
    content = function(file) {
      a <- agg_subdivisions()
      png(file, width = 2000, height = 1600, res = 200)
      if (is.null(a) || all(is.na(a$value))) {
        plot.new(); text(0.5, 0.5, "No data for current filters"); dev.off(); return()
      }
      title_txt <- switch(input$spill_flag,
                          "dry_d1"       = "Dry Spills – Dry Day 1",
                          "dry_d2"       = "Dry Spills – Dry Day 2",
                          "dry_ea"       = "Dry Spills – EA Dry Spill",
                          "dry_bbc"      = "Dry Spills – BBC Dry Spill",
                          "wet_spill"    = "Wet Spills",
                          "all_spills"   = "Total Spills",
                          "never_loose"  = "Never Spilled (Loose)",
                          "never_strict" = "Never Spilled (Strict)")
      leg_title <- if (input$spill_flag %in% c("never_loose","never_strict")) {
        "Never-spilled sites"
      } else if (isTRUE(input$rate_mode)) "Events per site" else "Spill events"

      p <- ggplot(a) +
        geom_sf(aes(fill = value), color = "grey40", linewidth = 0.2) +
        scale_fill_gradientn(colors = RColorBrewer::brewer.pal(9,"YlOrRd"),
                             name = leg_title, na.value = "grey90") +
        labs(title = paste0("English Subdivision Heatmap — ", title_txt),
             subtitle = if (isTRUE(input$rate_mode)) "Metric: events per unique site" else "Metric: total events",
             caption  = "Source: merged EDM spill dataset / Never-spilled exports / ONS CED boundaries (May 2024)") +
        theme_minimal(base_size = 11) +
        theme(legend.position = "right")
      print(p)
      dev.off()
    }
  )
}

# ---- Run ----
shinyApp(ui, server)
