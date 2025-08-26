# app.R — Interactive Dry/Wet Spills Map (+ never-spilled toggles)

# ---- Packages ----
suppressPackageStartupMessages({
  library(shiny)
  library(dplyr)
  library(lubridate)
  library(sf)
  library(here)
  library(fs)
  library(rnaturalearth)
  library(rnaturalearthdata)
  library(scales)
  library(leaflet)
  library(ggplot2)     # only for PNG snapshot export
  library(readr)
  library(stringr)
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

# ---- Add time fields & wet/dry flags ----
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

# ---- Helper: aggregate by site for a given flag ----
agg_sites <- function(data, flag) {
  data %>%
    filter(!is.na(easting), !is.na(northing)) %>%
    group_by(site_id, water_company, easting, northing) %>%
    summarise(spill_count = sum(.data[[flag]], na.rm = TRUE), .groups = "drop") %>%
    filter(spill_count > 0)
}

# ---- Load never-spilled CSVs (auto, optional) ----
read_never_csv <- function(path) {
  if (is.null(path) || !fs::file_exists(path)) return(NULL)
  df <- suppressMessages(readr::read_csv(path, show_col_types = FALSE))
  nm <- tolower(names(df))
  if (!all(c("easting","northing") %in% nm)) return(NULL)
  df %>%
    rename_with(~tolower(.x)) %>%
    filter(!is.na(easting), !is.na(northing)) %>%
    mutate(spill_count = 0L) %>%  # for consistent interface
    distinct(easting, northing, .keep_all = TRUE)
}

never_strict_df <- read_never_csv(strict_csv_auto)
never_loose_df  <- read_never_csv(loose_csv_auto)

# ---- Basemap (UK + Ireland). If it fails, we still render a base map. ----
uk_sf <- tryCatch(
  {
    rnaturalearth::ne_countries(scale = "medium", returnclass = "sf") %>%
      dplyr::filter(admin %in% c("United Kingdom", "Ireland")) %>%
      st_transform(4326)
  },
  error = function(e) NULL
)

# Convert tabular easting/northing to sf in WGS84
df_to_sf4326 <- function(df27700) {
  st_as_sf(df27700, coords = c("easting","northing"), crs = 27700) |>
    st_transform(4326)
}

# ---- UI ----
ui <- fluidPage(
  tags$head(tags$style(HTML("
    .control-panel { background: #ffffff; padding: 10px; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,.1); }
    .statbox { display:flex; gap:18px; margin-top:8px; }
    .stat { background:#f7f7f7; padding:10px 12px; border-radius:8px; }
    .notice { background:#fff3cd; border:1px solid #ffeeba; color:#856404; padding:10px; border-radius:8px; margin-bottom:10px; }
  "))),
  titlePanel("Dry, Wet & Total Spills Map"),
  sidebarLayout(
    sidebarPanel(
      if (!is.null(no_data_msg))
        div(class = "notice", pre(no_data_msg)),
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
          sliderInput("point_size", "Point size range", min = 2, max = 14, value = c(3, 10)),
          checkboxInput("label_points", "Show labels on hover", value = TRUE),
          hr(),
          downloadButton("dl_gpkg", "Download Sites (GPKG)"),
          downloadButton("dl_png",  "Download Map Snapshot (PNG)")
      )
    ),
    mainPanel(
      leafletOutput("map", height = 650),
      div(class="statbox",
          div(class="stat", strong("Sites displayed: "), textOutput("site_n", inline = TRUE)),
          div(class="stat", strong("Total events: "), textOutput("event_sum", inline = TRUE))
      )
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

  # Reactive filtered data (row-level; not used for never-spilled selections)
  filtered <- reactive({
    req(input$spill_flag)
    d <- spill_data
    if (!is.null(input$year_range)) {
      d <- d %>% filter(between(year, input$year_range[1], input$year_range[2]))
    }
    if (!is.null(input$companies) && length(input$companies) > 0) {
      d <- d %>% filter(water_company %in% input$companies)
    }
    d
  })

  # Reactive sf for plotting
  site_sf <- reactive({
    flag <- req(input$spill_flag)

    # Branch: never-spilled selections read from CSVs
    if (flag %in% c("never_loose","never_strict")) {
      df <- switch(flag,
                   never_loose  = never_loose_df,
                   never_strict = never_strict_df)
      if (is.null(df) || nrow(df) == 0) {
        return(st_as_sf(tibble(easting=numeric(), northing=numeric()),
                        coords = c("easting","northing"), crs = 27700) %>% st_transform(4326))
      }
      # Optional filter by company if column exists
      if (!is.null(input$companies) && length(input$companies) > 0 && "water_company" %in% names(df)) {
        df <- df %>% filter(water_company %in% input$companies)
      }
      s <- df_to_sf4326(df)
      if (!"water_company" %in% names(df)) s$water_company <- NA_character_
      s$spill_count <- 0L
      return(s)
    }

    # Normal event-based aggregation
    dat <- agg_sites(filtered(), flag)
    if (nrow(dat) == 0) {
      return(st_as_sf(dat, coords = c("easting","northing"), crs = 27700) %>% st_transform(4326))
    }
    st_as_sf(dat, coords = c("easting","northing"), crs = 27700) %>% st_transform(4326)
  })

  # Stats
  output$site_n <- renderText({ nrow(site_sf()) })
  output$event_sum <- renderText({
    if (input$spill_flag %in% c("never_loose","never_strict")) {
      "0"
    } else {
      s <- site_sf()
      if (nrow(s) == 0 || is.null(s$spill_count)) "0" else comma(sum(s$spill_count, na.rm = TRUE))
    }
  })

  # Leaflet map container
  output$map <- renderLeaflet({
    base <- leaflet(options = leafletOptions(minZoom = 4, zoomControl = TRUE)) %>%
      addProviderTiles(providers$CartoDB.Positron) %>%
      setView(lng = -2, lat = 54.5, zoom = 5)

    if (inherits(uk_sf, "sf")) {
      base %>% addPolygons(data = uk_sf, weight = 1, color = "#666", fillColor = "#f2f2f2", fillOpacity = 0.7)
    } else {
      base
    }
  })

  # ---- Map drawing (no clustering) ----
  observe({
    s <- site_sf()
    if (is.null(s) || nrow(s) == 0) {
      leafletProxy("map") %>% clearGroup("sites") %>% clearControls()
      return(invisible())
    }

    # Start with a clean layer/control state
    lp <- leafletProxy("map") %>% clearGroup("sites") %>% clearControls()

    # Style: for never-spilled, constant size & no legend; otherwise size/color by spill_count
    is_never <- input$spill_flag %in% c("never_loose","never_strict")

    if (!is_never) {
      counts <- s$spill_count
      counts[!is.finite(counts)] <- 0
      pal <- colorNumeric("Blues", domain = counts)

      rng <- range(counts %||% 0, na.rm = TRUE)
      scale_radius <- function(x) {
        if (!length(x)) return(numeric(0))
        minr <- input$point_size[1]; maxr <- input$point_size[2]
        if (!is.finite(diff(rng)) || diff(rng) == 0) return(rep((minr + maxr) / 2, length(x)))
        minr + (x - rng[1]) / diff(rng) * (maxr - minr)
      }
      radius <- scale_radius(counts)

      lp <- lp %>%
        addCircleMarkers(
          data = s,
          radius = radius, stroke = TRUE, weight = 1, color = "#2b6cb0",
          fillOpacity = 0.7, fillColor = pal(counts),
          group = "sites",
          label = if (isTRUE(input$label_points))
            ~sprintf("%s<br>Spill events: %s", water_company %||% "", scales::comma(spill_count)) %>% lapply(htmltools::HTML)
          else NULL
        ) %>%
        addLegend("bottomright", pal = pal, values = counts,
                  title = "Spill events", opacity = 1)

    } else {
      # Never-spilled: fixed radius, single color, no legend
      radius <- mean(input$point_size)

      lp <- lp %>%
        addCircleMarkers(
          data = s,
          radius = radius, stroke = TRUE, weight = 1, color = "#2b6cb0",
          fillOpacity = 0.7, fillColor = "#6baed6",
          group = "sites",
          label = if (isTRUE(input$label_points))
            ~sprintf("%s<br>Never-spilled site", water_company %||% "") %>% lapply(htmltools::HTML)
          else NULL
        )
    }
  })

  # ---- Downloads ----
  disable_if_no_data <- function(expr) if (nrow(spill_data) == 0) NULL else expr

  # GPKG of current sites
  output$dl_gpkg <- disable_if_no_data(downloadHandler(
    filename = function() {
      paste0("sites_", input$spill_flag, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".gpkg")
    },
    content = function(file) {
      s <- site_sf()
      tmp <- file.path(tempdir(), "sites_temp.gpkg")
      if (fs::file_exists(tmp)) fs::file_delete(tmp)
      st_write(s, tmp, quiet = TRUE, delete_dsn = TRUE)
      fs::file_copy(tmp, file, overwrite = TRUE)
    }
  ))

  # PNG snapshot
  output$dl_png <- disable_if_no_data(downloadHandler(
    filename = function() {
      paste0("map_", input$spill_flag, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")
    },
    content = function(file) {
      s <- site_sf()
      if (nrow(s) == 0) {
        png(file, width = 2000, height = 1600, res = 200)
        plot.new(); text(0.5, 0.5, "No data for current filters")
        dev.off()
        return()
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
      p <- ggplot() +
        {
          if (inherits(uk_sf, "sf"))
            geom_sf(data = uk_sf, fill = "grey95", linewidth = 0.2, color = "grey60")
          else NULL
        } +
        geom_sf(data = s, aes(size = if (!input$spill_flag %in% c("never_loose","never_strict")) spill_count else NULL),
                alpha = 0.8, color = "#2b6cb0") +
        scale_size_continuous(range = c(1.6, 6), labels = comma) +
        guides(size = guide_legend(title = if (!input$spill_flag %in% c("never_loose","never_strict")) "Spill events" else NULL)) +
        labs(
          title = title_txt,
          subtitle = if (!input$spill_flag %in% c("never_loose","never_strict")) "Point size = number of events" else NULL,
          caption  = "Source: merged EDM spill dataset / Never-spilled exports"
        ) +
        theme_minimal(base_size = 11)
      ggsave(file, plot = p, width = 9, height = 9, dpi = 300)
    }
  ))
}

# ---- Run ----
shinyApp(ui, server)
