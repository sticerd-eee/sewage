#A Shiny App that plots rainfall and spills (w/dry spill classification)

#load libraries
library(leaflet)
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(sf)
library(ncdf4)
library(raster)
library(shiny)

#setting dropbox path - EDIT TO PATH TO DATA FOLDER
dp_path <- "C:/Users/danan/Dropbox/sewage/data/"

#load in spill data outside of server
load(paste0(dp_path, "processed/merged_edm_1224_dry_spill_data.RData"))

#prep data for mapping:
dry_spills_defined<-dry_spills_defined%>%
  #changing to factor levels
  mutate(dry_day_1 = as.factor(dry_day_1),
         dry_day_2 = as.factor(dry_day_2),
         ea_dry_spill = as.factor(ea_dry_spill),
         bbc_dry_spill = as.factor(bbc_dry_spill)) %>%
  #adding startdate for filtering
  mutate(startdate = as.Date(
    substr(block_start, 1, 10)))


#Declare as SF object
spills_sf<-st_as_sf(dry_spills_defined, coords = c("easting", "northing"), crs = 27700)

#convert to WGS84 for Leaflet compatability 
spills_sf <- st_transform(spills_sf, 4326)

rm(dry_spills_defined)


#Color palette: red for dry spills, black for wet spills
#light yellow for undefined -- handled in the plotting function itself
spill_color<-colorFactor(
  palette = c("black", "red"),
  domain = c("no", "yes")
)



#(code is disabled here - skip for convenience)
#load in full rainfall array:
#load(paste0(dp_path, "/processed/rain_array.RData"))


#get x and y bounds from one NetCDF file:
#nc_filepath<-paste0(dp_path, "raw/haduk_rainfall_data/rainfall_2021_01.nc")

#jan_21_nc<-nc_open(nc_filepath)
#x_bnds<-ncvar_get(jan_21_nc, "projection_x_coordinate_bnds")
#y_bnds<-ncvar_get(jan_21_nc, "projection_y_coordinate_bnds")

#nc_close(jan_21_nc)

#rm(nc_filepath, jan_21_nc, dry_spills_defined2)


#making our rain array a rasterstack() (may be very expensive computationally)
#save as RData afterwards if possible - will be large


make_stack <- function(array_data, x_bound_matrix, y_bound_matrix) {
  #initialise counter for progress
  counter <- 0
  
  #make empty raster stack
  raster_stack <- stack()
  
  for (t in 1:dim(array_data)[3]) {
    r <- raster(array_data[,,t])  
    
    extent(r) <- c(min(x_bound_matrix), max(x_bound_matrix), min(y_bound_matrix), max(y_bound_matrix))
    crs(r) <- CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +datum=OSGB36 +units=m +no_defs")
    
    r <- t(r)  
    r <- flip(r, direction = "y")  
    
    extent(r) <- c(-200000, 700000, -200000, 1250000)  
    
    
    raster_stack <- addLayer(raster_stack, r)
    
    #track progress with counter
    counter <- counter + 1
    
    if (counter %% 20 == 0) {
      cat(" Processed:", counter, "\n")
    }
  }
  
  return(raster_stack)
}


#raster_stack_1<-make_stack(rain, x_bnds, y_bnds)

#save(raster_stack_1, file = paste0(dp_path, "/processed/rainfall_raster_stack_for_mapping.RData"))


#Load in raster stack for rainfall at this point
load(paste0(dp_path, "processed/rainfall_raster_stack_for_mapping.RData"))

raster_stack <- raster_stack_1

rm(raster_stack_1)


#rain color palette:
blue_palette <- colorBin(
  palette = c("white", "lightgray", "#D6EAF8", "#AED6F1", "#5DADE2", "#154360"),
  bins = c(0, 0.25, 1, 10, 30, Inf),  
  na.color = "transparent"
)
# >0.25 is a wet spill.
# rest are related to Bhutan's Weather & Climate Service Division (easily accessible information)
#can be updated


#defining the length of each month (non leap years)
month_days <-list(
  "01" = 1:31, "02" = 1:28, "03" = 1:31, "04" = 1:30,
  "05" = 1:31, "06" = 1:30, "07" = 1:31, "08" = 1:31,
  "09" = 1:30, "10" = 1:31, "11" = 1:30, "12" = 1:31
)



ui<-fluidPage(
  titlePanel("Spills and Rainfall"),
  
  div(
    style = "background-color: #f8f9fa; padding: 10px; border-radius: 10px; margin-bottom: 10px;",
    tags$p(
      "This app allows users to select any month within our 
      data timeframe (2021 to 2023 incl.) and observe both the
      daily rainfall and when sewage overflow sites are spilling.
      Press the start button on the select day slider to animate
      through any chosen month. Click on any spill sites that appear
      to get more information - such as the WaSC and site name (recieving
      waterbody will be added). Dry spills (BBC methodology) are plotted in red,
      wet spills in black. Rain in blue is rain >0.25mm on that day" ,
      style = "font-size: 12px; color: #555;"
    )
  ),
  
  
  selectInput("month_chosen", "Choose Month:",
              list("2021" = list("01-2021", "02-2021", "03-2021", "04-2021", "05-2021", "06-2021",
                                 "07-2021", "08-2021", "09-2021", "10-2021", "11-2021", "12-2021"),
                   "2022" = list("01-2022", "02-2022", "03-2022", "04-2022", "05-2022", "06-2022",
                                 "07-2022", "08-2022", "09-2022", "10-2022", "11-2022", "12-2022"),
                   "2023" = list("01-2023", "02-2023", "03-2023", "04-2023", "05-2023", "06-2023",
                                 "07-2023", "08-2023", "09-2023", "10-2023", "11-2023", "12-2023"),
                   #adding a default value as January 2021
                   selected = "01-2021")),
  
  #text outputs are for checking the reactive expressions behaviour (can remove)
  
  
  uiOutput("date_slider"),
  #(works)
  
  textOutput("full_date"),
  #(works)
  
  
  
  textOutput("raster_test1"),
  #(works - shows existence for January not February as we'd expect)
  
  leafletOutput("map", height = "600px")
  
)



server <- function(input, output, session) {
  
  #takes only the month section of our selected date.
  selected_month <-reactive({
    substr(input$month_chosen, 1, 2)
  })
  
  
  output$date_slider <- renderUI({
    
    if (is.null(selected_month()) || !(selected_month() %in% names(month_days))) {
      return(NULL)  
    }
    #only shows up once we've selected a month on the drop-down
    
    
    days_of_month <- month_days[[selected_month()]]
    
    if (length(days_of_month) == 0 || is.null(days_of_month)) {
      return(NULL)  
    }
    #simple edge case handling to prevent crashes
    
    
    sliderInput("selected_day", "Select Day in Month", #would be ideal to update this title dynamically too
                min = 1, max = max(days_of_month),
                value = 1, step = 1, animate = animationOptions(interval = 3500, loop = F)
    )
    
  })
  
  
  
  selected_date <- reactive({
    date_string <- paste(input$selected_day, input$month_chosen, sep = "-")
    date_string <- ifelse(str_length(date_string) == 9, paste0("0", date_string), date_string)
    as.Date(date_string, format = "%d-%m-%Y")
  })
  
  output$full_date <- renderText(as.character(selected_date()))
  
  #filter spills data for the date selected
  spills_filtered <- reactive({
    spills_sf %>%
      filter(startdate == selected_date())
  })
  
  
  z <- reactive({
    
    as.numeric(difftime(selected_date(), as.Date("2020-12-01"))) + 1
    
  })
  
  
  
  #get raster layer for the day selected
  r_day <-reactive({
    # prevent this from runnning before an input is selected
    if (is.null(z()) || is.na(z()) || z() < 1 || z() > nlayers(raster_stack)) {
      return(NULL)
    }
    
    raster_stack[[z()]]
    
  })
  
  #troubleshooting (can remove)
  output$raster_test1 <- renderText({
    if (is.null(r_day())) {
      "r_day raster is NULL"
    } else {
      paste("raster exists for z =", z())
    }
  })
  
  #base map
  output$map <- renderLeaflet({
    leaflet(options = leafletOptions(minZoom = 5, maxZoom = 10)) %>%
      addProviderTiles("CartoDB.Positron") %>%
      
      setView(lng = -2, lat = 54, zoom = 6) %>%
      
      #add legends for our palettes
      addLegend("bottomright", 
                pal = blue_palette, 
                values = c(0, 30), 
                title = "Rainfall (mm)", 
                opacity = 1) %>%
      
      
      addLegend("bottomright", 
                colors = c("red", "black", "lightyellow"),
                labels = c("Wet Spill", "Dry Spill", "Undefined"), 
                title = "Spill Type", 
                opacity = 1)
  })
  
  #add in rainfall data
  observe({
    if (!is.null(r_day())) {
      leafletProxy("map") %>%
        clearImages() %>% 
        addRasterImage(
          r_day(), colors = blue_palette,
          opacity = 0.7
        )
    }
  })
  
  #add in spill data
  observe({
    leafletProxy("map")%>%
      clearMarkers() %>%
      addCircleMarkers(
        data = spills_filtered(),
        color = ~ ifelse(is.na(bbc_dry_spill), "lightyellow",spill_color(bbc_dry_spill)),
        fillOpacity = 1,
        radius = 1.5,
        #add a POP up for information when you click - wasc, Name + recieving water would be useful! also start time would be useful
        popup = ~paste0(
          "<b>WASC:</b> ", water_company, "<br>",
          "<b>Name:</b> ", site_name_ea, "<br>",
          #"<b>Receiving Water:</b> ", RECEIVING_WATER, "<br>",
          "<b>Start Time:</b> ", block_start
        )
        
      )
  })
  
}




shinyApp(ui, server)

