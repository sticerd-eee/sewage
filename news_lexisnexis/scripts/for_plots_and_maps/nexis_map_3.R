#map of dry spills with news coverage - but limited matches to places included within headlines

library(leaflet)
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(sf)
library(pbapply)
library(rnrfa)
library(data.table)
library(shiny)


#setting dropbox path to processed data
processed <- "C:/Users/danan/Dropbox/news_lexisnexis/data/processed"



#local alternative:
#dp_path <- "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/dp_backup/"


##Loading and preparing dry spill data

#load in spill data outside of server
load(paste0(processed, "/dry_spills_defined_with_eastnorth.RData"))




dry_spills_defined2<-dry_spills_defined2%>%
  #changing to factor levels
  mutate(dry_spill_1 = as.factor(dry_spill_1),
         dry_spill_2 = as.factor(dry_spill_2),
         dry_spill_3 = as.factor(dry_spill_3),
         dry_spill_4 = as.factor(dry_spill_4)) %>%
  #adding startdate for filtering
  mutate(startdate = as.Date(
    substr(block_start, 1, 10)))


#Declare as SF object
spills_sf<-st_as_sf(dry_spills_defined2, coords = c("easting", "northing"), crs = 27700)

#convert to WGS84 for Leaflet compatability 
spills_sf <- st_transform(spills_sf, 4326)


#Color palette: red for dry spills, black for wet spills
#light yellow for undefined -- handled in the plotting function itself
spill_color<-colorFactor(
  palette = c("black", "red"),
  domain = c("no", "yes")
)




##Loading and preparing news data


load(paste0(processed, "/searches_89_matched_final.RData"))




news_89$article_id <- seq_len(nrow(news_89))

ipn_head_places <- rbindlist(
  lapply(seq_len(nrow(news_89)), function(i) {
    df <- news_89$ipn_matches_heading[[i]]
    if (is.null(df) || nrow(df) == 0) return(NULL)
    df$article_id   <- news_89$article_id[i]
    df$heading      <- news_89$heading[i]
    df$article      <- news_89$article[i]
    df$date         <- news_89$date[i]
    df$source       <- news_89$source[i]
    df$search_term  <- news_89$search_term[i]
    df
  }),
  fill = TRUE
)
ipn_head_places<-ipn_head_places%>%
  filter(str_length(grid1km) != 0)
#2145



results <- pblapply(ipn_head_places$grid1km, function(x) {
  parsed <- osg_parse(x)
  return(c(as.numeric(parsed[1]), as.numeric(parsed[2])))
})


eastnorth_df <- do.call(rbind, results)
colnames(eastnorth_df) <- c("easting", "northing")

ipn_head_places <- cbind(ipn_head_places, eastnorth_df)

#make sf object
ipn_sf <- st_as_sf(ipn_head_places, coords = c("easting", "northing"), crs = 27700)
ipn_sf <- st_transform(ipn_sf, 4326)





#map






##defining the length of each month (non leap years)
month_days <-list(
  "01" = 1:31, "02" = 1:28, "03" = 1:31, "04" = 1:30,
  "05" = 1:31, "06" = 1:30, "07" = 1:31, "08" = 1:31,
  "09" = 1:30, "10" = 1:31, "11" = 1:30, "12" = 1:31
)



#Shiny app:


ui<-fluidPage(
  titlePanel("Spills and News Coverage"),
  
  div(
    style = "background-color: #f8f9fa; padding: 10px; border-radius: 10px; margin-bottom: 10px;",
    tags$p(
      "This app allows users to select any month within our 
      spill data timeframe (2021 to 2023 incl.) and observe both
      when sewage overflow sites are spilling and related news
      coverage - matched to Index of Place Names within headlines.
      Press the start button on the select day slider to animate
      through any chosen month. Click on any spill sites that appear
      to get more information - such as the WaSC, site name and recieving
      waterbody. Dry spills (BBC methodology) are plotted in red,
      wet spills in black" ,
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
  
  
  
  
  
  
  #filter news data for the date selected
  
  news_filtered <- reactive({
    ipn_sf %>%
      filter(date == selected_date())
  })
  
  
  
  
  
  
  #base map
  output$map <- renderLeaflet({
    leaflet(options = leafletOptions(minZoom = 5, maxZoom = 10)) %>%
      addProviderTiles("CartoDB.Positron") %>%
      
      setView(lng = -2, lat = 54, zoom = 6) %>%
      
      #add legends for our palettes
      addLegend("bottomright", 
                colors = c("purple"),
                labels = c("Article"),
                title = "News", 
                opacity = 1) %>%
      
      
      addLegend("bottomright", 
                colors = c("red", "black", "lightyellow"),
                labels = c("Wet Spill", "Dry Spill", "Undefined"), 
                title = "Spill Type", 
                opacity = 1)
  })
  
  
  #add in spill data
  observe({
    leafletProxy("map")%>%
      clearMarkers() %>%
      addCircleMarkers(
        data = spills_filtered(),
        color = ~ ifelse(is.na(dry_spill_4), "lightyellow",spill_color(dry_spill_4)),
        fillOpacity = 1,
        radius = 1.5,
        #add a POP up for information when you click - wasc, Name + recieving water would be useful! also start time would be useful
        popup = ~paste0(
          "<b>WASC:</b> ", wasc, "<br>",
          "<b>Name:</b> ", site, "<br>",
          "<b>Receiving Water:</b> ", RECEIVING_WATER, "<br>",
          "<b>Start Time:</b> ", block_start
        )
        
      )
    
    
    
    
  })
  
  
  observe({
    leafletProxy("map") %>%
      addAwesomeMarkers(
        data = news_filtered(),
        icon = makeAwesomeIcon(
          icon = "stop",           # square shape
          library = "fa",
          markerColor = "purple"
        ),
        popup = ~paste0(
          "<b>Headline:</b> ", heading, "<br>",
          "<b>Source:</b> ", source, "<br>",
          "<b>Date:</b> ", date, "<br>",
          "<b>Place Matched:</b> ", place23nm
        )
      )
  })
  
  
}




shinyApp(ui, server)

