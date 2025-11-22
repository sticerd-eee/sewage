#coverage map workings out

#load libraries

library(data.table)
library(leaflet)
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(sf)
library(shiny)
library(pbapply)
library(rnrfa)


#load for later
load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/LexisNexis 2025 rerun/processed/ner_match_news_searches_8_9.RData")


#Q1: how to unnest our news data (duplicate rows for each match)?

news_89$article_id <- seq_len(nrow(news_89))

ipn_places <- rbindlist(
  lapply(seq_len(nrow(news_89)), function(i) {
    df <- news_89$ipn_matches[[i]]
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


#Q2: how to set as a spatial object, for leaflet?


#can use lat / lon as given?

#or let's reuse our get eastings function from previous scripts
#and apply to our BNG code

#remove empty grid references
ipn_places<-ipn_places%>%
  filter(str_length(grid1km) != 0)


results <- pblapply(ipn_places$grid1km, function(x) {
  parsed <- osg_parse(x)
  return(c(as.numeric(parsed[1]), as.numeric(parsed[2])))
})


eastnorth_df <- do.call(rbind, results)
colnames(eastnorth_df) <- c("easting", "northing")

ipn_places <- cbind(ipn_places, eastnorth_df)




#Q3: how to plot a single date on leaflet?

#just choosing 4th Jul 2023:



jul_ipn<-ipn_places%>%
  filter(date == as.Date("2023-07-04"))


jul_sf <- st_as_sf(jul_ipn, coords = c("easting", "northing"), crs = 27700)
jul_sf <- st_transform(jul_sf, 4326)



leaflet(jul_sf) %>%
  addTiles() %>%
  addCircleMarkers(
    radius = 5,
    stroke = FALSE,
    fillOpacity = 0.8,
    popup = ~paste0(
      "<b>Article:</b> ", heading, "<br>",
      "<b>Date:</b> ", date, "<br>",
      "<b>Source:</b> ", source, "<br>",
      "<b>Place:</b> ", place23nm, "<br>",
      "<b>Health region:</b> ", hlth23nm, "<br>",
      "<b>Historic county:</b> ", ctyhistnm
    )
  )



#Q4: how to customise this plot to how we want?

#square markers:

leaflet(jul_sf) %>%
  addTiles() %>%
  addAwesomeMarkers(
    icon = makeAwesomeIcon(
      icon = "stop",         
      library = "fa",         
      markerColor = "purple"  
    ),
    popup = ~paste0(
      "<b>Article:</b> ", heading, "<br>",
      "<b>Date:</b> ", date, "<br>",
      "<b>Place:</b> ", place23nm
    )
  )


#but what do I notice:
#lot of limitations with our place name matches, namely that a lot of the one word "places"
#aren't unique references to individual places (eg "river")
#also - eg Brighton - some are plotted for one place, but likely refer to another
#Brighton is plotted at a Brighton in the Southwest, not the major city the article probably refers to
#Also a lot of the wider areas that are matched in our data, do not have a grid reference
#as they are too broad... (which damages the usefulness of the matching)
#(Also still some false positive articles)


#exact popups
leaflet(jul_sf) %>%
  addTiles() %>%
  addAwesomeMarkers(
    icon = makeAwesomeIcon(
      icon = "stop",           # Font Awesome square icon
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

#a good map -> can carry over into Shiny App format now...