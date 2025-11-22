#plotting lexisnexis news coverage (yearly) on sewage between different countries and cities

#loading relevant packages:
library(dplyr)
library(ggplot2)
library(lubridate)
library(stringr)
#library(ggpubr)

###file roots (to be set  as needed)

#to the raw data folder:
nexis_path <- "C:/Users/danan/Dropbox/news_lexisnexis"

co_path <- paste0(folder_path, "data/raw/coverage")

#to place plots
plot_folder <- paste0(nexis_path, "/plots")





###Declaring functions to be used:

#to merge coverage data for different countries
merge_coverage <- function(country_list, filter_method = c("Source Location", "Geography", "Both"),
                           search_terms = c("search_0", "search_1" , "general"), region_type = c("country", "city")){
  
  filter_method <- match.arg(filter_method)
  search_terms <- match.arg(search_terms)
  region_type <- match.arg(region_type)
  
  filter_parse <- ifelse(filter_method == "Source Location", "/source_location",
                         ifelse(filter_method == "Geography", "/geography", "/lo_and_geo"))
  
  search_parse <- ifelse(search_terms == "search_1", "search1",
                         ifelse(search_terms == "search_0", "search0", "general"))
  
  
  coverage_data <- as.data.frame(NULL)
  
  
  for (i in country_list){
    
    lo_name_i <- tolower(i)
    lo_name_i <- gsub(" ", "", lo_name_i)
    
    
    path_i <- ifelse(search_terms == "general", paste0(co_path, "/", search_parse, "_", lo_name_i, ".csv")
                     ,paste0(co_path, filter_parse, "_", search_parse, "_", lo_name_i, ".csv"))
    
    
    file_i <- read.csv(path_i)
    
    if(search_terms != "general") {
      colnames(file_i) <- c("year", "coverage")
    } else {
      colnames(file_i) <- c("year", "general")
    }
    
    file_i <- file_i %>%
      mutate(year = as.numeric(substr(year, 7, 10)))
    
    
    if(region_type == "country"){
      file_i <- file_i %>%
        mutate(country = i) %>%
        mutate(country = as.factor(country))
    } else {
      file_i <- file_i %>%
        mutate(city = i) %>%
        mutate(city = as.factor(city))
    }
    
    coverage_data <- rbind(coverage_data, file_i)
  }
  
  
  
  #fill missing years (when coverage is 0 for year 2000 at least)
  if(region_type == "country"){
    coverage_data <- tidyr::complete(coverage_data, country, year)
  } else {
    coverage_data <- tidyr::complete(coverage_data, city, year)
  }
  
  
  
  
  if(search_terms != "general") {
    coverage_data<-coverage_data%>%
      mutate(coverage = ifelse(is.na(coverage), 0, coverage))
  }else {
    coverage_data <- coverage_data%>%
      mutate(general = ifelse(is.na(general), 0, general))
  }
  
  return(coverage_data)  
  
}


#to plot individual graphs easily


solo_plot <- function(country_list, filter_method = c("Source Location", "Geography"), normalized = c("yes", "no"),
                      search_terms = c("search_0", "search_1" , "general"), region_type = c("country", "city"), data_input){
  
  filter_method <- match.arg(filter_method)
  normalized <- match.arg(normalized)
  search_terms <-match.arg(search_terms)
  region_type <- match.arg(region_type)
  
  
  filter_parse <- ifelse(filter_method == "Source Location", "/source_location", "/geography")
  
  normalised_parse <- ifelse(normalized == "yes", "normalized_", "")
  
  search_parse <- ifelse(search_terms == "search_1", "search1",
                         ifelse(search_terms == "search_0", "search0", "general"))
  
  for (i in country_list){
    
    low_i <-tolower(i)
    low_i <-gsub(" ", "", low_i)
    
    if(region_type == "country"){
      data_i <- data_input %>%
        filter(country == i)
    } else {
      data_i <- data_input %>%
        filter(city == i)
    }
    
    #plot the solo line plot
    
    plot_i <- ggplot(data_i, aes(x = year, y = coverage)) +
      geom_line() +
      labs(x = "Year", y = "Coverage") +
      ggtitle(paste0("Media Coverage of Sewage in ", i))
    
    
    ggsave(
      filename = paste0(plot_folder,filter_parse,"_", search_parse, "_", normalised_parse, low_i,".png"),
      plot = plot_i,         
      width = 12, height = 6,       
      dpi = 300                    
    )
    
    
  }
  
}

#to normalise the raw coverage numbers by the total coverage in the region for any year
normalise_coverage <- function(coverage_data, general_data, region_type = c("country", "city")) {
  
  region_type <- match.arg(region_type)
  
  if(region_type == "country"){
    norm_data <- left_join(coverage_data, general_data, by = c("year", "country")) %>%
      mutate(n_coverage = 100*coverage / general)
    
  } else {
    norm_data <- left_join(coverage_data, general_data, by = c("year", "city"))%>%
      mutate(n_coverage = ifelse(is.na(general) | general == 0, NA, 100 * coverage / general))
  }
  
  return(norm_data)
  
}


#Aesthetic schemes for plotting:

#country colour palette
country_colors <- c(
  "UK"        = "#e41a1c", 
  "USA"       = "#377eb8",
  "Canada"    = "#4daf4a",
  "Australia" = "#984ea3",
  "France"    = "#1b9e77", 
  "Germany"   = "#a65628"  
)

#monotone alternative
country_colors_mono <- c(
  "UK"        = "black",
  "USA"       = "grey20",
  "Canada"    = "grey40",
  "Australia" = "grey60",
  "France"    = "grey70",
  "Germany"   = "grey50"
)
country_linetypes <- c(
  "UK" = "solid",
  "USA" = "dashed",
  "Canada" = "dotted",
  "Australia" = "dotdash",
  "France" = "longdash",
  "Germany" = "twodash"
)


#city colors to match with country colors visually
city_colors <- c(
  "London"        = "#e41a1c",
  "Birmingham"    = "#f47778",
  
  "New York"      = "#377eb8",
  "Washington DC" = "#75aadb",
  "Los Angeles"   = "#274b7a",
  
  "Ottawa"        = "#4daf4a",
  "Vancouver"     = "#7fcf7b",
  "Toronto"       = "#2e7030",
  
  "Sydney"        = "#984ea3",
  "Melbourne"     = "#c27ac9",
  "Canberra"      = "#6a3471",
  
  "Paris"         = "#1b9e77",
  "Berlin"        = "#a65628"
)





#region lists:

#country list
country_list <- list("UK", "USA", "Canada", "Australia", "France", "Germany")


#city list

city_list <- list("London", "Birmingham", "New York", "Washington DC",
                  "Los Angeles", "Paris", "Berlin", "Sydney", "Melbourne",
                  "Canberra", "Ottawa", "Vancouver", "Toronto")


#####COUNTRY LEVEL COMPARISONS:

###Plots for search_1:



##via source location filtering (ideal):
coverage1 <- merge_coverage(country_list = country_list, filter_method = "Source Location", search_terms = "search_1", region_type = "country")

#combined plot (colour)

ggplot(coverage1) +
  geom_line(aes(x = year, y = coverage, group = country, color = country)) +
  scale_color_manual(values = country_colors) +
  labs(x = "Year", y = "Coverage") +
  guides(color = guide_legend(title = "Country")) +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("Media Coverage of Sewage")


#ggsave(
#  filename = paste0(plot_folder,"/comparison_source_location_search1_colour.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)


#combined plot (monotone)

ggplot(coverage1) +
  geom_line(aes(x = year, y = coverage, group = country, color = country, linetype = country)) +
  scale_color_manual(values = country_colors_mono) +
  scale_linetype_manual(values = country_linetypes) +
  labs(x = "Year", y = "Coverage", color = "Country", linetype = "Country") +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("Media Coverage of Sewage")


#ggsave(
#  filename = paste0(plot_folder,"/comparison_source_location_search1_mono.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)


#just plotting the difference between the UK and US:

uk_us <- coverage1 %>%
  filter(country == "UK" | country == "USA")

ggplot(uk_us) +
  geom_line(aes(x = year, y = coverage, group = country, color = country, linetype = country)) +
  scale_color_manual(values = country_colors_mono) +
  scale_linetype_manual(values = country_linetypes) +
  labs(x = "Year", y = "Coverage", color = "Country", linetype = "Country") +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("Media Coverage of Sewage: UK vs US")


#ggsave(
#  filename = paste0(plot_folder,"/comparison_uk_us_source_location_search1_mono.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)




#solo_plot(country_list = country_list, filter_method = "Source Location",
#          normalized = "no", search_terms = "search_1", region_type = "country", coverage1)
#worked


#via geography filtering (less interpretable):

coverage2 <- merge_coverage(country_list = country_list, filter_method = "Geography",
                            search_terms = "search_1")



#combined plot (colour)

ggplot(coverage2) +
  geom_line(aes(x = year, y = coverage, group = country, color = country)) +
  scale_color_manual(values = country_colors) +
  labs(x = "Year", y = "Coverage") +
  guides(color = guide_legend(title = "Country")) +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("Media Coverage of Sewage")


#ggsave(
#  filename = paste0(plot_folder,"/comparison_geography_search1_colour.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)


#very similar plot to the source location one, but the UK isn't always the most covered in media
#as recently as 2020 it wasn't distinguished from other countries


#combined plot (monotone)

ggplot(coverage2) +
  geom_line(aes(x = year, y = coverage, group = country, color = country, linetype = country)) +
  scale_color_manual(values = country_colors_mono) +
  scale_linetype_manual(values = country_linetypes) +
  labs(x = "Year", y = "Coverage", color = "Country", linetype = "Country") +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("Media Coverage of Sewage")


#ggsave(
#  filename = paste0(plot_folder,"/comparison_geography_search1_mono.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)



#solo_plot(country_list = country_list, filter_method = "Geography", coverage2,
#          search_terms = "search_1", normalized = "no")


# ~ we can move forward using source location, for clarity and since the difference is small.
# ~ the divergence between media attention to UK sewage spills and elsewhere is robust to country filtering method used in LexisNexis




##Normalising by the general number of articles on LexisNexis for any given region

general1 <- merge_coverage(country_list = country_list, filter_method = "Source Location",
                           search_terms = "general", region_type = "country")

s1_norm <- normalise_coverage(coverage_data = coverage1, general_data = general1)


ggplot(s1_norm) +
  geom_line(aes(x = year, y = n_coverage, group = country, color = country)) +
  scale_color_manual(values = country_colors) +
  labs(x = "Year", y = "% Coverage") +
  guides(color = guide_legend(title = "Country")) +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("% Media Coverage about Sewage")


#ggsave(
#  filename = paste0(plot_folder,"/comparison_source_location_search1_normalized_colour.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)

ggplot(s1_norm) +
  geom_line(aes(x = year, y = n_coverage, group = country, color = country, linetype = country)) +
  scale_color_manual(values = country_colors_mono) +
  scale_linetype_manual(values = country_linetypes) +
  labs(x = "Year", y = "% Coverage", color = "Country", linetype = "Country") +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("% Media Coverage about Sewage")

#ggsave(
#  filename = paste0(plot_folder,"/comparison_source_location_search1_normalized_mono.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)




##SEARCH_0

coverage0 <- merge_coverage(country_list = country_list, filter_method = "Source Location", search_terms = "search_0", region_type = "country")

#combined plot (colour)

ggplot(coverage0) +
  geom_line(aes(x = year, y = coverage, group = country, color = country)) +
  scale_color_manual(values = country_colors) +
  labs(x = "Year", y = "Coverage") +
  guides(color = guide_legend(title = "Country")) +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("Media Coverage of Sewage")


#ggsave(
#  filename = paste0(plot_folder,"/comparison_source_location_search0_colour.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)


#combined plot (monotone)

ggplot(coverage0) +
  geom_line(aes(x = year, y = coverage, group = country, color = country, linetype = country)) +
  scale_color_manual(values = country_colors_mono) +
  scale_linetype_manual(values = country_linetypes) +
  labs(x = "Year", y = "Coverage", color = "Country", linetype = "Country") +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("Media Coverage of Sewage")


#ggsave(
#  filename = paste0(plot_folder,"/comparison_source_location_search0_mono.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)




uk_us_0 <- coverage0 %>%
  filter(country == "UK" | country == "USA")

ggplot(uk_us_0) +
  geom_line(aes(x = year, y = coverage, group = country, color = country, linetype = country)) +
  scale_color_manual(values = country_colors_mono) +
  scale_linetype_manual(values = country_linetypes) +
  labs(x = "Year", y = "Coverage", color = "Country", linetype = "Country") +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("Media Coverage of Sewage: UK vs US")


#ggsave(
#  filename = paste0(plot_folder,"/comparison_uk_us_source_location_search0_mono.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)


solo_plot(country_list = country_list, filter_method = "Source Location",
          normalized = "no", search_terms = "search_0", region_type = "country", coverage0)


#Normalising by total articles published
s0_norm <- normalise_coverage(coverage_data = coverage0, general_data = general1)



ggplot(s0_norm) +
  geom_line(aes(x = year, y = n_coverage, group = country, color = country)) +
  scale_color_manual(values = country_colors) +
  labs(x = "Year", y = "% Coverage") +
  guides(color = guide_legend(title = "Country")) +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("% Media Coverage about Sewage")


#ggsave(
#  filename = paste0(plot_folder,"/comparison_source_location_search0_normalized_colour.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)

ggplot(s0_norm) +
  geom_line(aes(x = year, y = n_coverage, group = country, color = country, linetype = country)) +
  scale_color_manual(values = country_colors_mono) +
  scale_linetype_manual(values = country_linetypes) +
  labs(x = "Year", y = "% Coverage", color = "Country", linetype = "Country") +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("% Media Coverage about Sewage")

#ggsave(
#  filename = paste0(plot_folder,"/comparison_source_location_search0_normalized_mono.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)








####CITY LEVEL COVERAGE COMPARISONS
#(all use source location filtering)

##SEARCH_0

city_coverage_0 <- merge_coverage(country_list = city_list, filter_method = "Source Location", search_terms = "search_0", region_type = "city")


#comparison plot in color
ggplot(city_coverage_0) +
  geom_line(aes(x = year, y = coverage, group = city, color = city)) +
  scale_color_manual(values = city_colors) +
  labs(x = "Year", y = "Coverage") +
  guides(color = guide_legend(title = "City")) +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("Media Coverage of Sewage")


#ggsave(
#  filename = paste0(plot_folder,"/city_comparison_source_location_search0_colour.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)

#solo_plot(country_list = city_list, filter_method = "Source Location",
#          normalized = "no", search_terms = "search_0", region_type = "city", city_coverage_0)




##SEARCH_1

city_coverage_1 <- merge_coverage(country_list = city_list, filter_method = "Source Location", search_terms = "search_1", region_type = "city")


#comparison plot in color
ggplot(city_coverage_1) +
  geom_line(aes(x = year, y = coverage, group = city, color = city)) +
  scale_color_manual(values = city_colors) +
  labs(x = "Year", y = "Coverage") +
  guides(color = guide_legend(title = "City")) +
  theme(
    axis.title.y = element_text(angle = 0, vjust = 0.5),
    legend.position = "right"
  ) +
  ggtitle("Media Coverage of Sewage")


#ggsave(
#  filename = paste0(plot_folder,"/city_comparison_source_location_search1_colour.png"),
#  plot = last_plot(),         
#  width = 12, height = 6,       
#  dpi = 300                    
#)

#solo_plot(country_list = city_list, filter_method = "Source Location",
#          normalized = "no", search_terms = "search_1", region_type = "city", city_coverage_1)



