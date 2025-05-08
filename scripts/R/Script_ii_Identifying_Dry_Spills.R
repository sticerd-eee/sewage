#Danan
#These scripts move my 12 24 codes and weather data merging to Jacopo's spill data
#follows from Script_i_Applying_1224_counting

#Script ii: Dry Spills Identification
#it follows the methodology set out in X WORD DOC - consult this
 


library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(ncdf4)
library(rnrfa)
library(abind)
library(pbapply)

##set dropbox path to data folder:
dp_path<- "C:/Users/danan/Dropbox/sewage/data/"
#local alternative
#dp_path <- "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/dp_backup/"

##Getting Rainfall Data (as a single array):


file_paths <- c(
  paste0(dp_path, "raw/haduk_rainfall_data/rainfall_2020_12.nc"), #December 2020
  paste0(dp_path, sprintf("raw/haduk_rainfall_data/rainfall_2021_%02d.nc", 1:12)), #2021
  paste0(dp_path, sprintf("raw/haduk_rainfall_data/rainfall_2022_%02d.nc", 1:12)), #2022
  paste0(dp_path, sprintf("raw/haduk_rainfall_data/rainfall_2023_%02d.nc", 1:12)) #2023
)

#(need to add the path for 2024 once that EDM data is available)
#the 2024 rainfall data is already downloaded in that raw/haduk_rainfall_data/ folder
#(same naming convention
)

#simple function to append them on the Z (time) dimension
combine_rainfall <- function(file_paths) {
  rain_combined <- NULL
  
  #for a tracker on progress
  arraynumber <- 0
  
  # for loop through netcdf files
  for (file in file_paths) {
    nc_data <- nc_open(file)
    
    rainfall_data <- ncvar_get(nc_data, "rainfall")
    
    
    nc_close(nc_data)
    
    
    if (is.null(rain_combined)) {
      #first array (dec 2020)
      rain_combined <- rainfall_data
    } else {
      # append other files
      rain_combined <- abind(rain_combined, rainfall_data, along = 3)
    }
    
    # Remove temporary objects and free memory
    rm(nc_data, rainfall_data)
    gc()
    
    #update tracker and print the progress
    arraynumber<-arraynumber + 1
    
    print(paste("appended array", arraynumber))
  }
  
  # Return the combined array
  return(rain_combined)
}



rain <- combine_rainfall(file_paths)


# Check dimensions of the result (should be 900 by 1450 by 1126)
print(dim(rain))
#[1]  900 1450 1126

rm(file_paths)


#can load rain array directly here, from Dropbox
#load(paste0(dp_path, "processed/rain_array.RData"))



#load in 12 24 data, and prepare for matching:
load(paste0(dp_path, "processed/1224_spills.RData"))
#(edit if needed)


spills1224<-spills1224%>%
  mutate(outlet_discharge_ngr = gsub("\\s+", " ", outlet_discharge_ngr))%>%
  mutate(outlet_discharge_ngr = gsub(" ", "", outlet_discharge_ngr))

#see if any NGRs are not 12 characters long
check<-spills1224%>%
  filter(str_length(outlet_discharge_ngr) != 12 |
           is.na(outlet_discharge_ngr))

unique_check<-check%>%
  distinct(outlet_discharge_ngr)

print(unique(str_length(unique_check$outlet_discharge_ngr)))
#10 8 and 13

#let's observe the 13
unique_13<-unique_check%>%
  filter(str_length(outlet_discharge_ngr) == 13)
#just one site

check_13 <- spills1224%>%
  filter(str_length(outlet_discharge_ngr) == 13)
#affecting 12 spills

#"filey waste water treatment works" all only in 2023

check_filey<-spills1224%>%
  filter(grepl("filey", site_name_ea, ignore.case = T) |
           grepl("filey", site_name_wa_sc, ignore.case = T))

check_filey<-check_filey%>%
  arrange(block_start)

#problematic - need to trace where this error in matching came from
#filey waste water treatment works
#187 spills from this site
#in 2023, some have a different spill NGR (13 digits - not possible)
#all others (2021, 2022 and some 2023) have a consistent NGR
#not sure if these different NGR ones would be duplicates of some of the original spills with the correct NGR
#but it means my use of NGR for the 12 24 counting leads to these spills not being declared correctly
#we will remove this site for now

load(paste0(dp_path, "processed/merged_edm_data.RData"))
data_check<-data%>%
  filter(grepl("filey", site_name_ea, ignore.case = T) |
           grepl("filey", site_name_wa_sc, ignore.case = T))
data_check<-data_check%>%
  distinct(outlet_discharge_ngr, year, .keep_all = T)
#3 different ngrs for this site in 2023 - need to check for such duplicates in the data



#rest have seemingly valid NGRs

spills1224<-spills1224%>%
  filter(!grepl("filey", site_name_ea, ignore.case = T))
#1093801



##Getting X and Y dimensions (to match spill sites to the rainfall data grid):

#apply osg_parse() to get Eastings and Northings
results <- pblapply(spills1224$outlet_discharge_ngr, function(x) {
  parsed <- osg_parse(x)
  return(c(as.numeric(parsed[1]), as.numeric(parsed[2])))
})

eastnorth_df <- do.call(rbind, results)
colnames(eastnorth_df) <- c("easting", "northing")


spills1224 <- cbind(spills1224, eastnorth_df)


save(spills1224, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Codes/migrating to Jacopo data/1224_spills_witheastnorth.RData")


#converting easting and northing into relevant x and y dimensions
nc_i <- nc_open(paste0(dp_path,"processed/haduk_rainfall_data/rainfall_2020_12.nc"))

ybound <- ncvar_get(nc_i, "projection_y_coordinate_bnds")
xbound <- ncvar_get(nc_i, "projection_x_coordinate_bnds")

nc_close(nc_i)
rm(nc_i)


spills1224 <- spills1224 %>%
  rowwise() %>%
  mutate(
    xval = ifelse(easting%% 1000 == 0,
                  list(which(xbound[1, ] <= easting+1 & xbound[2, ] >= easting+1)),
                  list(which(xbound[1, ] <= easting & xbound[2, ] >= easting))
    ),
    yval = ifelse(northing%% 1000 ==0,
                  list(which(ybound[1, ] <= northing+1 & ybound[2, ] >= northing+1)),
                  list(which(ybound[1, ] <= northing & ybound[2, ] >= northing))
    )
  )


save(spills1224, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Codes/migrating to Jacopo data/1224_spills_withxy.RData")


# Getting Z dimension representing time in days:


ref_date <- as.POSIXct("01-12-2020", format = "%d-%m-%Y")
#(first date in rainfall data)

days_results <- pblapply(spills1224$block_start, function(x) {
  as.numeric(difftime(as.Date(x), ref_date, units = "days"))  
})

#Convert list to  dataframe column
days_df <- do.call(rbind, days_results)
colnames(days_df) <- "days"

#Append th column to our dataframe
spills1224 <- cbind(spills1224, days_df)

#Zval is 1 plus days since (as first day has index z=1)
spills1224$zval <- 1 + spills1224$days

spills1224$days <- NULL

save(spills1224, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Codes/migrating to Jacopo data/1224_spills_withxyz.RData")




##Match all necessary rainfall data:



#merging rainfall data format: "rain_t[X1]_s[X2]
#[X1] will be a number 1 to 4, with 1 being rainfall data for the start date (date of the start of the spill),
#2 being the day prior, 3 being the day before that, and 4 the day before that
#[X2] will be a number 1 to 9, referring to the grid cell (of the 9 we find important), left to right, up to down,
#meaning the grid cell the spill site lies in will be labelled 5


spills_rain<-spills1224%>%
  rowwise()%>%
  mutate(
    rain_t1_s1 = rain[xval-1, yval+1, zval],
    rain_t1_s2 = rain[xval, yval+1, zval],
    rain_t1_s3 = rain[xval+1, yval+1, zval],
    
    rain_t1_s4 = rain[xval-1, yval, zval],
    rain_t1_s5 = rain[xval, yval, zval],
    rain_t1_s6 = rain[xval+1, yval, zval],
    
    rain_t1_s7 = rain[xval-1, yval-1, zval],
    rain_t1_s8 = rain[xval, yval-1, zval],
    rain_t1_s9 = rain[xval+1, yval-1, zval])%>%
  
  mutate(
    rain_t2_s1 = rain[xval-1,yval+1,zval-1],
    rain_t2_s2 = rain[xval, yval+1, zval-1],
    rain_t2_s3 = rain[xval+1, yval+1, zval-1],
    
    rain_t2_s4 = rain[xval-1, yval, zval-1],
    rain_t2_s5 = rain[xval, yval, zval-1],
    rain_t2_s6 = rain[xval+1, yval, zval-1],
    
    rain_t2_s7 = rain[xval-1, yval-1, zval-1],
    rain_t2_s8 = rain[xval, yval-1, zval-1],
    rain_t2_s9 = rain[xval+1, yval-1, zval-1],
  )%>%
  
  mutate(
    rain_t3_s1 = rain[xval-1,yval+1,zval-2],
    rain_t3_s2 = rain[xval, yval+1, zval-2],
    rain_t3_s3 = rain[xval+1, yval+1, zval-2],
    
    rain_t3_s4 = rain[xval-1, yval, zval-2],
    rain_t3_s5 = rain[xval, yval, zval-2],
    rain_t3_s6 = rain[xval+1, yval, zval-2],
    
    rain_t3_s7 = rain[xval-1, yval-1, zval-2],
    rain_t3_s8 = rain[xval, yval-1, zval-2],
    rain_t3_s9 = rain[xval+1, yval-1, zval-2],
  )%>%
  
  mutate(
    rain_t4_s1 = rain[xval-1,yval+1,zval-3],
    rain_t4_s2 = rain[xval, yval+1, zval-3],
    rain_t4_s3 = rain[xval+1, yval+1, zval-3],
    
    rain_t4_s4 = rain[xval-1, yval, zval-3],
    rain_t4_s5 = rain[xval, yval, zval-3],
    rain_t4_s6 = rain[xval+1, yval, zval-3],
    
    rain_t4_s7 = rain[xval-1, yval-1, zval-3],
    rain_t4_s8 = rain[xval, yval-1, zval-3],
    rain_t4_s9 = rain[xval+1, yval-1, zval-3],
  )

#save(spills_rain, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Codes/migrating to Jacopo data/spills_with_rain.RData")
#local save

#saved in processed (dropbox) under "spills_with_rain.RData"


#creating statistics for dry spill identification
rainfall_stats <- function(row) {
  return(c(
    #mean_rain_1 is the mean rainfall that fell in the 9 cell area ON the start date
    mean_rain_1 = mean(as.numeric(row[grepl("rain_t1_s", names(row))]), na.rm = TRUE),
    
    #mean_rain_2 is the mean rainfall of all 9 cells in the preceding 4 days
    mean_rain_2 = mean(as.numeric(row[grepl("rain_t", names(row))]), na.rm = TRUE),
    
    #max_rain_1 is the max rainfall any of the 9 cells had ON the spill date
    max_rain_1 = max(as.numeric(row[grepl("rain_t1_s", names(row))]), na.rm = TRUE),
    
    #max_rain_2 is the max rainfall of any of the 9 cells on the day or day before
    max_rain_2 = max(
      max(as.numeric(row[grepl("rain_t1_s", names(row))]), na.rm = TRUE),
      max(as.numeric(row[grepl("rain_t2_s", names(row))]), na.rm = TRUE)
    ),
    
    #max_rain_3 is the maximum value of rainfall for any of the 9 cells in the 4 days
    max_rain_3 = max(as.numeric(row[grepl("rain_t", names(row))]), na.rm = TRUE)
  ))
}


pb <- txtProgressBar(min = 0, max = nrow(spills_rain), style = 3)

counter <- 0  # Initialize counter

dry_spills <- as.data.frame(
  t(pbapply(spills_rain, 1, function(row) {
    counter <<- counter + 1  # Increment counter
    setTxtProgressBar(pb, counter)  # Update progress bar
    rainfall_stats(row)
  }))
)

close(pb) 


#save(dry_spills, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Codes/migrating to Jacopo data/rainfall_stats.RData")
#local save

check_na<-dry_spills%>%
  filter(is.na(mean_rain_1) |
           is.na(mean_rain_2) |
           is.na(max_rain_1) |
           is.na(max_rain_2) |
           is.na(max_rain_3))
#some are missing the rainfall statistics. Not sure why?
#7505 to be exact (quite few given over 1 Million total obs)


#vs only 3724 when applied to my data?


problem_index<-which(dry_spills$max_rain_1 == -Inf | is.na(dry_spills$mean_rain_1))

check<-spills_rain[problem_index, ]

unique_all_missing<-check%>%
  distinct(outlet_discharge_ngr, .keep_all = T)%>%
  select(outlet_discharge_ngr, site_name_ea, site_name_wa_sc, water_company)
#38 sites

#from manual checking, they look valid (outside of land area of UK - so no available rainfall data)


#add in any_miss -> binary variable to state whether any rainfall values are missing



#less than 0.25mm is a dry spill

#4 results:
#1: exact 1km grid rain is less than 0.25mm ON THE SPILL START DATE (dry on the day)
#2: the 9 surrounding squares ALL have less than 0.25mm rain ON THE SPILL DATE (dry on the day - more robust to NGR)
#3: (EA) the 9 surrounding grid squares each have less than 0.25mm rain on the spill date and prior day (possible dry spill EA methodology)
#4: (BBC) none of the 9 grid cells have more than 0.25mm rain on any of the 4 preceding days (possible dry spill BBC methodology)


dry_spills <- cbind(spills_rain, dry_spills)


dry_spills<-dry_spills%>%
  mutate(any_miss = ifelse(if_any(starts_with("rain_t"), is.na), 1, 0))


dry_spill_defined<-dry_spills%>%
  #a dry day (loose definition) just the 1km grid for that specific day < 0.25mm
  mutate(dry_spill_1 = if_else(rain_t1_s5 < 0.25, "yes", "no"))%>%
  #a dry day (loose definition 2) - the 9 surrounding grids all have < o.25mm rain ON the specific day
  mutate(dry_spill_2 = if_else(max_rain_1 < 0.25, "yes", "no"))%>%
  #EA definition taken literally (not allowing for heterogenous drain down times) 9 cells have < 0.25mm on the day and the prior day
  mutate(dry_spill_3 = if_else(max_rain_2 < 0.25, "yes", "no"))%>%
  #BBC methodology - 9 cells for 4 days prior < 0.25mm
  mutate(dry_spill_4 = if_else(max_rain_3 < 0.25, "yes", "no"))


check<-dry_spill_defined%>%
  filter(dry_spill_4 == "yes")
#40,000

check<-dry_spill_defined%>%
  filter(dry_spill_4 == "yes" &
           year == 2022)
#11621 - so just like with my data. 


#check the process, then once more against the BBC.

#after that, just continue and write up




dry_spills_defined<-dry_spill_defined%>%
  select(outlet_discharge_ngr, block_start, block_end,
         duration, year, water_company, site_name_ea,
         site_name_wa_sc, permit_reference_ea, activity_reference,
         sum_spill_duration_hrs_yr, sum_spill_count_yr,
         max_spill_duration_hrs_yr, key_join, permit_reference_wa_sc,
         unique_id, site_code, storm_discharge_asset_type_treatment_works,
         ea_id, event_duration_in_hours, site_id, easting, northing, any_miss,
         mean_rain_1, mean_rain_2, max_rain_1, max_rain_2, max_rain_3,
         dry_spill_1, dry_spill_2, dry_spill_3, dry_spill_4)



#rename to clearer names

#dry_spill_1 -> dry_day_1
#dry_spill_2 -> dry_day_2
#dry_spill_3 -> ea_dry_spill
#dry_spill_4 -> bbc_dry_spill

dry_spills_defined<-dry_spills_defined%>%
  rename(dry_day_1 = dry_spill_1, dry_day_2 = dry_spill_2,
         ea_dry_spill = dry_spill_3, bbc_dry_spill = dry_spill_4)




#now - because of the way I applied 12 24 counting and the way the data was matched to EDM data before
#(i.e by start or end time's year to each EDM returns)
#there are some spills in this dry_spills_defined dataframe that do not have matched EDM data
#for example - discharge site at NGR = "SJ2223084820"
#this has a "spill" that starts in 2022 October and ends in July 2023
#So this spill had been matched to EDM data given year = "2022" originally
#but now, once the spill is split, some lie in 2023
#which was never matched to this spill site

#for these cases - we will add the attributes that do not vary by year
#and set the varying attributes to NA (missing)
#[eg "sum_spill_duration_in_hrs_yr"]

#this does not account for the unmatched ones from the start (still need sorting)



extra_missing<-dry_spills_defined%>%
  filter(is.na(water_company))

extra_unique<-extra_missing%>%
  distinct(outlet_discharge_ngr, year)
#29 


#adding EDM attributes...

load(paste0(dp_path, "processed/merged_edm_data.RData"))

data_sites<-data%>%
  distinct(outlet_discharge_ngr, .keep_all = T)%>%
  select(-year)

extra_unique_matched<-left_join(extra_unique, data_sites, by = "outlet_discharge_ngr")%>%
  mutate(sum_spill_duration_hrs_yr = NA,
         sum_spill_count_yr = NA,
         max_spill_duration_hrs_yr = NA,
         event_duration_in_hours = NA)%>%
  select(-start_time, -start_time_og, -end_time, -end_time_og)

extra_missing_2<-extra_missing%>%
  select(outlet_discharge_ngr, block_start, block_end, duration, year, easting, northing,
         any_miss, mean_rain_1, mean_rain_2, max_rain_1, max_rain_2, max_rain_3,
         dry_day_1, dry_day_2, ea_dry_spill, bbc_dry_spill)


extra_missing_matched<-left_join(extra_missing_2, extra_unique_matched, by = c("outlet_discharge_ngr", "year"))




dry_spills_defined_2<-dry_spills_defined%>%
  filter(!is.na(water_company))


dry_spills_defined<-rbind(dry_spills_defined_2, extra_missing_matched)%>%
  arrange(outlet_discharge_ngr, block_start)



extra_missing_check<-dry_spills_defined%>%
  filter(is.na(water_company))
#0



#save(dry_spills_defined, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Codes/migrating to Jacopo data/merged_edm_1224_dry_spill_data.RData")
#local save

#saved in dropbox under processed "merged_edm_1224_dry_spill_data.RData"

