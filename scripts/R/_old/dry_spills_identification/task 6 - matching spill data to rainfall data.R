#task 6 - actual application of rainfall matching, to identify dry spills

#in task 5 we figured out how to identify dry spills properly, and did this for all 2021 data
#we will now follow the same method to match ALL data (2021 to 2023)


#rm(list = ls())
#gc()


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



##Getting Rainfall Array Data:

#follows the simplified naming I gave each rainfall NetCDF file upon downloading
#rainfall_YYYY_MM


file_paths <- c(
  paste0(dp_path, "raw/haduk_rainfall_data/rainfall_2020_12.nc"), # December 2020
  
  sprintf("%sraw/haduk_rainfall_data/rainfall_2021_%02d.nc", dp_path, 1:12), # January to December 2021
  
  sprintf("%sraw/haduk_rainfall_data/rainfall_2022_%02d.nc", dp_path, 1:12), #January to December 2022
  
  sprintf("%sraw/haduk_rainfall_data/rainfall_2023_%02d.nc", dp_path, 1:12) #January to December 2023
  
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


#save(rain, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/rain_array.RData")



#this is saved as "rain_array.RData"
##located in dropbox/sewage/data/processed






##Getting the 12 24 spill data:

load(paste0(dp_path, "processed/1224_processed_data_matched/all_spills_1224.RData"))


all_spills<-all_spills_1224%>%
  filter(block_start >=as.POSIXct("2020-12-01", format = "%Y-%m-%d"))
#1,128,840 rows (1 less spill - spill from 2020)

rm(all_spills_1224)


#remove true missing values and correct some outlier NGR locations

check<-all_spills%>%
  filter(str_length(DISCHARGE_NGR) != 12 |
           is.na(DISCHARGE_NGR))%>%
  distinct(DISCHARGE_NGR, .keep_all = T)
#just 14  (vs only 7 from 2021)


all_spills <- all_spills %>%
  mutate(DISCHARGE_NGR = trimws(DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("\\s+", " ", DISCHARGE_NGR)) %>% # Normalise spaces
  mutate(DISCHARGE_NGR = gsub("NZ3694368873 / NZ3694268961", "NZ3694368873", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("NU0228749514, NU0236149575", "NU0228749514", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("NZ1923451825 \\(to a culverted watercourse which opens at NZ1972351654\\)", "NZ1923451825", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("NZ1646227588 & NZ1646527572", "NZ1646227588", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("SP 5020076350", "SP5020076350", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("CATM.3173", "SU7376054780", DISCHARGE_NGR))%>%
  mutate(DISCHARGE_NGR = gsub("TA 05992 64313 Not from consents database", "TA0599264313", DISCHARGE_NGR))%>%
  mutate(DISCHARGE_NGR = gsub("SK 38518 90505 Not from consents database", "SK3851890505", DISCHARGE_NGR))%>%
  mutate(DISCHARGE_NGR = gsub(" ", "", DISCHARGE_NGR))



check<-all_spills%>%
  filter(str_length(DISCHARGE_NGR) != 12 |
           is.na(DISCHARGE_NGR))%>%
  distinct(DISCHARGE_NGR, .keep_all = T)
#now 76 - but this is due to removing spaces - so it shows the sites that have 8 digit rather than 10 digit NGRs (which had spaces to make char count = 12 before)
#only 8 digit NGR sites and true NA remain
rm(check)

#remove the missing values 

all_spills<-all_spills%>%
  filter(!is.na(DISCHARGE_NGR))
#1128712 left


##Getting X and Y dimensions (to match spill sites to the rainfall data grid):

#apply osg_parse() to get Eastings and Northings
results <- pblapply(all_spills$DISCHARGE_NGR, function(x) {
  parsed <- osg_parse(x)
  return(c(as.numeric(parsed[1]), as.numeric(parsed[2])))
})


eastnorth_df <- do.call(rbind, results)
colnames(eastnorth_df) <- c("easting", "northing")

all_spills <- cbind(all_spills, eastnorth_df)

rm(eastnorth_df)
rm(results)


#converting the easting and northing into x and y dimensions - using x projection and y projection bound matrices

nc_i <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/rainfall_2020_12.nc"))
                
ybound <- ncvar_get(nc_i, "projection_y_coordinate_bnds")
xbound <- ncvar_get(nc_i, "projection_x_coordinate_bnds")

nc_close(nc_i)
rm(nc_i)


all_spills <- all_spills %>%
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

#save(all_spills, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_spills_temp2_beforeZ.RData")
#1128712 of 15 var



#load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_spills_temp2_beforeZ.RData")

## Getting Z dimension representing time in days:

#all_spills<-all_spills%>%
#  mutate(
#    Date = date(block_start),
#    days = difftime(Date, as.POSIXct("01-12-2020", format = "%d-%m-%Y"), units = "days"),
#    days = gsub(" days", "", days),
#    zval = 1 + as.numeric(days)
#  )%>%
#  select(-c(days, Date))

#far too slow / computationally inefficient and no transparency on progress (no progress bar)



#replace with

ref_date <- as.POSIXct("01-12-2020", format = "%d-%m-%Y")


days_results <- pblapply(all_spills$block_start, function(x) {
  as.numeric(difftime(as.Date(x), ref_date, units = "days"))  # Convert to numeric immediately
})

#Convert list to  dataframe column
days_df <- do.call(rbind, days_results)
colnames(days_df) <- "days"

#Append th column to our dataframe
all_spills <- cbind(all_spills, days_df)

#Zval is 1 plus days since (as first day has index z=1)
all_spills$zval <- 1 + all_spills$days

all_spills$days <- NULL


rm(ref_date, days_df, days_results)

#save(all_spills, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_spills_xyz1.RData")



#load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/rain_array.RData")


#load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_spills_xyz1.RData")



#



#remove the out of bounds spills (in 2024)

all_spills<-all_spills%>%
  filter(block_start < as.POSIXct("2024-01-01", format = "%Y-%m-%d"))
#1,128,648 left


##Match all necessary rainfall data:



#merging rainfall data format: "rain_t[X1]_s[X2]
#[X1] will be a number 1 to 4, with 1 being rainfall data for the start date (date of the start of the spill),
#2 being the day prior, 3 being the day before that, and 4 the day before that
#[X2] will be a number 1 to 9, referring to the grid cell (of the 9 we find important), left to right, up to down,
#meaning the grid cell the spill site lies in will be labelled 5



all_spills<-all_spills%>%
  rowwise()%>%
  mutate(
    rain_t1_s1 = rain[xval-1,yval+1,zval],
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


#save(all_spills, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_spills_matched_rainfall.RData")

rm(rain)



missing<-all_spills%>%
  filter(if_all(starts_with("rain_t"), is.na))
#


#Now we have all the raw rainfall data we need to identify dry spills and create some useful covariates for rainfall



##Identify Dry Spills



#a function to get the averages and max values we need
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


pb <- txtProgressBar(min = 0, max = nrow(all_spills), style = 3)

counter <- 0  # Initialize counter

dry_spills <- as.data.frame(
  t(pbapply(all_spills, 1, function(row) {
    counter <<- counter + 1  # Increment counter
    setTxtProgressBar(pb, counter)  # Update progress bar
    rainfall_stats(row)
  }))
)

close(pb) 


dry_spills<-cbind(all_spills, dry_spills)
#57 variables


problem_index<-which(dry_spills$max_rain_1 == -Inf | is.na(dry_spills$mean_rain_1))
#3724 so not many missing at all given 1 million observations




#less than 0.25mm is a dry spill
#we will take this definition in 4 ways

#4 methods:
#1: exact 1km grid rain is less than 0.25mm ON THE SPILL START DATE
#2: the 9 surrounding squares ALL have less than 0.25mm rain ON THE SPILL DATE
#3: (EA) the 9 surrounding grid squares each have less than 0.25mm rain on the spill date and prior day
#4: (BBC) none of the 9 grid cells have more than 0.25mm rain on any of the 4 preceding days


rm(all_spills, pb, counter, problem_index, rainfall_stats)

#save(dry_spills, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_spills_matched_with_rainfall_stats.RData")


#load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_spills_matched_with_rainfall_stats.RData")



#less than 0.25mm is a dry spill
#we will take this definition in 4 ways

#4 methods:
#1: exact 1km grid rain is less than 0.25mm ON THE SPILL START DATE
#2: the 9 surrounding squares ALL have less than 0.25mm rain ON THE SPILL DATE
#3: (EA) the 9 surrounding grid squares each have less than 0.25mm rain on the spill date and prior day
#4: (BBC) none of the 9 grid cells have more than 0.25mm rain on any of the 4 preceding days



dry_spill_defined<-dry_spills%>%
  #a dry spill (loose definition) just the 1km grid for that specific day < 0.25mm
  mutate(dry_spill_1 = if_else(rain_t1_s5 < 0.25, "yes", "no"))%>%
  #loose definition - the 9 surrounding grids don't have > o.25mm rain ON the specific day
  mutate(dry_spill_2 = if_else(max_rain_1 < 0.25, "yes", "no"))%>%
  #EA definition taken literally (not allowing for heterogenous drain down times)
  mutate(dry_spill_3 = if_else(max_rain_2 < 0.25, "yes", "no"))%>%
  #BBC methodology - 9 cells for 4 days prior
  mutate(dry_spill_4 = if_else(max_rain_3 < 0.25, "yes", "no"))


#add in a column for whether ANY rainfall data is missing for a spill 
#binary 1 if yes 0 if no

dry_spill_defined<-dry_spill_defined%>%
  mutate(any_miss = ifelse(if_any(starts_with("rain_t"), is.na), 1, 0))




dry_spills_defined<-dry_spill_defined%>%
  select(permit_reference, site, RECEIVING_WATER, REC_ENV_CODE_DESCRIPTION, DISCHARGE_NGR, 
         wasc, block_start, block_end, duration, postcode, mean_rain_1, mean_rain_2,
         dry_spill_1, dry_spill_2, dry_spill_3, dry_spill_4, any_miss)


#save(dry_spills_defined, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/dry_spills_defined.RData")

#saved as dry_spills_defined
#in dropbox/sewage/data/processed/dry_spills_defined.RData



#now move on to summary statistics (task 7)

rm(dry_spill_defined, dry_spills)



dry_spill_index<-which(dry_spills_defined$dry_spill_4 == "yes")
#38 750
#a LOT higher than BBC's 6000 possible in 2020

data22<-dry_spills_defined%>%
  filter(block_start > as.POSIXct("2022-01-01", format = "%Y-%m-%d")&
           block_start < as.POSIXct("2022-12-31", format = "%Y-%m-%d"))

dry_spill_22<-which(data22$dry_spill_4 == "yes")

#11916 - so double what the BBC has?? WHY??

#check through the code & also the dataframes to ensure i'm taking the max rainfall correctly.


#what about if they exclude all sites with any missing rainfall

data22<-dry_spills_defined%>%
  filter(any_miss == 0)%>%
  filter(block_start > as.POSIXct("2022-01-01", format = "%Y-%m-%d")&
           block_start < as.POSIXct("2022-12-31", format = "%Y-%m-%d"))

dry_spill_22<-which(data22$dry_spill_4 == "yes")

#9651 then, still high


#some edits to the files:


#some sites have all missing rainfall data

check3<-dry_spills_defined%>%
  filter(is.na(mean_rain_1) | is.na(mean_rain_2))
#how many unique sites

check4<-check3%>%
  distinct(DISCHARGE_NGR, .keep_all = T)

dry_spills_defined<-dry_spills_defined%>%
  mutate(dry_spill_1 = ifelse(is.na(mean_rain_1) | is.na(mean_rain_2), NA, dry_spill_1))%>%
  mutate(dry_spill_2 = ifelse(is.na(mean_rain_1) | is.na(mean_rain_2), NA, dry_spill_2))%>%
  mutate(dry_spill_3 = ifelse(is.na(mean_rain_1) | is.na(mean_rain_2), NA, dry_spill_3))%>%
  mutate(dry_spill_4 = ifelse(is.na(mean_rain_1) | is.na(mean_rain_2), NA, dry_spill_4))


#saved as dry_spills_defined
#in dropbox/sewage/data/processed/dry_spills_defined.RData


#add in eastings and northings back to the data for plotting easier

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_spills_temp2_beforeZ.RData")
#1128712 of 15 var

eastnorth<-all_spills%>%
  select(DISCHARGE_NGR, easting, northing)%>%
  distinct(DISCHARGE_NGR, .keep_all = T)


dry_spills_defined2<-left_join(dry_spills_defined, eastnorth, by = "DISCHARGE_NGR")


#saved as dry_spills_defined_with_eastnorth
#in dropbox/sewage/data/processed/dry_spills_defined_with_eastnorth.RData




