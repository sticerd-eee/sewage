#task 5 - working out how to match ALL data & identify dry spills

# -	Match Anglian 2023 January with the 9 surrounding cells, for previous 4 days – and add a column for maximum; also average
# -	Add two columns (binary): one that indicated whether a dry spill via simple EA definition (ignoring differences in drain down / catchment area); one with BBC’s exact methodology
# -	Compare the results in number of dry spills labelled
# -	Make a system to referencing each month’s rainfall data (options: merge arrays via z dimension or make a common naming system)
# -	Ensure x and y dimensions are the same for all rainfall months (should be) – check x and y projection bound matrices
# -	Match all data – have column for each dry spill definition, and average, min, max rainfall in last 4 days, also average, min, max rainfall on the day & preceding day

#script v3

#v2 made progress so we can pretty much identify all dry spills in 2021.
#some had missing rainfall data as a valid missing value (the discharge points are in the sea)
#a few others had missing rainfall data due to some data issue - namely the discharge ngr was not 10 digits
#these had some spaces that meant i didn't realise when checking for non 10 digit (12 character) NGRs
#it seems to have impacted the easting / northing calculation with osg_parse()

#eg: 	
#CAMELFORD WWTW at SX10618334
#written as SX 1061 8334
#had easting 201061 and northing 8334 originally 
#but when spaces were removed...
#had easting 210610 and northing 83340

#checking online:
#210610 , 083340
#from https://gridreferencefinder.com/#gr=SX6910047290|SX_s_6910_s_4729|1,SX6910047290|SX_s_6910_s_4729|1,SX6910347291|Point_s_C|1,SZ6288082450|SZ6288082450|1,SV0000000000|21_s_SS57754719|1,SX1061083340|SX10618334|1

#therefore we must remove these spaces before finding the relevant grid cells

#this should remove the missing values that shouldn't be missing and leave only those discharging outside of UK land boundaries

#so this script will do that, and thus redo 2021 dry spills identification
#then we will identify all dry spills using the same method in one go


rm(list = ls())
gc()


library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(ncdf4)
library(rnrfa)
library(abind)
library(pbapply)


#set dropbox path to data folder:
dp_path<- "C:/Users/danan/Dropbox/sewage/data/"


#appending dec 2020 and all 2021 daily rainfall arrays

file_paths <- c(
  paste0(dp_path, "raw/haduk_rainfall_data/rainfall_2020_12.nc"), # December 2020
  sprintf("%sraw/haduk_rainfall_data/rainfall_2021_%02d.nc", dp_path, 1:12) # Jan-Dec 2021
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



rain1 <- combine_rainfall(file_paths)


# Check dimensions of the result
print(dim(rain1))


#spills for 2021
load(paste0(dp_path, "processed/1224_processed_data_matched/all_spills_1224.RData"))



#making the filtering strict based on previous issue of midnight new years and a 2020 spill
all_spills_2021<-all_spills_1224%>%
  filter(block_start < as.POSIXct("2022-01-01", format = "%Y-%m-%d")&
           block_start >=as.POSIXct("2020-12-01", format = "%Y-%m-%d"))

#376764 last time vs 376870 this time - the NA values still need to be removed

rm(all_spills_1224)


all_spills_2021 <- all_spills_2021 %>%
  mutate(DISCHARGE_NGR = trimws(DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("\\s+", " ", DISCHARGE_NGR)) %>% # Normalise spaces
  mutate(DISCHARGE_NGR = gsub("NZ3694368873 / NZ3694268961", "NZ3694368873", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("NU0228749514, NU0236149575", "NU0228749514", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("NZ1923451825 \\(to a culverted watercourse which opens at NZ1972351654\\)", "NZ1923451825", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("NZ1646227588 & NZ1646527572", "NZ1646227588", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("SP 5020076350", "SP5020076350", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("SK 38518 90505 Not from consents database", "SK3851890505", DISCHARGE_NGR))


all_spills_2021<-all_spills_2021%>%
  filter(site != "FALSE")
#376764 as before


#now we need to deal with the spaces in DISCHARGE_NGR

all_spills_2021<-all_spills_2021%>%
  mutate(DISCHARGE_NGR = gsub(" ", "", DISCHARGE_NGR))

test<-all_spills_2021%>%
  filter(str_length(DISCHARGE_NGR) != 12 | is.na(DISCHARGE_NGR))
#750 without 12 character (10 digit NGR)
#these have 8 digit instead

testunique<-test%>%
  distinct(permit_reference)
#16 sites have this 

rm(test, testunique)



#apply osg_parse() to get Eastings and Northings
results <- pblapply(all_spills_2021$DISCHARGE_NGR, function(x) {
  parsed <- osg_parse(x)
  return(c(as.numeric(parsed[1]), as.numeric(parsed[2])))
})

eastnorth_df <- do.call(rbind, results)
colnames(eastnorth_df) <- c("easting", "northing")

all_spills_2021 <- cbind(all_spills_2021, eastnorth_df)

rm(eastnorth_df)
rm(results)




#Z dimension
all_spills_2021<-all_spills_2021%>%
  mutate(
    Date = date(block_start),
    days = difftime(Date, as.POSIXct("01-12-2020", format = "%d-%m-%Y"), units = "days"),
    days = gsub(" days", "", days),
    zval = 1 + as.numeric(days)
  )%>%
  select(-c(days, Date))



#X and Y dimensions
nc_i <- nc_open(paste0(dp_path,"raw/haduk_rainfall_data/rainfall_2020_12.nc"))

ybound <- ncvar_get(nc_i, "projection_y_coordinate_bnds")
xbound <- ncvar_get(nc_i, "projection_x_coordinate_bnds")

nc_close(nc_i)
rm(nc_i)


all_spills_2021 <- all_spills_2021 %>%
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

#save(all_spills_2021, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_spills_2021_xyz_dim.RData")



#merging rainfall data
# format: "rain_t[X1]_s[X2]
#[X1] will be a number 1 to 4, with 1 being rainfall data for the start date (date of the start of the spill),
#2 being the day prior, 3 being the day before that, and 4 the day before that
#[X2] will be a number 1 to 9, referring to the grid cell (of the 9 we find important), left to right, up to down,
#meaning the grid cell the spill site lies in will be labelled 5



all_spills_2021_matched<-all_spills_2021%>%
  rowwise()%>%
  mutate(
    rain_t1_s1 = rain1[xval-1,yval+1,zval],
    rain_t1_s2 = rain1[xval, yval+1, zval],
    rain_t1_s3 = rain1[xval+1, yval+1, zval],
    
    rain_t1_s4 = rain1[xval-1, yval, zval],
    rain_t1_s5 = rain1[xval, yval, zval],
    rain_t1_s6 = rain1[xval+1, yval, zval],
    
    rain_t1_s7 = rain1[xval-1, yval-1, zval],
    rain_t1_s8 = rain1[xval, yval-1, zval],
    rain_t1_s9 = rain1[xval+1, yval-1, zval])%>%
  
  mutate(
    rain_t2_s1 = rain1[xval-1,yval+1,zval-1],
    rain_t2_s2 = rain1[xval, yval+1, zval-1],
    rain_t2_s3 = rain1[xval+1, yval+1, zval-1],
    
    rain_t2_s4 = rain1[xval-1, yval, zval-1],
    rain_t2_s5 = rain1[xval, yval, zval-1],
    rain_t2_s6 = rain1[xval+1, yval, zval-1],
    
    rain_t2_s7 = rain1[xval-1, yval-1, zval-1],
    rain_t2_s8 = rain1[xval, yval-1, zval-1],
    rain_t2_s9 = rain1[xval+1, yval-1, zval-1],
  )%>%
  
  mutate(
    rain_t3_s1 = rain1[xval-1,yval+1,zval-2],
    rain_t3_s2 = rain1[xval, yval+1, zval-2],
    rain_t3_s3 = rain1[xval+1, yval+1, zval-2],
    
    rain_t3_s4 = rain1[xval-1, yval, zval-2],
    rain_t3_s5 = rain1[xval, yval, zval-2],
    rain_t3_s6 = rain1[xval+1, yval, zval-2],
    
    rain_t3_s7 = rain1[xval-1, yval-1, zval-2],
    rain_t3_s8 = rain1[xval, yval-1, zval-2],
    rain_t3_s9 = rain1[xval+1, yval-1, zval-2],
  )%>%
  
  mutate(
    rain_t4_s1 = rain1[xval-1,yval+1,zval-3],
    rain_t4_s2 = rain1[xval, yval+1, zval-3],
    rain_t4_s3 = rain1[xval+1, yval+1, zval-3],
    
    rain_t4_s4 = rain1[xval-1, yval, zval-3],
    rain_t4_s5 = rain1[xval, yval, zval-3],
    rain_t4_s6 = rain1[xval+1, yval, zval-3],
    
    rain_t4_s7 = rain1[xval-1, yval-1, zval-3],
    rain_t4_s8 = rain1[xval, yval-1, zval-3],
    rain_t4_s9 = rain1[xval+1, yval-1, zval-3],
  )

#save(all_spills_2021_matched, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_spills_21_matched_v2.RData")


#check missing rainfall values:

#where are all 9 cells missing for all 4 periods (days)

#missing<-all_spills_2021_matched%>%
#  filter(if_any(starts_with("rain_t"), is.na))
#35000 missing all rainfall data. almost 10% of all spills
#incorrect logic - need ALL missing


missing<-all_spills_2021_matched%>%
  filter(if_all(starts_with("rain_t"), is.na))
#1158 missing ~ 0.25% of spills

unique_missing<-missing%>%
  distinct(permit_reference, .keep_all = T)
#just 17 sites have missing rainfall data

#all of these are discharging into seas or the channel - so it makes sense they are not within the rainfall bounds
rm(missing, unique_missing)


#identifying dry spills




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



pb <- txtProgressBar(min = 0, max = nrow(all_spills_2021_matched), style = 3)

counter <- 0  # Initialize counter

all_21_dry_spill <- as.data.frame(
  t(pbapply(all_spills_2021_matched, 1, function(row) {
    counter <<- counter + 1  # Increment counter
    setTxtProgressBar(pb, counter)  # Update progress bar
    rainfall_stats(row)
  }))
)

close(pb) 


all_21_dry_spill_v2<-cbind(all_spills_2021_matched, all_21_dry_spill)
#57 variables

problem_index<-which(all_21_dry_spill_v2$max_rain_1 == -Inf | is.na(all_21_dry_spill$mean_rain_1))
#1158 so the ones that are valid missing values


#now to add the binary variables that define dry spills:

#save(all_21_dry_spill_v2, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_21_dry_spill_v2.RData")


#less than 0.25mm is a dry spill
#we will take this definition in 4 ways

#4 methods:
#1: exact 1km grid rain is less than 0.25mm ON THE SPILL START DATE
#2: the 9 surrounding squares ALL have less than 0.25mm rain ON THE SPILL DATE
#3: (EA) the 9 surrounding grid squares each have less than 0.25mm rain on the spill date and prior day
#4: (BBC) none of the 9 grid cells have more than 0.25mm rain on any of the 4 preceding days


all_21_dry_spill_v3<-all_21_dry_spill_v2%>%
  #a dry spill (loose definition) just the 1km grid for that specific day < 0.25mm
  mutate(dry_spill_1 = if_else(rain_t1_s5 < 0.25, "yes", "no"))%>%
  #loose definition - the 9 surrounding grids don't have > o.25mm rain ON the specific day
  mutate(dry_spill_2 = if_else(max_rain_1 < 0.25, "yes", "no"))%>%
  #EA definition taken literally (not allowing for heterogenous drain down times)
  mutate(dry_spill_3 = if_else(max_rain_2 < 0.25, "yes", "no"))%>%
  #BBC methodology - 9 cells for 4 days prior
  mutate(dry_spill_4 = if_else(max_rain_3 < 0.25, "yes", "no"))


dryspills_2021<-all_21_dry_spill_v3%>%
  select(permit_reference, site, RECEIVING_WATER, REC_ENV_CODE_DESCRIPTION, DISCHARGE_NGR, 
         wasc, block_start, block_end, duration, postcode, mean_rain_1, mean_rain_2,
         dry_spill_1, dry_spill_2, dry_spill_3, dry_spill_4)

#identified fully for 2021..


#save these dataframes then apply to all data


#save(all_21_dry_spill_v3, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_21_dry_spill_v3.RData")


#save(dryspills_2021, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/dryspills_2021_identified.RData")



dry_spill_index<-which(dryspills_2021$dry_spill_4 == "yes")
#17, 830 dry spills via BBC methodology.



#move onto task 6 (actual application of matching)