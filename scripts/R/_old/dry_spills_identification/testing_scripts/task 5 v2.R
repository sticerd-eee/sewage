
#task 5 - working out how to match ALL data & identify dry spills

# -	Match Anglian 2023 January with the 9 surrounding cells, for previous 4 days – and add a column for maximum; also average
# -	Add two columns (binary): one that indicated whether a dry spill via simple EA definition (ignoring differences in drain down / catchment area); one with BBC’s exact methodology
# -	Compare the results in number of dry spills labelled
# -	Make a system to referencing each month’s rainfall data (options: merge arrays via z dimension or make a common naming system)
# -	Ensure x and y dimensions are the same for all rainfall months (should be) – check x and y projection bound matrices
# -	Match all data – have column for each dry spill definition, and average, min, max rainfall in last 4 days, also average, min, max rainfall on the day & preceding day

#script v2


#dry spill identification for 2021

#rm(list = ls())
#gc()


library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(ncdf4)
library(rnrfa)
library(abind)


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
#900 1450 396
#exactly the dimensions we expect (365 days of 2021, plus 31 days of Dec 2020)



#get spill data for 2021
load(paste0(dp_path, "processed/1224_processed_data_matched/all_spills_1224.RData"))



all_spills_2021<-all_spills_1224%>%
  filter(block_start <= as.POSIXct("2022-01-01", format = "%Y-%m-%d" ))

rm(all_spills_1224)


all_spills_2021 <- all_spills_2021 %>%
  mutate(DISCHARGE_NGR = trimws(DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("\\s+", " ", DISCHARGE_NGR)) %>% # Normalize spaces
  mutate(DISCHARGE_NGR = gsub("NZ3694368873 / NZ3694268961", "NZ3694368873", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("NU0228749514, NU0236149575", "NU0228749514", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("NZ1923451825 \\(to a culverted watercourse which opens at NZ1972351654\\)", "NZ1923451825", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("NZ1646227588 & NZ1646527572", "NZ1646227588", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("SP 5020076350", "SP5020076350", DISCHARGE_NGR)) %>%
  mutate(DISCHARGE_NGR = gsub("SK 38518 90505 Not from consents database", "SK3851890505", DISCHARGE_NGR))

#we have some real NA values left for DISCHARGE_NGR - so let's remove them


all_spills_2021<-all_spills_2021%>%
  filter(site != "FALSE")

#test<-all_spills_2021%>%
#  filter(str_length(DISCHARGE_NGR) != 12 | is.na(DISCHARGE_NGR))
#none without 12 character (10 digit) NGR



all_21<-all_spills_2021%>%
  mutate(
    easting = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[1])),
    northing = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[2]))
  )


#very long time to run - could make it a function to track progress?

#OR use a progress bar from pbapply


results <- pbapply::pblapply(all_spills_2021$DISCHARGE_NGR, function(x) {
  parsed <- osg_parse(x)
  return(c(as.numeric(parsed[1]), as.numeric(parsed[2])))
})

#pblapply helps - progress bar & showed 06m 24s elapsed to run the function

#extra step vs mutate() which is appending the data frame


eastnorth_df <- do.call(rbind, results)
colnames(eastnorth_df) <- c("easting", "northing")

all_spills_2021i <- cbind(all_spills_2021, eastnorth_df)


#any missing?

#missing<-all_spills_2021i%>%
#  filter(is.na(easting) | is.na(northing))

#none


#check validity of this pblapply application:

#r1<-all_spills_2021i[(1:20),]


#r2<-all_spills_2021[(1:20),]


#r2<-r2%>%
#  mutate(
#    easting = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[1])),
#    northing = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[2]))
#  )

#same




#get z dimension in spills data:

#now, this will be DAYS since 01-12-2020
#since we added dec 2020 and all months of 2021
all_21<-all_spills_2021i%>%
  mutate(
    Date = date(block_start),
    days = difftime(Date, as.POSIXct("01-12-2020", format = "%d-%m-%Y"), units = "days"),
    days = gsub(" days", "", days),
    zval = 1 + as.numeric(days)
  )%>%
  select(-c(days, Date))


#let's take a random sample of this data and check


rdmsample1<-sample_n(all_21, 20, replace = FALSE)%>%
  subset(select = c(block_start, days, zval))
#seems good


#now we add the x and y dimensions based on the eastings and northings


#load in x and y bounds
nc_i <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/rainfall_2020_12.nc"))

                
ybound <- ncvar_get(nc_i, "projection_y_coordinate_bnds")
xbound <- ncvar_get(nc_i, "projection_x_coordinate_bnds")

nc_close(nc_i)




all_21i <- all_21 %>%
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


#doesn't take long to run - so won't use pmap



#now we need to retrieve the useful rainfall data
#as written in task 5 (v1) script - 9 surrounding cells for previous 4 days are "useful"

# format: "rain_t[X1]_s[X2]
#[X1] will be a number 1 to 4, with 1 being rainfall data for the start date (date of the start of the spill),
#2 being the day prior, 3 being the day before that, and 4 the day before that
#[X2] will be a number 1 to 9, referring to the grid cell (of the 9 we find important), left to right, up to down,
#meaning the grid cell the spill site lies in will be labelled 5



#save(all_21i, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_21i.RData")


all_21_matched<-all_21i%>%
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

#some row was out of bounds

see1<-all_21i[12383,]
#issue on the time dimension

see2<-all_21i[(12380:12390),]

#yeah it starts at midnight new years. 

#need to remove these

#ohh i put <= should have just been <

all_21ii<-all_21i%>%
  filter(block_start < as.POSIXct("2022-01-01", format = "%Y-%m-%d" ))
#376,765 compared with 376,781 so around 120 on the cutoff


#let's try again


all_21_matched<-all_21ii%>%
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

#now an issue with row 306158

#returning a full time series for a point, not one time

see3<-all_21ii[(306158:306170),]
#a 2020 spill giving negative days (from wessex water)

all_21ii<-all_21ii%>%
  filter(block_start >= as.POSIXct("2020-12-01", format = "%Y-%m-%d" ))
#376764 vs 376765 so just the one out of time bounds

#clearly need to be more specific on the filtering at the start going forward

#attempt again...

all_21_matched<-all_21ii%>%
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

#save(all_21_matched, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_21_matched.RData")


#successfully matched them in this way

#now to define the spills as wet/dry using 4 different methods
#AND generate some rain variables (to be used as covariates?) - average and min/max rain 


#4 methods 
#1: exact 1km grid rain is less than 0.25mm ON THE SPILL START DATE
#2: the 9 surrounding squares ALL have less than 0.25mm rain ON THE SPILL DATE
#3: (EA) the 9 surrounding grid squares each have less than 0.25mm rain on the spill date and prior day
#4: (BBC) none of the 9 grid cells have more than 0.25mm rain on any of the 4 preceding days



#average rainfall variables

all_21_dry_spill<-all_21_matched%>%
  mutate(avg1 = rowMeans(.,select(rain_t1_s1, rain_t1_s2, rain_t1_s3, 
                                  rain_t1_s4, rain_t1_s5, rain_t1_s6,
                                  rain_t1_s7, rain_t1_s8, rain_t1_s9,),
                         na.rm = TRUE))


all_21_dry_spill <- all_21_matched %>%
  mutate(avg1 = rowMeans(select(., starts_with("rain_t1_s")), na.rm = TRUE))


testX<-all_21_matched[(1:4),]

testXi<-testX%>%
  rowwise() %>% 
  mutate(mean_rain_1 = mean(c_across(starts_with("rain_t1_s")), na.rm = TRUE))


testXi[,55]

#applied fine




all_21_dry_spill<-all_21_matched%>%
  rowwise() %>% 
  #mean_rain_1 is the mean rainfall that fell in the 9 cell area ON the start date
  mutate(mean_rain_1 = mean(c_across(starts_with("rain_t1_s")), na.rm = TRUE))%>%
  #mean_rain_2 is the mean rainfall of all 9 cells in the preceding 4 days
  mutate(mean_rain_2 = mean(c_across(starts_with("rain_t")), na.rm = TRUE))%>%
  #max_rain_1 is the max rainfall any of the 9 cells had ON the spill date
  mutate(max_rain_1 = max(c_across(starts_with("rain_t1_s")), na.rm = TRUE))%>%
  #max_rain_2 is the max rainfall of any of the 9 cells on the day or day before
  mutate(max_rain_2 = max(max_rain_1, max(c_across(starts_with("rain_t2_s")), na.rm = TRUE)))%>%
  #max_rain_3 is the maximum value of rainfall for any of the 9 cells in the 4 days
  mutate(max_rain_3= max(c_across(starts_with("rain_t")), na.rm = TRUE))%>%
  ungroup()


#a very slow function - need to add a progress tracker/bar

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


#now using pbapply to apply the function with a progress bar

library(pbapply)

#makes the progress bar - style 3 gives percentage completion and an end point
pb <- txtProgressBar(min = 0, max = nrow(all_21_matched), style = 3)

# Apply function row-wise with pbapply()
all_21_dry_spill <- as.data.frame(
  t(pbapply(all_21_matched, 1, function(row) {
    setTxtProgressBar(pb, which(row == all_21_matched[nrow(all_21_matched), ]))  # Update progress
    rainfall_stats(row)
  }))
)

# Close progress bar
close(pb)




pb <- txtProgressBar(min = 0, max = nrow(all_21_matched), style = 3)

counter <- 0  # Initialize counter

all_21_dry_spill <- as.data.frame(
  t(pbapply(all_21_matched, 1, function(row) {
    counter <<- counter + 1  # Increment counter
    setTxtProgressBar(pb, counter)  # Update progress bar
    rainfall_stats(row)
  }))
)

close(pb) 


#i get dry spills dataframe now - but it is separate to the matched data - so need to append.

#some missing values

missing_in_column_inf <- all_21_dry_spill %>% filter(mean_rain_1 == -Inf | is.na(mean_rain_1))


#1845 that are missing - NA for means, -Inf for max

#let's look at these

problem_index<-which(all_21_dry_spill$max_rain_1 == -Inf | is.na(all_21_dry_spill$mean_rain_1))


problems<-all_21_matched[problem_index,]

unique_problems<-problems%>%
  distinct(permit_reference, .keep_all = T)

#there are 30 sites (in 2021 data) that do not have any matching rainfall data


#a lot have recieving water as seas or english channel - so perhaps are out of the rainfall data bounds
#a couple however are discharging into rivers, so you'd think there's associated rainfalld data


#some have spaces - though osg_parse() seems robust to this (may want to remove when using all data anyway)


#example SX 6910 4729 discharging into river avon estuary

#yes it's on land. I see some issue, perhaps due to the spaces - leaves northing reduced by x10


#a different example 	SZ6288082450 - discharging into the Channel

#yes in water - so it makes sense we cannot have rainfall for this


#the ones with the rivers discharging seem to be fewer - and each has spaces in the discharge ngr


all_spaces<-all_21_matched%>%
  filter(grepl(" ", DISCHARGE_NGR))

all_spaces_unique<-all_spaces%>%
  distinct(permit_reference, .keep_all = T)




problem_test<-unique_problems%>%
  select(DISCHARGE_NGR, site, RECEIVING_WATER, easting, northing, xval, yval)

problem_test_spaces<-problem_test%>%
  filter(grepl(" ", DISCHARGE_NGR))

#very similar list to those with spaces from all spills - so spaces are likely a cause of the issue


problem_test_1<-problem_test%>%
  mutate(DISCHARGE_NGR = gsub("\\s+", " ", DISCHARGE_NGR))%>%
  mutate(DISCHARGE_NGR = gsub(" ", "", DISCHARGE_NGR))


#ah I see. Another issue is that they have fewer digits in the Discharge NGR. 
#these are the less precise ones that were in there - but due to the spaces - they had the same number of characters so weren't highlighted before



problem_test_1<-problem_test_1%>%
  rowwise()%>%
  mutate(
    neweasting = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[1])),
    newnorthing = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[2]))
  )%>%
  ungroup()



#this could solve the issue of matching for these cases

#let's save the useful data we did create and restart on v3...


all_21_dry_spill_v1<-cbind(all_21_matched, all_21_dry_spill)

#save(all_21_dry_spill_v1, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/all_21_matched.RData")


