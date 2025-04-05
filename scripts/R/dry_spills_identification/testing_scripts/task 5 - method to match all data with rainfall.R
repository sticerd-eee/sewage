
#rm(list = ls())
#gc()


library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)


#task 5 - working out how to match ALL data & identify dry spills

# -	Match Anglian 2023 January with the 9 surrounding cells, for previous 4 days – and add a column for maximum; also average
# -	Add two columns (binary): one that indicated whether a dry spill via simple EA definition (ignoring differences in drain down / catchment area); one with BBC’s exact methodology
# -	Compare the results in number of dry spills labelled
# -	Make a system to referencing each month’s rainfall data (options: merge arrays via z dimension or make a common naming system)
# -	Ensure x and y dimensions are the same for all rainfall months (should be) – check x and y projection bound matrices
# -	Match all data – have column for each dry spill definition, and average, min, max rainfall in last 4 days, also average, min, max rainfall on the day & preceding day



#Matching all 9 grid cells for 4 days to Anglian data:

#set DropBox path to data:

dp_path<- "C:/Users/danan/Dropbox/sewage/data/"


library(ncdf4)
#open january 2023 rainfall data
nc_jan23 <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/hadukjan23.nc"))


#retrieve data
rainfall_jan23 <- ncvar_get(nc_jan23, "rainfall")

ybound <- ncvar_get(nc_jan23, "projection_y_coordinate_bnds")
xbound <- ncvar_get(nc_jan23, "projection_x_coordinate_bnds")

nc_close(nc_jan23)




#load spill data and get anglian for january 2023
load(paste0(dp_path, "processed/1224_processed_data_matched/all_spills_1224.RData"))



anglian23jan<-all_spills_1224%>%
  filter(wasc == "anglian" &
           block_start >= as.POSIXct("2023-01-01", format = "%Y-%m-%d") & 
           block_start <= as.POSIXct("2023-02-01", format = "%Y-%m-%d" ))

rm(all_spills_1224)




#get easting and northing of Anglian spill sites
library(rnrfa)

a_jan23<-anglian23jan%>%
  mutate(
    easting = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[1])),
    northing = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[2]))
  )


#get z dimension in spills data
a_jan23i<-a_jan23%>%
  mutate(
    Date = date(block_start),
    days = difftime(Date, as.POSIXct("01-01-2023", format = "%d-%m-%Y"), units = "days"),
    days = gsub(" days", "", days),
    zval = 1 + as.numeric(days)
  )%>%
  select(-c(days, Date))




#get x and y dimensions for each spill site
a_jan23ii <- a_jan23i %>%
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
  )%>%
  unnest(cols = c(xval))



#Now we want a dataframe with ALL the rainfall data we might need to define the dry spills

#9 surrounding 1km grid cells,
#4 days incl spill start date



#retrieving the rainfall data
#also giving each rainfall data column a useful name
# format: "rain_t[X1]_s[X2]
#[X1] will be a number 1 to 4, with 1 being rainfall data for the start date (date of the start of the spill),
#2 being the day prior, 3 being the day before that, and 4 the day before that
#[X2] will be a number 1 to 9, referring to the grid cell (of the 9 we find important), left to right, up to down,
#meaning the grid cell the spill site lies in will be labelled 5

a_jan23matched<-a_jan23ii%>%
  rowwise()%>%
  mutate(
    rain_t1_s1 = rainfall_jan23[xval-1,yval+1,zval],
    rain_t1_s2 = rainfall_jan23[xval, yval+1, zval],
    rain_t1_s3 = rainfall_jan23[xval+1, yval+1, zval],
    
    rain_t1_s4 = rainfall_jan23[xval-1, yval, zval],
    rain_t1_s5 = rainfall_jan23[xval, yval, zval],
    rain_t1_s6 = rainfall_jan23[xval+1, yval, zval],
    
    rain_t1_s7 = rainfall_jan23[xval-1, yval-1, zval],
    rain_t1_s8 = rainfall_jan23[xval, yval-1, zval],
    rain_t1_s9 = rainfall_jan23[xval+1, yval-1, zval]#,
    
    
    #rain_t2_s1 = rainfall_jan23[xval-1,yval+1,zval-1],
    #rain_t2_s2 = rainfall_jan23[xval, yval+1, zval-1],
    #rain_t2_s3 = rainfall_jan23[xval+1, yval+1, zval-1],
    
    #as i'm writing this, I realise that as we take away days,
    #we need to retrieve data from previous month
    #so seems the best way to match might be appending all the rainfall arrays via z dimension
    #would need to work out the days since 1st Deecember 2020?
    #in this case we might run into issues with how difftime() behaves when it crosses DST changes (no leap years to worry about in our data)
    
  )




#some have NA in some of the surrounding squares - so need to check these too


some_missing<-a_jan23matched%>%
  filter(is.na(rain_t1_s1) |
           is.na(rain_t1_s2) |
           is.na(rain_t1_s3) |
           is.na(rain_t1_s4) |
           is.na(rain_t1_s5) |
           is.na(rain_t1_s6) |
           is.na(rain_t1_s7) |
           is.na(rain_t1_s8) |
           is.na(rain_t1_s9))

#204 of these observations

unique_with_missing<-some_missing%>%
  distinct(permit_reference, .keep_all = T)
#51 sites have this

#a few of these have missing rainfall data on their exact location too
#i wonder aobout them - should check soon if that makes any sense




#let's do one year of data, for all wascs (so 2021 makes sense to try)






rm(list = ls())
gc()


library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(ncdf4)
library(rnrfa)

#set DropBox path to data:

dp_path<- "C:/Users/danan/Dropbox/sewage/data/"


#rainfall_YYYY_MM
#^ the naming system I've given rainfall data


nc_20_12 <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/rainfall_2020_12.nc"))



rainfall_2020_12 <- ncvar_get(nc_20_12, "rainfall")

nc_close(nc_20_12)


nc_21_01 <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/rainfall_2021_01.nc"))



rainfall_2021_01 <- ncvar_get(nc_21_01, "rainfall")

nc_close(nc_21_01)

library(abind)

rain1<-abind(rainfall_2020_12, rainfall_2021_01, along = 3)
#dim(rain1)
#[1]  900 1450   62


#free RAM
rm(nc_20_12, nc_21_01, rainfall_2020_12, rainfall_2021_01)
gc()



nc_21_02 <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/rainfall_2021_02.nc"))



rainfall_2021_02 <- ncvar_get(nc_21_02, "rainfall")

nc_close(nc_21_02)


rain1<-abind(rain1, rainfall_2021_02, along = 3)
#dim(rain1)
#[1]  900 1450   90


rm(nc_21_02, rainfall_2021_02)
gc()



#I understand how to merge these data now. 
#I will do it programmatically from here






rm(list = ls())
gc()


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
  }
  
  # Return the combined array
  return(rain_combined)
}



rain1 <- combine_rainfall(file_paths)

# Check dimensions of the result
print(dim(rain1))
#900 1450 396
#exactly the dimensions we expect (365 days of 2021, plus 31 days of Dec 2020)

#function isn't optimized for efficiency probably but not bad

#save(rain1, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/dry spills/rain1.RData")



#now to match 2021 data, then identify dry spills





#load spill data and get 2021 data specifically
load(paste0(dp_path, "processed/1224_processed_data_matched/all_spills_1224.RData"))



all_spills_2021<-all_spills_1224%>%
  filter(block_start <= as.POSIXct("2022-01-01", format = "%Y-%m-%d" ))

rm(all_spills_1224)



all_21<-all_spills_2021%>%
  rowwise()%>%
  mutate(
    easting = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[1])),
    northing = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[2]))
  )


#get z dimension in spills data
all_21<-all_21%>%
  mutate(
    Date = date(block_start),
    days = difftime(Date, as.POSIXct("01-01-2023", format = "%d-%m-%Y"), units = "days"),
    days = gsub(" days", "", days),
    zval = 1 + as.numeric(days)
  )%>%
  select(-c(days, Date))




#get x and y dimensions for each spill site
all_21 <- all_21 %>%
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
  )%>%
  unnest(cols = c(xval))

#slow functions - likely the osg_parse() function
#rowwise() or parallel application could speed it up



#error encountered



#the error lies with this function


#all_21<-all_spills_2021%>%
#  rowwise()%>%
#  mutate(
#    easting = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[1])),
#    northing = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[2]))
#  )


#the length of the returned "vector" differs, is either 0 or 1.
#So some empty vectors?

#Check the length of each of discharge_ngr

unique(str_length(all_spills_2021$DISCHARGE_NGR))

#[1] 12 27 26 69 13 NA 41
#wait what??


check<-all_spills_2021%>%
  filter(str_length(DISCHARGE_NGR) == 27)


#NZ3694368873 / NZ3694268961
#this is strange


#so we need to look at all of these cases

check<-all_spills_2021%>%
  filter(str_length(DISCHARGE_NGR) != 12 |
           is.na(DISCHARGE_NGR))
#324 observations


#ok only slight variety and some NAs from true missing values

#will correct these, then see if the application of osg_parse encounters any more errors:


unique(check$DISCHARGE_NGR)
#7 causing issues

#"NZ3694368873 / NZ3694268961"                                          
#[2] "NU0228749514, NU0236149575"                                           
#[3] "NZ1923451825 (to a culverted watercourse which opens at NZ1972351654)"
#[4] "NZ1646227588 & NZ1646527572"                                          
#[5] "SP 5020076350"                                                        
#[6] NA                                                                     
#[7] "SK 38518 90505 Not from consents database" 

#since it's so few - easiest to just deal with each individually

check_index<-which(str_length(all_spills_2021$DISCHARGE_NGR) != 12 |
                     is.na(all_spills_2021$DISCHARGE_NGR))

all_spills_2021i <- all_spills_2021 %>%
  mutate(DISCHARGE_NGR = trimws(DISCHARGE_NGR)) %>%
  mutate(check1 = gsub("\\s+", " ", DISCHARGE_NGR)) %>% # Normalize spaces
  mutate(check1 = gsub("NZ3694368873 / NZ3694268961", "NZ3694368873", check1)) %>%
  mutate(check1 = gsub("NU0228749514, NU0236149575", "NU0228749514", check1)) %>%
  mutate(check1 = gsub("NZ1923451825 \\(to a culverted watercourse which opens at NZ1972351654\\)", "NZ1923451825", check1)) %>%
  mutate(check1 = gsub("NZ1646227588 & NZ1646527572", "NZ1646227588", check1)) %>%
  mutate(check1 = gsub("SP 5020076350", "SP5020076350", check1)) %>%
  mutate(check1 = gsub("SK 38518 90505 Not from consents database", "SK3851890505", check1))


test<-all_spills_2021i[check_index,]

test2<-test%>%
  filter(str_length(check1) != 12 | is.na(check1))

#only the true missing values left



#now we can continue back to applying osg_parse() to extract the eastings / northings



#move onto task 5 v2 script


