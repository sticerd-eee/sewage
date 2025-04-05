#rm(list = ls())
#gc()

#task 4 - match weather data to one company for one month

# ~ this was the initial aim, but task 2 went well, so we can be confident in our matching method
# ~ so aim is to match 1 company for 1 month
# ~ but not only to it's weather data (specific grid)
# ~ we also want to move closer to dry spill definition
# ~ so get the rainfall data for all 9 surrounding grid cells too

#4.1. match one month, one company to it's speciifc grid cell's rainfall data
#4.2. match with all 9 surrounding grid cell's weather data
#4.3. match with all 9 AND previous day[s] according to BBC methodology



library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)

#4.1:


#Set DropBox file path to data:

dp_path<- "C:/Users/danan/Dropbox/sewage/data/"



library(ncdf4)
#open january 2023 rainfall data
nc_jan23 <- nc_open(paste0(dp_path, "/raw/haduk_rainfall_data/hadukjan23.nc"))


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


#anglian has 2606 spills (by 12 24 definition) in Jan 23

#get easting and northing of Anglian spill sites


library(rnrfa)

a_jan23<-anglian23jan%>%
  mutate(
    easting = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[1])),
    northing = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[2]))
  )
#(took a few seconds to run - may be slow over all data)


#add days for z dimension
a_jan23i<-a_jan23%>%
  mutate(
    Date = date(block_start),
    days = difftime(Date, as.POSIXct("01-01-2023", format = "%d-%m-%Y"), units = "days"),
    days = gsub(" days", "", days),
    zval = 1 + as.numeric(days)
  )%>%
  select(-c(days, Date))
#(assign as different df to keep track of any errors)



#find out the grid cells containing each spill site
a_jan23ii <- a_jan23i %>%
  rowwise() %>%
  mutate(
    xval = list(which(xbound[1, ] <= easting & xbound[2, ] >= easting)),
    yval = list(which(ybound[1, ] <= northing & ybound[2, ] >= northing))
  ) %>%
  unnest(cols = c(xval, yval))

#we got extra rows for some reason?????



#any missing?
check_na<-a_jan23ii%>%
  filter(is.na(xval) | is.na(yval) | is.na(zval))
#none missing


#retrieve rainfall value for the grid cell on the start date
a_jan23matched<-a_jan23ii%>%
  mutate(
    rain = rainfall_jan23[xval,yval,zval]
  )
#ERROR - from argument "rain = rainfall_jan23[xval, yval, zval]
#cannot allocate vector of size 144.5 Gb

#this method of matching takes too much memory for R
#need to make it more efficient...

#this is the efficiency consideration planned for task 5
#do we extract the vector at specific points for each site location?
#i.e for each unique compbination of x and y we have, we take the 31 z values and just match the z to spill data
#instead of looking for x an dy uniquely for each observation AND z 


#trying purrr
library(purrr)

a_jan23matchedx <- a_jan23ii %>%
  mutate(
    rain = map2_dbl(xval, yval, ~ rainfall_jan23[.x, .y, zval])
  )



#need to redo the xval and yval creation and application to months data to retrieve rainfall
#since we're generating extra rows somehow,
#and then when matching, we're taking too much memory


#....


rm(list = ls())
gc()


library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)

#4.1:

dp_path<- "C:/Users/danan/Dropbox/sewage/data/"



library(ncdf4)
#open january 2023 rainfall data
nc_jan23 <- nc_open(paste0(dp_path, "/raw/haduk_rainfall_data/hadukjan23.nc"))


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


#anglian has 2606 spills (by 12 24 definition) in Jan 23

#get easting and northing of Anglian spill sites


library(rnrfa)

a_jan23<-anglian23jan%>%
  mutate(
    easting = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[1])),
    northing = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[2]))
  )
#(took a few seconds to run - may be slow over all data)


#add days for z dimension
a_jan23i<-a_jan23%>%
  mutate(
    Date = date(block_start),
    days = difftime(Date, as.POSIXct("01-01-2023", format = "%d-%m-%Y"), units = "days"),
    days = gsub(" days", "", days),
    zval = 1 + as.numeric(days)
  )%>%
  select(-c(days, Date))
#(assign as different df to keep track of any errors)



#the 1st problematic code was here:
#a_jan23ii <- a_jan23i %>%
#  rowwise() %>%
#  mutate(
#    xval = list(which(xbound[1, ] <= easting & xbound[2, ] >= easting)),
#    yval = list(which(ybound[1, ] <= northing & ybound[2, ] >= northing))
#  ) %>%
#  unnest(cols = c(xval, yval))

#we went from 2606 observations to 2687

#why? - we know none of the bounds overlap in the xbound or ybound matrices

#it may be that a spill site lands exactly ON a boundary - eg Easting = 200 000 

#let's check for multiples of 1000

edge_cases <- a_jan23i %>%
  filter(easting %% 1000 == 0 | northing %% 1000 == 0)
#81 - which is exactly the number of additonal observations we generated

#so now we need to think logically about how to handle these esge cases

#assigning to the right/left or upper/lower grid cell is equally valid
#it will matter slightly less when we take into account the 9 surrounding cells, as there will be overlap






#aassign edge cases to the right cell, or upper cell
#reasonable given:
#the 9 surrounding grid cells (used in BBC methodology) will have 6 in common whether we choose left/right or upper/lower
#the daily rainfall data for adjacent grid cells is likely very similar


#how do i know if adding northing / easting makes it go up & right?


a_jan23ii <- a_jan23i %>%
  rowwise() %>%
  mutate(
    xval = list(which(xbound[1, ] <= easting & xbound[2, ] >= easting)),
    yval = list(which(ybound[1, ] <= northing & ybound[2, ] >= northing))
  ) %>%
  unnest(cols = c(xval, yval))


a_jan23ii <- a_jan23i %>%
  rowwise() %>%
  mutate(
    xval = ifelse(easting%% 1000 == 0,
                  list(which(xbound[1, ] <= easting+1 & xbound[2, ] >= easting+1)),
                  list(which(xbound[1, ] <= easting & xbound[2, ] >= easting))
    )
  )%>%
  unnest(cols = c(xval))
#no more extra rows

#can apply to yval too

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


#check one edge case then do the check properly

check<-a_jan23ii%>%
  filter(permit_reference == "AECTS11056")


#yeah looks good


#so next - once we make that code fully modular - is to tackle the application of our logic to all our data

#so that may involve pmap or rowwise


a_jan23matched<-a_jan23ii%>%
  rowwise()%>%
  mutate(
    rain = rainfall_jan23[xval,yval,zval]
  )


#oh did that fix it immediately?

#i wonder why


#we should test output to see validity though


validity<-a_jan23matched%>%
  filter(permit_reference=="AECNF1004")
#just 5 rows - simple enough to check
print(validity$rain)


rainfall_jan23[803, 496, 10]
#it's retrieving the values correctly at least

#just check once more the x and y have comparable lat/lon
#then we can confidently apply to the rest of the data, 
#and make minor changes to use the BBC's logic


#next major task will be to define the dry spills









