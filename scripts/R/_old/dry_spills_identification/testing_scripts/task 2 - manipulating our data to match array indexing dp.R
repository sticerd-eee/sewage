#rm(list = ls())
#gc()

#task 2 - manipulating our data to match array indexing

library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)

#Set DropBox file path to data:

dp_path<- "C:/Users/danan/Dropbox/sewage/data/"




#load in January 2023 rainfall data (next task will log exactly how to deal with this file type)
library(ncdf4)

nc_data <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/hadukjan23.nc"))

names(nc_data$var)

#rainfall is what we want

#but the other data might be important to extract:

#[1] "rainfall"                     "transverse_mercator"          "time_bnds"                   
#[4] "projection_y_coordinate_bnds" "projection_x_coordinate_bnds" "latitude"                    
#[7] "longitude" 


rainfall1 <- ncvar_get(nc_data, "rainfall")
grid1 <- ncvar_get(nc_data, "transverse_mercator")
#gives a value (metadata for the grid type)
timebound <-ncvar_get(nc_data, "time_bnds")
ybound <- ncvar_get(nc_data, "projection_y_coordinate_bnds")
xbound <- ncvar_get(nc_data, "projection_x_coordinate_bnds")
lat<- ncvar_get(nc_data, "latitude")
#comes as a matrix for some reason
lon<- ncvar_get(nc_data, "longitude")


nc_close(nc_data)



#so let's observe the metadata given, and our resulting array dimensions:


#the latitude and longitude are given as matrices - dim [900, 1450]

#time bound is given as a 2 by 31 populated by some values - cannot interpret right now

#x bound is given as a 2 by 900, y bound given as a 2 by 1450 - relates to longitude and latitude dimenstions too
#but again - populated by numbers I cannot yet interpret, in this case all are large and begative



dim(rainfall1)
#[1]  900 1450   31
#if this is the case - 900 meters one way and 1450 the other - then how do these refer to 1 by 1 grids?

# so these relate to x, y and days since the start of the month


#testing where values are in the array
#(the x and y dimensions refer to some grid size leading to lots of empty values)


rainfall1[500,500,1]
#5.727674 ~ so yes, this isn't an empty value

#so we need to find out how to refer to these dimensions, to extract the values we want.


#checking national grid reference of spills:
load(paste0(dp_path, "processed/1224_processed_data_matched/all_spills_1224.RData"))

anglian23jan<-all_spills_1224%>%
  filter(wasc == "anglian" &
           block_start >= as.POSIXct("2023-01-01", format = "%Y-%m-%d") & 
           block_start <= as.POSIXct("2023-02-01", format = "%Y-%m-%d" ))

rm(all_spills_1224)

s1<-anglian23jan[1,]

s1<-s1%>%
  mutate(gridref = gsub("(.{4})(.{3})(.{2})(.{3})(.*)", "\\1\\4\\5", DISCHARGE_NGR))


s1<-s1%>%
  mutate(grid1 = substr(DISCHARGE_NGR, 1, 4))%>%
  mutate(grid2 = substr(DISCHARGE_NGR, 8, 9))%>%
  mutate(gridref = paste(grid1, grid2, sep = ""))

#TM0295

#apparently this is 202km Eastings and 595km Northings

rainfall1[202, 595, 10]

#but this is NA 


rainfall1[595, 202, 10]

#also NA



#let's try the TR6740 from before

#Eastings - 467, northings - 340

rainfall1[467, 340, 1]
#0.7531591

#at least it is populated

#how can we find out if this is the right approach?



rainfall1[467, 340, 10]
#again - populated


#but how to check?




#TM0295
#https://gridreferencefinder.com/#gr=SV0000000000|TR|1,TR6700040000|TR6740|1,TM0200095000|TM0295|1
#says this is 602, 295 - not same as Chat GPT

rainfall1[602, 295, 10]
#3.208905
# - so maybe the conversion before was wrong - need the exact way to do it


#conversion to eastings & northings:

#install.packages("rnrfa")

#spill data exploration - qmd <- check thiss
library(rnrfa)
test <- osg_parse(s1$DISCHARGE_NGR)



s1<-s1%>%
  mutate(easting = sapply(osg_parse(s1$DISCHARGE_NGR), "[[",1))



s1<-s1%>%
  mutate(easting = osg_parse(s1$DISCHARGE_NGR))



# Use mutate to add new columns

s1i <- s1 %>%
  mutate(
    easting = sapply(gridref, function(x) osg_parse(x)[1]),
    northing = sapply(gridref, function(x) osg_parse(x)[2])
  )


#so we can access the rainfall data simply using this
#but we have to make sure the eastings and northings correspond to the x and y dimensions
#of the rainfall data with certainty



rm(list = ls())
gc()


library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)

#load in January 2023 rainfall data (next task will log exactly how to deal with this file type)
library(ncdf4)

nc_data <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/hadukjan23.nc"))


names(nc_data$var)


#let's look at the data attributes:

ncatt_get(nc_data, 0)
#$comment
#[1] "Daily resolution gridded climate observations"

#$creation_date
#[1] "2024-05-17T15:01:37"

#$frequency
#[1] "day"

#$institution
#[1] "Met Office"

#$references
#[1] "doi: 10.1002/gdj3.78"

#$short_name
#[1] "daily_rainfall"

#$source
#[1] "HadUK-Grid_v1.3.0.0"

#$title
#[1] "Gridded surface climate observations data for the UK"

#$version
#[1] "v20240514"

#$Conventions
#[1] "CF-1.7"


ncatt_get(nc_data, "rainfall")
#$`_FillValue`
#[1] 1e+20

#$standard_name
#[1] "lwe_thickness_of_precipitation_amount"

#$long_name
#[1] "Total rainfall"

#$units
#[1] "mm"

#$description
#[1] "Total rainfall"

#$label_units
#[1] "mm"

#$level
#[1] "NA"

#$plot_label
#[1] "Total rainfall (mm)"

#$cell_methods
#[1] "time: sum"

#$grid_mapping
#[1] "transverse_mercator"

#$coordinates
#[1] "latitude longitude"


ncatt_get(nc_data, "transverse_mercator")
#$grid_mapping_name
#[1] "transverse_mercator"

#$longitude_of_prime_meridian
#[1] 0

#$semi_major_axis
#[1] 6377563

#$semi_minor_axis
#[1] 6356257

#$longitude_of_central_meridian
#[1] -2

#$latitude_of_projection_origin
#[1] 49

#$false_easting
#[1] 4e+05

#$false_northing
#[1] -1e+05

#$scale_factor_at_central_meridian
#[1] 0.9996013



#so we can assume that the dimensions match.
#come back to this assumption if matching fails or looks incorrect








rainfall1 <- ncvar_get(nc_data, "rainfall")


nc_close(nc_data)



load(paste0(dp_path, "processed/1224_processed_data_matched/all_spills_1224.RData"))



anglian23jan<-all_spills_1224%>%
  filter(wasc == "anglian" &
           block_start >= as.POSIXct("2023-01-01", format = "%Y-%m-%d") & 
           block_start <= as.POSIXct("2023-02-01", format = "%Y-%m-%d" ))

rm(all_spills_1224)

anglian23jan<-anglian23jan%>%
  arrange(block_start)

t1<-anglian23jan[(1:10),]


#so now we have 10 observations to test spatial matching with

t1<-t1%>%
  mutate(first2 = substr(DISCHARGE_NGR, 1, 2))%>%
  mutate(digits = as.character(substr(DISCHARGE_NGR, 3, nchar(DISCHARGE_NGR))))%>%
  mutate(length1 = nchar(digits))%>%
  mutate(lengthhalf = floor(length1/2))%>%
  mutate(half1 = as.character(substr(digits, 1, lengthhalf)))%>%
  mutate(half2 = as.character(substr(digits, lengthhalf + 1, nchar(digits))))%>%
  mutate(match_ngr = paste(first2, substr(half1, 1, 2), substr(half2, 1, 2), sep = ""))%>%
  select(-c(first2, digits, length1, lengthhalf, half1, half2))


library(rnrfa)

t1i <- t1 %>%
  mutate(
    easting = as.numeric(sapply(match_ngr, function(x) osg_parse(x)[1])),
    northing = as.numeric(sapply(match_ngr, function(x) osg_parse(x)[2])),
    x = easting/1000,
    y = northing/1000
  )


#so x and y should match with x and y in the array rainfall data


#we also need time dimension to match

#so logic like difftime(, units = "days") then string change & as.numeric 
#but remove any rounding by taking the day out of block_start date-time object first


t1ii<-t1i%>%
  mutate(
    date = date(block_start),
    days = difftime(block_start, as.POSIXct("01-01-2023", format = "%d-%m-%Y"), units = "days"),
    days = gsub(" days", "", days),
    z = 1 + as.numeric(days)
  )%>%
  select(-c(days, date))


#so now we have x, y and z that should match the array



test1<-t1ii%>%
  mutate(rain = rainfall1[x,y,z])


#3 / 10 are missing values
#hard to verify if these are missing as genuine missing values
#or if they are missing due to my understanding of the array and x, y, z dimensions' meaning is wrong

#first way to just check this - we can look on a map and see where the points are
#why wouldn't there be rainfall data there

rainfall1[539, 287,]
#all NA at this point

#surrunding?
rainfall1[540, 287,]
#NAs

rainfall1[538, 287,]

rainfall1[540, 288,]

rainfall1[539, 288,]

#all surrounding is NA too...


#have verified this point lies on land
#and i think the rainfall data should cover it
#so issue with my understanding of the array?


#i will check the latitude and longitude values from the nc_data

nc_data <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/hadukjan23.nc"))



names(nc_data$var)


lon<-ncvar_get(nc_data, "longitude")

lat<-ncvar_get(nc_data, "latitude")

nc_close(nc_data)


#lets look at what the HadUK dataset (for rainfall) gives as latitude and longitude values for this point
#(point TL3987)
#and compare this with our OS map 
#https://gridreferencefinder.com/#gr=TL3900087000|TL3987|1

lon[539, 287]
#-2.870459

lat[539, 287]
#50.67433


#the website gives
#Latitude , Longitude (degs, mins, secs)
#52°27′48″N , 000°02′40″E

# in size - seem somewhat similar maybe? (not sure how close a degree or 2 difference makes)
# both are 2 degrees larger (i think?) on the website

#this is a good conversion site
#https://webapps.bgs.ac.uk/data/webservices/convertForm.cfm

#Easting: 338615
#Longitude: -2.870000
#Lng DecMinSec: -2° 52' 12.00"

#Northing: 85954
#Latitude: 50.670000
#Lat DecMinSec: 50° 40' 12.00"


#^ results from inputting the given latitude and longitude from the rainfall data at [539, 287]

#which gives SY386859 grid reference (off the Southern Coast - NOT NEAR THE SPILL SITE)

#so our understanding that the x and y dimensions refer to the easting/northing is incorrect


#need to find another way to match them
#perhaps by using the latitude and longitude matrices?
#if so how do we go about this?



#one latitude matrix for example let's take a value we see -> 48.37484
#how many squares have this value?


matching_indices <- which(lat == 48.37484, arr.ind = TRUE)
#no data in table?????

lat[38, 882]
#55.69873

matching_indices <- which(lat == 55.69873, arr.ind = TRUE)
#no data.
#even though it's there


tolerance <- 1e-6

# Find indices where the matrix values are approximately equal to the target
matching_indices <- which(abs(lat - 55.69873) < tolerance, arr.ind = TRUE)
#[38, 882] as required.
#But why is there only one value that corresponds to this latitude value??

#not every place with the same latitude should have the same x and y dimensions
#cause they should have varying longitude values and hence be different places
#with different rainfall values

#why might it be the case that every single grid square has a slightly different latitude and longitude?




#well - let's continue with our assumption that the longitude and latitude matrices are the only way to match our data to 
#the rainfall data...


#process would be use the full 10 digit NGR in spills data to get latitude and longitude
#find the closest match to each in the longitude and latitude matrices
#use the overlap in these closest matches to find out the x and y values
#use these to retrieve the rainfall data...



#back to t1 (10 observations) data

#let's see if osg_parse can correctly transform ngr to lat/lon

t1x<-t1%>%
  mutate(
    lon = sapply(DISCHARGE_NGR, function(x) osg_parse(x, coord_system = "WGS84")[1]),
    lat = sapply(DISCHARGE_NGR, function(x) osg_parse(x, coord_system = "WGS84")[2])
  )

#well it gives some values - let's try to verify them...
#https://gridreferencefinder.com/#gr=TL3950087200|TL3950087200|1
#yes seem correct

#now let's find closest match to these in the latitude longitude matrices...


#seeing with first observation:
#52.4649277696457 - lat
#0.0519947735671573 - lon

#latitude first

#exact match?
tolerance <- 1e-6

# Find indices where the matrix values are approximately equal to the target
matching_indices <- which(abs(lat - 52.4649277696457) < tolerance, arr.ind = TRUE)

#none

#higher tolerance?

tolerance <- 1e-4
matching_indices <- which(abs(lat - 52.4649277696457) < tolerance, arr.ind = TRUE)
#23 close


tolerance <- 1e-5
matching_indices <- which(abs(lat - 52.4649277696457) < tolerance, arr.ind = TRUE)
#none


#hmmm
#what about closest match
abs_differences <- abs(lat - 52.4649277696457)

# Find the indices of the minimum difference
closest_indices <- which(abs_differences == min(abs_differences), arr.ind = TRUE)
#2 equally close

# - [552, 486] and [649, 486] - both not the same as our x and y directly from ngr..


#so for longitude - is there an overlap??

abs_differences2 <- abs(lon - 0.0519947735671573)

# Find the indices of the minimum difference
closest_indices2 <- which(abs_differences2 == min(abs_differences2), arr.ind = TRUE)

# ~ [742, 413] - does not match what we see for latitude. Not acutally even close to the latitude ones
#So we cannot match on lat/lon either?



#HadUK is produced on the national grid
#so spatial joins should be simple
#given we also have the national grid reference for each of the spill sites
#so why is it not clearly matching?
#how can i learn exactly what the x and y dimensions in the array data are?


#let's download some data at a different resolution (eg 5km squared) and see the dimensions for this...




#5km grid for jan 2023

nc_5km <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/haduk5kmgridjan23.nc"))


names(nc_5km$var)


rain5km<-ncvar_get(nc_5km, "rainfall")

lon5km<-ncvar_get(nc_5km, "longitude")

lat5km<-ncvar_get(nc_5km, "latitude")

nc_close(nc_5km)


dim(rain5km)
#180 290 31

#each x and y dimension is precisely 5x smaller than the 1km grid
#so yes, x and y do refer spatially to EACH grid square


#we should look at lat/lon of square [1,1]
#these should be similar


lat[1,1]
#47.82438
lon[1,1]
#-10.01291

lat5km[1,1]
#47.84405
lon5km[1,1]
#-9.9892

#so yes - similar.

#I assume that is empty (no rainfall data)?
rainfall1[1,1,]
#yes all NA

#this reveals something though

#x and y cannot refer to the eastings / northings of the centre point.

#if they did - then the 5km and 1km should have the same dimensions - 5km would just have more missing values

#the arrays are simply efficient ways to store the rainfall data



#going to look again at all the variables from the NetCDF file
#then jsut try to plot it and see if that helps...



#something about the x and y bounds are interesting to me
#let's see them in the 5km data too



nc_5km <- nc_open(paste0(dp_path, "raw/haduk_rainfall_data/haduk5kmgridjan23.nc"))

ybound5km <- ncvar_get(nc_5km, "projection_y_coordinate_bnds")
xbound5km <- ncvar_get(nc_5km, "projection_x_coordinate_bnds")

nc_close(nc_5km)


#if my suspicion is right - that these bounds give the easting or northing bound for each x and y value
#then that should be clear when i check the first box on each

#first 1 by 1 box "bounds"
xbound[,1]
# -200000 , -199 000
ybound[,1]
# -200000, -199 000

#perhaps yes - this is 1km difference each direction - promising so far...

#first 5 by 5 box "bounds"
xbound5km[,1]
# - 200000 , -195000
ybound5km[,1]
# -200000, -195000

#PERFECT ! 

#OK so best way to match is using the eastings and Northings we found
#and finding the bounds within which each observation lies
#from these we can get the x and y dimensions that correspond with the rainfall array
#and simply retrieve the rainfall value

xbound[,895]

matching_xbound <- which(xbound[1, ] <= 694433 & xbound[2, ] >= 694433, arr.ind = TRUE)

#so yes, this can retrieve the value


#let's go back to t1 and apply this to these 10 observations
#then we can check the logic




t1z<-t1%>%
  mutate(
    easting = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[1])),
    northing = as.numeric(sapply(DISCHARGE_NGR, function(x) osg_parse(x)[2]))
  )


t1zi<-t1z%>%
  mutate(
    xval = which(xbound[1, ] <= easting & xbound[2, ] >= easting, arr.ind = TRUE),
    yval = which(ybound[1, ] <= northing & ybound[2, ] >= northing, arr.ind = TRUE)
  )


#error

t1zi <- t1z %>%
  rowwise() %>%
  mutate(
    xval = list(which(xbound[1, ] <= easting & xbound[2, ] >= easting)),
    yval = list(which(ybound[1, ] <= northing & ybound[2, ] >= northing))
  ) %>%
  unnest(cols = c(xval, yval))

#well it looks as though it may have worked?

#add in z dimension (time)

t1zii<-t1zi%>%
  mutate(
    date = date(block_start),
    days = difftime(block_start, as.POSIXct("01-01-2023", format = "%d-%m-%Y"), units = "days"),
    days = gsub(" days", "", days),
    zval = 1 + as.numeric(days)
  )%>%
  select(-c(days, date))



test2<-t1zii%>%
  mutate(
    rain = rainfall1[xval,yval,zval]
  )

#no missing values - pretty sure this matches


test2check<-test2%>%
  mutate(
    grid_lat = lat[cbind(xval, yval)],
    grid_lon = lon[cbind(xval, yval)],
    site_lon = sapply(DISCHARGE_NGR, function(x) osg_parse(x, coord_system = "WGS84")[1]),
    site_lat = sapply(DISCHARGE_NGR, function(x) osg_parse(x, coord_system = "WGS84")[2])
  )
