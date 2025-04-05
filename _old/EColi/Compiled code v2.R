rm(list=ls())
gc()

#This code exists to clean up and make clear what I've done so far

#steps so far
# ~ cleaned River trust data for EDM locations
# ~ cleaned sewage spill data for Wessex and Southern
# ~ merged these data
# ~ got E coli data
# ~ scraped NHS organisation data
# ~  ^(for South West and East regions [all], for CCGs specifically, and for NHS acute trusts)
# ~ analysed E coli and Spills for report 1
# ~ updated report 1 after meeting 09/08/24
# ~ converted data to 12 24 count method
# ~ [work with rainfall array data and merge using sf package (in progress)]


library(dplyr)

#script is in folds/sections below:

# * Rivers Trust data cleaning---------------

#we start with 2023 data, but this is insufficient for matching to sewage spills, since some EDMs close each year
Rivertrust2023<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Rivers Trust EDM Map 2023.csv")
Rivertrust2023<-Rivertrust2023%>%
  mutate(permitReferenceWaSC=substr(permitReferenceWaSC, 1, 6))

#count NAs in lat/long x/y
NAlat<-sum(is.na(Rivertrust2023$Latitude))
#4 NAs exist
NAxy<-sum(is.na(Rivertrust2023$x))
#4 NAs exist

#looking at these NAs
NAindex1<-which(is.na(Rivertrust2023$Latitude))
Test1<-Rivertrust2023[NAindex1, ]
#these 4 have NA for all geolocation data but not Southern or Wessex water so its fine for now
rm(NAindex1, Test1, NAlat, NAxy)


Rivertrust2022<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Rivers Trust EDM Map.csv")


Rivertrust2022<-Rivertrust2022%>%
  mutate(permitReferenceWaSC=as.character(permitReferenceWaSC))

NAlat2<-sum(is.na(Rivertrust2022$Latitude))
#105 NAs exist
NAxy2<-sum(is.na(Rivertrust2022$x))
#105 NAs exist


#Let's look at these NAs
NAindex2<-which(is.na(Rivertrust2022$Latitude))
Test2<-Rivertrust2022[NAindex2, ]

#Again none for Southern or Wessex but somewhat concerning anyway
rm(NAindex2, Test2, NAlat2, NAxy2)


Rivertrust2021<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Rivers Trust EDM Map 2021.csv")
Rivertrust2021<-Rivertrust2021%>%
  rename(permitReferenceWaSC = WaSC.Supplementary.Permit.Reference..if.applicable.,siteNameEA=Site.Name..EA.Consents.Database., siteNameWASC = Site.Name..WaSC.operational.name...if.applicable., permitReferenceEA=EA.Permit.Reference..EA.Consents.Database., waterCompanyName=Water.Company.Name)%>%
  mutate(UID=as.character(UID))%>%
  mutate(permitReferenceWaSC=substr(permitReferenceWaSC, 1, 6))
##IMPORTANT: this substring logic only makes sense for Wessex Water
##CHANGE IF WaSC SITE NAME NEEDED FOR OTHERS

Rivertrust2020<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Rivers Trust EDM Map 2020.csv")
Rivertrust2020<-Rivertrust2020%>%
  rename(siteNameEA=Site_Name, waterCompanyName=Company_name, x=X, y=Y, permitReferenceEA=Permit_number)


mergedi<-bind_rows(Rivertrust2023, Rivertrust2022, Rivertrust2021, Rivertrust2020)

#We only want to keep distinct EDMs

#This following logic may not make immediate sense
#but after matching with EDM individual spill data for Southern and Wessex Water I got NAs
#This logic is specific to each of these two companies
#to reduce NA values
#it works around the fact each company has different ways to identify their EDM on the Rivers trust data
#and in their own spill data
#Som eit may be the EA permit reference
#others it may be the site name itself
#it depends on what information is available in their individual spills data

#this is for matching to Southern Water
mergedi2<-mergedi%>%
  distinct(permitReferenceEA, .keep_all = TRUE)

#this is for matching to Wessex water
mergedi3<-mergedi%>%
  distinct(permitReferenceWaSC, .keep_all = TRUE)

#removing redundant dataframes (until other companies are added to spill data)
rm(Rivertrust2019,Rivertrust2020, Rivertrust2021, Rivertrust2022, Rivertrust2023)


#for Southern water
Riverstrust1<-subset(mergedi2, select = c(waterCompanyName, siteNameEA, siteNameWASC, permitReferenceEA, permitReferenceWaSC, Eastings, Northings, Latitude, Longitude, country, localAuthority, constituencyWestminster, x, y))
Riverstrust1<-Riverstrust1%>%
  rename(wasc=waterCompanyName, Overflow.Name=siteNameEA)%>%
  mutate(wasc=str_remove_all(wasc," Water"))

#for Wessex water
Riverstrust2<-subset(mergedi3, select = c(waterCompanyName, siteNameEA, siteNameWASC, permitReferenceEA, permitReferenceWaSC, Eastings, Northings, Latitude, Longitude, country, localAuthority, constituencyWestminster, x, y))
Riverstrust2<-Riverstrust2%>%
  rename(wasc=waterCompanyName, Overflow.Name=siteNameEA)%>%
  mutate(wasc=str_remove_all(wasc," Water"))








# * Wessex and Southern individual spill cleaning-----

##Southern Water

#2023 Southern Water individual spill data
Southern2023<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Southernwater/Southernwater2023individual with emergency.csv")
Southern2023<-Southern2023%>%
  separate(Start.Time, into = c("startdate", "starttime"), sep =" ")%>%
  separate(End.Time, into = c("enddate", "endtime"), sep =" ")%>%
  mutate(startdate=as.Date(startdate, "%d/%m/%Y"))%>%
  mutate(enddate=as.Date(enddate, "%d/%m/%Y"))%>%
  rename(
    Overflow=OTE,
    DischargeDuration=Discharge.Duration,
    DischargePeriod=Discharge.Period,
    EAnumber=EA.Number)

#2022 Southern Water
Southern2022<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Southernwater/Southernwater2022individual.csv")
#here, we have start date and time already separated.
# making column names consistent to merge the data
Southern2022<-Southern2022%>%
  rename(
    EAnumber=CR_EANumber,
    Activity=CR_ScheduleType,
    startdate=CR_StartDate,
    starttime=CR_StartTime,
    enddate=CR_EndDate,
    endtime=CR_EndTime,
    DischargeDuration=CR_DischargeDuration,
    DischargePeriod=CR_DischargePeriod)%>%
  mutate(startdate=as.Date(startdate, "%d/%m/%Y"))%>%
  mutate(enddate=as.Date(enddate, "%d/%m/%Y"))

#bind these into a merged dataset
SouthernWater<-bind_rows(Southern2022, Southern2023)

rm(Southern2022, Southern2023)

#2021 Southern Water
Southern2021<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Southernwater/Southernwater2021individual.csv")
Southern2021<-Southern2021%>%
  rename(EAnumber=CR_EANumber,
         Activity=CR_ScheduleType,
         startdate=CR_StartDate,
         starttime=CR_StartTime,
         enddate=CR_EndDate,
         endtime=CR_EndTime,
         Duration=CR_DischargeDuration,
         Period=CR_DischargePeriod,
         Spills=CR_Spills,
  )%>%
  mutate(startdate=as.Date(startdate, "%d/%m/%Y"))%>%
  mutate(enddate=as.Date(enddate, "%d/%m/%Y"))


#2021 data gives duration and period in numeric value, in terms of hours same as Wessex water
#2022 and 23 give data as hrs:minutes:seconds

#converting the 2022 and 23 data to numeric by splitting the columns and operating to get hourly units

#Firstly, Duration

SouthernWater1<-SouthernWater%>%
  separate_wider_delim(DischargeDuration, delim = ":", names = c("hours", "minutes", "seconds"))%>%
  mutate(hours=as.numeric(hours), minutes=as.numeric(minutes), seconds=as.numeric(seconds))%>%
  #now making each in terms of hours
  mutate(minutes=minutes/60)%>%
  mutate(seconds=seconds/3600)%>%
  mutate(Duration=round(hours+minutes+seconds, digits = 2))%>%
  separate_wider_delim(DischargePeriod, delim = ":", names = c("hours1", "minutes1", "seconds1"))%>%
  mutate(hours1=as.numeric(hours1), minutes1=as.numeric(minutes1), seconds1=as.numeric(seconds1))%>%
  #now making each in terms of hours
  mutate(minutes1=minutes1/60)%>%
  mutate(seconds1=seconds1/3600)%>%
  mutate(Period=round(hours1+minutes1+seconds1, digits = 2))

SouthernWater1<-subset(SouthernWater1, select = -c(hours, minutes, seconds, hours1, minutes1, seconds1))

#remove the redundant dataframe
rm(SouthernWater)

SouthernWater1<-bind_rows(Southern2021, SouthernWater1)
rm(Southern2021)

#2020 Southern Water Data
Southern2020<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Southernwater/Southernwater2020individual.csv")
Southern2020<-subset(Southern2020, select = -c(Catchment, ST2))%>%
  #Site name has the code from another column at the end which should be removed
  mutate(NSiteTitle=substr(NSiteTitle, 1, nchar(NSiteTitle)-9))%>%
  rename(EAnumber=CR_EANumber,
         Activity=CR_ScheduleType,
         startdate=CR_StartDate,
         starttime=CR_StartTime,
         enddate=CR_EndDate,
         endtime=CR_EndTime,
         Duration=CR_DischargeDuration,
         Period=CR_DischargePeriod,
         Spills=CR_Spills)%>%
  rename(Folio=CR_FS,
         Overflow=CR_NSUN,
         Overflow.Name=NSiteTitle)%>%
  mutate(startdate=as.Date(startdate, "%d/%m/%Y"))%>%
  mutate(enddate=as.Date(enddate, "%d/%m/%Y"))

#bind and drop
SouthernWater1<-bind_rows(Southern2020, SouthernWater1)
rm(Southern2020)


#2017 - 2019 Southern Water
Southern1719<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Southernwater/Southernwater17to19.csv")
Southern1719<-subset(Southern1719, select = -c(Year))%>%
  mutate(Folio.Schedule=substr(Folio.Schedule, 1, nchar(Folio.Schedule)-4))%>%
  rename(Overflow=Site.Number,
         Overflow.Name=Site.Name,
         Folio=Folio.Schedule,
         EAnumber=EA.Number,
         Activity=Schedule.Type,
         startdate=Start.Date,
         starttime=Start.Time,
         enddate=End.Date,
         endtime=End.Time,
         Duration=Discharge.Duration..hours.,
         Period=Discharge.Period..hours.,
         Spills=Spill.Count)%>%
  mutate(EAnumber=na_if(EAnumber, "#N/A"))%>%
  mutate(startdate=as.Date(startdate, "%d/%m/%Y"))%>%
  mutate(enddate=as.Date(enddate, "%d/%m/%Y"))

#bind and drop
SouthernWater1<-bind_rows(Southern1719, SouthernWater1)
rm(Southern1719)


#Add column for WaSC
SouthernWater1<-SouthernWater1%>%
  mutate(wasc="Southern")



#Wessex Water

#currently only have 2022 and 2023 data
Wessex2022<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Wessexwater/Wessex2022individual.csv")
library(tidyr)
library(stringr)

#2022 Wessex Water
Wessex2022<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Wessexwater/Wessex2022individual.csv")

Wessex2022<-Wessex2022%>%
  separate(Spill.Start, sep = " ", into = c("startdate", "starttime"))%>%
  separate(Spill.End, sep = " ", into = c("enddate", "endtime"))%>%
  rename(
    Overflow.Name=Site.Name,
    Duration=Report.Spill.Duration.Hours
  )%>%
  mutate(startdate=as.Date(startdate, "%d/%m/%Y"))%>%
  mutate(enddate=as.Date(enddate, "%d/%m/%Y"))

#2023 Wessex Water
Wessex2023<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Wessexwater/Wessex2023individual.csv")

Wessex2023<-subset(Wessex2023, select = -c(Site.Id))%>%
  separate(Discharge.Start, sep = " ", into = c("startdate", "starttime"))%>%
  separate(Discharge.End, sep = " ", into = c("enddate", "endtime"))%>%
  rename(
    Duration=Total.Duration..Hrs.,
    SO.ID=Unique.Id)%>%
  mutate(startdate=as.Date(startdate, "%d/%m/%Y"))%>%
  mutate(enddate=as.Date(enddate, "%d/%m/%Y"))%>%
  mutate(Duration=str_remove_all(Duration, ","))%>%
  mutate(Duration=as.numeric(Duration))

#binding 
Wessex<-bind_rows(Wessex2023, Wessex2022)

#drop redundant dataframes
rm(Wessex2022, Wessex2023)

#adding WaSC column
Wessex<-Wessex%>%
  mutate(wasc="Wessex")








# * Merging Rivers Trust to Spill data ---------

Wessex2<-left_join(Wessex, Riverstrust2, by = c("SO.ID" = "permitReferenceWaSC"))
match_check<-sum(is.na(Wessex2$Latitude))
print(match_check)
#528 missing geospatial
rm(match_check)

Southern2<-left_join(SouthernWater1, Riverstrust1, by = c("Folio" = "permitReferenceEA"))
match_check2<-sum(is.na(Southern2$x))
print(match_check2)
#1794 missing geospatial
rm(match_check2)


Data1<-bind_rows(Wessex2, Southern2)
Data1<-subset(Data1, select = c(wasc.x, SO.ID, Folio, permitReferenceEA, startdate, starttime, enddate, endtime, Duration, Period, siteNameWASC, localAuthority, Eastings, Northings, x, y, Latitude, Longitude))

#SAVING THIS DATA - used often later
save(Data1, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/Data1.RData")


# * NHS location data scraping------
library(rvest)

#creating a function to return the addresses from NHS website
getaddy<-function(ccg_code) {
  url <- paste0("https://www.dsptoolkit.nhs.uk/OrganisationSearch/", ccg_code)
  webpage <- read_html(url)
  #start as NA in case of errors
  address <- NA
  
  # XPath to extract address (location)
  tryCatch({
    address <- webpage %>% 
      html_node(xpath = "/html/body/main/div[3]/p[2]") %>% 
      html_text()
  }, error = function(e) {
    cat(paste("Error for CCG code", ccg_code, ":", conditionMessage(e), "\n"))
  })
  
  return(address)
}


#looping through all NHS organisations in SOUTH EAST and SOUTH WEST
#reading in data of all NHS organisation codes
NHSorg<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Health/NHS codes South and Wessex.csv")

#turning this into a vector we can loop through
ccgcode<-NHSorg$Code


#making an empty dataframe for our loop
result <- data.frame(nhscode = character(), address = character(), stringsAsFactors = FALSE)


#making a counter to check progress 
counter<-0

#loop through the vector and add to empty dataframe
for (i in ccgcode) {
  address <- getaddy(i)
  result <- rbind(result, data.frame(nhscode = i, address = address, stringsAsFactors = FALSE))
  #add to counter each iteration
  counter <- counter + 1
  
  # Print progress every 100 codes
  if (counter %% 100 == 0) {
    cat(paste(" Processed", counter, "out of", length(ccgcode)))
    flush.console()
  }
}


save(result, file ="C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/NHSscrape Wessex Southern.RData")

library(writexl)

write_xlsx(result, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/NHSscrape Wessex Southern.xlsx")




#let's check our try catch to see if there was any NA values

NAcheck<-sum(is.na(result$address))
print(NAcheck)
#0 - all are populated
#manually, they look good too
rm(NAcheck)

#now we want to clean the addresses 
#have a clean postcode, and the full address in a separate column just in case we need that

cleaned<-result%>%
  mutate(address = trimws(address))%>%
  mutate(postcode=substr(address, nchar(address)-7, nchar(address)))%>%
  mutate(address=substr(address, 1, nchar(address)-8))%>%
  #some must have had two spaces - those with 3 digit, 3 digit postcodes rather than 4, 3
  mutate(address = trimws(address))%>%
  mutate(address=gsub("Address: ", "", address))%>%
  mutate(address=sub(",$", "", address))%>%
  mutate(postcode=trimws(postcode))


#so now it's cleaned with postcode in one column.

#now this needs to be matched with E coli data


#save result and move onto another script


save(cleaned, file ="C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/NHSscrape Wessex Southern clean.RData")

write_xlsx(cleaned, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/NHSscrape Wessex Southern clean.xlsx")


#same code for only acute trusts (in all NHS regions)



allacute<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Health/E coli acute trusts only all.csv")
allacute<-allacute%>%
  distinct(Organisation.code, .keep_all=TRUE)%>%
  filter(Organisation.type=="NHS acute trust")

#138 distinct acute trusts


acutecode<-allacute$Organisation.code

#initialise empty frame
result2 <- data.frame(nhscode = character(), address = character(), stringsAsFactors = FALSE)


counter<-0

for (i in acutecode) {
  address <- getaddy(i)
  result2 <- rbind(result2, data.frame(nhscode = i, address = address, stringsAsFactors = FALSE))
  #add to counter each iteration
  counter <- counter + 1
  
  # Print progress every 100 codes
  if (counter %% 10 == 0) {
    cat(paste(counter, "done "))
    flush.console()
  }
}

#getting clean postcodes for the NHS acute trusts
acuteclean<-result2%>%
  mutate(address = trimws(address))%>%
  mutate(postcode=substr(address, nchar(address)-7, nchar(address)))%>%
  mutate(address=substr(address, 1, nchar(address)-8))%>%
  #some must have had two spaces - those with 3 digit, 3 digit postcodes rather than 4, 3
  mutate(address = trimws(address))%>%
  mutate(address=gsub("Address: ", "", address))%>%
  mutate(address=sub(",$", "", address))%>%
  mutate(postcode=trimws(postcode))%>%
  mutate(postcode=gsub(", ", "", postcode))

#Saving this dataframe
save(acuteclean, file ="C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/NHSscrape acute trusts clean.RData")

write_xlsx(acuteclean, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/NHSscrape acute trusts clean.xlsx")




# * Report 1 pre-processing--------

#CAN load Data1 from here: [already in environment in this script]
#load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/Data1.RData")


#getting only unique EDMs and their location

#for Wessex, used "WaSC permit reference" basically, their internal reference
#for Southern, used their "EA permit reference", the reference used by EA

uniquew<-Data1%>%
  distinct(SO.ID, .keep_all = TRUE)%>%
  filter(!is.na(SO.ID))
#1172 EDMs for wessex

uniques<-Data1%>%
  distinct(Folio, .keep_all = TRUE)%>%
  filter(!is.na(Folio))
#1039 EDMs for Southern Water

edmsw<-bind_rows(uniquew, uniques)%>%
  subset(select=-c(startdate, enddate, starttime, endtime, Duration, Period))
#2211 together

#export this to excel, unique EDMs and locations
save(edmsw, file ="C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/Southern Wessex EDMs.RData")

write_xlsx(edmsw, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/Southern Wessex EDMs.xlsx")


rm(edmsw, uniques, uniquew)


#load in e.coli data 
Ecoli<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Health/E coli merged.csv")

#adapt to Data1 naming
Ecoli2<-Ecoli%>%
  mutate(YearMonth=ymd(paste(Year, Month, "01", sep = "-")))%>%
  rename(nhscode=Organisation.code)%>%
  #have to filter on total cases
  filter(Metric=="Total cases")



#add a prefex w and s for each company respectively to make a unique id column for both together
monthly<-Data1%>%
  mutate(id = ifelse(!is.na(SO.ID), paste("w", SO.ID, sep = "_"), paste("s", Folio, sep = "_")))

## ~ split duration across months (interpolate) when aggregating spill data into months


library(lubridate)



interspill<-function(startdate, enddate, Duration){
  #an empty list
  result<-list()
  #works out number of days a spill spans
  totaldays<-as.numeric(difftime(enddate, startdate, unit="days"))+1
  #now we need to go through the days
  #starts with start date
  currentdate<-startdate
  #while loop
  while(currentdate<=enddate){
    #gets end of month date
    monthend<-floor_date(currentdate, "month") + months(1) - days(1)
    #now working out days in current month
    monthdays<-min(monthend, enddate) - currentdate + 1
    #need to work out proportion to interpolate duration
    proportion<-as.numeric(monthdays/totaldays)
    monthduration<-proportion*Duration
    #append
    result<-append(result, list(data.frame(
      YearMonth = floor_date(currentdate, "month"),
      Duration = monthduration)))
    currentdate<-monthend+days(1)
  }
  return(do.call(rbind, result))
}

# ~ use row 552328 to check funcion works - this has a long duration and spans across months
# - manually, this would give:
# - 199.5 spill hours in march, 119.7 hours in april

#looptest<-monthly[552328, ]
#library(purrr)
#library(tidyr)
#splittest<-looptest%>%
  #mutate(Split = pmap(list(startdate, enddate, Duration), interspill)) %>%
  #select(id, Split) %>%
  #unnest(Split)
#successful function - apply to full spill data

rm(splittest, looptest)


#applying to full data - !! takes a long time to run - data after is saved
library(purrr)
library(tidyr)
monthly2<-monthly%>%
  mutate(Split = pmap(list(startdate, enddate, Duration), interspill)) %>%
  select(id, Split) %>%
  unnest(Split)

#Save this dataframe since the function takes some time to run
save(monthly2, file ="C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/Southern Wessex Monthly spill duration.RData")

write_xlsx(monthly2, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/Southern Wessex Monthly spill duration.xlsx")


#loading data for rerunning code
load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/Southern Wessex Monthly spill duration.RData")


#aggregating across months for each EDM
monthly3 <- monthly2 %>%
  group_by(id, YearMonth) %>%
  summarise(monthlyduration = sum(Duration, na.rm = TRUE))

#for each location this gives the duration of spills for each month that data is available


#finding spillcount - number of spills per month for each EDM
monthlycount<-monthly%>%
  mutate(YearMonth = floor_date(startdate, "month"))%>%
  group_by(id, YearMonth) %>%
  summarise(spillcount = n()) %>%
  ungroup()
#gives number of spills that start in each month for each EDM


#now we have monthly duration and spill counts for each EDM.
#next step is to aggregate these for their respective NHS acute trust


#loading in the nearest NHS location dataset
#this is exported from ArcGIS pro, and gives the closest NHS acute trust within 20km of each edm
#it is effectively the buffers, without allowing EDMs to be assigned to several acute trusts
near<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/EDMs_closestNHSacutetrust.csv")

near<-near%>%
  subset(select=c(wasc_x, SO_ID, Folio, USER_nhscode))%>%
  rename(wasc=wasc_x, nhscode=USER_nhscode)%>%
  mutate_all(na_if, "")%>%
  mutate(id = ifelse(!is.na(SO_ID), paste("w", SO_ID, sep = "_"), paste("s", Folio, sep = "_")))


#merge these data

#monthly EDM spill duration with their nearest acute trust
spillduration2<-left_join(monthly3, near, by = "id")%>%
  #not a many to many relationship
  group_by(nhscode, YearMonth) %>%
  summarise(totalduration = sum(monthlyduration, na.rm = TRUE)) %>%
  ungroup()

#monthly EDM spill count with nearest acute trust
spillcount<-left_join(monthlycount, near, by = "id")%>%
  #many to many again
  group_by(nhscode, YearMonth) %>%
  summarise(totalcount = sum(spillcount, na.rm = TRUE)) %>%
  ungroup()


#merge with e coli data
mergedvii<-left_join(spillduration2, Ecoli2, by = c("nhscode", "YearMonth"))%>%
  subset(select=c(nhscode, YearMonth, totalduration, Figure))%>%
  rename(ecoli=Figure)

#merge with spill count
mergedviii<-left_join(mergedvii, spillcount, by = c("nhscode", "YearMonth"))

#save data
save(mergedviii, file ="C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/spillduration3 merged fully 26 JUL 24.RData")


#now removing 2021 from NHS acute trust coded RNZ - as straddles Southern and Wessex water boundaries
#an issue as Southern has data available for more years than Wessex

mergedviii<-mergedviii%>%
  mutate(totalduration=ifelse(nhscode == "RNZ" & format(YearMonth, "%Y") == "2021", NA, totalduration))%>%
  mutate(totalcount=ifelse(nhscode == "RNZ" & format(YearMonth, "%Y") == "2021", NA, totalcount))


mergedviii<-mergedviii%>%
  filter(!is.na(nhscode))



# * Report 1 additions-----

#models with only one independent variable
library(plm)
panel1<-pdata.frame(mergedviii, index=c("nhscode", "YearMonth"))

poolOLS1<- plm(ecoli ~ totalduration, data = panel1, model = "pooling")
summary(poolOLS1)

poolOLS2<- plm(ecoli ~ totalcount, data = panel1, model = "pooling")
summary(poolOLS2)


FE1<-plm(ecoli ~ totalduration, data = panel1, model = "within")
summary(FE1)

FE2<-plm(ecoli ~ totalcount, data = panel1, model = "within")
summary(FE2)


Btwn1<-plm(ecoli ~ totalduration, data = panel1, model = "between")
summary(Btwn1)

Btwn2<-plm(ecoli ~ totalcount, data = panel1, model = "between")
summary(Btwn2)

FE3<-plm(ecoli ~ totalduration, data = panel1, model = "within", effect = "twoways")
summary(FE3)

FE4<-plm(ecoli ~ totalcount, data = panel1, model = "within", effect = "twoways")
summary(FE4)


#a supplementary analysis with only "major" spills analysed: 12 Aug 24


#loading back in data:

#load spill data
load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/Data1.RData")

#load NHS buffer data
near<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/EDMs_closestNHSacutetrust.csv")

#same processing as before:
near<-near%>%
  subset(select=c(wasc_x, SO_ID, Folio, USER_nhscode))%>%
  rename(wasc=wasc_x, nhscode=USER_nhscode)%>%
  mutate_all(na_if, "")%>%
  mutate(id = ifelse(!is.na(SO_ID), paste("w", SO_ID, sep = "_"), paste("s", Folio, sep = "_")))


#now we only want to focus on "major" spills
# ~ how do we define this?
# ~ one way can be to take those with the longest duration - a certain quantile

#let's see the distribution of our duration data
library(ggplot2)

histogram<-ggplot(Data1, aes(Duration)) +
  geom_histogram(color = "#000000", fill = "#B4CDC7")


histogram2<-ggplot(Data1, aes(Duration)) +
  geom_histogram(color = "#000000", fill = "#B4CDC7") +
  xlim(0, 100)
histogram2


#let's take top 10%
Data2<-Data1%>%
  filter(quantile(Duration, 0.9)<Duration)

min(Data2$Duration)
2.34
max(Data2$Duration)
4552.86


#it makes most sense to construct our spill count variable for this top 10%
#rather than spill duration, since we are skewing the distribution of spill duration

Data2<-Data2%>%
  mutate(id = ifelse(!is.na(SO.ID), paste("w", SO.ID, sep = "_"), paste("s", Folio, sep = "_")))

monthlycount2<-Data2%>%
  mutate(YearMonth = floor_date(startdate, "month"))%>%
  group_by(id, YearMonth) %>%
  summarise(spillcount = n()) %>%
  ungroup()


spillcount2<-left_join(monthlycount2, near, by = "id")%>%
  #many to many again
  group_by(nhscode, YearMonth) %>%
  summarise(totalcount = sum(spillcount, na.rm = TRUE)) %>%
  ungroup()
#1267 observations (why fewer than previous iteration: 1391)


#e coli data (repeat)
#load in e.coli data 
Ecoli<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/Health/E coli merged.csv")

#adapt
Ecoli2<-Ecoli%>%
  mutate(YearMonth=ymd(paste(Year, Month, "01", sep = "-")))%>%
  rename(nhscode=Organisation.code)%>%
  #have to filter on total cases
  filter(Metric=="Total cases")

#need to merge wtih E coli data
spillcount2<-left_join(spillcount2, Ecoli2, by = c("nhscode", "YearMonth"))%>%
  subset(select=c(nhscode, YearMonth, totalcount, Figure))%>%
  rename(ecoli=Figure)

library(plm)

panel3<-pdata.frame(spillcount2)


PooledOLS3<-plm(ecoli ~ totalcount, data = panel3, model = "pooling")
summary(PooledOLS3)

FEi<-plm(ecoli ~ totalcount, data = panel3, model = "within", effect = "twoways")
summary(FEi)




# * 12 24 hour conversion-----

#libraries for this code seciton
#library(dplyr)
#library(lubridate)

#two functions to create 12 24 spills from raw spill data
#first one creates the outer limits that encompass the start and stop stimes of spills,
#and include 12 hours, then n X 24 hours
#second splits this into the 12 hour and 24 hour spills

# ~ still to do: deal with edge cases (daylight savings) - logic adaptation in create_blocks


#first function

create_blocks<-function(spills){
  #sort spills by their starttime
  spills <- spills %>% arrange(id, startdatetime)
  
  #create an empty dataframe to store 12 24 counted spills
  blocks<-data.frame(
    id=character(),
    block_start = as.POSIXct(character()),
    block_end = as.POSIXct(character()),
    stringsAsFactors = FALSE
  )
  
  #initialise a counter to track progress
  counter<-0
  
  unique_ids <- unique(spills$id)
  for (current_id in unique_ids) {
    spills_id <- spills %>% filter(id == current_id)
    
    #Initialise block variables - needed for 12/24 logic
    current_block_start <- NULL
    current_block_end <- NULL
    
    for(i in 1:nrow(spills_id)) {
      
      #counter for progress tracking
      counter <- counter + 1
      
      # Print count every 20 rows
      if (counter %% 20 == 0) {
        cat(" Processed:", counter, "\n")
      }
      
      #sets some variables representing spill start and stop times to the relevant data
      spill_start <- spills_id$startdatetime[i]
      spill_end <- spills_id$enddatetime[i]
      
      #now applying current logic
      if (is.null(current_block_start)) {
        # this is the case this is the first spill
        current_block_start <- spill_start
        current_block_end <- spill_start + hours(12)  #originally given as min(spill_start+hours(12), spill_end) - but this doesn't make sense right?
        #so this makes the block end 12 hours after the start
        
        #if the spill extends beyond this 12 hours...
        #a while loop to keep adding 24 hours until the block encompasses our spill
        while (current_block_end < spill_end) {
          current_block_end <- current_block_end + hours(24)
        }
        blocks <- rbind(blocks, data.frame(id=current_id, block_start = current_block_start, block_end = current_block_end))
      } else {
        #now we go to the cases that are not the first spill
        #for these we have to consider the three possible cases:
        # ~ that they will be fully encompasses by the previous blocks
        # ~ they cause an extention to the previous blocks
        # ~ they are beyond 24 hours after the previous spill block, and so start a new one entirely
        
        if (spill_end <= current_block_end) {
          # Spill is completely within the previous blocks
          next #skip this spill as it's already  accounted for
        } else if (spill_start <= current_block_end + hours(24)) {
          # Spill extends the current block ~ since this logic means
          #these observations have an end beyond the previous block end,
          #but also start before 24 hours after the previous block end
          while (current_block_end < spill_end) {
            current_block_end <- current_block_end + hours(24)
          }
          blocks$block_end[nrow(blocks)] <- current_block_end 
          #^ this may not be what I want, this seems just to amend our current block
          #although - logically it doesn't matter for my next steps tbf
        } else {
          # Start a new block ~ this is the case we are beyond 24 hours since last block
          current_block_start <- spill_start
          current_block_end <- spill_start + hours(12)#NOTE *** fixed 05/08/24
          while (current_block_end < spill_end) {
            current_block_end <- current_block_end + hours(24)
          }
          blocks <- rbind(blocks, data.frame(id=current_id, block_start = current_block_start, block_end = current_block_end))
        }
      }
    }
  }
  return(blocks)
  
}



#second function


split_blocks<-function(blocks){
  
  #creates an empty dataframe to store initial for loop
  firstsplit<-data.frame(
    id=character(),
    block_start = as.POSIXct(character()),
    block_end = as.POSIXct(character()),
    duration = numeric(),
    stringsAsFactors = FALSE
  )
  
  #initialise empty variables for the loops
  current_block_start <- NULL
  current_block_end <- NULL
  current_id<-NULL
  
  #loop through our blocks to separate first 12 hour spill blocks
  for(i in 1:nrow(blocks)) {
    #assigns variables to individual observations
    current_id<-blocks$id[i]
    current_block_start<-blocks$block_start[i]
    current_block_end<-blocks$block_end[i]
    #gives first spill end (always 12 hours)
    firstinterval_end<-blocks$block_start[i]+hours(12)
    firstinterval_duration<-12
    #now we wish to see "duration" - to check function works properly
    remainder_duration<-difftime(current_block_end, firstinterval_end, units = "hours")
    
    #append 12 hour interval and remainder into dataframe
    firstsplit<-rbind(firstsplit, data.frame(id=current_id, block_start=current_block_start, block_end=firstinterval_end, duration = firstinterval_duration))
    firstsplit<-rbind(firstsplit, data.frame(id=current_id, block_start=firstinterval_end, block_end=current_block_end, duration = remainder_duration))
  }
  
  #separate 12 hour spills from remainder
  
  first12<-firstsplit%>%
    filter(duration==12)
  
  remainder<-firstsplit%>%
    filter(duration!=12 & duration!= 0)
  
  #create an empty dataframe for second loop
  remainder_split<-data.frame(
    id=character(),
    block_start = as.POSIXct(character()),
    block_end = as.POSIXct(character()),
    duration = numeric(),
    stringsAsFactors = FALSE
  )
  
  
  for(i in 1:nrow(remainder)){
    #loop for splitting up the remainder into the 24 hour spills
    current_id<-remainder$id[i]
    current_block_start<-remainder$block_start[i]
    current_block_end<-remainder$block_end[i]
    
    #this gives the times that separate spills within our remainder
    times <- seq(from = current_block_start, to = current_block_end, by = "24 hours")
    
    #this adds in the final endtime - however this logic may cause inconsistency in daylight savings
    if (times[length(times)] != current_block_end) {
      times <- c(times, current_block_end)
    }
    
    #empty dataframe for results of the interval splits within the loop, as we're generating and saving several observations per observation
    intervals <- data.frame(
      id = current_id,
      block_start = times[-length(times)],
      block_end = times[-1],
      duration = NA, #to ensure rbind() can operate correctly (same no. of columns)
      stringsAsFactors = FALSE
    )
    
    remainder_split<-rbind(remainder_split, intervals)
  }
  
  #merge 12 hour blocks with 24 hour blocks
  miniblock<-rbind(first12, remainder_split)%>%
    arrange(id, block_start)
  
  return(miniblock)
}

# ~ still want some counter to track progress of this function



#applying to data as a test: choosing august 2023 [will go back to October for edge cases later]
load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/Data1.RData")
Aug23<-Data1%>%
  filter(startdate >= as.Date("2023-08-01") & startdate <= as.Date("2023-08-31"))%>%
  #make id column
  mutate(id = ifelse(!is.na(SO.ID), paste("w", SO.ID, sep = "_"), paste("s", Folio, sep = "_")))%>%
  #make datetime objejcts
  mutate(startdatetime = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M")) %>%
  select(-startdate, -starttime, -enddate, -endtime)
#3616 observations


Aug23i<-create_blocks(Aug23)
#1793 observations

Aug23ii<-split_blocks(Aug23i)%>%
  mutate(duration=as.numeric(difftime(block_end, block_start, units = "hours")))
#2253
min(Aug23ii$duration)
#12
max(Aug23ii$duration)
#24

#worked as intended

save(Aug23ii, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/Initial datasets/output frames/miniblocks for Aug 23.RData")

