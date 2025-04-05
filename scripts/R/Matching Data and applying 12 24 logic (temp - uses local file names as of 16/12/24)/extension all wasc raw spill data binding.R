rm(list=ls())
gc()


#edits on 19th Nov 2024 ~ found NAs - likely stemming from declaration of datetime objects
#must be inconsistencies I didn't account for in how dates are entered
#edits will remove these



#now we have access to the start & stop discharge times for each wasc from 2021 to 2023
#aims of this script are:
# ~ merge the data in a consistent way for analysis

#merging the data
library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)

#2021:

#anglian
anglian<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021A.csv")%>%
  select(-Activity.Reference)%>%
  mutate(wasc="anglian")%>%
  rename(EAsitename=Site.Name..EA.Consents.Database.)%>%
  rename(site=Site.Name..WaSC.operational...optional.)%>%
  rename(EAnumber=EA.Permit.Reference..EA.Consents.Database.)%>%
  rename(start=Start.date.and.time)%>%
  rename(end=End.date.and.time)%>%
  mutate(start = sub("Z$", "", start)) %>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  #end of issue block
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  #keep going with this logic
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  #logic to remove erroneous times based on DST changes in March
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)
#not making into a function since the column names are inconsistent across years  

#check for missing values
#check<-anglian%>%
#  filter(is.na(startdatetime1) | is.na(enddatetime1))
#11 are missing - the issue i faced in matching sheet

###ISSUE !! ALL OF THESE MISSING VALUES ARE 1AM - 2AM MARCH 28th 2021 
###THEY BECOME NA WHEN DATETIME OBJECT DUE TO DAYLIGHT SAVINGS CHANGES
###how to deal with these?

#optimal way I see currently - is to use lubridate for robust datetime operations
#it will automatically adjust these times to the next valid time 

#however, this will change a spill (start) at 1:15AM to 2AM, same as 1:30 to 2AM.
#in reality, they would be 2:15AM and 2:30 AM respectively

#therefore - logic chosen (19/11/2024) - is to add one hour to the missing values 
#resolved 19/11/2024



#welsh
welsh<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021Welsh.csv")%>%
  mutate(wasc="welsh")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.start)%>%
  rename(end=Discharge.stop)%>%
  mutate(start = sub("Z$", "", start)) %>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(startdatetime = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M")) %>%
  select(-startdate, -starttime, -enddate, -endtime)



#check for missing values
#check<-welsh%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#no missing values



#northumbrian
northumbrian<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021N.csv")%>%
  mutate(wasc="northumbrian")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  mutate(start = sub("Z$", "", start))%>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(startdatetime = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M")) %>%
  select(-startdate, -starttime, -enddate, -endtime)%>%
  filter(!is.na(startdatetime))

#nas from separate - check row 243 and 1106
#check<-northumbrian[243,]
#check<-northumbrian[1106,]
#ok so just those CSOs listed with no discharges
#na_index<-which(is.na(northumbrian$startdatetime) | is.na(northumbrian$enddatetime))
#re ran read.csv to get original northumbrian data
#checking<-northumbrian[na_index,]
#yep all are no discharges
#so remove these missing values 19/11/2024





#severn trent
severn<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021ST.csv")%>%
  mutate(wasc="severntrent")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.start)%>%
  rename(end=Discharge.stop)%>%
  rename(misc_code=Site.Code)%>%
  mutate(start = sub("Z$", "", start))%>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end))%>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  #keep going with this logic
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  #logic to remove erroneous times based on DST changes in March
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)


  
#check for missing values
#check<-severn%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#3 missing values found 19/11/2024
#create index
#na_index<-which(is.na(severn$startdatetime) | is.na(severn$enddatetime))
#look at original data
#checking<-severn[na_index,]
#DST issue like Anglian water found 19/11/2024
#issue resolved 19/11/2024




#south west
southwest<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021SW.csv")%>%
  mutate(wasc="southwest")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  separate(start, into = c("startdate", "starttime"), sep = " ")%>%
  mutate(end = sub("Z$", "", end))%>%
  separate(end, into = c("enddate", "endtime"), sep = " ")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  #keep going with this logic
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%d/%m/%Y %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%d/%m/%Y %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%d/%m/%Y %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%d/%m/%Y %H:%M"))%>%
  #logic to remove erroneous times based on DST changes in March
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)%>%
  filter(!is.na(startdatetime))

#check for missing values
#check<-southwest%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#some of these lie in the DST problem time found 20/11/2024
#others seem to be blank entries
#na_index<-which(is.na(southwest$startdatetime) | is.na(southwest$enddatetime))
#reload south west data
#checking<-southwest[na_index,]
#yes a large amount are blank entries
#so apply DST logic, then remove remaining NAs
#fixed 20/11/2024



#southern
southern<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021S.csv")%>%
  mutate(wasc="southern")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  separate(start, into = c("startdate", "starttime"), sep = " ")%>%
  mutate(end = sub("Z$", "", end))%>%
  separate(end, into = c("enddate", "endtime"), sep = " ")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  #keep going with this logic
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  #logic to remove erroneous times based on DST changes in March
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)



#check for NAs
#check<-southern%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#one missing - lies within the DST change noticed 20/11/2024
#na_index<-which(is.na(southern$startdatetime) | is.na(southern$enddatetime))
#look at reloaded southern data
#checking<-southern[na_index,]
#yes lies within DST so apply these changes 20/11/2024



#thames
thames<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021T.csv")%>%
  mutate(wasc="thames")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  mutate(start=trimws(start))%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=gsub("/", "-", start))%>%
  mutate(start=sub("T", " ", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=gsub("/", "-", end))%>%
  mutate(end=sub("T", " ", end))%>%
  separate(start, into = c("startdate", "starttime"), sep = " ")%>%
  separate(end, into = c("enddate", "endtime"), sep = " ")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  #keep going with this logic
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%d-%m-%Y %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%d-%m-%Y %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%d-%m-%Y %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%d-%m-%Y %H:%M"))%>%
  #logic to remove erroneous times based on DST changes in March
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)


  
#checking for missing values
#check<-thames%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#again missing values based on DST
#also brings up an important consideration that some of the times recorded when DST ends
#could also be misleading (since the EDMs don't change their clocks exactly with DST)
#however, there's no way of knowing this
#so perhaps the best course of action in those cases is to assume the EDMs changed their clocks
#at the right time

#na_index<-which(is.na(thames$startdatetime) | is.na(thames$enddatetime))

#checking<-thames[na_index,]
#fixed 20/11/2024




#united utilities comes in 3 sections
united1<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021U 1.csv")
united2<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021U 2.csv")
united3<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021U 3.csv")

check1<-united1%>%
  filter(is.na(Discharge.start) | is.na(Discharge.stop))
check2<-united2%>%
  filter(is.na(Discharge.start) | is.na(Discharge.stop))
check3<-united3%>%
  filter(is.na(Discharge.start) | is.na(Discharge.stop))


united<-rbind(united1, united2, united3)
rm(united1, united2, united3)


united<-united%>%
  mutate(wasc="united")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.start)%>%
  rename(end=Discharge.stop)%>%
  mutate(start=trimws(start))%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=gsub("/", "-", start))%>%
  mutate(start=sub("T", " ", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=gsub("/", "-", end))%>%
  mutate(end=sub("T", " ", end))%>%
  mutate(startdatetime = as.POSIXct(start, format="%Y-%m-%d %H:%M"),
         enddatetime = as.POSIXct(end, format="%Y-%m-%d %H:%M")) %>%
  select(-start, -end)%>%
  filter(!is.na(startdatetime))

#NAs produced - check these
#check<-united[594768,]
#check<-united[594777,]
# these are unpopulated rows. not sure why they exist 

#check<-united%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))

#a bunch of missing rows in the middle
#i wonder why this is.
#problem is 100k missing in united 2
#let's look at these
#na_index<-which(is.na(united$startdatetime) | is.na(united$enddatetime))
#checking<-united2[na_index,]
#hmm yeah strange they're just missing for some reason
#just remove them
#united<-filter(united, !is.na(startdatetime))





#wessex
wessex<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021W.csv")%>%
  mutate(wasc="wessex")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.start)%>%
  rename(end=Discharge.end)%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=gsub("/", "-", start))%>%
  mutate(start=sub("T", " ", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=gsub("/", "-", end))%>%
  mutate(end=sub("T", " ", end))%>%
  separate(start, into = c("startdate", "starttime"), sep = " ")%>%
  separate(end, into = c("enddate", "endtime"), sep = " ")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2, -Storm.Discharge.Asset.Type..Treatment.Works.)


#check<-wessex%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#one missing due to DST too found 20/11/2024
#apply same logic to solve it - done 20/11/2024
#na_index<-which(is.na(wessex$startdatetime) | is.na(wessex$enddatetime))
#checking<-wessex[na_index,]






#yorkshire
yorkshire<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2021Y.csv")%>%
  mutate(wasc="yorkshire")%>%
  rename(EAsitename=Site.Name..EA.Consents.Database.)%>%
  rename(start=SpillCountStartDatetimeUtc)%>%
  rename(end=SpillCountEndDatetimeUtc)%>%
  rename(id=YWDischargeURN)%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=gsub("/", "-", start))%>%
  mutate(start=sub("T", " ", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=gsub("/", "-", end))%>%
  mutate(end=sub("T", " ", end))%>%
  
  separate(start, into = c("startdate", "starttime"), sep = " ")%>%
  separate(end, into = c("enddate", "endtime"), sep = " ")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)

         
         


#checking for missing values
#check<-yorkshire%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#14 are missing from DST
#na_index<-which(is.na(yorkshire$startdatetime) | is.na(yorkshire$enddatetime))
#checking<-yorkshire[na_index,]
#solved


#end of 2021

#important note: only Yorkshire and Anglian explicitly gives a WaSC reference
#Yorkshire and Anglian are the only WaSCs to confirm the names match the EA consents database
#since it's EA data - I assume they match
#but be careful that WaSCs have consistent naming across years and that they match the EA consents


#let's trial with 2021 then:
#aims:
# ~ match an id number and geolocation from EA consents microsoft access database
# ~ apply 12 24 logic [new strategy is to just apply then adjust any 25 or 23 hour blocks - for now?]
#(guess issue with that is it may affect when subsequent blocks start)


#merging the data

data21<-bind_rows(anglian, northumbrian, severn, southern, southwest, thames, united, welsh, wessex, yorkshire)

rm(anglian, northumbrian, severn, southern, southwest, thames, united, welsh, wessex, yorkshire)


#export this frame for ease later on 
save(data21, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/2021spillraw.RData")
#overwritten after missing values corrected

check<-data21%>%
  filter(is.na(startdatetime) | is.na(enddatetime))
#southwest is somehow an issue 



#go from here
rm(list=ls())
gc()

library(dplyr)
library(tidyr)
library(lubridate)


load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/2021spillraw.RData")
#2,236,468 observations in 2021 data
#2,236,158 now all NAs have been corrected or removed




#2022:
#(expect format to be roughly the same for companies across years, although col names differ sometimes)

#anglian
anglian<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022A.csv")%>%
  select(-Activity.Reference.on.Permit)%>%
  mutate(wasc="anglian")%>%
  rename(EAsitename=Site.Name..EA.Consents.Database.)%>%
  rename(site=Site.Name..WaSC.operational...optional.)%>%
  rename(EAnumber=EA.Permit.Reference..EA.Consents.Database.)%>%
  rename(start=Start.Time)%>%
  rename(end=End.Time)%>%
  mutate(start = sub("Z$", "", start)) %>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(startdatetime = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M")) %>%
  select(-startdate, -starttime, -enddate, -endtime)


#check<-anglian%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#no NA values




#welsh
welsh<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022Welsh.csv")%>%
  mutate(wasc="welsh")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.start)%>%
  rename(end=Discharge.stop)%>%
  mutate(start = sub("Z$", "", start)) %>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(startdatetime = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M")) %>%
  select(-startdate, -starttime, -enddate, -endtime)%>%
  filter(!is.na(enddatetime))

#one missing date for end
#ROW 14208
#site ST BRIAVELS SEWAGE TREATMENT WORKS
#should remove? decided YES 18/11/2024
#entry was NULL for end time






#northumbrian
northumbrian<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022N.csv")%>%
  mutate(wasc="northumbrian")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  mutate(start = sub("Z$", "", start))%>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(startdatetime = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M")) %>%
  select(-startdate, -starttime, -enddate, -endtime)%>%
  filter(!is.na(enddatetime))
#NAs but only in relation to actual empty entries

#na_index<-which(is.na(northumbrian$startdatetime) | is.na(northumbrian$enddatetime))
#288 missing
#reload northumbrian to see these rows
#checking<-northumbrian[na_index,]
#all no discharges



#severn trent
severn<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022ST.csv")%>%
  mutate(wasc="severntrent")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.start)%>%
  rename(end=Discharge.stop)%>%
  rename(misc_code=Site.Code)%>%
  mutate(start = sub("Z$", "", start))%>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end))%>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  #keep going with this logic
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  #logic to remove erroneous times based on DST changes in March
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)

#checking for missing values
#check<-severn%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#2 missing
#due to DST change at 2am on 27th March 
#na_index<-which(is.na(severn$startdatetime) | is.na(severn$enddatetime))
#solved




#south west
southwest<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022SW.csv")%>%
  mutate(wasc="southwest")%>%
  rename(site=Site.Name.)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  mutate(start=gsub("  ", " ", start))%>%
  mutate(end=gsub(" ", " ", end))%>%
  mutate(startdatetime = as.POSIXct(start, format="%d/%m/%Y %H:%M"),
         enddatetime = as.POSIXct(end, format="%d/%m/%Y %H:%M")) %>%
  select(-start, -end)

southwest<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022SW.csv")%>%
  mutate(wasc="southwest")%>%
  rename(site=Site.Name.)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  mutate(start=gsub("  ", "", start))%>%
  mutate(end=gsub("  ", " ", end))%>%
  #editing row 1 due to incorrect formatting
  mutate(start=ifelse(row_number() ==1, "21/12/2022 10:15:00", start))%>%
  mutate(end=ifelse(row_number()==1, "21/12/2022 10:30:00", end))%>%
  separate(start, into = c("startdate", "starttime"), sep = " ")%>%
  mutate(end = sub("Z$", "", end))%>%
  separate(end, into = c("enddate", "endtime"), sep = " ")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%d/%m/%Y %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%d/%m/%Y %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%d/%m/%Y %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%d/%m/%Y %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)%>%
  filter(!is.na(startdatetime))



#checking for nas
#check<-southwest%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#one lies within the DST change 
#na_index<-which(is.na(southwest$startdatetime) | is.na(southwest$enddatetime))
#another of these has a double space [1,]
#however [1,] also has incorrect formatting:
#it has m:d:Y rather than d:m:Y
#this requires manual editing
#checking<-southwest[na_index,]
#solved





#Southern
southern<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022S.csv")%>%
  mutate(wasc="southern")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  mutate(startdatetime = as.POSIXct(start, format="%Y-%m-%d %H:%M"),
         enddatetime = as.POSIXct(end, format="%Y-%m-%d %H:%M")) %>%
  select(-start, -end)

#checking for NA
#check<-southern%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#none
#and also note for matching - these have some number after - which may represent some id to match on



#Thames
thames<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022T.csv")%>%
  mutate(wasc="thames")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  mutate(start=trimws(start))%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=gsub("/", "-", start))%>%
  mutate(start=sub("T", " ", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=gsub("/", "-", end))%>%
  mutate(end=sub("T", " ", end))%>%
  separate(start, into = c("startdate", "starttime"), sep = " ")%>%
  separate(end, into = c("enddate", "endtime"), sep = " ")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%d-%m-%Y %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%d-%m-%Y %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%d-%m-%Y %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%d-%m-%Y %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)



#check<-thames%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#one from daylight savings change
#solved


#united utilities comes in 3 sections again
united1<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022U 1.csv")
united2<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022U 2.csv")
united3<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022U 3.csv")

united<-rbind(united1, united2, united3)
rm(united1, united2, united3)


united<-united%>%
  mutate(wasc="united")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.start)%>%
  rename(end=Discharge.stop)%>%
  mutate(start=trimws(start))%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=gsub("/", "-", start))%>%
  mutate(start=sub("T", " ", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=gsub("/", "-", end))%>%
  mutate(end=sub("T", " ", end))%>%
  mutate(startdatetime = as.POSIXct(start, format="%Y-%m-%d %H:%M"),
         enddatetime = as.POSIXct(end, format="%Y-%m-%d %H:%M")) %>%
  select(-start, -end)

#check<-united%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#No NA



#wessex
wessex<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022W.csv")%>%
  mutate(wasc="wessex")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.start)%>%
  rename(end=Discharge.end)%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=gsub("/", "-", start))%>%
  mutate(start=sub("T", " ", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=gsub("/", "-", end))%>%
  mutate(end=sub("T", " ", end))%>%
  separate(start, into = c("startdate", "starttime"), sep = " ")%>%
  separate(end, into = c("enddate", "endtime"), sep = " ")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2, -Storm.Discharge.Asset.Type..Treatment.Works.)



#check<-wessex%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#two missing
#one from DST in March
#another in March, starting march 11 - need to check why
#na_index<-which(is.na(wessex$startdatetime) | is.na(wessex$enddatetime))
#reload wessex
#checking<-wessex[na_index,]
#both overlap with DST
#which means one spill lasted for almost an entire month continuously.
#applied DST logic and solved



#yorkshire
yorkshire<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2022Y.csv")%>%
  mutate(wasc="yorkshire")%>%
  rename(EAsitename=Site.Name..EA.Consents.Database.)%>%
  rename(start=EventStartDatetimeUtc)%>%
  rename(end=EventEndDatetimeUtc)%>%
  rename(id=YWDischargeURN)%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=gsub("/", "-", start))%>%
  mutate(start=sub("T", " ", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=gsub("/", "-", end))%>%
  mutate(end=sub("T", " ", end))%>%
  separate(start, into = c("startdate", "starttime"), sep = " ")%>%
  separate(end, into = c("enddate", "endtime"), sep = " ")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2, -EventDurationInHours)

yorkshire$EventDurationInHours




#check<-yorkshire%>%
#  filter(is.na(enddatetime) | is.na(startdatetime))
#3 missing
#one definitely from DST - check other 2
#na_index<-which(is.na(yorkshire$startdatetime) | is.na(yorkshire$enddatetime))
#reload yorkshire
#checking<-yorkshire[na_index,]
#all 3 are missing due to DST
#solved



#merge these
data22<-bind_rows(anglian, northumbrian, severn, southern, southwest, thames, united, welsh, wessex, yorkshire)

rm(anglian, northumbrian, severn, southern, southwest, thames, united, welsh, wessex, yorkshire)

data22<-data22%>%
  select(-WaSC.Supplementary.Permit.Ref...optional.)

#save this data
save(data22, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/2022spillraw.RData")
#2,130,183
#2,129,895 with NAs solved 


#go from here
rm(list=ls())
gc()

library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)

#last raw spill data to merge

#2023:

#anglian
anglian<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2023A.csv")%>%
  mutate(wasc="anglian")%>%
  rename(unique_id=Unique.ID)%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.Stop)%>%
  mutate(start = sub("Z$", "", start)) %>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)

#anglian water now (like all 2023 data) only has some unique id, name and start & stop times
#no more EA name or code
#so when matching - should use an index from 2021 and 2022 data, and then deal with any missing



#checking for missing values
#check<-anglian%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#2 in March - likely DST which changed 2:00AM March 26th 2023
#applying DST logic
#na_index<-which(is.na(anglian$startdatetime) | is.na(anglian$enddatetime))
#checking<-anglian[na_index,]
#well now that i changed the anglian script I don't have the na_index to check exactly
#but I can still look for NAs
#no NAs anymore  

#welsh
welsh<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2023Welsh.csv")%>%
  mutate(wasc="welsh")%>%
  rename(unique_id=Unique.ID)%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.Stop)%>%
  mutate(start = sub("Z$", "", start)) %>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)

#check for DST issues
#check<-welsh%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#one missing
#na_index<-which(is.na(welsh$startdatetime) | is.na(welsh$enddatetime))
#checking<-welsh[na_index,]
#DST issue
#solved






#northumbrian
northumbrian<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2023N.csv")%>%
  rename(unique_id=Unique.ID)%>%
  mutate(wasc="northumbrian")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.start)%>%
  rename(end=Discharge.Stop)%>%
  mutate(start = sub("Z$", "", start))%>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)



#NAs from DST
#check<-northumbrian%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#na_index<-which(is.na(northumbrian$startdatetime) | is.na(northumbrian$enddatetime))
#checking<-northumbrian[na_index,]
#solved






#severn trent
severn<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2023ST.csv")%>%
  rename(unique_id=Unique.ID)%>%
  mutate(wasc="severntrent")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.start)%>%
  rename(end=Discharge.stop)%>%
  mutate(start = sub("Z$", "", start))%>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)


  
  
  

#missing values check
#check<-severn%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#14 missing
#some clearly DST
#na_index<-which(is.na(severn$startdatetime) | is.na(severn$enddatetime))
#checking<-severn[na_index,]
#solved



#south west
southwest<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2023SW.csv")%>%
  rename(unique_id=Unique.ID)%>%
  mutate(wasc="southwest")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.Stop)%>%
  mutate(start = sub("Z$", "", start))%>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)


  
  
#missing values
#check<-southwest%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#169 missing - this is significant
#large proportion are clearly DST issues
#na_index<-which(is.na(southwest$startdatetime) | is.na(southwest$enddatetime))
#checking<-southwest[na_index,]
#yes all 169 were DST issues
#solved





#southern
southern<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2023S.csv")%>%
  rename(unique_id=SRN)%>%
  mutate(wasc="southern")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  mutate(start = sub("Z$", "", start))%>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)


#checking for missing values
#check<-southern%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#12 missing
#na_index<-which(is.na(southwest$startdatetime) | is.na(southwest$enddatetime))
#checking<-southwest[na_index,]
#solved with DST logic






#thames
thames<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2023T.csv")%>%
  rename(unique_id=Unique.ID)%>%
  mutate(wasc="thames")%>%
  rename(site=Site.name)%>%
  rename(start=Start_Timestamp)%>%
  rename(end=Stop_Timestamp)%>%
  
  mutate(start = sub("Z$", "", start))%>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)


#checking for missing values
#check<-thames%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#8 missing all DST
#na_index<-which(is.na(thames$startdatetime) | is.na(thames$enddatetime))
#checking<-thames[na_index,]
#solved





#united utilities just one file for 2023
united<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2023U.csv")%>%
  rename(unique_id=Unique.ID)%>%
  mutate(wasc="united")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.Stop)%>%
  mutate(start=trimws(start))%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=sub("T", " ", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=sub("T", " ", end))%>%
  mutate(startdatetime = as.POSIXct(start, format="%Y-%m-%d %H:%M"),
         enddatetime = as.POSIXct(end, format="%Y-%m-%d %H:%M")) %>%
  select(-start, -end)

#checking for missing values
#check<-united%>%
#  filter(is.na(startdatetime) | is.na(enddatetime))
#0 missing






#wessex
wessex<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2023W.csv")%>%
  rename(unique_id=EA.ID)%>%
  mutate(wasc="wessex")%>%
  rename(site=Site.Name)%>%
  rename(start=Discharge.Start)%>%
  rename(end=Discharge.End)%>%
  #mutate(startdatetime = as.POSIXct(start, format="%d/%m/%Y %H:%M"),
  #       enddatetime = as.POSIXct(end, format="%d/%m/%Y %H:%M"))%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=gsub("/", "-", start))%>%
  mutate(start=sub("T", " ", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=gsub("/", "-", end))%>%
  mutate(end=sub("T", " ", end))%>%
  separate(start, into = c("startdate", "starttime"), sep = " ")%>%
  separate(end, into = c("enddate", "endtime"), sep = " ")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%d-%m-%Y %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%d-%m-%Y %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%d-%m-%Y %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%d-%m-%Y %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)

#checking for DST missing values
check<-wessex%>%
  filter(is.na(startdatetime) | is.na(enddatetime))
#185 missing this is significant
na_index<-which(is.na(wessex$startdatetime) | is.na(wessex$enddatetime))
checking<-wessex[na_index,]
#all solved



#yorkshire
yorkshire<-read.csv("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/Individual/2023Y.csv")%>%
  rename(unique_id=Unique.ID)%>%
  mutate(wasc="yorkshire")%>%
  rename(site=Site.Name)%>%
  rename(start=EventStartDatetimeUtc)%>%
  rename(end=EventEndDatetimeUtc)%>%
  mutate(start = sub("Z$", "", start))%>%
  mutate(start=gsub("/", "-", start))%>%
  mutate(end=trimws(end))%>%
  mutate(end = sub("Z$", "", end))%>%
  mutate(end=gsub("/", "-", end))%>%
  mutate(start = sub("Z$", "", start))%>%
  separate(start, into = c("startdate", "starttime"), sep = "T")%>%
  mutate(end = sub("Z$", "", end)) %>%
  separate(end, into = c("enddate", "endtime"), sep = "T")%>%
  mutate(starttime2=as.numeric(substr(starttime, 1, 2))+1)%>%
  mutate(endtime2=as.numeric(substr(endtime, 1, 2))+1)%>%
  mutate(startsub=substr(starttime, 3, nchar(starttime)))%>%
  mutate(endsub=substr(endtime, 3, nchar(endtime)))%>%
  mutate(starttimei=paste(starttime2, startsub))%>%
  mutate(starttimei=gsub(" ", "", starttimei))%>%
  mutate(endtimei=paste(endtime2, endsub))%>%
  mutate(endtimei=gsub(" ", "", endtimei))%>%
  mutate(starttimei = if_else(nchar(starttimei) == 7, str_pad(starttimei, width = 8, side = "left", pad = "0", use_width = FALSE), starttimei))%>%
  mutate(endtimei = if_else(nchar(endtimei) == 7, str_pad(endtimei, width = 8, side = "left", pad = "0", use_width = FALSE), endtimei))%>%
  mutate(startdatetime1 = as.POSIXct(paste(startdate, starttime), format="%Y-%m-%d %H:%M"),
         enddatetime1 = as.POSIXct(paste(enddate, endtime), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime2 = as.POSIXct(paste(startdate, starttimei), format="%Y-%m-%d %H:%M"),
         enddatetime2 = as.POSIXct(paste(enddate, endtimei), format="%Y-%m-%d %H:%M"))%>%
  mutate(startdatetime = coalesce(startdatetime1, startdatetime2))%>%
  mutate(enddatetime = coalesce(enddatetime1, enddatetime2))%>%
  select(-startdate, -starttime, -enddate, -endtime, -starttime2, -endtime2, -startsub, -endsub, -starttimei, -endtimei, -startdatetime1, -startdatetime2, -enddatetime1, -enddatetime2)


#checking for DST missing values
check<-yorkshire%>%
  filter(is.na(startdatetime) | is.na(enddatetime))
#25 missing from DST
na_index<-which(is.na(yorkshire$startdatetime) | is.na(yorkshire$enddatetime))
checking<-yorkshire[234865,]





#merge these
data23<-bind_rows(anglian, northumbrian, severn, southern, southwest, thames, united, welsh, wessex, yorkshire)

rm(anglian, northumbrian, severn, southern, southwest, thames, united, welsh, wessex, yorkshire)

#save this data
save(data23, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/2023spillraw.RData")
#1,808,508 observations
#same no. after dealing with NAs (all DST)



#merging the 3 dataframes

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/2021spillraw.RData")
load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/2022spillraw.RData")
load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/2023spillraw.RData")

data21<-data21%>%
  mutate(unique_id=NA)%>%
  select(-EventDurationInHours)
data22<-data22%>%
  mutate(unique_id=NA)
data23<-data23%>%
  mutate(EAnumber=NA)%>%
  mutate(misc_code=NA)%>%
  mutate(id=NA)%>%
  mutate(EAsitename=NA)


raw_spill<-rbind(data21, data22, data23)
#6,175,159
#6,174,561 after dealing with all NAs and DST issues causing missing values
#9 columns
save(raw_spill, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/raw_spill.RData")

