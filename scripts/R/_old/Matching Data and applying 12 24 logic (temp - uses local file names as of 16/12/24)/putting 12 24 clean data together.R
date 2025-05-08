#now that data has been matched, cleaned and processed in 12 24 method
#join all wasc dataframes

rm(list=ls())
gc()



library(dplyr)
library(tidyr)
library(lubridate)
library(stringr)


load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/raw_spill.RData")
#this is all spill data merged

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/anglianclean.RData")

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/northumbrianclean.RData")

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/severnclean.RData")

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/southernclean.RData")

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/southwestclean.RData")

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/thamesclean.RData")

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/unitedclean.RData")

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/welshclean.RData")

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/wessexclean.RData")

load("C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/yorkshireclean.RData")



clean_list <- list(anglianclean, northumbrianclean, 
                   severnclean, southernclean, southwestclean, 
                   thamesclean, unitedclean, welshclean, 
                   wessexclean, yorkshireclean)

all_spills_1224 <- do.call(bind_rows, clean_list)%>%
  distinct(site, .keep_all = TRUE)


df1<-rbind(anglianclean, northumbrianclean)
df2<-rbind(severnclean, southernclean)
df3<-rbind(southwestclean, thamesclean)
df4<-rbind(unitedclean, welshclean)
df5<-rbind(wessexclean, yorkshireclean)


all_spills_1224<-rbind(df1, df2)
all_spills_1224<-rbind(all_spills_1224, df3)
all_spills_1224<-rbind(all_spills_1224, df4)
all_spills_1224<-rbind(all_spills_1224, df5)
#1,128,841 total 12 24 spills


save(all_spills_1224, file = "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/all_spills_1224.RData")

#write to 2 excel files


library(writexl)

first_half<-all_spills_1224[(1:500000),]
second_half<-all_spills_1224[-(1:500000),]

write_xlsx(first_half, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/all_spills_1224_firsthalf.xlsx")

write_xlsx(second_half, "C:/Users/danan/Documents/2024 Internship/Circular economy project/Data/Storm overflow/EDM data/output/12 24 data/all_spills_1224_secondhalf.xlsx")
