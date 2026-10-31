library(taskscheduleR)#lire à une date donnée
library(parallel)
library(bigmemory)
library(DBI)
library(dplyr)
library(dbplyr)
library(RODBC)
library(odbc)
library(tidyverse)
library(sparklyr)
library(SparkR)
library(Rcpp, Amelia)
library(ggplot2, plotly)
library(SparkR)
library(RSQLite)
library(forecast)
library(bigmemory)
library(htmltools)
library(RColorBrewer)
library(xts)
library(plotly)
library(dygraphs)
library(lubridate)
library(SimDesign)
library(gbm)      
library(xgboost) 
library(jsonlite)
library(rjson)
library(googledrive)
conn <- odbcDriverConnect('driver={SQL Server};server=imar-sql;database=WaterLevelArchiver.WaterLevelDatabase;trusted_connection=true')
observation <- sqlFetch(conn, "Observations")
#observation$Time<- strftime(observation$Time, "%Y-%m-%dT%H:%M:%SZ")
observation$Time<-ymd_hms(observation$Time,tz = "UTC")
#observation$Time<-force_tz(observation$Time,tzone = "")
#observation$Time<-ymd_hms(observation$Time, tz = "EST")
prediction <- sqlFetch(conn, "Predictions")
prediction<-read.csv2("C:/Users/ediagne/OneDrive/R_Data/predictions_2016-2020.csv", header = TRUE, dec = ",")
prediction$WaterLevel<-as.numeric(prediction$WaterLevel)
prediction$Time<-ymd_hms(prediction$Time,tz = "UTC")
#prediction$Time<-force_tz(prediction$Time,tzone = "")
observation1<-observation[observation$Station_Id==1,]
#observation1<- observation1 %>% 
  #filter(abs(WaterLevel-mean(WaterLevel))<=2*sd(WaterLevel))## Drop outliers WaterLevel
observation2<-observation[observation$Station_Id==3,]
observation2<- observation2 %>% 
  filter(abs(WaterLevel-mean(WaterLevel))<=2*sd(WaterLevel))## Drop outliers WaterLevel
observation3<-observation[observation$Station_Id==5,]
observation3<- observation3 %>% 
  filter(abs(WaterLevel-mean(WaterLevel))<=2*sd(WaterLevel))## Drop outliers WaterLevel
#prediction$Time<-as.POSIXct(prediction$Time, tz = "UTC")#; prediction$WaterLevel<-as.double(prediction$WaterLevel)
prediction1<-prediction[prediction$Station_Id==1,]
prediction2<-prediction[prediction$Station_Id==3,]
prediction3<-prediction[prediction$Station_Id==5,]
##
#predictions <- sqlFetch1
newdata<-read.csv2("C:/Users/ediagne/OneDrive/R_Data/predictions-2016-octobre2020.csv", header = TRUE, dec = ",")
newdata<-newdata[,c(-1,-4)]
newdata$Time<-ymd_hms(newdata$Time, tz = "UTC");newdata$Time<-strftime(newdata$Time, "%Y-%m-%dT%H:%M:%SZ")
newdata$WaterLevel<-as.double(newdata$WaterLevel); newdata$WaterLevel<-round(newdata$WaterLevel, digits = 4)
newdata<-rename(newdata, WaterLevel.pred = WaterLevel)
#############################################STATION I FORECASTING ###########################################
newdata1<-newdata[newdata$Station_Id==1,]
newdata1<-newdata1[substr(newdata1$Time, 1, 19)> today() & substr(newdata1$Time, 1, 19)<= today()+5,]
data1<-merge(observation1, prediction1, by = "Time")
obs1<-as.data.frame(data1[c(1,4,9)])
obs1<-obs1 %>% 
  rename(
    WaterLevel = WaterLevel.x,
    Prediction_SHC = WaterLevel.y
  )
obs1<-obs1[substr(obs1$Time, 1,19)<="Sys.Time()",]
#obs1$Time<-strftime(obs1$Time,"%Y-%m-%dT%H:%M:%SZ", tz = "UTC"); obs1$Time<-force_tz(obs1$Time,tzone = "")
train1<-obs1[substr(obs1$Time,1,10)<"2017-03-15",]
gbm.fit.final <- gbm(
  formula = WaterLevel~Prediction_SHC,
  data = obs1,
  distribution  = "laplace",
  n.trees = 500,
  interaction.depth = 25,
  shrinkage = 0.1,
  n.minobsinnode = 5,
  bag.fraction = .85, 
  train.fraction = 1,
  n.cores = 7, # will use all cores by default
  verbose = FALSE
)  
predict_model.gbm <- function(x, newdata, ...) {
  pred <- predict(x, newdata, n.trees = x$n.trees)
  return(as.data.frame(pred))
}
pred <- predict(gbm.fit.final, n.trees = gbm.fit.final$n.trees, newdata1)
newdata1<-rename(newdata1, Prediction_SHC = WaterLevel.pred)
newdata1$Prediction_Imar<-pred
newdata1$Prediction_Imar<-round(newdata1$Prediction_Imar, digits = 3)
newdata<-newdata[,-1]
write.csv(newdata1, "C:/Users/ediagne/OneDrive/R_Data/Resultats_Modele_html/Output_S1.csv", row.names = FALSE)

drive_rm("Output_S1.csv")
drive_upload ("C:/Users/ediagne/OneDrive/R_Data/Resultats_Modele_html/obs1.csv", name = "Output_S1.csv") #paste("obs1_", Sys.time()))
file <- "Output_S1.csv" %>%
  drive_share(
    role = "reader",
    type = "user",
    emailAddress = "ediagne@imar.ca"
  )
#############################################STATION II FORECASTING ###########################################
newdata2<-newdata[newdata$Station_Id==2,]
newdata2<-newdata2[substr(newdata2$Time, 1, 19)> today() & substr(newdata2$Time, 1, 19)<= today()+5,]
data2<-merge(observation2, prediction2, by = "Time")
obs2<-as.data.frame(data2[c(1,4,9)])
obs2<-obs2 %>% 
  rename(
    WaterLevel.obs = WaterLevel.x,
    WaterLevel.pred = WaterLevel.y
  )
obs2<-obs2[substr(obs2$Time, 1,19)<="Sys.Time()",]
gbm.fit.final <- gbm(
  formula = WaterLevel.obs~WaterLevel.pred,
  data = obs2,
  distribution  = "laplace",
  n.trees = 500,
  interaction.depth = 25,
  shrinkage = 0.1,
  n.minobsinnode = 5,
  bag.fraction = .85, 
  train.fraction = 1,
  n.cores = 7, # will use all cores by default
  verbose = FALSE
)  
predict_model.gbm <- function(x, newdata, ...) {
  pred2 <- predict(x, newdata, n.trees = x$n.trees)
  return(as.data.frame(pred))
}
pred2 <- predict(gbm.fit.final, n.trees = gbm.fit.final$n.trees, newdata2)
newdata2<-rename(newdata2, Prediction_SHC = WaterLevel.pred)
newdata2$Prediction_Imar<-pred2
newdata2$Prediction_Imar<-round(newdata2$Prediction_Imar, digits = 3)
newdata2<-newdata2[,-1]
write.csv(newdata2, "C:/Users/ediagne/OneDrive/R_Data/Resultats_Modele_html/Output_S2.csv", row.names = FALSE)

drive_rm("Output_S2.csv")
drive_upload ("C:/Users/ediagne/OneDrive/R_Data/Resultats_Modele_html/Output_S2.csv", name = "Output_S2.csv") #paste("obs1_", Sys.time()))
file <- "Output_S2.csv" %>%
  drive_share(
    role = "reader",
    type = "user",
    emailAddress = "ediagne@imar.ca"
  )

#############################################STATION III FORECASTING ###########################################
newdata3<-newdata[newdata3$Station_Id==3,]
newdata3<-newdata1[substr(newdata3$Time, 1, 19)> today() & substr(newdata31$Time, 1, 19)<= today()+5,]
data3<-merge(observation3, prediction3, by = "Time")
obs3<-as.data.frame(data3[c(1,4,9)])
obs3<-obs3 %>% 
  rename(
    WaterLevel.obs = WaterLevel.x,
    WaterLevel.pred = WaterLevel.y
  )
obs3<-obs1[substr(obs3$Time, 1,19)<="Sys.Time()",]
gbm.fit.final <- gbm(
  formula = WaterLevel.obs~WaterLevel.pred,
  data = obs3,
  distribution  = "laplace",
  n.trees = 500,
  interaction.depth = 25,
  shrinkage = 0.1,
  n.minobsinnode = 5,
  bag.fraction = .85, 
  train.fraction = 1,
  n.cores = 7, # will use all cores by default
  verbose = FALSE
)  
predict_model.gbm <- function(x, newdata, ...) {
  pred3 <- predict(x, newdata, n.trees = x$n.trees)
  return(as.data.frame(pred))
}
pred3 <- predict(gbm.fit.final, n.trees = gbm.fit.final$n.trees, newdata3)
newdata3<-rename(newdata3, Prediction_SHC = WaterLevel.pred)
newdata3$Prediction_Imar<-pred3
newdata3$Prediction_Imar<-round(newdata3$Prediction_Imar, digits = 3)
newdata3<-newdata3[,-1]
write.csv(newdata3, "C:/Users/ediagne/OneDrive/R_Data/Resultats_Modele_html/Output_S3.csv", row.names = FALSE)

drive_rm("Output_S3.csv")
drive_upload ("C:/Users/ediagne/OneDrive/R_Data/Resultats_Modele_html/Output_S3.csv", name = "Output_S3.csv") #paste("obs1_", Sys.time()))
file <- "Output_S3.csv" %>%
  drive_share(
    role = "reader",
    type = "user",
    emailAddress = "ediagne@imar.ca"
  )







