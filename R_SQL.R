library(bigmemory)
library(DBI)
library(dplyr)
library(dbplyr)
library(odbc)
library(RODBC)
library(tidyverse)
library(sparklyr)
library(SparkR)
library(Rcpp, Amelia)
library(ggplot2, plotly)
library(SparkR)
library(RSQLite)
#####Data_base_connexion
connStr <- paste("Server= imar-sql",
                 "UserName = ediagne", 
                 "Password = Wddd#807",
                 "Driver = {SQL Server}", 
                 Sep =";"
)
### data use from sql  PC-AI\SQLEXPRESS
con <- odbcDriverConnect("driver={SQL Server};server=(local)\\SQLEXPRESS;database=WaterLevelArchiver.WaterLevelDatabase;trusted_connection=yes")

#library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#sparkR.session(sparkPackages = "com.databricks:spark-avro_2.11:3.0.0")
conn <- odbcDriverConnect('driver={SQL Server};server=imar-sql;database=WaterLevelArchiver.WaterLevelDatabase;trusted_connection=true')
datafetch <- sqlFetch(connStr, "DataFetchLogs")
diffstation <- sqlFetch(conn, "DiffStations")
hiloes <- sqlFetch(conn, "HiLoes")
observation <- sqlFetch(con, "Observations")
predictions <- sqlFetch(con, "predictions")
stations <- sqlFetch(con, "Stations")
tides <- sqlQuery(con, "Tides")
### Identifier le nombre<de valeurs unique dans chaque variables
sapply(datafetch, function(x) length(unique(x)))
sapply(hiloes, function(x) length(unique(x)))
### Identifier le nombre de valeurs manquqntes dans chaque variables
sapply(datafetch, function(x) sum(is.na(x)))
sapply(diffstation, function(x) sum(is.na(x)))
sapply(hiloes, function(x) sum(is.na(x)))### NA in Hilotype 
sapply(observations, function(x) sum(is.na(x)))### NA in Hiloflag
sapply(predictions, function(x) sum(is.na(x)))### NA in Hiloflag
sapply(stations, function(x) sum(is.na(x)))###
sapply(tides, function(x) sum(is.na(x)))### 
## Les lignes presentqnt NA 
hiloes[is.na(hiloes$HiLoType),]
observations[is.na(observations$HiLoFlag),]
predictions[is.na(predictions$HiLoFlag),]
### graph ;issing value
missmap(predictions, main = "Graphiaue des valeurs manqantes")
### Traitement Datafetch
### Traitement Diffstations
### Traitement Hiloes
         ### Extraction de l'année et du mois dans la variable Time
hiloes$Year<-substr(hiloes$Time, 1,4)
hiloes$month<-substr(hiloes$Time, 6,7)
        ###Visualisation
ggplot(hiloes, aes(hiloes$month, hiloes$WaterLevel)) + geom_point(mapping = aes(colour = hiloes$HiOrLo, size=Source))
ggplot(hiloes, aes(hiloes$Year, hiloes$WaterLevel)) + geom_line(mapping = aes(colour = Source, size=HiOrLo)) + facet_wrap(~hiloes$HiOrLo, ncol = 2)
ggplot(hiloes, aes(Year, WaterLevel)) + geom_boxplot(aes(colour = HiOrLo))
ggplot(hiloes, aes(Year, WaterLevel)) + geom_jitter(aes(colour = Source)) #### visualisation du bruit et valeurs abberrantes
##################### cmparaison zater level hiloes predic
#merge hiloes predictions
hiloes$date<-substr(hiloes$Time,1, 10 )
predictions$date<-predictions$Time
hilo_pred<- merge(hiloes, predictions,by = 'date', all.x=TRUE)### merge hiloes  predictions by date
par(mfrow = c(2,1))
ggplot(hilo_pred, aes(date, WaterLevel.x)) + geom_jitter(aes(colour = Station_Id.x)) #### visualisation du bruit et valeurs abberrantes
ggplot(hilo_pred, aes(date, WaterLevel.y)) + geom_jitter(aes(colour = Station_Id.y)) #### visualisation du bruit et valeurs abberrantes

###### Modelisation#################################################################################

if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, data.table,catools, astsa,forecast, metrics)
library(rmarkdown)
library(knitr)
library(gridExtra)
library(sparklyr)
library(dplyr)
library(DBI)
library(sparkapi)#############"
library(ggplot2)
library(plotly)
library(dygraphs)
library(xts)
library(RODBC)
library(tidyverse)
library(bigmemory)
library(forecast)
library(htmltools)
library(RColorBrewer)
# xts, forecast,zoo,bigmemory
dbconnection <- odbcDriverConnect("Driver=SQL Server;Server=(local)\\SQLEXPRESS2012;Database=WaterLevelArchiver.WaterLevelDatabase;trusted_connection=yes")
#observation <- sqlFetch(dbconnection, 'dbo.Observations', colnames=FALSE)
#prediction<- sqlFetch(dbconnection, 'dbo.predictions', colnames=FALSE, rows_at_time=1000)
##data partition
observation<- sqlFetch(conn, "Observations")
set.seed(123)
#data$id <- 1:nrow(observation)
#obs <- observation %>% dplyr::sample_frac(.0015)
#obs<-obs[order(obs$Time),]
obs_16<-observation[(substr(observation$Time,1,4) == 2016),]
obs_17<-observation[(substr(observation$Time,1,4) == 2017),]
obs_18<-observation[(substr(observation$Time,1,4) == 2018),]
obs_19<-observation[(substr(observation$Time,1,4) == 2019),]
obs_20<-observation[(substr(observation$Time,1,4) == 2020)  & observation$Id<=1774868,]
                    #& substr(observation$Time,1,10) != "2020-03-15" 
                    #& substr(observation$Time,1,10) !="2020-03-14",] ## Laisser le jour 14 pour 
## Prévision l'usage du niveau d'eau moyen par jour
obs_20$date<-substr(obs_20$Time, 1,10)
  obs_20J<-obs_20 %>% 
    group_by(date) %>% 
    mutate(mean_wl = mean(WaterLevel)) %>% slice(1)
ts1<-ts(obs_20J$mean_wl,)
fit1<-auto.arima((ts1))
pred1<-forecast(fit1, 5)
autoplot(pred1, main = "Prédiction du niveau d'eau moyen des 5 prochains jour")
observées1<-xts(pred1$x, order.by = obs_20J$Time)
ajustées1<-xts(pred1$fitted, order.by = obs_20J$Time)
my_data1<-cbind(observées1,ajustées1)
dygraph(my_data1) %>% browsable(tagList(dyg)) %>% dyOptions(colors = brewer.pal(2,"Set2"))
##############################################################
## Prévision l'usage du niveau d'eau moyen par heure
obs_20$date1<-substr(obs_20$Time,1, 13)
obs_20H<-obs_20 %>% 
  group_by(date1) %>% 
  mutate(mean_wl1 = mean(WaterLevel)) %>% slice(1)
ts2<-ts(obs_20H$mean_wl1)
fit2<-auto.arima((ts2))
pred2<-forecast(fit2, 24)
autoplot(pred2, main = "Prédiction du niveau d'eau moyen des prochaines 24 heures")
observées2<-xts(pred2$x, order.by = obs_20H$Time)
ajustées2<-xts(pred2$fitted, order.by = obs_20H$Time)
my_data2<-cbind(observées2,ajustées2)
dygraph(my_data2, main= "Données observées et predites de l'élevation du niveau d'eau mars 2020") %>% browsable(tagList(dyg)) %>% dyOptions(colors = brewer.pal(2,"Set2"))
## Prévision l'usage du niveau d'eau 3 par mn##############
obs1<-observation
obs1$date2<-substr(obs1$Time, 1, 16)
obs_20m<-obs1 %>%
  group_by(date2) %>%
  mutate(mean_wl3 = mean(WaterLevel))# %>% slice(1)
ts3<-ts(obs_20m$mean_wl3)
fit3<-auto.arima((ts3))
pred3<-forecast(fit3, 10)
summary(pred3)
observées3<-xts(pred3$x, order.by = obs_20m$Time)
ajustées3<-xts(pred3$fitted, order.by = obs_20m$Time)
#SHC<-xts(pred3$x, order.by = obs_20m$Time)
my_data<-cbind(observées3,ajustées3)
dygraph(my_data) %>% browsable(tagList(dyg)) %>% dyOptions(colors = brewer.pal(3,"Dark2"))%>%dyRangeSelector()

dygraph<- list(
  dygraph(my_data2, main = "Modèle2 (mape = 4.67)"),
  dygraph(my_data, main = "Modèle3 (mape = 0.75)")
)
browsable(tagList(dygraph)) 
###### Ajout des infos de la prediction SHC
pred_20<-predictions[((substr(predictions$Time,1,4) == 2020) & (substr(predictions$Time,6,10) < "03-15")),]
pred$year<substr(pred_20$Time,1, 4 )











