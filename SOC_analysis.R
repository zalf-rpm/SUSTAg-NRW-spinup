rm(list = ls())
library(datasets)
library(ggplot2)
library(dplyr)
library(tidyr)

setwd("C:/Users/stella/Documents/GitHub/SUSTAg-NRW-spinup/SOC_out/final/")

#read the dataset
file <- "00_SOC_data_all.csv"
dataset <- read.table(file, header=TRUE, sep=",")
dataset$exp_id <- factor(dataset$exp_id)
dataset$res_mgt <- factor(dataset$res_mgt)
dataset$cc <- factor(dataset$cc)
dataset$rotation <- factor(dataset$rotation)
dataset$KA5_txt <- factor(dataset$KA5_txt)
dataset$soil_type <- factor(dataset$soil_type)
dataset$orgN_kreis <- factor(dataset$orgN_kreis)
#dataset$cc_return_t <- factor(dataset$cc_return_t)

levels(dataset$rotation)[levels(dataset$rotation)=="1110"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="1120"] <- "WW-WB-WTr-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="1130"] <- "GM-WTr"
levels(dataset$rotation)[levels(dataset$rotation)=="2110"] <- "WB-SM-WTr"
levels(dataset$rotation)[levels(dataset$rotation)=="2120"] <- "WW-WB-GM"
levels(dataset$rotation)[levels(dataset$rotation)=="2130"] <- "SM-GM"
levels(dataset$rotation)[levels(dataset$rotation)=="3110"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="3120"] <- "WW-WB-WTr-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="3130"] <- "WW-GM"
levels(dataset$rotation)[levels(dataset$rotation)=="4110"] <- "WW-SM-WB-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="4120"] <- "WW-WB-WTr-GM"
levels(dataset$rotation)[levels(dataset$rotation)=="5110"] <- "WW-WB-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="5120"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="5130"] <- "WW-WTr-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="6110"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="6120"] <- "WW-WRa-GM"
levels(dataset$rotation)[levels(dataset$rotation)=="6130"] <- "WW-SBee-WW-PO"
levels(dataset$rotation)[levels(dataset$rotation)=="7110"] <- "WW-WB-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="7120"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="7130"] <- "WW-SB-WTr-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="8110"] <- "WW-WB-SBee"
levels(dataset$rotation)[levels(dataset$rotation)=="8120"] <- "WW-SM-WW-PO"
levels(dataset$rotation)[levels(dataset$rotation)=="8130"] <- "WW-WW-SBee"
levels(dataset$rotation)[levels(dataset$rotation)=="9110"] <- "WW-SB-WRa"
levels(dataset$rotation)[levels(dataset$rotation)=="9120"] <- "WW-WB-SM"
levels(dataset$rotation)[levels(dataset$rotation)=="9130"] <- "WW-WB-WTr"

####################################################
#Random forest
library(randomForest)
#memory.limit(size = 25000)

sample_dataset <- sample_n(dataset, 10000) #needed for memory limitations

set.seed(12312)
set.seed(84561)
RF_SOC4750 <- randomForest(SOC4750 ~  res_mgt + cc + refSOC9902 + rotation 
                            + p_id + KA5_txt + orgN_kreis,
                            importance=TRUE, proximity=TRUE, data=sample_dataset, na.action=na.omit, ntree=500)

RF_SOC4750

importance(RF_SOC4750)

varImpPlot(RF_SOC4750)

plot_RF_SOC4750<-sort(importance(RF_SOC4750, type=1)[,1], decreasing=FALSE)
####################################################

#(for testing convenience)
#sample_dataset <- subset(dataset, rotation == "7130")
#sample_dataset <- sample_n(dataset, 10000)

#convert data to long
data_long <- gather(dataset, year, rel_SOC, SOC7174:SOC4750, factor_key=TRUE)
#data_long <- gather(sample_dataset, year, rel_SOC, SOC7174:SOC4750, factor_key=TRUE)

#convert years (e.g. SOC7174) to numbers (1972)
data_long$year <- as.character(data_long$year)
data_long$year[data_long$year == "SOC7174"] <- "1972"
data_long$year[data_long$year == "SOC7578"] <- "1976"
data_long$year[data_long$year == "SOC7982"] <- "1980"
data_long$year[data_long$year == "SOC8386"] <- "1984"
data_long$year[data_long$year == "SOC8790"] <- "1988"
data_long$year[data_long$year == "SOC9194"] <- "1992"
data_long$year[data_long$year == "SOC9598"] <- "1996"
data_long$year[data_long$year == "SOC9902"] <- "2000"
data_long$year[data_long$year == "SOC0306"] <- "2004"
data_long$year[data_long$year == "SOC0710"] <- "2008"
data_long$year[data_long$year == "SOC1114"] <- "2012"
data_long$year[data_long$year == "SOC1518"] <- "2016"
data_long$year[data_long$year == "SOC1922"] <- "2020"
data_long$year[data_long$year == "SOC2326"] <- "2024"
data_long$year[data_long$year == "SOC2730"] <- "2028"
data_long$year[data_long$year == "SOC3134"] <- "2032"
data_long$year[data_long$year == "SOC3538"] <- "2036"
data_long$year[data_long$year == "SOC3942"] <- "2040"
data_long$year[data_long$year == "SOC4346"] <- "2044"
data_long$year[data_long$year == "SOC4750"] <- "2048"
#data_long$year_numeric <- as.numeric(data_long$year)
data_long$year_factor <- as.factor(data_long$year)

#levels(data_long$rotation)[levels(data_long$rotation)=="1110"] <- "WW-WB-SM"
levels(data_long$rotation)


RelSOCboxplot<-function(df, x, fill, legend){
  boxplot <- ggplot(df, aes(x=x, y=rel_SOC)) + 
    geom_boxplot(aes(fill=factor(reorder(fill, rel_SOC))), outlier.shape=NA)+
    xlab("Year") +
    ylab(NULL)
  boxplot <- boxplot + geom_hline(yintercept = 0) +
    coord_cartesian(ylim = c(-1, 1)) +
    ggtitle("Relative SOC change from base year (2000)") +
    guides(fill=guide_legend(title=legend)) +
    theme_bw()
  boxplot
}

#PLOTS
#1. all rotations
#1a. disaggregate residue management
RelSOCboxplot(df=data_long, x=data_long$year_factor, fill=data_long$res_mgt, legend="residue management")
ggsave("id0-11_SOCchange_resmgt.png", dpi=300)

#1b. disaggregate CC freq
RelSOCboxplot(df=data_long, x=data_long$year_factor, fill=data_long$cc, legend="cc freq")
ggsave("id0-11_SOCchange_cc.png", dpi=300)

#1c. disaggregate rotations
RelSOCboxplot(df=data_long, x=data_long$year_factor, fill=data_long$rotation, legend="rotation")
ggsave("id0-11_SOCchange_rot.png", dpi=300)

#1.d disaggregate res_mgt * rotations - I could not wrap this in a function due to facet_wrap...
boxplot <- ggplot(data_long, aes(x = year_factor, y = rel_SOC)) + 
  geom_boxplot(aes(fill=factor(reorder(res_mgt, rel_SOC))), outlier.shape=NA)+
  xlab("Year") +
  ylab(NULL)
boxplot <- boxplot + facet_wrap(~ rotation)
boxplot <- boxplot + geom_hline(yintercept = 0) +
  coord_cartesian(ylim = c(-1, 1)) +
  ggtitle("Relative SOC change from base year (2000)") +
  guides(fill=guide_legend(title="residue management")) +
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90, hjust = 0))
boxplot

ggsave("id0-11_SOCchange_resmgt_rot.png", width=15, height=10, dpi=200)

#2. for each rotation

#subset
for (rot in levels(data_long$rotation)){
  print(rot)
  #for facet label text: http://www.cookbook-r.com/Graphs/Facets_(ggplot2)/#modifying-facet-label-text
  
  dataset_rotation <- subset(data_long, rotation == rot)
  
  boxplot <- ggplot(dataset_rotation, aes(x = year_factor, y = rel_SOC)) + 
    geom_boxplot(aes(fill=factor(reorder(res_mgt, rel_SOC))), outlier.shape=NA)+
    xlab("Year") +
    ylab(NULL)
  boxplot <- boxplot + facet_wrap(~ cc)
  boxplot <- boxplot + geom_hline(yintercept = 0) +
    coord_cartesian(ylim = c(-1, 1)) +
    ggtitle(paste(rot,": Relative SOC change from base year (2000)", sep="")) +
    guides(fill=guide_legend(title="residue management")) +
    theme_bw() +
    theme(axis.text.x=element_text(angle=90, hjust=0, vjust=0.5))
  
  ggsave(paste("id0-11_SOCchange_rot", rot, ".png", sep=""), dpi=200)
  
}


#3. params uncertainty contribution
boxplot <- ggplot(data_long, aes(x = year_factor, y = rel_SOC)) + 
  geom_boxplot(aes(fill=factor(reorder(p_id, rel_SOC))), outlier.shape=NA)+
  xlab("Year") +
  ylab(NULL)
boxplot <- boxplot + facet_wrap(~ res_mgt)
boxplot <- boxplot + geom_hline(yintercept = 0) +
  coord_cartesian(ylim = c(-1, 1)) +
  ggtitle("Relative SOC change from base year (2000)") +
  guides(fill=guide_legend(title="parameter id")) +
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90, hjust = 0))
boxplot

ggsave("id0-11_SOCchange_resmgt_pids.png", width=20, height=13, dpi=250)
