###########################
###sample size estimates###
###########################

#this is file that lets us know what the amount of data we need to get. it's currently set for Georgia,
#but with a limited amount of manual work, it can be reconfigured for other regions.

library(tidyverse)

#data entry for previous years. this was done manually. it tells us what the number of bks in a
#particular court of a particular kind of bankruptcy case was.

D_sample_sizes<-rbind(
  c(2019,"GMB","11",15),
  c(2019,"GMB","13",5448),
  c(2019,"GMB","7",75),
  c(2019,"GNB","11",111),
  c(2019,"GNB","13",14191),
  c(2019,"GNB","7",14474),
  c(2019,"GSB","11",11),
  c(2019,"GSB","13",4991),
  c(2019,"GSB","7",1172),
  c(2018,"GMB","11",24),
  c(2018,"GMB","13",5596),
  c(2018,"GMB","7",59),
  c(2018,"GNB","11",104),
  c(2018,"GNB","13",15016),
  c(2018,"GNB","7",15019),
  c(2018,"GSB","11",11),
  c(2018,"GSB","13",5128),
  c(2018,"GSB","7",1309),
  c(2017,"GMB","11",25),
  c(2017,"GMB","13",5668),
  c(2017,"GMB","7",126),
  c(2017,"GNB","11",96),
  c(2017,"GNB","13",14889),
  c(2017,"GNB","7",15734),
  c(2017,"GSB","11",7),
  c(2017,"GSB","13",5231),
  c(2017,"GSB","7",1397),
  c(2016,"GMB","11",25),
  c(2016,"GMB","13",5928),
  c(2016,"GMB","7",985),
  c(2016,"GNB","11",94),
  c(2016,"GNB","13",15169),
  c(2016,"GNB","7",17244),
  c(2016,"GSB","11",10),
  c(2016,"GSB","13",5929),
  c(2016,"GSB","7",1434)
) %>% data.frame %>% 
  mutate(X4 = as.numeric(as.character(X4), X1 = as.numeric(as.character(X1))))

names(D_sample_sizes) <-c("year","court","chapter","filings")
D_sample_sizes$year<-as.numeric(as.character(D_sample_sizes$year))

court_assign<-D_sample_sizes %>% group_by(year,court) %>%
  summarise( n = sum(filings)) %>%group_by(year) %>% 
  mutate(perc=n/sum(n)) %>% group_by(court) %>% summarize(m= mean(perc))

after_court_assign<-D_sample_sizes %>% group_by(year,court) %>% 
  mutate(perc=filings/sum(filings)) %>%  ungroup %>% 
  group_by(court, chapter) %>% summarize(m= mean(perc))

#estimating 2008 and 2009
W_mat<-left_join(court_assign, after_court_assign, by = "court") %>% mutate(
  w=m.x*m.y) %>% mutate(n_08 = w*61524, n_09=w*75145) %>% 
  select(court, chapter,n_08, n_09) %>%
  pivot_longer(-c(court, chapter),names_to = "year",  values_to="filings") %>% 
  mutate(year = ifelse(year=="n_08", 2008, 2009)) %>% 
  select(year, court, chapter, filings)

#function for returning a proportion
p = .07
samp_func<-function(x){
  if(x<30){
    return(x)
  }else if(x*p<30){
    return(30)
  }else{
    return(x*p)
  }
}

samp_f<-Vectorize(samp_func)

D_tot <- rbind(D_sample_sizes, W_mat) %>% 
  filter(year%in%c(2009,2008,2018,2019)) %>% 
  mutate(filings_to_samp = samp_f(filings))

D_tot_21<-D_tot %>% filter(year==2018) %>% 
  mutate(year = 2020, filings = filings * 1.3, filings_to_samp = filings_to_samp*1.3)

D_tot<-rbind(D_tot, D_tot_21)

#filter to the personals we need to purchase in the years we want to purchase them. multiply by 3 for upper bound on the
#cost of a voluntary petition
potential_sample<-D_tot %>% filter(chapter!=11, year%in%c(2018,2019, 2020)) %>% 
  mutate(filings_to_samp = case_when(filings_to_samp>200~filings_to_samp*.9, filings_to_samp<=200~filings_to_samp) %>% 
           round)

write_csv(potential_sample, file = "~/Desktop/PACER_sample_guide.csv")

#this is a quick function for calculating the cost of getting PACER samples and the cost of using
#AWS to parse them. 
  (potential_sample%>% pull(filings_to_samp) %>% sum)*3- #sample PACER costs assuming all filings hit the $3 threshold
  (potential_sample%>% pull(filings_to_samp) %>% sum)*(15*.015+10*.05) # aws fifteen pages of forms and 10 pages of tables

#this can be used for initial budgeting