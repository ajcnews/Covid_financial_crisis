###############################
###MSRB variable calculation###
###############################

###The first step in the MSRB analysis is to derive some liquidity variables from the MSRB trades 
###we get. This is the script that does that. A lot of this file is commented out because it's 
###either only used once or needs to be stepped through by hand. This assumes you have already 
###acquired the data from the MSRB. This is quite a process and requires a process for downloading
###files automatically from the MSRB. Part of this (historical data) should be done through an FTP
###server. Part of this is done automatically from a page the MSRB provides.

###MSRB data upload
library(tidyverse)
library(RMySQL)
library(keyring)
library(lubridate)
library(tidycensus)
library(tigris)
options(tigris_use_cache = TRUE)
source("~/Desktop/helper_dirich.R")

#This commented section should be run once to filter the initial data down to just the GA cases.
#the data is quite big otherwise and not useful for our purposes. People using this code for other
#states should update the GA filter to their region of interest.

# # ###first we get our data
# db_user <- 'nthieme'
# db_password <- key_get("sql")
# db_name <- 'project_cfc'
# db_host <- key_get("sql_intranet")
# db_port <- 3306
# 
# mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
#                    dbname = db_name, host = db_host, port = db_port)
# 
# # ##these are the muni bonds
# #D_table <- read_csv("~/Desktop/Covid_financial_crisis/ajc-msrb/MSRB_final.csv")
# D_trades_1 <- dbReadTable(mydb, 'msrb_trades')
# D_trades_1 <-  fetch(rs, n = -1)%>%data.frame()%>% filter(str_detect(file_name, "T20")) %>% select(-file_name)
# # 
# 
# D_trades_2 <- dbReadTable(mydb, 'msrb_trades_older') %>% mutate(trade_date = ymd(trade_date)) %>% 
#   filter(str_detect(file_name, "T20"))
# # 
# D_trades_3<- dbReadTable(mydb, "msrb_historical")
# 
# # 
# setwd("~/Desktop/Covid_financial_crisis/ajc-msrb/")
# top_level_dir<-list.files()
# D_MSRB_hist <- data.frame()
# 
# for(i in 1:length(top_level_dir)){
#   setwd(top_level_dir[i])
# 
#   dirs_in_dir <- list.files()
#   
#   for(j in 1:length(dirs_in_dir)){
#     setwd(dirs_in_dir[j])
#     
#     files_in_dir <- list.files()
#     
#     for(k in 1:length(files_in_dir)){
#       D_curr <- read_csv(files_in_dir[k], col_names = FALSE) %>% add_column(file_name = files_in_dir[k])
#       D_MSRB_hist <- rbind(D_MSRB_hist, D_curr)
#     }
#     
#     
#     setwd(str_c("~/Desktop/Covid_financial_crisis/ajc-msrb/",top_level_dir[i]))
#   }
#   
#   setwd("~/Desktop/Covid_financial_crisis/ajc-msrb/")
# }
# 
# name_l <-c("rtrs_num", "trade_type","cusip","sec_desc",
#            "dated_date", "coupon", "maturity_date", "when_issued",
#            "ass_settled_date","trade_date", "time_of_trade",
#            "settlement_date", "par_traded","dollar_price",
#            "yield", "brokers_broker", "wpi", "list_offer",
#            "rtrs_pub_date","rtrs_pub_time","vers_num","not_veri",
#            "ats_ind","ntbc","file_name")
# 
# names(D_MSRB_hist)<-name_l
# 
# D_MSRB_hist_2<-D_MSRB_hist %>% filter(str_detect(file_name, "T20")|str_detect(file_name, "hist"))
# days_not_in_sql <- D_MSRB_hist_2$trade_date %>% table %>% names
# days_already_in_sql<-D_trades_f_2$trade_date %>% table %>% names
# days_to_add<-days_not_in_sql[which(days_not_in_sql%in%days_already_in_sql==FALSE)]
# D_MSRB_hist_3 <-D_MSRB_hist_2 %>% filter(trade_date%in%days_to_add)
# 
# 
# # #
# dbWriteTable(mydb, "msrb_all_times",D_trades_f , row.names=F, append=T, overwrite=F)
# 
# D_f <- read_csv("~/Desktop/credit_spread_data/msrb_all_times.csv")
# #these are the 4-wk t-bill yields
#D_risk_free<-read_csv("~/Desktop/credit_spread_data/TB4WK.csv")
# # 
# # name_l <-c("rtrs_num", "trade_type","cusip","sec_desc",
# #            "dated_date", "coupon", "maturity_date", "when_issued",
# #            "ass_settled_date","trade_date", "time_of_trade",
# #            "settlement_date", "par_traded","dollar_price",
# #            "yield", "brokers_broker", "wpi", "list_offer",
# #            "rtrs_pub_date","rtrs_pub_time","vers_num","not_veri",
# #            "ats_ind","ntbc", "file_name")
# # 
# # names(D_trades_f_2)<-name_l
# 
# # D_trades_f_GA<-D_trades_f_2 %>% filter((str_detect(sec_desc, " GA "))|(str_detect(sec_desc, "GEORGIA")))
# 
# #D_trades_f_GA %>% group_by(cusip) %>% summarise(n = n()) %>% arrange(desc(n))
# 
# #here we make some variables that are needed for matching with t-bill yields
# # D_trades_f_GA_2<-D_trades_f_GA %>% mutate(dated_date = ymd(dated_date),
# #                          maturity_date = ymd(maturity_date),
# #                          trade_date = ymd(trade_date),
# #                          trade_date_full = ymd_hms(str_c(trade_date, " ",time_of_trade)),
# #                          ttm = round(days(maturity_date-trade_date)/years(1)),
# #                          week_of_year = isoweek(trade_date),
# #                          year = year(trade_date)
# #                          ) %>% distinct
# 
# D_trades_f_GA_2 <- read_csv("~/Desktop/credit_spread_data/D_msrb_GA_geo.csv")

##Once the data is filtered down and we have datasets of the historical data and the current stream
##trade data, we use this section to calculate more liquidity variables and to filter out trades
##that Schwert 2017 suggests we filter.

D_trades_older <- read_csv("~/Desktop/msrb_trades_older.csv")
D_trades_curr <- read_csv("~/Desktop/msrb_trades.csv")
D_trades_hist <- read_csv("~/Desktop/msrb_historical.csv")

D_trades_older_f<-D_trades_older%>% filter(str_detect(file_name, "T20")) %>% select(-file_name)%>%
  mutate(trade_date = ymd(trade_date))

D_trades_curr_f<-D_trades_curr%>% filter(str_detect(file_name, "T20")) %>% select(-file_name)%>%
  mutate(trade_date = ymd(trade_date))

D_trades_hist_f<-D_trades_hist%>% mutate(trade_date = ymd(trade_date))
# 
# # 
names(D_trades_hist_f)<-names(D_trades_older_f)
names(D_trades_curr_f)<-names(D_trades_older_f)

D_trades_f <- rbind(D_trades_older_f, D_trades_hist_f,D_trades_curr_f) %>% distinct

D_trades_f_2<-D_trades_f %>% mutate(trade_date = ymd(trade_date),
                                    maturity_date = ymd(maturity_date),
                                    coupon = as.numeric(coupon))

D_trades_f_2_GA <- D_trades_f_2%>% filter((str_detect(sec_desc, " GA "))|(str_detect(sec_desc, "GEORGIA")))
D_trades_f_GA_2<-D_trades_f_2_GA
# 
# ##Data cleaning
no_mat_or_coup<-D_trades_f_GA_2 %>% group_by(cusip) %>% summarise(num_mat=maturity_date %>% na.omit %>% unique %>% length,
                                                                  num_coup=coupon %>% na.omit %>% unique %>% length) %>%
  filter(num_mat==0|num_coup==0) %>% pull(cusip)

coup_too_high <- which(D_trades_f_GA_2$coupon>20)

if(length(coup_too_high)>0){
  coup_too_high<-D_trades_f_GA_2[which(D_trades_f_GA_2$coupon>20),]$cusip
}else{
  coup_too_high<-character(0)
}

ytm<-interval(ymd(D_trades_f_GA_2$maturity_date),ymd(D_trades_f_GA_2$dated_date))/years(1)
ytm_rm<-D_trades_f_GA_2[which(ytm<(-100)),]$cusip
trade_after_maturity<-which(D_trades_f_GA_2$trade_date>D_trades_f_GA_2$maturity_date)

bonds_not_enough<- D_trades_f_GA_2 %>% group_by(cusip) %>% summarise(n = n()) %>% filter(n <10) %>% pull(cusip) %>%
  unique

floating_rate_bond<-D_trades_f_GA_2 %>% group_by(cusip) %>% summarise(range = max(coupon)-min(coupon)) %>%
  arrange(desc(range)) %>% filter(range>0) %>% pull(cusip)

bonds_to_rm <- c(no_mat_or_coup, coup_too_high,ytm_rm,bonds_not_enough, floating_rate_bond)
D_trades_f_GA_2_f<-D_trades_f_GA_2[-trade_after_maturity,] %>% filter(cusip%in%bonds_to_rm==FALSE)

D_trades_f_GA_2_f_2<-D_trades_f_GA_2_f%>% mutate(dated_date = ymd(dated_date),
                                                 maturity_date = ymd(maturity_date),
                                                 trade_date = ymd(trade_date),
                                                 trade_date_full = ymd_hms(str_c(trade_date, " ",time_of_trade)),
                                                 ttm = round(days(maturity_date-trade_date)/years(1)),
                                                 week_of_year = isoweek(trade_date),
                                                 year = year(trade_date),
                                                 par_traded = as.numeric(par_traded)
) %>% distinct

# #these are the treasury yield curves
D_yield_curve <- read.table("~/Desktop/credit_spread_data/yield_curve.csv", sep = ",", header = TRUE) %>%
  mutate(date = mdy(Date))

list_of_secs<-D_trades_f_GA_2_f_2$cusip %>% unique
zcy_i <- names(D_yield_curve) %>% str_detect("SVENY") %>% {which(.)}
zcy_n <- names(D_yield_curve)
zcy <- zcy_n[zcy_i]

D_amihud <- data.frame()
D_interval <- data.frame()
# 
# ##we then calculate liquidity measures that we need for decomposing yield spreads in to liquidity and
# ##credit components
# 
# 

D_trades_to_use<-D_trades_f_GA_2_f_2 %>% group_by(cusip, coupon, maturity_date, trade_date_full, par_traded,
                                                  dollar_price, yield,ttm) %>% 
  summarise(n = n(),trade_date = trade_date[1]) %>% 
  mutate(year = year(trade_date_full), month = month(trade_date_full) )

for( i in 1:length(list_of_secs)){
  D_this_cusip<-D_trades_to_use %>% filter(cusip==list_of_secs[i]) %>% arrange(trade_date_full)
  
  #roll
  days <- D_this_cusip$trade_date
  roll_i<-rep(0, length(days))
  
  for(d_i in 2:length(days)){
    #convert to numeric for faster comparison
    bot_lim<-as.numeric(days[d_i]-days(30))
    top_lim <- as.numeric(days[d_i])
    
    D_last_30<-D_this_cusip %>% filter(between(as.numeric(trade_date), bot_lim,top_lim))
    
    if(nrow(D_last_30<4)){
      roll_i[d_i]<-NA
    }
    
    D_offset<-cbind(c(0,D_last_30$dollar_price %>% unique), c(D_last_30$dollar_price %>% unique,0))
    D_offset_f<-D_offset[-c(1,nrow(D_offset)),] %>% matrix(ncol=2)
    roll_i_tmp<-cov(D_offset_f[,1], D_offset_f[,2])
    roll_i[d_i]<-ifelse(roll_i_tmp>0,0,roll_i_tmp)
  }
  
  D_roll<-data.frame(cusip=list_of_secs[i], date = days, roll= roll_i) %>% mutate(year = year(date), month = month(date))
  
  #calculate trading interval
  D_this_reduced<- D_this_cusip
  
  diff_i <- rep(0, nrow(D_this_reduced)-1)
  
  for(d_i in 2:nrow(D_this_reduced)){
    d_n<-days(round(D_this_reduced$trade_date_full[d_i]-D_this_reduced$trade_date_full[d_i-1]))
    diff_i[d_i-1]<-str_split(d_n, "d")[[1]][1] %>% as.numeric %>% ifelse(is.na(.),0,.)
  }
  
  diff_i<-c(0, diff_i)
  
  D_diff <- data.frame(cusip = D_this_reduced$cusip , trade_date = D_this_reduced$trade_date ,
                       trading_interval = diff_i) %>% group_by(cusip, trade_date) %>%
    summarise(trading_interval = min(trading_interval))
  
  poss_dates <- min(ymd(str_sub(D_diff$trade_date,1,10))):max(ymd(str_sub(D_diff$trade_date,1,10)))
  missing_dates<-as_date(poss_dates[which(poss_dates%in%ymd(str_sub(D_diff$trade_date,1,10))==FALSE)])
  
  if(length(missing_dates)>0){
    D_missing <- data.frame(cusip = D_this_reduced$cusip %>% unique, trade_date = missing_dates, trading_interval = NA)
    D_diff_f <- rbind(D_diff, D_missing) %>% arrange(trade_date)
  }else{
    D_diff_f<-D_diff %>% arrange(trade_date)
  }
  
  D_diff_f_2<-D_diff_f %>% mutate(zero_day = ifelse(is.na(trading_interval), TRUE, FALSE))
  
  days_since <- 0 
  
  for(w in 1:nrow(D_diff_f_2)){
    no_trade_today<-is.na(D_diff_f_2$trading_interval[w])
    
    if(no_trade_today){
      days_since = days_since+1
      D_diff_f_2$trading_interval[w]<-days_since
      
    }else{
      days_since <- 0 
    }
  }
  
  D_diff_f_3<-D_diff_f_2  %>% mutate(year = year(trade_date), month = month(trade_date)) %>% group_by(year,month) %>%
    summarise(avg_trading_interval = mean(trading_interval), zero_days = length(which(zero_day))) %>%
    add_column(.before = 1, cusip = D_diff_f_2$cusip %>% unique)
  
  mats <- D_trades_to_use %>% filter(cusip==list_of_secs[i]) %>% pull(maturity_date)
  
  if(any(is.na(mats))){
    next
  }
  
  year_l <- D_this_cusip$year %>% unique
  
  ttm <- D_this_cusip %>% pull(ttm) %>% unique %>% na.omit
  
  for(k in 1:length(ttm)){
    if(nchar(ttm[k])==1){
      ttm[k] <- str_c("0",ttm)
    }
  }
  
  D_vars_this_yrs <- data.frame()
  
  for(j in 1:length(year_l)){
    D_this_year <- D_this_cusip %>% filter(year == year_l[j])
    months_l <- D_this_year %>% pull(month) %>% unique()
    
    amihud_i<- rep(0, length(months_l))
    amihud_risk_i<- rep(0, length(months_l))
    irc_i<- rep(0, length(months_l))
    irc_risk_i<- rep(0, length(months_l))
    disp_i <- rep(0, length(months_l))
    trades_per_mo_i <- rep(0, length(months_l))
    
    ##here we calculate the different variables we're going to use when decomposing risk into credit
    ##and liquidity. All definitions come from the appendix to Schwert (2016)
    
    for(k in 1:length(months_l)){
      D_this_month<-D_this_year %>% filter(month==months_l[k]) %>%arrange(trade_date_full)
      
      days <- D_this_month %>% pull(trade_date) %>% unique
      amihud_daily<-rep(0,length(days))
      irc_daily<-rep(0,length(days))
      disp_daily<-rep(0, length(days))
      
      for(day_i in 1:length(days)){
        D_this_day <- D_this_month %>% filter(trade_date==days[day_i])
        
        if(nrow(D_this_day)<2){
          amihud_daily[day_i]<-NA
          irc_daily[day_i]<-NA
        }else{
          
          #amihud section
          amihud_t <- rep(0, nrow(D_this_day))
          
          for(w in 1:(nrow(D_this_day)-1)){
            amihud_t[w]<-abs((D_this_day[w+1,]$dollar_price-D_this_day[w,]$dollar_price)/D_this_day[w,]$dollar_price)/
              D_this_day[w,]$par_traded
          }
          
          amihud_daily[day_i]<-sum(amihud_t)/length(amihud_t)
          
          #irc section
          irc_daily[day_i] <- D_this_day %>% select(trade_date, par_traded, dollar_price) %>%
            group_by(trade_date, par_traded) %>%
            summarise(max_p= max(dollar_price), min_p=min(dollar_price), n = n(),
                      irc = (max_p-min_p)/min_p) %>% pull(irc) %>% mean
          
        }
        
        if(nrow(D_this_day)>0){
          M_t<-sum(D_this_day$par_traded/sum(D_this_day$par_traded)*D_this_day$dollar_price)
          
          disp_daily[day_i]<-sum((D_this_day$dollar_price-M_t)^2*D_this_day$par_traded)/sum(D_this_day$par_traded)
        }else{
          disp_daily[day_i]<-NA
        }
      }
      
      amihud_i[k]<- mean(amihud_daily, na.rm=TRUE)
      amihud_risk_i[k]<-sd(amihud_daily, na.rm=TRUE)
      irc_i[k]<- mean(irc_daily, na.rm=TRUE)
      irc_risk_i[k]<-sd(irc_daily, na.rm=TRUE)
      disp_i[k] <- mean(disp_daily, na.rm=TRUE)
      trades_per_mo_i[k] <- D_this_month %>% nrow
      
    }
    
    D_vars_this<-data.frame(cusip = list_of_secs[i],year = year_l[j], month = months_l, amihud=amihud_i,
                            amihud_risk=amihud_risk_i, irc = irc_i,irc_risk = irc_risk_i,
                            disp = disp_i, trades_per_mo = trades_per_mo_i)
    
    
    D_vars_this_yrs<-rbind(D_vars_this_yrs,D_vars_this)
  }
  
  D_roll_f<-  D_roll %>% group_by(year, month) %>% summarise(roll = mean(roll, na.rm=TRUE))
  
  D_covs_this_cusip<-D_diff_f_3 %>% 
    left_join(D_roll_f, by = c("year", "month")) %>% left_join(D_vars_this_yrs, by = c("year","month")) %>% 
    filter(is.na(cusip.y)==FALSE)
  
  D_amihud<-rbind(D_amihud,D_covs_this_cusip)
  
  print(str_c(i," out of ", length(list_of_secs)," bonds calculated"))
}


 write_csv(D_amihud, "~/Desktop/credit_spread_data/D_amihud_new_2.csv")
# 
# ##this line checks to make sure we got the data grouped correctly
D_amihud %>% mutate(rep_day = ymd(str_c(year,"-01-01"))+7*week) %>% group_by(rep_day) %>% summarise(n = n()) %>%
  ggplot(aes(x = rep_day, y = n))+geom_point()

D_risk_free_2 <- tibble(date = c(), tb4wk = c())

for(i in 1:(nrow(D_risk_free)-1)){
  D_curr<-tibble(date = D_risk_free$DATE[i]:(D_risk_free$DATE[i+1]-1), tb4wk = D_risk_free$TB4WK[i])
  D_risk_free_2<- rbind(D_risk_free_2,D_curr)
}

D_yield_curve_tid<-D_yield_curve %>% filter(dplyr::between(ymd(date),ymd("2018-01-01"),ymd("2022-01-01"))) %>%
  select(date, starts_with("SVENY")) %>%
  pivot_longer(cols = starts_with("SVENY"),names_to = "year_yield", values_to = "yield") %>% na.omit %>%
  mutate(ttm=year_yield %>% str_sub(.,nchar(.)-1, nchar(.)) %>%
           ifelse(as.numeric(.)<10,str_sub(.,nchar(.),nchar(.)),.) %>% as.numeric
         ) %>% select(-year_yield)


#finally, we have the tax-adjusted muni yield spread. we divide by factors related to the top federal and
#state income marginal tax rates to relatively inflate muni bonds bc of their tax-free status
D_spread <- D_trades_f_GA_2 %>% mutate(trade_date = as.numeric(trade_date), yield = as.numeric(yield)) %>%
  left_join(D_risk_free_2, by = c("trade_date"="date")) %>%
  mutate(spread = yield/((1-.37)*(1-.0575))-tb4wk)

write_csv(D_spread, "~/Desktop/credit_spread_data/spread_data.csv")

# One of the essential steps in our pipeline is getting the GEOIDs for the counties in which 
#issuers are located. Much of the process can be done automatically, but there's some non-negligible
#manual work done, also. the steps that find and fix the GEOIDs are done below. I'm leaving them 
#commented out because the this should be walked through line by line.

# D_spread<-read_csv("~/Desktop/credit_spread_data/spread_data.csv")
# D_amihud <- read_csv("~/Desktop/credit_spread_data/D_amihud_new.csv")
# D_issuer<-read_csv("~/Desktop/credit_spread_data/D_issuer.csv") %>% filter(State%in%c("Georgia","none"))
# 
# D_amihud_f<-D_amihud %>% mutate(issuer_n = str_sub(cusip,1,6)) %>% left_join(D_issuer, by ="issuer_n" )
# D_amihud_addr <- D_amihud_f %>% select( issuer_n, Address, Address.Type, City, State, Name) %>% distinct
# 
# #there are issuer numbers that are in the issuer database that aren't in the constructed variables database
# missing_cusips<-D_issuer[which(D_issuer$issuer_n%in%D_amihud_f$issuer_n==FALSE),]$issuer_n
# 
# #i want to know where they came from 
# #first half of match is the dataframe of securities just before we clean it 
# #second half of the equality are the issuer numbers that are in the issuer database, but not in D_amihud.
# #116/118 of the missing cusips are in the data before filtering
# 
# og_issuers<-str_sub(D_trades_f_GA_2$cusip,1,6)
# in_missing_before_filt<-og_issuers[which(og_issuers%in%missing_cusips)] %>% unique
# 
# #first half of match is the dataframe of securities just before we clean it 
# #second half of the equality are the issuer numbers that are in the issuer database, but not in D_amihud.
# #none of them are after
# filt_issuers<-str_sub(D_trades_f_GA_2_f_2$cusip,1,6)
# in_missing_after_filt<-filt_issuers[which(filt_issuers  %in%missing_cusips)] %>% unique
# 
# # what that tells me is that the missing cusips are the ones that were filtered out for legitamate reasons.
# #that's good.
# 
# #first the section where we do have addresses
# D_amihud_addr_good <- D_amihud_addr %>% filter(Address!="none")
# 
# #matching address
# D_amihud_addr_good_2<-D_amihud_addr_good %>% mutate(city_n = tolower(City))
# 
# places_GA<-tigris::places(state="GA", cb = "TRUE") %>% mutate(NAME = NAME %>% tolower)
# 
# D_amihud_addr_good_geo <-data.frame()
# 
# D_amihud_addr_good_2[which(D_amihud_addr_good_2$city_n=="greenboro"),]$city_n<-"greensboro"
# 
# for(i in 1:nrow(D_amihud_addr_good_2)){
#   poss_ind<-which(str_detect(places_GA$NAME, D_amihud_addr_good_2$city_n[i]))
#   
#   if(length(poss_ind)>1){
#     poss_ind<-poss_ind[which.min(adist(places_GA$NAME[poss_ind ],D_amihud_addr_good_2$city_n[i]))]
#   }
#   
#   if(length(poss_ind)>0){
#     tmp_row<-places_GA[poss_ind,] %>% add_column(D_amihud_addr_good_2[i,])
#     
#     D_amihud_addr_good_geo<-rbind(D_amihud_addr_good_geo, tmp_row)
#   }
# }
# 
# 
# ##moving on to missed addresses  
# #of those, find the ones where we either didn't have an address or the address was NA
# D_amihud_addr_bad <- D_amihud_addr %>% filter(Address=="none"|is.na(Address)) %>% 
#   mutate(poss_area = str_split(Name," GA")%>% lapply(., function(x)return(x[[1]] %>% trimws)) %>% 
#            unlist %>% str_replace("CNTY", "COUNTY"))
# 
# D_amihud_counties_1 <- D_amihud_addr_bad %>% 
#   mutate(second_word = str_split(poss_area," ") %>% lapply(function(x)return(x[[length(x)]] %>% trimws)) %>% unlist) 
# 
# #then take the NA / no addresses and find the ones where the second word is county. keep in mind, we're still missing
# #the ones where the second word isn't county
# D_amihud_counties_good<-D_amihud_counties_1%>% filter(second_word=="COUNTY") 
# 
# #joining county date
# 
# counties<-tigris::counties( state = "GA") %>%  mutate(county = NAME %>%str_remove(.," County, Georgia") %>% tolower)
# 
# #we then link the second county words with the county shapefiles.
# D_amihud_counties_good_2<-D_amihud_counties_good %>% mutate(county = str_remove(poss_area, " COUNTY") %>% tolower)
# 
# ##these are the issuers that matched one second word county
# D_amihud_counties_good_2_geo<-D_amihud_counties_good_2 %>% left_join(counties, by = "county") %>% 
#   filter(is.na(GEOID)==FALSE)
# 
# ##these are the issuers that had a second word as county but that we couldn't match here
# D_amihud_counties_good_2_geo_bad <-D_amihud_counties_good_2 %>% filter(county%in%D_amihud_counties_good_2_geo$county==FALSE)
# 
# #then we take the ones we couldn't match which did have the second word as county and look for them in a loop
# D_amihud_counties_good_2_geo_bad_geod<-data.frame()
# D_amihud_counties_good_2_geo_bad[which(D_amihud_counties_good_2_geo_bad$county=="douglas-coffee"),]$county = "coffee"
# 
# for(i in 1:nrow(counties)){
#   poss_ind<-which(str_detect(D_amihud_counties_good_2_geo_bad$county, counties$county[i]))
#   
#   
#   if(length(poss_ind)>0){
#     tmp_row<-D_amihud_counties_good_2_geo_bad[poss_ind,] %>% select(-county) %>% add_column(counties[i,])
#     
#     if(D_amihud_counties_good_2_geo_bad[poss_ind,]$issuer_n%in%D_amihud_counties_good_2_geo_bad_geod$issuer_n){
#       #this is used to keep only the first instance of a county
#       next
#     }
#     
#     D_amihud_counties_good_2_geo_bad_geod<-rbind(D_amihud_counties_good_2_geo_bad_geod, tmp_row)
#   }
# }
# 
# 
# #what we're missing at this stage are the issuers that didn't have county as the their second word and didn't have 
# #an address
# 
# #finding missing counties
# 
# D_amihud_counties_bad<-D_amihud_counties_1 %>% filter(second_word!="COUNTY"|is.na(second_word)) %>% 
#   mutate(second_word=tolower(second_word))
# 
# #get city data
# #join the issuers missing the county with place names from census. these will be city names mostly.
# D_amihud_cities_good<-places_GA %>% left_join(D_amihud_counties_bad, by = c("NAME"="second_word")) %>% 
#   filter(is.na(State)==FALSE)
# 
# #we get some but not all, so run a similar loop on the remaining ones
# D_amihud_cities_bad<-D_amihud_counties_bad %>% filter(D_amihud_counties_bad$Name%in%D_amihud_cities_good$Name==FALSE) %>% 
#   mutate(poss_area = tolower(poss_area))
# 
# D_amihud_cities_str_det_good <-data.frame()
# 
# for(i in 1:nrow(D_amihud_cities_bad)){
#   #the conditions needs to be in this order or every time you succeed on a 0 length poss ind, you incorrectly trigger the
#   #next conditiond
#   poss_ind<-which(str_detect(places_GA$NAME, 
#                              D_amihud_cities_bad$poss_area[i] %>% str_remove("\\)") %>% str_remove("\\(") %>% tolower))
#   
#   
#   if(length(poss_ind)>0){
#     tmp_row<-places_GA[poss_ind,] %>% add_column(D_amihud_cities_bad[i,])
#     
#     if(nrow(tmp_row)>1){
#       poss_ind<-poss_ind[which.min(adist(places_GA[poss_ind,]$NAME,D_amihud_cities_bad$poss_area[i] %>% str_remove("&amp")))]
#       tmp_row<-places_GA[poss_ind,] %>% add_column(D_amihud_cities_bad[i,])
#     }
#     
#     D_amihud_cities_str_det_good<-rbind(D_amihud_cities_str_det_good, tmp_row)
#   }
#   
# 
#   if(length(poss_ind)==0){
#     poss_ind<-which(str_detect(D_amihud_cities_bad$poss_area[i] %>% str_remove("\\)") %>% str_remove("\\(") %>% tolower,
#                                places_GA$NAME))
#     
#     tmp_row<-places_GA[poss_ind,] %>% add_column(D_amihud_cities_bad[i,])
#     
#     if(nrow(tmp_row)>1){
#       poss_ind<-poss_ind[which.min(adist(places_GA[poss_ind,]$NAME,D_amihud_cities_bad$poss_area[i] %>% str_remove("&amp")))]
#       tmp_row<-places_GA[poss_ind,] %>% add_column(D_amihud_cities_bad[i,])
#     }
#     
#     D_amihud_cities_str_det_good<-rbind(D_amihud_cities_str_det_good, tmp_row)
#   }
# }
# 
# 
# #at this point, we have almost all the issuers. just give one last go on the remaining ones
# D_amihud_cities_good_2<-D_amihud_cities_str_det_good
# 
# D_amihud_cities_bad_2<-D_amihud_cities_bad %>% filter(issuer_n%in%D_amihud_cities_good_2$issuer_n==FALSE)
# 
# D_amihud_cities_str_det_rev_good <-data.frame()
# 
# for(i in 1:nrow(places_GA)){
#   poss_ind<-which(str_detect(tolower(D_amihud_cities_bad_2$poss_area),places_GA$NAME[i]))
#   
#   if(length(poss_ind)>0){
#     tmp_row_2 <-D_amihud_cities_bad_2[poss_ind,]  %>% add_column(places_GA[i,])
#     
#     D_amihud_cities_str_det_rev_good<-rbind(D_amihud_cities_str_det_rev_good, tmp_row_2)
#   }
# }
# 
# D_amihud_cities_bad_3<-D_amihud_cities_bad_2%>% filter(issuer_n%in%D_amihud_cities_str_det_rev_good$issuer_n==FALSE)
# 
# #now we get the last 33 by hand, unfortunately
# D_amihud_cities_manual <- data.frame()
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "BLECKLEY CNTY &amp; DODGE CNTY JT DEV AUTH GA REV" ) %>%
#   add_column(counties %>% filter(county=="dodge"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "FULTON DEKALB GA HOSP AUTH HOSP REV") %>%
#   add_column(counties %>% filter(county=="fulton"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "SINCLAIR WTR AUTH GA REV") %>%
#   add_column(counties %>% filter(county=="putnam"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "COLLEGE PK BUSINESS &amp; INDL DEV AUTH GA ECONOMIC DEV REV") %>%
#   add_column(counties %>% filter(county=="clayton"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "LA GRANGE GA DEV AUTH REV") %>%
#   add_column(counties %>% filter(county=="troup"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "POLK GA SCH DIST") %>%
#   add_column(counties %>% filter(county=="polk"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "DEKALB PRIVATE HOSP AUTH GA REV ANTIC CTFS") %>%
#   add_column(counties %>% filter(county=="dekalb"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "EAST PT GA TAX ALLOCATION") %>%
#   add_column(counties %>% filter(county=="fulton"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "SOUTH COBB REDEV AUTH GA REV") %>%
#   add_column(counties %>% filter(county=="cobb"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "MC DONOUGH GA URBAN REDEV AGY REV") %>%
#   add_column(counties %>% filter(county=="henry"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# tmp_row<-D_amihud_cities_bad_3 %>% filter(Name == "GEO L SMITH II GA WORLD CONGRESS CTR AUTH CONVENTION CTR HOTEL REV") %>%
#   add_column(counties %>% filter(county=="fulton"))
# 
# D_amihud_cities_manual<-rbind(D_amihud_cities_manual, tmp_row)
# 
# 
# ##now getting the good ones together

# # D_amihud_addr_good_geo #good addresses
# # D_amihud_counties_good_2_geo #second word county that we could join
# # D_amihud_counties_good_2_geo_bad_geod #second word county that we couldn't join
# # D_amihud_cities_good # these are the issuers we could join with place names
# # D_amihud_cities_str_det_good #these are the issuers we couldn't join with place but looped
# 
# D_amihud_addr_good_geo_f<-D_amihud_addr_good_geo %>% select(-AFFGEOID, -city_n)
# 
# D_amihud_counties_good_2_geo_bad_geod_f<-D_amihud_counties_good_2_geo_bad_geod %>% 
#   select(c(-NAMELSAD, -CLASSFP, -MTFCC,-CSAFP,-CBSAFP,-METDIVFP, -FUNCSTAT,-INTPTLAT, -INTPTLON, -second_word,
#            -county, -poss_area)) %>% 
#   rename(PLACEFP = COUNTYFP, PLACENS =COUNTYNS) %>% st_as_sf
# 
# D_amihud_counties_good_2_geo_f<-D_amihud_counties_good_2_geo %>% 
#   select(c(-NAMELSAD, -CLASSFP, -MTFCC,-CSAFP,-CBSAFP,-METDIVFP, -FUNCSTAT,-INTPTLAT, -INTPTLON, -second_word,
#                                           -county, -poss_area)) %>% 
#   rename(PLACEFP = COUNTYFP, PLACENS =COUNTYNS) %>% st_as_sf
# 
# D_amihud_cities_manual_f<-D_amihud_cities_manual %>% 
#   select(c(-NAMELSAD, -CLASSFP, -MTFCC,-CSAFP,-CBSAFP,-METDIVFP, -FUNCSTAT,-INTPTLAT, -INTPTLON, -second_word,
#            -county, -poss_area)) %>% 
#   rename(PLACEFP = COUNTYFP, PLACENS =COUNTYNS) %>% st_as_sf
# 
# # D_amihud_cities_str_det_rev_good_f<-D_amihud_cities_str_det_rev_good %>% st_as_sf %>% 
# #   select(-second_word,-poss_area,-AFFGEOID)
# 
# D_amihud_cities_str_det_good_f<-D_amihud_cities_str_det_good %>% select(-second_word,-poss_area,-AFFGEOID)
# D_amihud_cities_good_f<-D_amihud_cities_good %>% select(-poss_area,-AFFGEOID)
# 
# st_crs(D_amihud_counties_good_2_geo_bad_geod_f)<-st_crs(D_amihud_cities_str_det_good_f)
# st_crs(D_amihud_addr_good_geo_f)<-st_crs(D_amihud_cities_str_det_good_f)
# st_crs(D_amihud_cities_good_f)<-st_crs(D_amihud_cities_str_det_good_f)
# #st_crs(D_amihud_cities_good_2)<-st_crs(D_amihud_cities_str_det_good_f)
# #st_crs(D_amihud_cities_str_det_rev_good_f)<-st_crs(D_amihud_cities_str_det_good_f)
# st_crs(D_amihud_counties_good_2_geo_f)<-st_crs(D_amihud_cities_str_det_good_f)
# st_crs(D_amihud_cities_manual_f)<-st_crs(D_amihud_cities_str_det_good_f)
# 
# D_amihud_geod<-rbind(
#   D_amihud_cities_good_f %>% add_column(type = "c_good"),
#   #D_amihud_cities_str_det_rev_good_f,
#   D_amihud_cities_str_det_good_f %>% add_column(type = "str_good"),
#   D_amihud_addr_good_geo_f %>% add_column(type = "good_geo"),
#   D_amihud_counties_good_2_geo_bad_geod_f %>% add_column(type = "geod_f"),
#   D_amihud_counties_good_2_geo_f %>% add_column(type = "geo_f"),
#   D_amihud_cities_manual_f %>% add_column(type = "man")
#   ) %>% select(-STATEFP, -PLACEFP, -PLACENS,-LSAD, -ALAND, -AWATER, -Address, -Address.Type, -City, -State)
# 
# ##checking for issuer multiples. no issuer duplicates
# duplicated_ind<-which((D_amihud_geod$Name %>% table)>1) %>% names
# 
# D_amihud_geod_f<-D_amihud_geod %>% select(GEOID, NAME, issuer_n)
# 
# D_amihud_final<-D_amihud_f %>% left_join(D_amihud_geod_f, by = "issuer_n") %>% st_as_sf
# 
# st_write(D_amihud_geod_f,"~/Desktop/credit_spread_data/D_issuers_geo.shp")
# write_csv(D_amihud_f, "~/Desktop/credit_spread_data/D_amihud_f.csv")


