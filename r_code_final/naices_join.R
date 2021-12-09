############################
####NAICS joiner####
############################

#This file clusters the creditor names and joins them back with the individual debts. The step that
#is included here that isn't elsewhere is filtering the full business dataset to just the variables we
#need. the data is too large to load in memory so we read it filter it on the way in, reduce it,
#and save it with data table.

#It relies on the CompuStat database within WRDS. We were able to access this through a relationship
#with Columbia University. I believe many universities have access to this database. It provides
#data on a great majority of business in the U.S. We use it link the listed creditors on bankruptcy
#filings with their NAICS codes to understand the type of debt filers owe.

#
library(tidyverse)
library(RMySQL)
library(data.table)
library(tools)
library(stringdist)
library(keyring)
source("~/Desktop/Covid_financial_crisis/r_code/R_code_library.R")


##read in data and reduce
##this reads in the unsecured data. it's it's too large to load in memory, we can use this 
##instead
# D_naics <- read_csv(
#   "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/Other/unsecured_allZips.csv")
#   
# dat <- fread("/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/Other/unsecured_allZips.csv",
#              select = c("archive_version_year","parent_number","state","primary_naics_code",
#                         "naics8_descriptions","company", "zipcode","city"))
# 
# D_creds_r <-read_csv("~/Desktop/secured_creditor_list.csv")
# 
# D_naics_red<-dat[
#   dat[,
#                 .I[which.max(archive_version_year)],
#                 by=.(company,zipcode,city,state)
#           ]$V1
#   ]
# 
# fwrite(D_naics_red,
#        "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/Other/NAICS_all_zips_red.csv"
#        )

##read in the reduced forms of both datasets
##NAICS data
D_naics_1 <- fread(
  "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/Other/NAICS_all_zips_red.csv"
  )

D_naics_2 <- fread(
  "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/Other/NAICS_zips_red.csv"
) %>% select(archive_version_year, parent_number,state,primary_naics_code,
             naics8_descriptions,company,zipcode,city)

D_naics <- rbind(D_naics_1, D_naics_2) %>% distinct

#creditor data
db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet") 
db_port <- 3306

#connect to server
mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

bk_cases_q <- "SELECT * FROM `pacer_creditors_unsecured_cl`"
rs <- dbSendQuery(mydb, bk_cases_q)
D_unsecured <-  fetch(rs, n = -1)%>%data.frame()

bk_cases_q <- "SELECT * FROM `pacer_creditors_unsec_naics_f`"
rs <- dbSendQuery(mydb, bk_cases_q)
D_unsecured_old <-  fetch(rs, n = -1)%>%data.frame()

D_unsecured<-D_unsecured %>% filter(bk_id%in%D_unsecured_old$bk_id==FALSE)

#this formats the creditors in a way that makes dedup usable
D_unsecured_creds<-get_list_of_creditors(D_unsecured)

D_cred_matched <- data.frame()

##the problem is that this doesn't match everything because of incorporated companies in other
##zips

# D_naics_f<-clean_creditor_names(D_naics %>% mutate(creditor = tolower(company))) %>%
#   add_column(Id = 1:nrow(.),orig = "no") %>%select(order(colnames(.)))

# D_naics_f<-D_naics %>% mutate(creditor = tolower(company)) %>%
#   add_column(Id = 1:nrow(.),orig = "no") %>%select(order(colnames(.)))

#filter our businesses to just those in the current zip. group by creditor to avoid 
#matching things we don't need to match. add a group id so we can join later
D_creditors_f_unsec_rb<-D_unsecured_creds %>% 
  rename(creditor = company_name) %>%  select(order(colnames(.))) %>% group_by(creditor) %>% 
  mutate(group_id = cur_group_id())

cred_join_ind<-D_creditors_f_unsec_rb %>% select(Id, group_id)

#what we do is include all businesses in the zip along with our businesses in the zip
#and see which things get grouped together. since we really only care about oure businesses
#we add a flag variable so we can filter the results to what we care about
creds_to_label<-D_creditors_f_unsec_rb %>% select(creditor, group_id) %>% distinct() %>% 
  add_column(orig = "yes") %>% mutate(Id = group_id, creditor = tolower(creditor))

# D_naics_f<-clean_creditor_names(D_naics %>% mutate(creditor = tolower(company))) %>%
#   add_column(Id = 1:nrow(.),orig = "no") %>%select(order(colnames(.)))

D_naics_f<-D_naics %>% mutate(creditor = tolower(company)) %>%
  add_column(Id = 1:nrow(.),orig = "no") %>%select(order(colnames(.)))

#this takes a while. it calls out to a .py file that does the ML deduping
D_cred_matched<-match_creditors_ml(D_naics_f,creds_to_label)
  
##using the output of this loop, we find the best fit for each creditor across all of the blocks
##we take a relative distance of .6 as a good threshold. for all matches over that, we say this
##method doesn't work. That gives us a 77% match. We then turn to matching the other 23%
matched_group<-D_cred_matched %>% group_by(group_id) %>% 
  summarise(min_ind_name = which.min(name_dist),
            min_cred_name = company[min_ind_name],
            creditor_name_og = creditor_og[1],
            name_dist_agg = name_dist[min_ind_name]) %>% 
  mutate(dist_osa_add_ok = stringdist(min_cred_name %>% tolower, creditor_name_og,
                                      weight = c(d = .1, i=0.1, s = 1, t= 1)),
         rel_dist_osa_add = dist_osa_add_ok/nchar(creditor_name_og),
         rel_dist_osa = name_dist_agg/nchar(creditor_name_og) )%>% 
  filter(rel_dist_osa <.4) %>% distinct

##we look for the longest common substring in the full data for the remaining 170 creditors and 
##do a pretty good job, this get us to 90% of creditors. 86% for unsecured

unmatched_group<-D_cred_matched %>% group_by(group_id) %>% 
  summarise(min_ind_name = which.min(name_dist),
            min_cred_name = company[min_ind_name],
            creditor_name_og = creditor_og[1],
            name_dist_agg = name_dist[min_ind_name]) %>% 
  mutate(dist_osa_add_ok = stringdist(min_cred_name %>% tolower, creditor_name_og,
                                      weight = c(d = .1, i=0.1, s = 1, t= 1)),
         rel_dist_osa_add = dist_osa_add_ok/nchar(creditor_name_og),
         rel_dist_osa = name_dist_agg/nchar(creditor_name_og) )%>% 
  filter(rel_dist_osa >=.4) %>% distinct

##we write this table out because it's the result of many hours of computation
dbWriteTable(mydb, "pacer_creditors_unsec_dedupe_prelim", D_cred_matched, row.names=F, 
             append=F, overwrite= T)

##group by the company name and take the mode of their naics code. this is to smooth out
##naics inconsistencies
D_cred_matched_f_1 <- D_cred_matched %>% filter(company%in%matched_group$min_cred_name) %>% 
  group_by(company, state) %>% summarise(
                                  creditor = creditor[1], 
                                  naics = Modes(as.character(naics8_descriptions)),
                                  naics_code = Modes(as.character(primary_naics_code)),
                                  parent_number = Modes(as.character(parent_number)) ) %>%
  distinct

D_cred_matched_f<-D_cred_matched_f_1 %>% 
  left_join(matched_group, by = c("company"="min_cred_name"))

min_unmatched <- rep(0, nrow(unmatched_group))
naics_comps_low<-D_naics_f$company %>% tolower
dists_v <- rep(0, nrow(unmatched_group))

for(i in 1:nrow(unmatched_group)){
  dists_i<-stringdist(unmatched_group$creditor_name_og[i], naics_comps_low, method ="lcs")
  min_unmatched[i]<- which.min(dists_i)
  dists_v[i] <- min(dists_i)
}

D_cred_unmatched <- D_naics_f[min_unmatched,]
D_cred_unmatched$creditor_og<- unmatched_group$creditor_name_og
D_cred_unmatched$group_id <- unmatched_group$group_id
D_cred_unmatched$name_dist<- dists_v

#pretty arbitrary cutoff of .44 
D_cred_unmatched_f<-D_cred_unmatched %>% 
  mutate(lcs_d = 
           stringdist(company %>% tolower,
                      creditor_og, 
                      method = "lcs")/nchar(creditor_og)) %>% 
  arrange(desc(lcs_d)) %>% data.frame %>% 
  filter(lcs_d<.4)%>% 
  group_by(company, state, group_id) %>% summarise(
    creditor = creditor[1], 
    naics = Modes(as.character(naics8_descriptions)),
    naics_code = Modes(as.character(primary_naics_code)),
    parent_number = Modes(as.character(parent_number)) ) %>% distinct

D_cred_matched_f<-D_cred_matched_f_1 %>% 
  left_join(matched_group, by = c("company"="min_cred_name")) %>% 
  filter(is.na(group_id)==FALSE) %>% 
  select(company, state,creditor, naics, naics_code, parent_number, group_id)

#go back and rejoin to individual creditors using group id and then to debts using Id
D_cred_naics <- rbind(D_cred_unmatched_f, D_cred_matched_f) %>% group_by(group_id,state) %>% 
  summarise( creditor,state = state[1], naics = naics[1],naics_code = naics_code[1],  
            parent = parent_number[1])

##sometimes wires get crossed. this is how we can fix it
naics_code_cross<-D_cred_naics %>% group_by(naics_code) %>% 
  summarise(naics_title = Modes(naics))

naics_8_cleaned <-read_csv("~/Desktop/naics_8_cross.csv") %>% mutate(naics_code = `8 Digits naics Based code`, naics = description ) %>% 
  mutate(naics_code = naics_code %>% as.character)

D_cred_naics_f<-D_cred_naics %>% left_join(naics_8_cleaned, by = "naics_code") %>% 
  select(group_id, state, creditor, naics_code, naics_high = naics.x, naics_low = naics.y, parent)

dbWriteTable(mydb, "pacer_creditors_unsec_creditor_table", D_cred_naics_f, row.names=F, 
             append=F, overwrite = T)

##one last little thing to get right. for some of the creditors we have multiple instances of
##the company in different states. first we match on those.
D_unsec_cred_naics_id_state<-D_creditors_f_unsec_rb %>%
  left_join(D_cred_naics_f, by = c("group_id","state")) %>%
  group_by(Id) %>% summarise(city = city[1], creditor_og = creditor.y[1],
                             creditor_n = creditor.x[1],naics_low = naics_low[1],
                             naics_high = naics_high[1],
                             naics_code = naics_code[1],  state = state[1], 
                             parent = parent[1]) %>% 
  left_join(D_unsecured, by = "Id") %>% 
  select(Id, bk_id, creditor_og, creditor_n, naics_low, naics_high, amount_owed,  
         #collateral, unsecured,
         type,
         city = city.y, state = state.y,parent) %>% filter(is.na(naics_high)==FALSE)

##once we have those, we look at the creditors that just have one instance in one state, and
##remove the earlier matches
D_unsec_cred_naics_id<-D_creditors_f_unsec_rb %>% left_join(D_cred_naics_f, by = c("group_id")) %>%
  group_by(Id) %>% summarise(city = city[1], state= state.x[1],creditor_og = creditor.y[1],
                             creditor_n = creditor.x[1],naics_low = naics_low[1],
                             naics_high = naics_high[1],
                             naics_code = naics_code[1], state = state[1], 
                             parent = parent[1]) %>% 
  left_join(D_unsecured, by = "Id") %>% 
  select(Id, bk_id, creditor_og, creditor_n, naics_low, naics_high,amount_owed,  
         type,
         #collateral, unsecured,
         city = city.y, state = state.y,parent) %>% 
  filter(Id%in%D_unsec_cred_naics_id_state$Id==FALSE)

##4.4% error rate
D_unsec_cred_naics<-rbind(D_unsec_cred_naics_id_state,D_unsec_cred_naics_id) %>% 
  mutate(naics_low = case_when(creditor_og=="none"~"none", creditor_og!="none"~naics_low))

dbWriteTable(mydb, "pacer_creditors_unsec_naics", D_unsec_cred_naics, row.names=F, append=F,
             overwrite = T)

##fixing naics codes

D_unsec_cred_naics_f<-D_unsec_cred_naics %>% select(-Id) %>% distinct %>%
  add_column(Id = 1:nrow(.))

# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="aaron"),]$naics_code<-53229921

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="aaron"),]$naics_low<-
  "Furniture"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="aaron"),]$naics_high<-
  "FURNITURE STORES"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="acceptance now"),]$naics_high<-
  "FURNITURE STORES"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="acceptance now"),]$naics_low<-
  "Furniture"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="american financial"),]$naics_high<-
  "INVESTMENT ADVICE"

# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="american financial"),]$naics_code<-
#   52393002

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="americash"),]$naics_high<-
  "CONSUMER LENDING"

# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="americash"),]$naics_code<-
#   52229103

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="automart"),]$naics_high<-
  "USED CAR DEALERS"

# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="automart"),]$naics_code<-
#   44112005

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="bellco credit union"),]$naics_high<-
  "CREDIT UNIONS"
# 
# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="bellco credit union"),]$naics_code<-
#   52213003

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="brooks furniture"),]$naics_high<-
  "FURNITURE STORES"

# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="brooks furniture"),]$naics_code<-
#   53229921

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="buddy's home furnishings"),]$naics_low<-
  "Furniture"


D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="columbus"),]$naics_high<-
  "unknown"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="columbus"),]$naics_code<-
  00000000

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="community"),]$naics_high<-
  "unknown"
# 
# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="community"),]$naics_code<-
#   00000000

# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="credit union loan source llc"),]$naics<-
#   "CONSUMER LENDING"

# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="credit union loan source llc"),]$naics_code<-
#   52229103

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "irs"),]$naics_low<-
  "Federal Government-Finance & Taxation"

# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="irs"),]$naics_code<-
#   92113003

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "irs"),]$naics_low<-
  "Public Finance Activities"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "rent-a-center"),]$naics_low<-
  "Furniture"
# 
# D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="rent-a-center"),]$naics_code<-
#   92113003


D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "aes"),]$naics_low<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "aes"),]$naics_high<-
  "Schools"

D_unsec_cred_naics_f[which(str_sub(D_unsec_cred_naics_f$creditor_n,1,3)=="Cb/"),]$naics_low<-
  "Credit Card & Other Credit Plans"

D_unsec_cred_naics_f[which(str_sub(D_unsec_cred_naics_f$creditor_n,1,3)=="Cb/"),]$naics_high<-
  "Financial Transactions Processing, Reserve, and Clearinghouse Activities"

D_unsec_cred_naics_f[which(str_sub(D_unsec_cred_naics_f$creditor_n,1,3)=="Cb "),]$naics_low<-
  "Credit Card & Other Credit Plans"
#51821007
D_unsec_cred_naics_f[which(str_sub(D_unsec_cred_naics_f$creditor_n,1,3)=="Cb "),]$naics_high<-
  "Financial Transactions Processing, Reserve, and Clearinghouse Activities"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Cba"),]$naics_low<-
  "Collection Agencies"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Cba"),]$naics_high<-
  "COLLECTION AGENCIES"

D_unsec_cred_naics_f[which(str_detect(str_sub(D_unsec_cred_naics_f$creditor_n,1,4), "Cba ")),]$naics_low<-
  "Collection Agencies"

D_unsec_cred_naics_f[which(str_detect(str_sub(D_unsec_cred_naics_f$creditor_n,1,4), "Cba ")),]$naics_high<-
  "COLLECTION AGENCIES"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n=="Comenity"),]$naics_low<-
  "Credit Card & Other Credit Plans"
#51821007
D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n=="Comenity"),]$naics_high<-
  "Financial Transactions Processing, Reserve, and Clearinghouse Activities"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Credence Resource Management"),]$naics_low<-
  "Collection Agencies"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Credence Resource Management"),]$naics_high<-
  "COLLECTION AGENCIES"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "eduloan servicing"),]$naics_low<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "eduloan servicing"),]$naics_high<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "eduloan servicing"),]$naics_low<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "eduloan servicing"),]$naics_high<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Enhanced Recovery"),]$naics_low<-
  "Collection Agencies"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Enhanced Recovery"),]$naics_high<-
  "COLLECTION AGENCIES"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="exeter finance"),]$naics_low<-
  "Loans-Automobile"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="exeter finance"),]$naics_high<-
  "AUTOMOBILE FINANCING"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Federal Loan Service"),]$naics_low<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Federal Loan Service"),]$naics_high<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="global lending svc"),]$naics_low<-
  "Loans-Automobile"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="global lending svc"),]$naics_high<-
  "AUTOMOBILE FINANCING"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "ic systems"),]$naics_low<-
  "Collection Agencies"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "ic systems"),]$naics_high<-
  "COLLECTION AGENCIES"

D_unsec_cred_naics_f[which(str_detect(D_unsec_cred_naics_f$creditor_og, "Kohl")),]$naics_high<-
  "DEPARTMENT STORES (EXCEPT DISCOUNT DEPT STORES)"

D_unsec_cred_naics_f[which(str_detect(D_unsec_cred_naics_f$creditor_og, "Kohl")),]$naics_low<-
  "Department store"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "Lvnv Funding"),]$naics_low<-
  "Collection Agencies"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "Lvnv Funding"),]$naics_high<-
  "COLLECTION AGENCIES"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="Macy's"),]$naics_high<-
  "DEPARTMENT STORES (EXCEPT DISCOUNT DEPT STORES)"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="Macy's"),]$naics_low<-
  "Department store"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="Macys"),]$naics_high<-
  "DEPARTMENT STORES (EXCEPT DISCOUNT DEPT STORES)"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og=="Macys"),]$naics_low<-
  "Department store"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "medical data systems inc"),]$naics_low<-
  "Medical Business Administration"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "medical data systems inc"),]$naics_high<-
  "OFFICE ADMINISTRATIVE SERVICES"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "medical payment ctr"),]$naics_low<-
  "Medical Business Administration"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "medical payment ctr"),]$naics_high<-
  "OFFICE ADMINISTRATIVE SERVICES"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "midland funding"),]$naics_low<-
  "Collection Agencies"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_og== "midland funding"),]$naics_high<-
  "COLLECTION AGENCIES"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Money Lion of Nevada, Llc"),]$naics_low<-
  "Loans-Personal"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Money Lion of Nevada, Llc"),]$naics_high<-
  "CONSUMER LENDING"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Netcredit"),]$naics_low<-
  "Credit Unions"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Netcredit"),]$naics_high<-
  "CREDIT UNIONS"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Navy Fcu"),]$naics_low<-
  "Credit Unions"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Navy Fcu"),]$naics_high<-
  "CREDIT UNIONS"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Nelnet Loans"),]$naics_low<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Nelnet Loans"),]$naics_high<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Paypal"),]$naics_low<-
  "Online Services"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Paypal"),]$naics_high<-
  "ALL OTHER TELECOMMUNICATIONS"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Piedmont"),]$naics_low<-
  "Medical Centers"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Piedmont"),]$naics_high<-
  "GENERAL MEDICAL & SURGICAL HOSPITALS"


D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Portfolio Recovery Associates"),]$naics_low<-
  "Collection Agencies"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Portfolio Recovery Associates"),]$naics_high<-
  "COLLECTIONS AGENCIES"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n=="Synchrony Bank"),]$naics_low<-
  "Credit Card & Other Credit Plans"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n=="Synchrony Bank"),]$naics_high<-
  "Financial Transactions Processing, Reserve, and Clearinghouse Activities"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n=="Target"),]$naics_low<-
  "Credit Card & Other Credit Plans"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n=="Target"),]$naics_high<-
  "Financial Transactions Processing, Reserve, and Clearinghouse Activities"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Us Dep Ed"),]$naics_low<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Us Dep Ed"),]$naics_high<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Usdoe/Glelsi"),]$naics_low<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n== "Usdoe/Glelsi"),]$naics_high<-
  "Schools"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n=="Webbank/Fhut"),]$naics_low<-
  "Credit Card & Other Credit Plans"

D_unsec_cred_naics_f[which(D_unsec_cred_naics_f$creditor_n=="Webbank/Fhut"),]$naics_high<-
  "Financial Transactions Processing, Reserve, and Clearinghouse Activities"

##credit union loan sourc

dbWriteTable(mydb, "pacer_creditors_unsec_naics_f", D_unsec_cred_naics_f, row.names=F, append=
               T, overwrite = F)

###secured section
db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet") 
db_port <- 3306

#connect to server
mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

bk_cases_q <- "SELECT * FROM `pacer_creditors_secured_cl`"
rs <- dbSendQuery(mydb, bk_cases_q)
D_secured <-  fetch(rs, n = -1)%>%data.frame()

bk_cases_q <- "SELECT * FROM `pacer_creditors_sec_naics_f`"
rs <- dbSendQuery(mydb, bk_cases_q)
D_secured_old <-  fetch(rs, n = -1)%>%data.frame()

D_secured<-D_secured %>% filter(bk_id%in%D_secured_old$bk_id==FALSE)

#this formats the creditors in a way that makes dedup usable
names(D_creditors)
D_secured_creds<-get_list_of_creditors(D_secured)

D_creditors_f_sec_rb<-D_secured_creds %>% 
  rename(creditor = company_name) %>%  select(order(colnames(.))) %>% group_by(creditor) %>% 
  mutate(group_id = cur_group_id())

cred_join_ind<-D_creditors_f_sec_rb %>% select(Id, group_id)

#filter our businesses to just those in the current zip. group by creditor to avoid 
#matching things we don't need to match. add a group id so we can join later
D_creditors_f_sec_rb<-D_secured_creds %>% 
  rename(creditor = company_name) %>%  select(order(colnames(.))) %>% group_by(creditor) %>% 
  mutate(group_id = cur_group_id())

cred_join_ind<-D_creditors_f_sec_rb %>% select(Id, group_id)

creds_to_label<-D_creditors_f_sec_rb %>% select(creditor, group_id) %>% distinct() %>% 
  add_column(orig = "yes") %>% mutate(Id = group_id, creditor = tolower(creditor))

D_naics_f<-D_naics %>% mutate(creditor = tolower(company)) %>%
  add_column(Id = 1:nrow(.),orig = "no") %>%select(order(colnames(.)))

#this is the code
D_cred_matched<-match_creditors_ml(D_naics_f,creds_to_label)

matched_group<-D_cred_matched %>% group_by(group_id) %>% 
  summarise(min_ind_name = which.min(name_dist),
            min_cred_name = company[min_ind_name],
            creditor_name_og = creditor_og[1],
            name_dist_agg = name_dist[min_ind_name]) %>% 
  mutate(dist_osa_add_ok = stringdist(min_cred_name %>% tolower, creditor_name_og,
                                      weight = c(d = .1, i=0.1, s = 1, t= 1)),
         rel_dist_osa_add = dist_osa_add_ok/nchar(creditor_name_og),
         rel_dist_osa = name_dist_agg/nchar(creditor_name_og) )%>% 
  filter(rel_dist_osa <.6) %>% distinct

##we look for the longest common substring in the full data for the remaining 170 creditors and 
##do a pretty good job, this get us to 90% of creditors

unmatched_group<-D_cred_matched %>% group_by(group_id) %>% 
  summarise(min_ind_name = which.min(name_dist),
            min_cred_name = company[min_ind_name],
            creditor_name_og = creditor_og[1],
            name_dist_agg = name_dist[min_ind_name]) %>% 
  mutate(dist_osa_add_ok = stringdist(min_cred_name %>% tolower, creditor_name_og,
                                      weight = c(d = .1, i=0.1, s = 1, t= 1)),
         rel_dist_osa_add = dist_osa_add_ok/nchar(creditor_name_og),
         rel_dist_osa = name_dist_agg/nchar(creditor_name_og) )%>% 
  filter(rel_dist_osa >=.6)

##we write this table out because it's the result of many hours of computation
dbWriteTable(mydb, "pacer_creditors_sec_dedupe_prelim", D_cred_matched, row.names=F, append=T)

##group by the company name and take the mode of their naics code
D_cred_matched_f_1 <- D_cred_matched %>% filter(company%in%matched_group$min_cred_name) %>% 
  group_by(company, state) %>% summarise(
    creditor = creditor[1], 
    naics = Modes(as.character(naics8_descriptions)),
    naics_code = Modes(as.character(primary_naics_code)),
    parent_number = Modes(as.character(parent_number)) ) %>% distinct

D_cred_matched_f<-D_cred_matched_f_1 %>% 
  left_join(matched_group, by = c("company"="min_cred_name"))

min_unmatched <- rep(0, nrow(unmatched_group))
naics_comps_low<-D_naics_f$company %>% tolower
dists_v <- rep(0, nrow(unmatched_group))

for(i in 1:nrow(unmatched_group)){
  dists_i<-stringdist(unmatched_group$creditor_name_og[i], naics_comps_low)
  min_unmatched[i]<- which.min(dists_i)
  dists_v[i] <- min(dists_i)
}

D_cred_unmatched <- D_naics_f[min_unmatched,]
D_cred_unmatched$creditor_og<- unmatched_group$creditor_name_og
D_cred_unmatched$group_id <- unmatched_group$group_id
D_cred_unmatched$name_dist<- dists_v

#pretty arbitrary cutoff of .44 
D_cred_unmatched_f<-D_cred_unmatched %>% 
  mutate(lcs_d = stringdist(company %>% tolower, creditor_og, method = "lcs")/nchar(creditor_og)) %>% 
  arrange(desc(lcs_d)) %>% data.frame %>% 
  filter(lcs_d<.44)%>% 
  group_by(company, state, group_id) %>% summarise(
    creditor = creditor[1], 
    naics = Modes(as.character(naics8_descriptions)),
    naics_code = Modes(as.character(primary_naics_code)),
    parent_number = Modes(as.character(parent_number)) ) %>% distinct

D_cred_matched_f<-D_cred_matched_f_1 %>% 
  left_join(matched_group, by = c("company"="min_cred_name")) %>% filter(is.na(group_id)==FALSE) %>% 
  select(company, state,creditor, naics, naics_code, parent_number, group_id)

#go back and rejoin to individual creditors using group id and then to debts using Id
D_cred_naics <- rbind(D_cred_unmatched_f, D_cred_matched_f) %>% group_by(group_id,state) %>% 
  summarise( creditor,state = state[1], naics = naics[1],naics_code = naics_code[1],  
             parent = parent_number[1])

##sometimes wires get crossed. this is how we can fix it
naics_code_cross<-D_cred_naics %>% group_by(naics_code) %>% 
  summarise(naics_title = Modes(naics))

naics_8_cleaned <-read_csv("~/Desktop/naics_8_cross.csv") %>% mutate(naics_code = `8 Digits naics Based code`, naics = description ) %>% 
  mutate(naics_code = naics_code %>% as.character)

D_cred_naics_f<-D_cred_naics %>% left_join(naics_8_cleaned, by = "naics_code") %>% 
  select(group_id, state, creditor, naics_code, naics_high = naics.x, naics_low = naics.y, parent)

dbWriteTable(mydb, "pacer_creditors_sec_creditor_table", D_cred_naics_f, row.names=F, append=T)

##one last little thing to get right. for some of the creditors we have multiple instances of
##the company in different states. first we match on those.
D_sec_cred_naics_id_state<-D_creditors_f_sec_rb %>% 
  left_join(D_cred_naics_f, by = c("group_id","state")) %>%
  group_by(Id) %>% summarise(city = city[1], creditor_og = creditor.y[1],
                             creditor_n = creditor.x[1],naics_low = naics_low[1],
                             naics_high = naics_high[1],
                             naics_code = naics_code[1],  state = state[1], 
                             parent = parent[1]) %>% 
  left_join(D_secured, by = "Id") %>% 
  select(Id, bk_id, creditor_og, creditor_n, naics_low, naics_high, amount_owed,  collateral, unsecured,
         city = city.y, state = state.y,parent) %>% filter(is.na(naics_high)==FALSE)

##once we have those, we look at the creditors that just have one instance in one state, and
##remove the earlier matches
D_sec_cred_naics_id<-D_creditors_f_sec_rb %>% left_join(D_cred_naics_f, by = c("group_id")) %>%
  group_by(Id) %>% summarise(city = city[1], state= state.x[1],creditor_og = creditor.y[1],
                             creditor_n = creditor.x[1],naics_low = naics_low[1],
                             naics_high = naics_high[1],
                             naics_code = naics_code[1], state = state[1], 
                             parent = parent[1]) %>% 
  left_join(D_secured, by = "Id") %>% 
  select(Id, bk_id, creditor_og, creditor_n, naics_low, naics_high,amount_owed,  collateral, unsecured,
         city = city.y, state = state.y,parent) %>% 
  filter(Id%in%D_sec_cred_naics_id_state$Id==FALSE)

D_sec_cred_naics<-rbind(D_sec_cred_naics_id_state,D_sec_cred_naics_id) %>% 
  mutate(naics_low = case_when(creditor_og=="none"~"none", creditor_og!="none"~naics_low))

dbWriteTable(mydb, "pacer_creditors_sec_naics", D_sec_cred_naics, row.names=F, append=T)

##fixing naics codes
bk_cases_q <- "SELECT * FROM `pacer_creditors_sec_naics`"
rs <- dbSendQuery(mydb, bk_cases_q)
D_cred_naics <-  fetch(rs, n = -1)%>%data.frame()


# D_cred_naics[which(D_cred_naics$creditor_og=="aaron"),]$naics_code<-53229921

D_cred_naics[which(D_cred_naics$creditor_og=="aaron"),]$naics_low<-
  "Furniture"

D_cred_naics[which(D_cred_naics$creditor_og=="aaron"),]$naics_high<-
  "FURNITURE STORES"

D_cred_naics[which(D_cred_naics$creditor_og=="acceptance now"),]$naics_high<-
  "FURNITURE STORES"

# D_cred_naics[which(D_cred_naics$creditor=="acceptance now",]$naics_code<-
#   53229921

D_cred_naics[which(D_cred_naics$creditor_og=="acceptance now"),]$naics_low<-
  "Furniture"

D_cred_naics[which(D_cred_naics$creditor_og=="american financial"),]$naics_high<-
  "INVESTMENT ADVICE"

# D_cred_naics[D_cred_naics$creditor=="american financial",]$naics_code<-
#   52393002

D_cred_naics[which(D_cred_naics$creditor_og=="americash"),]$naics_high<-
  "CONSUMER LENDING"

# D_cred_naics[D_cred_naics$creditor=="americash",]$naics_code<-
#   52229103

D_cred_naics[which(D_cred_naics$creditor_og=="automart"),]$naics_high<-
  "USED CAR DEALERS"

# D_cred_naics[D_cred_naics$creditor=="automart",]$naics_code<-
#   44112005

D_cred_naics[which(D_cred_naics$creditor_og=="bellco credit union"),]$naics_high<-
  "CREDIT UNIONS"

# D_cred_naics[D_cred_naics$creditor=="bellco credit union",]$naics_code<-
#   52213003

D_cred_naics[which(D_cred_naics$creditor_og=="brooks furniture"),]$naics_high<-
  "FURNITURE STORES"

# D_cred_naics[D_cred_naics$creditor=="brooks furniture",]$naics_code<-
#   53229921

D_cred_naics[which(D_cred_naics$creditor_og=="buddy's home furnishings"),]$naics_low<-
  "Furniture"

D_cred_naics[which(D_cred_naics$creditor_og=="columbus"),]$naics_high<-
  "unknown"

# D_cred_naics[D_cred_naics$creditor=="columbus",]$naics_code<-
#   00000000

D_cred_naics[which(D_cred_naics$creditor_og=="community"),]$naics_high<-
  "unknown"

# D_cred_naics[D_cred_naics$creditor=="community",]$naics_code<-
#   00000000

D_cred_naics[which(D_cred_naics$creditor_og=="credit union loan source llc"),]$naics_high<-
  "CONSUMER LENDING"

# D_cred_naics[D_cred_naics$creditor=="credit union loan source llc",]$naics_code<-
#   52229103

D_cred_naics[which(D_cred_naics$creditor_og=="exeter finance"),]$naics_low<-
  "Loans-Automobile"

D_cred_naics[which(D_cred_naics$creditor_og=="exeter finance"),]$naics_high<-
  "AUTOMOBILE FINANCING"

D_cred_naics[which(D_cred_naics$creditor_og=="global lending svc"),]$naics_low<-
  "Loans-Automobile"

D_cred_naics[which(D_cred_naics$creditor_og=="global lending svc"),]$naics_high<-
  "AUTOMOBILE FINANCING"

D_cred_naics[which(D_cred_naics$creditor_og== "irs"),]$naics_low<-
  "Federal Government-Finance & Taxation"

# D_cred_naics[D_cred_naics$creditor=="irs",]$naics_code<-
#   92113003

D_cred_naics[which(D_cred_naics$creditor_og== "irs"),]$naics_low<-
  "Public Finance Activities"

D_cred_naics[which(D_cred_naics$creditor_og== "rent-a-center"),]$naics_low<-
  "Furniture"

# D_cred_naics[D_cred_naics$creditor=="rent-a-center",]$naics_code<-
#   92113003

D_cred_naics[which(D_cred_naics$creditor_og== "us auto sales"),]$naics_low<-
  "Automobile Dealers-Used Cars"

D_cred_naics_f<-D_cred_naics %>% select(-Id) %>% distinct %>% add_column(Id= 1:nrow(.))

dbWriteTable(mydb, "pacer_creditors_sec_naics_f", D_cred_naics_f, row.names=F, append=T)
