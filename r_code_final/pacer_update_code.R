###################################
####PACER Bankruptcy Update Code###
###################################

#This is the R file that updates the bankruptcy data and builds the directory structure that the
#parsing file needs. It relies on PACER Monitor, which is a PACER access company that gave the AJC
#access to their API for this project.

#This file assumes that the metadata has already been acquired and that a process for choosing samples
#has already been decided upon. 

#If someone chooses another way to get the metadata, they can still use the
#directory building code here. The directory structure includes calls to AWS to build an AWS 
#file directory, as well.

library(tidyverse)
library(lubridate)
library(httr)
library(jsonlite)
library(aws.s3)
library(fs)
library(pdftools)
library(RMySQL)
library(keyring)

source("~/Desktop/Covid_financial_crisis/r_code/R_code_library.R")

#this is the code that gets new samples and updates what we already have. 
#there are a couple steps involved here. 

#I've generated a sample guide file that gets us enough samples to properly estimate but also 
#stays within budget.

#the first step is to load in that sample file. The breakdown of the sample is in a separate R file

####load sample guide
D_sample_to_get <- read_csv("~/Desktop/PACER_sample_guide.csv")

###load metadata
# we have the metadata for all cases and use that metadata to get the voluntary petitions 
#for our sample. load that in.

#

#define user and serverinformation
db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet") 
db_port <- 3306

#connect to server
mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

bk_cases_q <- "SELECT * FROM `pacer_monitor_bankruptcy_meta_t`"

#fetch and transform data
rs <- dbSendQuery(mydb, bk_cases_q)
D_just_data_f_sub_f <-  fetch(rs, n = -1)%>%data.frame()

###loading scraped data that we already have. The first time this is run, this line should be excluded
bk_cases_q <- "SELECT * FROM `pacer_filing_data_pers_t`"
rs <- dbSendQuery(mydb, bk_cases_q)
D_bk_pers <-  fetch(rs, n = -1)%>%data.frame()

D_samp_gotten_buckets<-D_bk_pers %>% mutate(d_year = year(filed_date)) %>% group_by(d_year, court_name, case_chapter) %>% 
  summarise(n = n()) %>% mutate(d_year = d_year -1, 
                                court_name = case_when(
                                  str_detect(court_name, "Middle")~"GMB",
                                  str_detect(court_name, "Northern")~"GNB",
                                  str_detect(court_name, "Southern")~"GSB"
                                  )
                                )

if(any(table(D_samp_gotten_buckets$d_year)==2020)==FALSE){
  d_2020<-tibble(d_year = rep(2020, 6), 
         court_name=c("GMB","GMB","GNB","GNB","GSB","GSB"),
         case_chapter=rep(c("7","13"),3) %>% as.numeric,
         n= rep(0, 6) %>% as.integer()
         )
  
  D_samp_gotten_buckets_f<-bind_rows(d_2020,D_samp_gotten_buckets)
}

#this code gives us the elements to sample if we want to keep the proportions of our intermediate
#sample close to the proportion of our final sample, so that we don't run into unbalanced issues 
#while analyzing in the mid-term
p = .33

D_samp_to_get_now<-D_sample_to_get %>% left_join(
  D_samp_gotten_buckets_f, 
  by = c("year"="d_year","court"="court_name", "chapter"="case_chapter")
  ) %>% 
  mutate(
    perc_of_final_sampled=1-(filings_to_samp-n)/filings_to_samp, 
    away_from_20_p=p-perc_of_final_sampled,
    num_to_samp_now = round(away_from_20_p*filings_to_samp)
    ) %>% 
  filter(away_from_20_p>0)

### combine metadata and sample guide file to get the file we'll use to pull voluntary petitions
#set the seed so we don't lose our sample
set.seed(1)

#each of these samples from the metadata the number of samples specified for that court and chapter in 
#the  guide
#year_c<-c(2019, 2018)
year_c <- 2020
D_sample_to_get_f<-D_samp_to_get_now %>% filter(year %in%year_c) 

#these are the cases we haven't purchased
D_just_data_f_sub_f_2<-D_just_data_f_sub_f %>% filter(case_ID%in%D_bk_pers$bk_id==FALSE)

#these lines then sample from the bk identifiers to choose which bk ids we're actually going to 
#pay for

#gnb 13
GNB_13_samps_d<-D_just_data_f_sub_f_2 %>% mutate(year_d = year(filed_date)) %>% 
  filter(case_chapter==13, type_of_debtor=="I", year_d %in%(year_c+1), court_name=="Georgia Northern Bankruptcy Court")

gnb_13_inds<-1:nrow(GNB_13_samps_d) %>% 
  sample(., D_sample_to_get_f %>% filter(court=="GNB", chapter==13) %>% pull(num_to_samp_now))

GNB_13_samps<-GNB_13_samps_d[gnb_13_inds,] %>% select(pacer_case_num, case_ID,court_name)

#gnb 7
GNB_7_samps_d<-D_just_data_f_sub_f_2 %>% mutate(year_d = year(filed_date)) %>% 
  filter(case_chapter==7, type_of_debtor=="I", year_d %in%(year_c+1), court_name=="Georgia Northern Bankruptcy Court")

gnb_7_inds<-1:nrow(GNB_7_samps_d) %>% 
  sample(., D_sample_to_get_f %>% filter(court=="GNB", chapter==7) %>% pull(num_to_samp_now))

GNB_7_samps<-GNB_7_samps_d[gnb_7_inds,] %>% select(pacer_case_num, case_ID,court_name)

#gmb 13
GMB_13_samps_d<-D_just_data_f_sub_f_2 %>% mutate(year_d = year(filed_date)) %>% 
  filter(case_chapter==13, type_of_debtor=="I", year_d %in%(year_c+1), court_name=="Georgia Middle Bankruptcy Court")

gmb_13_inds<-1:nrow(GMB_13_samps_d) %>% 
  sample(., D_sample_to_get_f %>% filter(court=="GMB", chapter==13) %>% pull(num_to_samp_now))

GMB_13_samps<-GMB_13_samps_d[gmb_13_inds,] %>% select(pacer_case_num, case_ID,court_name)

#gmb 7
GMB_7_samps_d<-D_just_data_f_sub_f_2 %>% mutate(year_d = year(filed_date)) %>% 
  filter(case_chapter==7, type_of_debtor=="I", year_d %in%(year_c+1), court_name=="Georgia Middle Bankruptcy Court")

gmb_7_inds<-1:nrow(GMB_7_samps_d) %>% 
  sample(., D_sample_to_get_f %>% filter(court=="GMB", chapter==7) %>% pull(num_to_samp_now))

GMB_7_samps<-GMB_7_samps_d[gmb_7_inds,] %>% select(pacer_case_num, case_ID,court_name)

#gsb 13
GSB_13_samps_d<-D_just_data_f_sub_f_2 %>% mutate(year_d = year(filed_date)) %>% 
  filter(case_chapter==13, type_of_debtor=="I", year_d %in%(year_c+1), court_name=="Georgia Southern Bankruptcy Court")

gsb_13_inds<-1:nrow(GSB_13_samps_d) %>% 
  sample(., D_sample_to_get_f %>% filter(court=="GSB", chapter==13) %>% pull(num_to_samp_now))

GSB_13_samps<-GSB_13_samps_d[gsb_13_inds,] %>% select(pacer_case_num, case_ID,court_name)

#gsb7                                      
GSB_7_samps_d<-D_just_data_f_sub_f_2 %>% mutate(year_d = year(filed_date)) %>% 
  filter(case_chapter==7, type_of_debtor=="I", year_d %in%(year_c+1), court_name=="Georgia Southern Bankruptcy Court")

gsb_7_inds<-1:nrow(GSB_7_samps_d) %>% 
  sample(., D_sample_to_get_f %>% filter(court=="GSB", chapter==7) %>% pull(num_to_samp_now))

GSB_7_samps<-GSB_7_samps_d[gsb_7_inds,] %>% select(pacer_case_num, case_ID,court_name)

#this is the final list of samples to request from PacerMonitor
samples_to_get<-rbind(GNB_13_samps,GNB_7_samps,GMB_13_samps,GMB_7_samps,GSB_13_samps,GSB_7_samps) %>% 
  mutate(pacer_case_num = pacer_case_num %>% str_replace_all(":","-")) %>% na.omit

samples_to_get_f<-samples_to_get[samples_to_get$pacer_case_num!=0,]

###Now we actually go and get the data from PACER
###api key
user<-key_get("pacer_monitor_user")
api_key<-key_get("pacer_monitor_api_key")
base_url<- "https://api.pacermonitor.com/v3/"

#get session key for PACER. if this times out, you need to re-run this line
pacer_auth<-POST(url = str_c(base_url,"auth/login"), 
                 query = list(username = user, password=api_key),
                 encode="json")

auth_token<-httr::content(pacer_auth)[[1]]

#this is the high-level function that downlaods pacer cases to the machine. The file location sets
#the stem. This folder needs to exist beforehand. This can be extremely expensive if you aren't 
#careful. You're paying for and downloading bk cases. If someone wanted to swap in another API here
#for buying cases, they certainly could. You would edit that in the code library
download_pacer_cases(
  cases_caseID_court=samples_to_get_f[1:nrow(samples_to_get_f),],
  filelocation = "~/Desktop/AJC_PACER/",
  auth = auth_token
  )

##once we have the pacer files and they've been written out to the directory specified above,
# we organize them into a directory structure that we'll rely on later.

#updating and making file directory
setwd("~/Desktop/AJC_PACER")
files<-list.files()

pdf_files<-files[which(str_detect(files, ".pdf"))]

#move everything to its own folder
for(i in 1:length(pdf_files)){
  new_fold_name <- str_remove(pdf_files[i],".pdf") %>% str_remove("p_bk_")
  dir.create(new_fold_name)
  file_move(pdf_files[i], new_fold_name)
}

#create images of all files. this is for the local cv
pdf_folders<-pdf_files %>% str_remove(".pdf") %>% str_remove("p_bk_")

for(i in 1:length(pdf_folders)){
  setwd(as.character(pdf_folders[i]))
  local_files<-list.files()
  pdf_file_ind<-str_detect(local_files, ".pdf") %>% {which(.)}
  pdf_file_name<-local_files[pdf_file_ind]
  tryCatch(pdf_convert(pdf_file_name, format= "png", dpi = 180), error = function(e) e)
  setwd("~/Desktop/AJC_PACER")
}

###now we add these new documents to aws s3 buckets to use with Textract
#these are the ajc nthieme keys
#set aws credentials
AWSAccessKeyId=key_get("aws_access_key_dev")
AWSSecretKey=key_get("aws_secret_key_dev", AWSAccessKeyId)

Sys.setenv(
  "AWS_ACCESS_KEY_ID" = AWSAccessKeyId, 
  "AWS_SECRET_ACCESS_KEY" = AWSSecretKey,
  "AWS_DEFAULT_REGION" = "us-east-1"
)

bucket_name <-  key_get('aws_data_bucket')

existing_files<-get_bucket(bucket_name, max =Inf)
existing_files_f<-lapply(existing_files, function(x)return(x$Key)) %>% unlist

files<-list.files("/Users/nthieme/Desktop/AJC_PACER")

files_f<-lapply(files,
  function(x){
    ff<-list.files(
      str_c(
        "/Users/nthieme/Desktop/AJC_PACER",
        
      )
    )
    
    detect<-str_detect(ff, "blocks") %>% any
    return(detect)
    }
)

files_to_add<-files[which(files_f==FALSE)]
files_to_add_chk<-str_c("p_bk_",files_to_add, ".pdf")
to_rm<-which(files_to_add_chk%in%existing_files_f)

if(length(to_rm)>0){
  files_to_add_chk<-files_to_add_chk[-to_rm]
}

files_to_add_f<-files_to_add_chk %>% str_remove("p_bk_") %>% str_remove(".pdf")

#this is a loop that goes through and adds pdfs to my docs in aws

for(i in 1:length(files_to_add_f)){
  setwd(files_to_add_f[i])
  local_files <- list.files()
  object_name <- local_files[local_files %>% str_detect(".pdf") %>% {which(.)}]
  
  if((object_name%in%existing_files_f)==FALSE){
    put_object(file=object_name, object = object_name, bucket =bucket_name,acl = "public-read")
  }else{
    
  }
  
  setwd("/Users/nthieme/Desktop/AJC_PACER")
}
