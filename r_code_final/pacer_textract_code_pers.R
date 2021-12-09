############################
####Amazon PACER scraper####
############################



#This file takes a set of already purchased and downloaded PACER bankruptcy documents and parses them
#into a series of tidy tables containing information about individual bankruptcy debtors and the 
#debts they hold. It uses AWS textract for computer vision as well as some homebrewed computer vision
#techniques to extract the data from both text and image PDFs. Pacer_monitor_case_metadata is the 
#file that downloads PACER files and creates the directory structure this file relies upon. 


library(tidyverse)
library(paws)
library(aws.iam)
library(aws.s3)
library(aws.signature)
library(aws.cloudtrail)
library(RMySQL)
library(jsonlite)
library(keyring)
library(tidygeocoder)
library(ggmap)

#load in a library of helper function many of which are essential. Documentation of the specifics 
#of those functions is included in the R_code_library_file.

source("~/Desktop/Covid_financial_crisis/r_code/R_code_library.R")

setwd("/Users/nthieme/Desktop/AJC_PACER")

#set aws credentials. These are function froms keyring that store passwords locally to avoid
#publishing keys online. They should be associated with the AWS account that will use Textract

AWSAccessKeyId=key_get("aws_access_key_dev")
AWSSecretKey=key_get("aws_secret_key_dev", AWSAccessKeyId)

Sys.setenv(
  "AWS_ACCESS_KEY_ID" = AWSAccessKeyId, 
  "AWS_SECRET_ACCESS_KEY" = AWSSecretKey,
  "AWS_DEFAULT_REGION" = "us-east-1"
)

#this is the textract object we use to interact with Amazon Textract. The region needs to be specified
#and depends on the user's region
textract_obj <- 
  paws::textract( 
    config = list(
      credentials = list(
        creds = list(
          access_key_id = AWSAccessKeyId,
          secret_access_key = AWSSecretKey
        )
      ),
      region = "us-east-1" #need to specify this
    )
  )

#this is the locally stored bucket name. 
bucket_name <- key_get("aws_data_bucket")

#get items in the bucket
bucket_items<-get_bucket(bucket_name, max = Inf)

##set SQL credentials. We do this to not re-parse bk cases we've already parsed. Because it costs
## to analyze the data, we manke sure we're not re-doing things. 

db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet")
db_port <- 3306

#connect to server
mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

bk_cases_q <- "SELECT * FROM `pacer_filing_data_pers_t_1`"
rs <- dbSendQuery(mydb, bk_cases_q)
D_run_already <-  fetch(rs, n = -1)%>%data.frame()

#this code filters the PDFs that are in the directory to the PDFs that we don't have in the tables
#yet

full_pdf_list<-lapply(bucket_items, function(x)return(x$Key)) %>% unlist
to_run_id<-which((full_pdf_list %>% str_remove("p_bk_") %>% str_remove(".pdf")) %in% D_run_already$bk_id==FALSE)
docs_to_run<-full_pdf_list[to_run_id] 

#if something goes wrong (internet connection issue, R crash,...) between when we try to download 
#the run results from AWS and when we produce the table, this is the code that get the jobID 
#from Amazon to download can get the stored results. 

#Event_history is produced by awscloudtrail on AWS UI (I'm sure there's a CLI for this also, but it 
#happened infrequently enough that I didn't need to learn it). you can filter to your user and 
#the time period during which the code was run, and download the results as JSON.

doc_names<-docs_to_run%>% str_remove("p_bk_") %>% str_remove(".pdf")
a<-fromJSON("~/Downloads/event_history(3).json")
a_start <- a$Records[which(a$Records$eventName=="StartDocumentAnalysis"),]
missing_table_blocks <- rep(0, length(doc_names))
files_in_folder <- list.files()

for(i in 1:length(doc_names)){
  missing_table_blocks[i]=list.files(str_c(
    #"/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/Pacer_vol_pets_pers/",
    "~/Desktop/AJC_PACER/",
    doc_names[i])) %>% str_detect("table_blocks") %>% any
}

to_get<-which(missing_table_blocks==0)
still_missing<-doc_names[to_get]

D_missed<-data.frame(location=a$Records$requestParameters$documentLocation$s3Object$name,
                     jobID=a$Records$responseElements$jobId,
                     time_run = parse_datetime(as.character(a$Records$eventTime))) %>%
  group_by(location) %>% summarise(jobID=jobID[which.max(time_run)]) %>% mutate_all(as.character) %>% 
  filter(str_detect(location, ".pdf"))

D_missed_f<-D_missed %>% mutate(bk_id =location %>% str_remove("p_bk_") %>% str_remove(".pdf")) %>% 
  filter(bk_id%in%still_missing)

docs_to_run_n<-D_missed_f$jobID

#this is the workhorse function that sends the PDF documents to aws textract to pull out the tables.
#the pdfs are already stored in an S3 bucket, this function points textract to the files in the bucket
#to start parsing them in parallel.

jobID_list_n<-start_document_analysis_parallel(bucket="cfc-pacer-docs-f",
                                                  files=docs_to_run, 
                                                  features=list("TABLES"))


#this is the function that pulls the parsed data from textract. The data comes back in a very 
#particular format that's quite large, so this function takes a good bit of time. We also save out
#the raw unparsed data into the file structure.

analysis_block_list<-get_document_analysis_parallel(jobID_list_f = jobID_list_n[3:length(jobID_list_n)], 
                                                    time_thresh = 10000,
                                                    docs_to_run_f=docs_to_run[3:length(jobID_list_n)],
                                                    kind_of_save="_table_blocks")

#docs_to_load<-lapply(bucket_items, function(x)return(x$Key)) %>% unlist%>% str_remove("p_bk_") %>% str_remove(".pdf")
docs_to_load<-docs_to_run %>% str_remove("p_bk_") %>% str_remove(".pdf")

#tables_list<-vector(mode = "list", length = length(docs_to_load))
check_list_2<-vector(mode = "list", length = length(docs_to_load))

#parsing the data from the PDFs takes two steps. The first is to parse PDFs with textract to get the
#text off of the pages. The second step is to go through the lists and organize the text into 
#datasets. This is the loop that accomplishes the second step

for(i in 1:length(docs_to_load)){
  #load in the saved amazon object from the directory
  analysis<-loadRData(
    str_c(
      "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/Pacer_vol_pets_pers/", 
      docs_to_load[i], 
      "/",
      docs_to_load[i],
      "_table_blocks")
    )
  
  #here is where we get the interesting info for personal debtors
  
  #Amazon adds metadata to their parsed data that tells us what kind of data is in each entry of the 
  #list. this filters down to the line level (as opposed to word level) where we extract the
  #page name of each entry this lets us filter to, for example, Form J on the bankruptcy form.
  
  D_form_titles<-rbindlist(filter_to_type(analysis,"LINE")[[1]] )%>%
    filter(str_detect(Text, "Official Form"), nchar(Text)<40)
  
  
  #D_form_titles_f_2 gives us crosswalk between pdf page numbers and document names
  D_form_titles_f <- D_form_titles[seq(from =1, to=nrow(D_form_titles), by = 2),]
  y_col<-lapply(D_form_titles_f$Geometry, function(x)return(x$Top)) %>% unlist
  
  D_form_titles_f_2<-D_form_titles_f %>% add_column( y = y_col) %>% filter(y > .9) %>% 
    select(Text, Page)
  
  pacer_ind=docs_to_load[i]
  
  #form 101: information about the filer
  d_101<-form_101_extract_pers(form_crosswalk=D_form_titles_f,
                               pacer_ind=pacer_ind,
                               analysis_f=analysis)

  #106I: income information
  d_106I<-form_106I_extract_pers(form_crosswalk=D_form_titles_f,
                                 pacer_ind=pacer_ind,
                                 analysis_f=analysis)
  
  #106J: expense information
  d_106J<-form_106J_extract_pers(form_crosswalk=D_form_titles_f,
                                 pacer_ind=pacer_ind,
                                 analysis_f=analysis)
  
 #107: marriage, living, and more income
  d_107<-form_107_extract_pers(form_crosswalk=D_form_titles_f,
                               pacer_ind=pacer_ind,
                               analysis_f=analysis)
 
  #106Sum: total summed income
  final_sums<-form_106S_extract_pers(form_crosswalk=D_form_titles_f,
                                     pacer_ind=pacer_ind,
                                     analysis_f=analysis)
  
  #store info for later
  check_list_2[[i]]<-  cbind(bk_id=pacer_ind, d_101,d_106I, d_106J,d_107,final_sums) %>% mutate_all(as.character) 
  
  if(is.na(as.numeric(check_list_2[[i]]$liabilities_r))){
    if(all(final_sums!="none")){
      break
      beep()
      print("broke because of empty liabilities")
    }
    
  }
  
  print(str_c(i, " of ", length(docs_to_run)))
}

rm_already<-lapply(check_list_2, function(x)return(all(x=="already_run"))) %>% unlist %>% {which(.)}

if(length(rm_already)>0){
  check_list_f<-lapply(check_list_2[-rm_already], function(x)return(x %>% mutate_all(as.character) %>% 
                                                                    mutate_all(replace_na)))
}else{
  check_list_f<-check_list_2
}

D_bk_pers<-do.call("rbind",check_list_f)

setwd("/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/intermediary_files/")
write_csv(D_bk_pers, str_c("D_bk_pers_pt_1_",str_sub(Sys.time(), 1,10),".csv"))
write.table(str_sub(Sys.time(), 1,10), "last_date_full_d.txt")
last_date <- read.table("last_date_full_d.txt")
D_bk_pers<-read_csv(str_c("D_bk_pers_pt_1_", last_date$x, ".csv"))

#The issue here is that textract doesn't always do a perfect job of getting info from the bk cases
#we detect which cases (generally image PDFs) didn't properly parse and we use homebrewed methods
#to parse them

#get the missing info
missing_checks<-which(is.na(D_bk_pers$type_of_payment)==TRUE)
missing_checks_title <- D_bk_pers[missing_checks,]$bk_id %>% as.character() %>% str_c("p_bk_",.,".pdf")
checks_missing <- vector(mode = "list", length = length(missing_checks))

#A lot of the parsing in our functions depends on correctly detecting check marks in data.
#many times Amazon doesn't pick up the check marks correctly, so we use a neat little computer
#vision trick to pick up on the checks. You can think of this loop as doing the same thing as the 
#previous one but using this cv method for the checkmarks instead of relying on textract

fold_names<-missing_checks_title %>% str_remove("p_bk_") %>% str_remove(".pdf")
setwd("/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/Pacer_vol_pets_pers/")

m_checks<-D_bk_pers[missing_checks,]$bk_id

for(i in 1:length(m_checks)){

  no_analy_block<-tryCatch(loadRData(
    str_c(
      "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/Pacer_vol_pets_pers/", 
      m_checks[i], 
      "/",
      m_checks[i],
      "_table_blocks")
  ), error = function(e)e)
  
  if(class(no_analy_block)[1]=="simpleError"){
    next
  }
  
  pacer_id<-m_checks[i]
  
  D_form_titles<-rbindlist(filter_to_type(no_analy_block,"LINE")[[1]] )%>%
    filter(str_detect(Text, "Official Form"), nchar(Text)<40)
  
  #getting 
  x_scale = 1600
  y_scale= 1980
  
  form_101_inds<-D_form_titles %>% filter(str_detect(Text,"101")) %>% pull(Page) %>% sort %>% unique

  #this uses opencv as well as some submatrix tricks to find the checks
  D_101_check_locs_curr<-tryCatch(open_cv_get_checks(bk_id = pacer_id,pg =form_101_inds[2]), error = function(e)e) 
  
  if(any(str_detect(tolower(D_101_check_locs_curr), "error"))){
    
    
  }else{
    #we do all these scaling tricks to correctly line up the page with the pixel sizes
    D_101_check_locs_curr<-D_101_check_locs_curr%>% 
      mutate(x_scaled = x  / x_scale, y_scaled = y / y_scale, w_scaled = w/x_scale, h_scaled = h/y_scale,
             r_scaled = x_scaled+w_scaled, b_scaled = y_scaled + h_scaled) %>% filter(perc_filled>.35) %>% 
      add_column(page = form_101_inds[2])
  }
  
  no_analy_block_pg_2<-filter_to_page(no_analy_block, form_101_inds[2])
  
  #get addr
  addr<-get_opencv_address(analy_blocks=no_analy_block_pg_2)

  #101
  D_101_check_locs_curr_c<-tryCatch(open_cv_get_checks(bk_id = pacer_id,pg =form_101_inds[3]), error = function(e)e)
  
  if(any(str_detect(tolower(class(D_101_check_locs_curr_c)), "error"))==FALSE){
    
    if(nrow(D_101_check_locs_curr_c)>6){
      f_101_prior_pg<-c("none","none","none","none","none")
    }else{
      D_101_check_locs_curr<-D_101_check_locs_curr_c %>% mutate(x_scaled = x  / x_scale, y_scaled = y / y_scale, w_scaled = w/x_scale, h_scaled = h/y_scale,
                                                                r_scaled = x_scaled+w_scaled, b_scaled = y_scaled + h_scaled) %>% filter(perc_filled>.35) %>% 
        add_column(page = form_101_inds[3])
      
      D_101_check_locs_curr=D_101_check_locs_curr_c
      
      no_analy_block_pg_3<-filter_to_page(no_analy_block, form_101_inds[3])
      
      f_101_prior_pg<-get_opencv_checked_text(analysis_blocks=no_analy_block_pg_3, 
                                              D_check_locs=D_101_check_locs_curr) %>% pull(Text) %>% as.character
    }
  }else{
    f_101_prior_pg<-c("none", "none", "none", "none", "none")
  }
  
  D_101_check_locs_curr<-tryCatch(open_cv_get_checks(bk_id = pacer_id,pg =form_101_inds[6] ), error = function(e)e) 
  
  if(any(str_detect(tolower(class(D_101_check_locs_curr)), "error"))==FALSE){
    D_101_check_locs_curr<-D_101_check_locs_curr%>% 
      mutate(x_scaled = x  / x_scale, y_scaled = y / y_scale, w_scaled = w/x_scale, h_scaled = h/y_scale,
             r_scaled = x_scaled+w_scaled, b_scaled = y_scaled + h_scaled) %>% filter(perc_filled>.33) %>% 
      add_column(page = form_101_inds[6])
    
    if(nrow(D_101_check_locs_curr)>7){
      f_101_ass_debt_pg_f<-c("none","none","none")
    }else{
      no_analy_block_pg_6<-filter_to_page(no_analy_block, form_101_inds[6])
      
      f_101_ass_debt_pg<-get_opencv_checked_text(analysis_blocks=no_analy_block_pg_6, 
                                                 D_check_locs=D_101_check_locs_curr)
      
      if(length(f_101_ass_debt_pg)==0){
        f_101_ass_debt_pg_f<-list(num_creditors="none",assets="none",liabilities="none")
      }else{
        f_101_ass_debt_pg_f<-extract_101_p6_chk(as.character(f_101_ass_debt_pg$Text)) 
      }
      
      ind_chg<-lapply(f_101_ass_debt_pg_f, length) %>% {which(.==0)}
      
      if(length(ind_chg>0)){
        f_101_ass_debt_pg_f[ind_chg]<-NA
      }
      
      f_101_ass_debt_pg_f<-f_101_ass_debt_pg_f %>% data.frame
    }
  }else{
    f_101_ass_debt_pg_f<-c("none", "none","none")
  }
  

  form_107_inds<-D_form_titles %>% filter(str_detect(Text,"107")) %>% pull(Page) %>% sort %>% unique
  
  if(length(form_107_inds)!=0){

    D_107_check_locs_curr<-tryCatch(open_cv_get_checks(bk_id = pacer_id,pg =form_107_inds[1] ), error = function(e)e)
    
    if(any(str_detect(tolower(D_107_check_locs_curr), "error"))){
      d_married <- data.frame(married = "none", lived_else = "none", cohabit = "none")
    }else{
      D_107_check_locs_curr<-D_107_check_locs_curr%>% 
        mutate(x_scaled = x  / x_scale, y_scaled = y / y_scale, w_scaled = w/x_scale, h_scaled = h/y_scale,
               r_scaled = x_scaled+w_scaled, b_scaled = y_scaled + h_scaled) %>% filter(perc_filled>.33) %>% 
        add_column(page = form_107_inds[1])
      
      no_analy_block_pg_1<-filter_to_page(no_analy_block, form_107_inds[1])
      
      married<-get_opencv_checked_text(analysis_blocks=no_analy_block_pg_1, 
                                       D_check_locs=D_107_check_locs_curr)
      
      d_married <- data.frame(married = married$Text[1], lived_else = married$Text[2], cohabit = married$Text[3])
    }
    
    D_107_check_locs_curr<-tryCatch(open_cv_get_checks(bk_id = pacer_id,pg =form_107_inds[4] ), error = function(e)e)
    
    if(any(str_detect(tolower(D_107_check_locs_curr), "error"))){
      d_extra_debt<-c("none", "none")
    }else{
      D_107_check_locs_curr<-D_107_check_locs_curr%>% 
        mutate(x_scaled = x  / x_scale, y_scaled = y / y_scale, w_scaled = w/x_scale, h_scaled = h/y_scale,
               r_scaled = x_scaled+w_scaled, b_scaled = y_scaled + h_scaled) %>% filter(perc_filled>.33) %>% 
        add_column(page = form_107_inds[4])
      
      no_analy_block_pg_1<-filter_to_page(no_analy_block, form_107_inds[4])
      
      pay_debt<-get_opencv_checked_text(analysis_blocks=no_analy_block_pg_1, 
                                       D_check_locs=D_107_check_locs_curr)
      
      d_extra_debt <-data.frame(insider_debt = pay_debt$Text[1], insider_ben=pay_debt$Text[2])
    }

    D_107_check_locs_curr<-tryCatch(open_cv_get_checks(bk_id = pacer_id,pg =form_107_inds[5] ), error = function(e)e)
    
    if(any(str_detect(tolower(D_107_check_locs_curr), "error"))){
      d_rep<-c("none", "none")
    }else{
      D_107_check_locs_curr<-D_107_check_locs_curr%>% 
        mutate(x_scaled = x  / x_scale, y_scaled = y / y_scale, w_scaled = w/x_scale, h_scaled = h/y_scale,
               r_scaled = x_scaled+w_scaled, b_scaled = y_scaled + h_scaled) %>% filter(perc_filled>.33) %>% 
        add_column(page = form_107_inds[5])
      
      no_analy_block_pg_1<-filter_to_page(no_analy_block, form_107_inds[5])
      
      reposses<-get_opencv_checked_text(analysis_blocks=no_analy_block_pg_1, 
                                        D_check_locs=D_107_check_locs_curr)
      
      if(reposses=="none"){
        d_rep<-data.frame(lawsuit = "none", reposses="none")
      }else{
        d_rep<-data.frame(lawsuit = reposses$Text[1], reposses=reposses$Text[2])
      }
      
      
    }
    
    D_107_check_locs_curr<-tryCatch(open_cv_get_checks(bk_id = pacer_id,pg =form_107_inds[6] ), error = function(e)e)
    
    if(any(str_detect(tolower(D_107_check_locs_curr), "error"))){
      d_extra_debt<-c("none", "none", "none")
    }else{
      D_107_check_locs_curr<-D_107_check_locs_curr%>% 
        mutate(x_scaled = x  / x_scale, y_scaled = y / y_scale, w_scaled = w/x_scale, h_scaled = h/y_scale,
               r_scaled = x_scaled+w_scaled, b_scaled = y_scaled + h_scaled) %>% filter(perc_filled>.33) %>% 
        add_column(page = form_107_inds[6])
      
      no_analy_block_pg_1<-filter_to_page(no_analy_block, form_107_inds[6])
      
      extra_debt<-get_opencv_checked_text(analysis_blocks=no_analy_block_pg_1, 
                                        D_check_locs=D_107_check_locs_curr)
      
      if(extra_debt == "none"){
        d_extra_debt<-data.frame(owed_debt = "none", assignee="none", gift="none")
      }else{
        d_extra_debt<-data.frame(owed_debt = extra_debt$Text[1], assignee=extra_debt$Text[2], gift=extra_debt$Text[3])
      }
      
      
    }

    D_107_check_locs_curr<-tryCatch(open_cv_get_checks(bk_id = pacer_id,pg =form_107_inds[7]), error = function(e)e)  
    
    if(any(str_detect((tolower(class(D_107_check_locs_curr))), "error"))||nrow(D_107_check_locs_curr)==0){
      d_charity<-data.frame(charity = "none", 
                            theft_fire_disaster_gamble="none",
                            ass_transfer= "none")
    }else{
      D_107_check_locs_curr<-D_107_check_locs_curr %>% mutate(
        x_scaled = x  / x_scale,
        y_scaled = y / y_scale,
        w_scaled = w/x_scale,
        h_scaled = h/y_scale,
        r_scaled = x_scaled+w_scaled,
        b_scaled = y_scaled + h_scaled
      ) %>% filter(perc_filled>.3) %>% add_column(page = form_107_inds[7])
      
      if(nrow(D_107_check_locs_curr)==0){
        d_charity<-data.frame(charity = "none", 
                              theft_fire_disaster_gamble="none",
                              ass_transfer= "none")
      }else{
        no_analy_block_pg_7<-filter_to_page(no_analy_block, form_107_inds[7])
        
        charity<-get_opencv_checked_text(analysis_blocks=no_analy_block_pg_7, 
                                         D_check_locs=D_107_check_locs_curr)
        
        d_charity<-data.frame(
          charity = charity$Text[1], 
          theft_fire_disaster_gamble=charity$Text[2],
          ass_transfer= charity$Text[3]
        )
      }
    }
    
    D_107_check_locs_curr_c<-tryCatch(open_cv_get_checks(bk_id = pacer_id,pg =form_107_inds[8] ), error= function(e)e) 
    
    if(any(str_detect(tolower(class(D_107_check_locs_curr_c)),"error"))){
      d_trans <- data.frame(cred_transfer = "none")
    }else{
      D_107_check_locs_curr=D_107_check_locs_curr_c%>% 
        mutate(x_scaled = x  / x_scale, y_scaled = y / y_scale, w_scaled = w/x_scale, h_scaled = h/y_scale,
               r_scaled = x_scaled+w_scaled, b_scaled = y_scaled + h_scaled) %>% filter(perc_filled>.3) %>% 
        add_column(page = form_107_inds[8])
      
      if(nrow(D_107_check_locs_curr)!=0){
        no_analy_block_pg_8<-filter_to_page(no_analy_block, form_107_inds[8])
        
        ass_transfer<-get_opencv_checked_text(analysis_blocks=no_analy_block_pg_8, 
                                              D_check_locs=D_107_check_locs_curr)
        
        d_trans <- data.frame(cred_transfer = ass_transfer$Text[1])
      }else{
        d_trans<-"none"
      }
    }

  }else{
    d_married=c("none", "none", "none")
    insider_debt = c("none","none")
    d_rep = c("none","none")
    d_extra_debt = c("none","none","none")
    d_charity = c("none", "none","none")
    d_trans = c("none")
    income_total = c("none","none","none")
  }
  
  form_106J_inds<-D_form_titles %>% filter(str_detect(Text,"106J")) %>% pull(Page) %>% sort %>% unique
  
  if(length(form_106J_inds)!=0){
    D_106J_check_locs_curr<-open_cv_get_checks(bk_id = pacer_id,pg =form_106J_inds[1] ) %>% 
      mutate(x_scaled = x  / x_scale, y_scaled = y / y_scale, w_scaled = w/x_scale, h_scaled = h/y_scale,
             r_scaled = x_scaled+w_scaled, b_scaled = y_scaled + h_scaled) %>% filter(perc_filled>.3) %>% 
      add_column(page = form_106J_inds[1])
    
    no_analy_block_pg_1<-filter_to_page(no_analy_block, form_106J_inds[1])
    
    deps<-get_opencv_checked_text(analysis_blocks=no_analy_block_pg_1, 
                                  D_check_locs=D_106J_check_locs_curr) %>% pull(Text) %>% 
      str_detect("Fill out this information") %>% any 
    
    deps<-ifelse(deps, "Yes","No")
    
    D_106J_check_locs_curr<-tryCatch(open_cv_get_checks(bk_id = pacer_id,pg =form_106J_inds[3] ), error = function(e)e)
    
    if(any(str_detect(tolower(class(D_106J_check_locs_curr)), "error"))){
      d_inc<-data.frame(inc_or_dec="none" %>% as.character)
    }else{
      D_106J_check_locs_curr<-D_106J_check_locs_curr%>% 
      mutate(x_scaled = x  / x_scale, y_scaled = y / y_scale, w_scaled = w/x_scale, h_scaled = h/y_scale,
             r_scaled = x_scaled+w_scaled, b_scaled = y_scaled + h_scaled) %>% filter(perc_filled>.3) %>% 
        add_column(page = form_106J_inds[3])
      
      no_analy_block_pg_3<-filter_to_page(no_analy_block, form_106J_inds[3])
      
      inc_or_dec<-get_opencv_checked_text(analysis_blocks=no_analy_block_pg_3, 
                                          D_check_locs=D_106J_check_locs_curr)
      
      d_inc<-data.frame(inc_or_dec=inc_or_dec$Text[1] %>% as.character)
    }

    analysis_106J<-filter_to_page(no_analy_block, form_106J_inds)
    tables_106J <- tryCatch(get_tables_pers_p(analysis_106J), error = function(e)e)
    inds_lose<-lapply(tables_106J, function(x)return(dim(x) %>% is.null)) %>% unlist %>% {which(.)}
    
    if(length(inds_lose)==0){
      tables_106J_f<-tables_106J%>% lapply(., function(x)return(tolower(x)))
    }else{
      tables_106J_f<-tables_106J[-inds_lose] %>% lapply(., function(x)return(tolower(x)))
    }
    
    d_incs<-tryCatch(get_106J_chk(tables_106J_f), error = function(e)e) 
    
    if(str_detect(class(d_incs), "error") %>% any){
      d_incs<-c("none", "none","none")
    }
  }else{
    deps = c("none","none")
    d_incs = c("none", "none")
  }
  
  D_additional<-data.frame(pacer_id, addr, type_of_payment=f_101_prior_pg[2],prior_bank=f_101_prior_pg[3], 
                           pend_bank=f_101_prior_pg[4], rent_or_own=f_101_prior_pg[5],
                           num_creditors = f_101_ass_debt_pg_f[1],assets=f_101_ass_debt_pg_f[2],
                           liabilities=f_101_ass_debt_pg_f[3], married = d_married[1], lived_else = d_married[2], 
                           cohabit = d_married[3],lawsuit = d_rep[1],
                           reposses=d_rep[2],owed_debt = d_extra_debt[1], assignee=d_extra_debt[2], gift=d_extra_debt[3],
                           charity = d_charity[1],theft_fire_disaster_gamble=d_charity[2],cred_transfer = d_trans[1],
                           ass_transfer= d_charity[3],this_year_inc=income_total[1],last_inc=income_total[2],
                           before_inc=income_total[3],dependents = deps[1],dep_liab=deps[2],mon_net_inc = d_incs[1],
                           mon_exp = d_incs[2], net_tot = d_incs[3])
  
  
  checks_missing[[i]]<-D_additional
  
  rm(income)
  rm(income_supp)
  print(i)
  
}

DD<-do.call("rbind",checks_missing)
DD_addr<-DD %>% group_by(pacer_id) %>% summarise(n = n(),addr = str_c(addr, collapse ="::"))
DD_n<-DD %>% select(-addr) %>% distinct %>% left_join(DD_addr, by = "pacer_id")

#this replaces the columns in the older bk file with the new extracted checks.
for(i in 1:length(checks_missing)){
  if(is.null(checks_missing[[i]])){
    next
  }
  #get the new checked entries
  df<-checks_missing[[i]]
  
  #this section is that in case we get more than one addr per pacer, we need to fix that. 
  #i take both addresses and concat them with a :::
  if(nrow(df)>1){
    addr<-str_c(df$addr, collapse = ":::")
    df$addr[1]<-addr
    df <- df[1,]
  }
  
  #remove the entries that are still NA
  df_n <- df[,colSums(is.na(df))<nrow(df)]
  
  #find the row in the original data that has the same pacer id as the new checked entry
  d_ind<-which(D_bk_pers$bk_id==as.character(df_n$pacer_id))
  
  #get the names from the original data
  names_l<-names(D_bk_pers)
  
  #go entry by entry 
  for(j in 3:length(names(df_n))){
    #find which column corresponds to the current variable of the current entry
    j_col_ind <-which(names_l==names(df_n[j]))
    
    #replace
    D_bk_pers[d_ind,j_col_ind]<-df_n[j][[1]] %>% as.character
  }

}

write_csv(D_bk_pers, 
          str_c("/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/intermediary_files/D_bk_pers_pt_1_",
                str_sub(Sys.time(), 1,10),".csv")
          )

db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet") 
db_port <- 3306



# #connect to server
 mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                    dbname = db_name, host = db_host, port = db_port)

bk_cases_q <- "SELECT * FROM `pacer_filing_data_pers_t`"
rs <- dbSendQuery(mydb, bk_cases_q)
D_old <-  fetch(rs, n = -1)%>%data.frame()

#Now we geolocate the bankruptcy filers so that we can join their cases with census information

D_bk_pers_1<-D_bk_pers %>% mutate(addr_f = addr %>% tolower %>% 
                       str_remove(", number, street, city, state & zip code") %>% 
                       str_remove(", city, zip code") %>% 
                       str_remove(", city, state, zip code") %>% 
                       str_remove(", number, street") %>% 
                       str_remove(", number street")) %>% 
  mutate(
post_zip_long_loc = (str_locate(addr_f, "[0-9]{5}-[0-9]{4},"))[,2],
post_zip_long_short = (str_locate(addr_f, "[0-9]{5},"))[,2],
  
  str_sub_end = case_when(is.na(post_zip_long_loc)&is.na(post_zip_long_short)~nchar(addr_f),
                          is.na(post_zip_long_loc)~post_zip_long_short,
                          is.na(post_zip_long_short)~post_zip_long_loc),
  
  addr_f = str_sub(addr_f,1,str_sub_end)
)
  
inds_to_rm_comm<-which(str_sub(D_bk_pers_1$addr_f, nchar(D_bk_pers_1$addr_f), 
                               nchar(D_bk_pers_1$addr_f))==",")

addr_rep<-D_bk_pers_1[inds_to_rm_comm,] %>%
  mutate(addr_f = str_sub(addr_f, 1, nchar(addr_f)-1))

D_bk_pers_1[inds_to_rm_comm,]$addr_f<-addr_rep$addr_f

geo_lat <- rep(0, length(D_bk_pers_1$addr_f))
geo_lon <- rep(0, length(D_bk_pers_1$addr_f))

for(i in 1:length(D_bk_pers_1$addr_f)){
  geo_v<-geo(address=D_bk_pers_1$addr_f[i]) %>% distinct
  geo_lat[i]<-geo_v$lat
  geo_lon[i]<-geo_v$long
}

D_bk_pers_1$lat <- geo_lat
D_bk_pers_1$lon <- geo_lon

D_to_google<-D_bk_pers_1[which(is.na(D_bk_pers_1$lat)|is.na(D_bk_pers_1$lon)),]

register_google(key = key_get("google_maps"))
locs_missing<-geocode(location = D_to_google$addr_f[20])
D_bk_pers_1[which(is.na(D_bk_pers_1$lat)|is.na(D_bk_pers_1$lon)),]$lat<-locs_missing$lat
D_bk_pers_1[which(is.na(D_bk_pers_1$lat)|is.na(D_bk_pers_1$lon)),]$lon<-locs_missing$lon
D_bk_pers_2<-D_bk_pers_1 %>% select(c(-post_zip_long_loc,-post_zip_long_short, -str_sub_end))

D_bk_pers_f<-rbind(D_bk_pers_2, 
                   D_old[,which(names(D_old)%in%names(D_bk_pers_2))])

write_csv(D_bk_pers_f, "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/cfc_pacer_csv_data/D_bk_pers_curr_2.csv")

##now to join with the metadata
db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet") 
db_port <- 3306

#connect to server
mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

bk_cases_q <- "SELECT * FROM `pacer_monitor_bankruptcy_meta_t`"

rs <- dbSendQuery(mydb, bk_cases_q)
D_just_data_f_sub_f <-  fetch(rs, n = -1)%>%data.frame()

D_bk_pers_tbl<-D_bk_pers_f %>%mutate(bk_id=as.numeric(bk_id)) %>% 
  left_join(
    D_just_data_f_sub_f %>% 
      select(case_ID,case_name, pacer_case_num, filed_date, court_name, case_chapter, type_of_debtor), 
    by = c("bk_id"="case_ID")
    )

dbWriteTable(mydb, "pacer_filing_data_pers_t", D_bk_pers_tbl, row.names=F, append=F, overwrite=T)


############
###TABLES###
############

#Another useful bit of data included in the bankruptcy filings are the actual debts that bankruptcy 
#filers owe. Unfortunately, the actual debt data (which includes the debtor, the amount of the debt, 
#the type of debt, and the creditor) appears on the bk forms in table format. Here we have our method
#for pulling the data out from those tables

pers_files<-list.files("/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/Pacer_vol_pets_pers/")

mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

bk_cases_q <- "SELECT * FROM `pacer_filing_data_pers_t`"

rs <- dbSendQuery(mydb, bk_cases_q)
D_run_already <-  fetch(rs, n = -1)%>%data.frame()

pers_files_2<-pers_files[which(pers_files%in%D_run_already$bk_id==TRUE)]
secured_debt_L<-vector(length = length(pers_files_2), mode = "list")
unsecured_debt_L<-vector(length = length(pers_files_2), mode = "list")

for(i in 1:length(pers_files_2)){
  
  file_l<-list.files(
    str_c(
      "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/Pacer_vol_pets_pers/",
      pers_files_2[i]
    )
  )
  
  ind<-file_l %>% str_detect(., "table_blocks") %>% {which(.)}
  true_ind<-which(str_detect(file_l[ind],"form")==FALSE)
  
  if(length(true_ind)==0){
    next
  }
  
  blocks_name<-str_c(
    "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/Pacer_vol_pets_pers/",
    pers_files_2[i],
    "/",
    file_l[ind][true_ind]
  )
  
  #the block data is the raw amazon parsing data
  analysis_t<-loadRData(blocks_name)
  
  #this is the loop that extract secured debts
  secured_debt_L[[i]]<-tryCatch(get_secured_debt(analysis_t) %>% add_column(bk_id = pers_files_2[i], .before = 1),
                                error = function(e)e)
  
  #this is the loop that extracts unsecured debts
  unsecured_debt_L[[i]]<-tryCatch(get_unsecured_debt(analysis_t)%>% add_column(bk_id = pers_files_2[i], .before = 1),
                                  error = function(e)e)
  
  print(i)
}

#this loop goes through the individual filer's creditor data and joins that into creditor tables
inds_chk<-lapply(secured_debt_L,
                 function(x) return(any(str_detect(tolower(class(x)[1]), "error")))) %>%
  unlist %>% {which(.)}

inds_chk_2<-lapply(unsecured_debt_L, 
                   function(x) return(any(str_detect(tolower(class(x)[1]), "error")))) %>%
  unlist %>% {which(.)}

D_secured <- data.frame()

for(i in 1:length(secured_debt_L)){
  if(i%in%inds_chk){
    secured_debt_L[[i]]<-data.frame(
      bk_id=pers_files_2[i],
      creditor = "none", 
      city="none", 
      state ="none",
      amount_owed = "none",
      collateral = "none",
      unsecured = "none")
  }
  
 D_this<- tryCatch(secured_debt_L[[i]] %>% data.frame %>% mutate(amount_owed = as.character(amount_owed),
                                                collateral = as.character(collateral),
                                                unsecured = as.character(unsecured))
                   , error= function(e)e)
 
 if(any(str_detect(tolower(class(D_this)), "error"))){
   D_this<- tryCatch(secured_debt_L[[i]] %>% data.frame %>% mutate(amount_owed = as.character(amount_owed),
                                                                   collateral = as.character(collateral),
                                                                   unsecured = as.character(unsecure)) %>% 
                       select(-unsecure)
                     )
 }
  
  D_secured<-rbind(D_secured, D_this)
}

D_unsecured <- data.frame()

for(i in 1:length(unsecured_debt_L)){
  
  if(i%in%inds_chk_2){
    unsecured_debt_L[[i]]<-rbind(
      data.frame(
        bk_id=pers_files_2[i],
        creditor = "none", 
        city="none", 
        state ="none", 
        amount_owed = "none",
        type = "priority"
      ),
      data.frame(
        bk_id=pers_files_2[i],
        creditor = "none", 
        city="none", 
        state ="none", 
        amount_owed = "none",
        type = "non-priority"
      )
    )
  }
  
  D_this<- unsecured_debt_L[[i]] %>% data.frame %>% mutate(amount_owed = as.character(amount_owed))
  D_unsecured<-rbind(D_unsecured, D_this)
  
}

write_csv(D_unsecured, 
          path = 
            "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/csv/credit_tables_pers_unsec.csv")

write_csv(D_secured, 
          path = 
            "/Volumes/thieme_ehd/AJC/Covid_financial_crisis/Data/Bankruptcy_data/PACER/csv/credit_tables_pers_sec.csv")


##now to join with the metadata
db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet") 
db_port <- 3306

#connect to server
mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

dbWriteTable(mydb, "pacer_creditors_unsecured", D_unsecured, row.names=F, append=F, overwrite=T)
dbWriteTable(mydb, "pacer_creditors_secured", D_secured, row.names=F, append=F, overwrite=T)

# for our analysis of which debtors owe certain kinds of debt, we needed to determine the NAICS 
# fields for particular creditors. this first reuires standardizing the names of creditors, which
# we do here.
get_cleaned_secured_creditors()
get_cleaned_unsecured_creditors()
