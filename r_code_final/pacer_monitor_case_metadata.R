###################################
####PACER Monitor case metadata#### 
###################################

#We rely on PACER Monitor to get the metadata for bankruptcy cases. This is an essential step
#because it allows us to build a population of bankrupcty cases from which we will draw our sample
#there are likely many ways to go about this. 

#The important part is finding a way to build a population of bankruptcy cases

library(tidyverse)
library(httr)
library(jsonlite)
library(lubridate)
library(RMySQL)
library(keyring)

#this is a function that creates a dataset from the pacer monitor metadata
get_D <- function(x){
  
  if(is_null(nrow(x))){
    new_y<-repair_names(x)
    new_y_2<-lapply(new_y, select_variables)
    
    ret_D<-do.call("rbind",new_y_2)
  }else{
    new_x<-repair_names(x)
    new_x_2<-select_variables(new_x)
    #new_x_2<-new_x %>% rename(court_name=court_name.courtName)
    ret_D<-new_x_2
  }
  
  return(as.data.frame(ret_D))
}

#an auxiliary function for the above
select_variables <- function(D){
  d_names<-names(D) %>% tolower
  
  pacer_case_num_ind_1<-str_detect(d_names, "casenumber") %>% {which(.)}
  pacer_case_num_ind_f<-d_names[pacer_case_num_ind_1] %>% str_detect(.,"pacer") %>% {which(.)}
  pacer_case_ind<-pacer_case_num_ind_1[pacer_case_num_ind_f]
  
  case_name<-str_detect(d_names, "casename") %>% {which(.)}
  
  case_date<-str_detect(d_names, "casefileddate") %>% {which(.)}
  
  case_court<-str_detect(d_names, "court") %>% {which(.)}
  
  case_id<-str_detect(d_names, "caseid") %>% {which(.)}
  
  maincase<-str_detect(d_names, "maincase") %>% {which(.)}
  
  case_chapter<-str_detect(d_names, "casechapter") %>% {which(.)}
  
  case_attributes<-str_detect(d_names, "attributes") %>% {which(.)}
  
  #get court info
  court_ind<-apply(as.matrix(D[,case_court]),2, function(x)return(any(str_detect(x, "Court")))) %>% 
    {which(.)}
  
  if(is_null(ncol(D[,case_court]))){
    court_col<- D[,case_court]
  }else{
    court_col <- D[,case_court][,court_ind] %>% unlist
  }
  
  #main case info
  main_case_poss=D[,maincase]
  
  if(length(maincase)==0){
    main_case_ID_col<-rep(NA, nrow(D))
  }else{
    if(is.null(ncol(main_case_poss))){
      main_case_ID_col<-D[,maincase]
    }else{
      main_case_ID_col<-D[,maincase][,1]
    }
  }
  
  
  
  tod_ind<-apply(D[,case_attributes], 2, function(x)return(any(x=="I"))) %>% {which(.)}
  tod_col <- D[,case_attributes][,tod_ind]
  
  D_int<-D[,c(pacer_case_ind,case_name,case_date,case_id,case_chapter,case_name)] %>% data.frame %>% 
    add_column(court = court_col, maincase_id = main_case_ID_col, tod = tod_col)
  
  D_ret<-D_int%>% 
    mutate(X_embedded.caseList.bankruptcyAttributes.estimatedAssets = NA,
           X_embedded.caseList.bankruptcyAttributes.estimatedLiabilities = NA,
           X_embedded.caseList.bankruptcyAttributes.numberOfCreditors=NA,
           X_embedded.caseList.bankruptcyAttributes.smallBusiness= NA,
           X_embedded.caseList.bankruptcyAttributes.natureOfBusiness=NA) %>% 
    dplyr::select(
      pacer_case_num=X_embedded.caseList.pacerFormatCaseNumber,
      case_name=X_embedded.caseList.caseName,
      filed_date=X_embedded.caseList.caseFiledDate,
      court_name=court,
      case_ID=X_embedded.caseList.caseId,
      main_case_id=maincase_id,
      case_chapter=X_embedded.caseList.caseChapter,
      estimated_assets=X_embedded.caseList.bankruptcyAttributes.estimatedAssets,
      estimated_liabilities=X_embedded.caseList.bankruptcyAttributes.estimatedLiabilities,
      nature_of_business=X_embedded.caseList.bankruptcyAttributes.natureOfBusiness,
      num_creditors=X_embedded.caseList.bankruptcyAttributes.numberOfCreditors,
      small_business=X_embedded.caseList.bankruptcyAttributes.smallBusiness,
      type_of_debtor=tod
    ) %>% distinct
  
  return(D_ret)
}

#helper function for the above
repair_names<-function(D_x){
  D_new <- D_x
  names_list<-lapply(D_x, names) %>% unlist %>% unique
  
  if(is_null(ncol(D_x))){
    for(i in 1:length(D_x)){
      D_curr<-D_x[[i]]
      ind_to_add<-which(names_list%in%names(D_curr)==FALSE)
      
      if(length(ind_to_add)>0){
        names_to_add<-names_list[ind_to_add]
        D_to_add<-matrix(NA, ncol = length(names_to_add), nrow = nrow(D_curr)) %>% data.frame
        names(D_to_add)<-names_to_add
        D_curr_f <- D_curr %>% cbind(D_to_add) %>% dplyr::select(order(colnames(.)))
      }else{
        D_curr_f<-D_curr %>% dplyr::select(order(colnames(.)))
      }
      
      D_new[[i]]<-D_curr_f
      
    }
  }else{
    ind_to_add<-which(names_list%in%names(D_x)==FALSE)
    
    if(length(ind_to_add)>0){
      names_to_add<-names_list[ind_to_add]
      D_to_add<-matrix(NA, ncol = length(names_to_add), nrow = nrow(D_x)) %>% data.frame
      names(D_to_add)<-names_to_add
      D_curr_f <- D_x %>% cbind(D_to_add) %>% dplyr::select(order(colnames(.)))
    }else{
      D_curr_f<-D_x %>% dplyr::select(order(colnames(.)))
    }
    D_new<-D_curr_f
  }
  
  
  return(D_new)
}

##This is where we start the process of pulling data from Pacer Monitor

##pull server data
db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet")
db_port <- 3306

#connect to server
mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

#first, we pull out already existing metadata from our server. This should be omitted on the first run

bk_cases_q <- "SELECT * FROM `pacer_monitor_bankruptcy_meta_t`"
rs <- dbSendQuery(mydb, bk_cases_q)

D_just_data_f_sub_f <-  fetch(rs, n = -1)%>%data.frame() %>% mutate(filed_date=str_sub(filed_date, 1,10) %>% ymd)
last_date<-D_just_data_f_sub_f$filed_date%>% max
last_date <- ymd(last_date)

###api key for Pacer Monitor
user<-"nicholas.thieme@ajc.com"
api_key<-"UYmxEPAjMycRiWNndgkwstwZfhRlgGzX"
base_url<- "https://api.pacermonitor.com/v3/"

pacer_auth<-POST(url = str_c(base_url,"auth/login"), 
                 query = list(username = user, password=api_key),
                 encode="json")

auth_token<-httr::content(pacer_auth)[[1]]

#number of pages total 
D_list_tot<-vector(mode="list", length = 2000)
start_date<-ymd(last_date)-1
end_date<-ymd(Sys.time() %>% str_sub(1,10))-1
time_length<-end_date-start_date

#This loop goes through all the days between the last metadata file downloaded and the current date.
#It collects the metadata for all bankruptcy cases in GA between those dates.

for(i in 1:time_length){
  
  case_query<-POST(url = str_c(base_url,"cases/search"), 
                   query = list(caseType = "bk", courtCode="ganb",courtCode="gasb",courtCode="gamb",
                                filedFromDate=as.character(start_date+i-1),
                                filedToDate=as.character(start_date+i),
                                size =250,
                                page = 0),
                   encode="json",
                   add_headers(Authorization=auth_token),
                   verbose())
  
  a<-rawToChar(as.raw(strtoi(case_query$content, 16L)))
  D_nb<-fromJSON(a) %>% data.frame
  total_pages<-D_nb$page.totalPages %>% unique
  
  if(total_pages>1){
    
    D_list_tot_mini<-vector(mode="list", length = total_pages-1)
    
    for(j in 2:total_pages){
      
      case_query<-POST(url = str_c(base_url,"cases/search"), 
                       query = list(caseType = "bk", courtCode="ganb",courtCode="gasb",courtCode="gamb",
                                    filedFromDate=as.character(start_date+i-1),
                                    filedToDate=as.character(start_date+i),
                                    size =250,
                                    page=j-1),
                       encode="json",
                       add_headers(Authorization=auth_token),
                       verbose())
      
      a<-rawToChar(as.raw(strtoi(case_query$content, 16L)))
      D_nb_mini<-fromJSON(a) %>% data.frame
      D_list_tot_mini[[j-1]]<-D_nb_mini
    }
    
    D_nb<-list(D_nb, D_list_tot_mini)
  }
  
  if(is_null(nrow(D_nb))){
    
  }else{
    if(nrow(D_nb)==1){
      print(str_c("didn't finish ", i))
      #break
    }
  }
  
  D_list_tot[[i]]<-D_nb
  
  Sys.sleep(1)
}

file_n<-str_c("D_list_PACER_meta",start_date %>% str_sub(5,10) %>% str_replace_all("-","_"),
              end_date %>% str_sub(5,10) %>% str_replace_all("-","_"))

saveRDS(D_list_tot, file = file_n)

#load back in
D_tot_bank_ga<-readRDS(file_n)
keep_ind_l<-lapply(D_tot_bank_ga, length) %>% {which(.>0)}
D_tot_bank_ga_c<-D_tot_bank_ga[keep_ind_l]

#now we format the collected data. we do this once for the days where there are more than 250 
#and once for the days with less than 250

#days with more than 250
lapply_inds<-lapply(D_tot_bank_ga_c, function(x)return(is_null(ncol(x)))) %>% unlist %>% 
  {which(.)}

D_just_data_lap_i<-lapply(D_tot_bank_ga_c[lapply_inds], function(y)return(lapply(y, get_D)))
D_just_data_f_lap_i<-lapply(D_just_data_lap_i, function(y)return(lapply(y,flatten)))
D_just_data_f_2_lap_i<-lapply(D_just_data_f_lap_i, function(y)return(do.call("rbind",y)))
D_just_data_f_3_lap_i <- do.call("rbind", D_just_data_f_2_lap_i)

#days with less than 250

if(length(lapply_inds)==0){
  D_just_data_lap_ni<-lapply(D_tot_bank_ga_c,get_D)
}else{
  D_just_data_lap_ni<-lapply(D_tot_bank_ga_c[-lapply_inds],get_D)
}

D_just_data_f_lap_ni<-lapply(D_just_data_lap_ni, flatten)
D_just_data_f_3_lap_ni<- do.call("rbind", D_just_data_f_lap_ni)

#now we put them together
if(exists("D_just_data_f_3_lap_i")==FALSE){
  D_just_data_f_true <- rbind(D_just_data_f_3_lap_ni)
}else{
  D_just_data_f_true <- rbind(D_just_data_f_3_lap_i,D_just_data_f_3_lap_ni)
}

write_csv(D_just_data_f_true, str_c("D_ga_metadata_",file_n %>% str_remove("D_list_PACER_meta_"), ".csv"))

write.table(str_c("D_ga_metadata_",file_n %>% str_remove("D_list_PACER_meta_"), ".csv"),"last_date.txt")

D_full_prev<-D_just_data_f_sub_f

D_new_curr <- rbind(D_full_prev,D_just_data_f_true) %>% distinct

##account for case ids that are related 
case_ids_related<-D_new_curr %>% pull(main_case_id)

case_ID_to_filter<-D_new_curr[which(case_ids_related%in%D_new_curr$case_ID),] %>%
  filter(case_ID!=main_case_id) %>% pull(case_ID)

D_new_curr_f<-D_new_curr %>% filter(case_ID%in%case_ID_to_filter==FALSE) %>% distinct
write_csv(D_new_curr_f, "D_ga_metadata_f_curr.csv")

###pushing data to the server

db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet") 
db_port <- 3306

mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

#we then write that metadata to our server
dbWriteTable(mydb, "pacer_monitor_bankruptcy_meta_t", D_new_curr_f, row.names=F, append=F, overwrite=T)

#We also find this to be a convenient time to update the weekly (Even though it says daily) 
#bankruptcy charts.

source("~/Desktop/Covid_financial_crisis/r_code/daily_count_inc_story_code.R")
