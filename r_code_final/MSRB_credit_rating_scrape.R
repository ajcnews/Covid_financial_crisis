##########################################################
###Scraping credit ratings for already gotten issuances###
##########################################################

# there are a couple different data sources used in the MSRB analysis. For the yield data that is 
#purchased from the MSRB, the easiest way to get credit ratings is to scrape them from the MSRB 
#website. We do that here.

library(tidyverse)
library(RSelenium)
library(RMySQL)
library(keyring)

cusip_list<-read_csv("~/Desktop/credit_spread_data/D_ts_spread.csv") %>% pull(cusip) %>% unique
ratings <- tesseract(options = list(tessedit_char_whitelist = "ABCDEFNR"))
moodys_ratings <- tesseract(options = list(tessedit_char_whitelist = "AaBbC"))
# D_results_ratings <- data.frame( )
# D_results_meta <- data.frame( )

##connect to server
remDr <- rsDriver(
  browser ="firefox", 
  port= 4122L,
  extraCapabilities = makeFirefoxProfile(
    list("browser.helperApps.neverAsk.saveToDisk"="application/octet-stream doc xls pdf txt")
  )
)

remDr_cli <- remDr$client

#go to cusip homepage
remDr_cli$navigate("https://emma.msrb.org/Home/Index")

#click form
form_select <- remDr_cli$findElements("id","quickSearchText")
form_select[[1]]$sendKeysToElement(list(cusip_list[1]))
form_button <- remDr_cli$findElements("id","quickSearchButton")
form_button[[1]]$clickElement()

Sys.sleep(3)

form_button <- remDr_cli$findElements("class","green-button-TOC")
form_button[[1]]$clickElement()

Sys.sleep(3)

form_button <- remDr_cli$findElements("id","acceptId")
form_button[[1]]$clickElement()

form_button <- remDr_cli$findElements("class","closeOverlay")
form_button[[1]]$clickElement()

initial_card <-  remDr_cli$findElements("class","card-body")

li_s<-initial_card[[2]]$findChildElements("tag name", "li")

results_col_1<- rep(0, length(li_s)-1)
results_col_2<- rep(0, length(li_s)-1)

for(j in 1:(length(li_s)-1)){
  actual_results_2<-li_s[[j]]$findChildElements("tag name", "span")
  
  results_col_1[j]<- actual_results_2[[1]]$getElementAttribute("innerHTML")
  results_col_2[j]<-actual_results_2[[2]]$getElementAttribute("innerHTML")
}

D_meta_cusip<-tibble(names = results_col_1 %>% unlist, vals = results_col_2 %>% unlist)

#now we click to the ratings 
tabs_div <- remDr_cli$findElements("id","tabs")
tabs_ul_s<-tabs_div[[1]]$findChildElements("tag name", "ul")
tabs_li_s<-tabs_ul_s[[1]]$findChildElements("tag name", "li") 
tabs_li_s[[2]]$clickElement()

#fitch
fitch_ratings_div <- remDr_cli$findElements("id","fitchRatingsDiv") 
fitch_ratings_elements<-fitch_ratings_div[[1]]$findChildElements("tag name","tr")

rating_image<-fitch_ratings_elements[[1]]$findChildElements("tag name", "img")
long_term_fitch_rating<-ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings) %>% str_remove("\\n")

rating_image<-fitch_ratings_elements[[3]]$findChildElements("tag name", "img")
short_term_fitch_rating<-ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings) %>% str_remove("\\n")

##mooy
moodys_ratings_div <- remDr_cli$findElements("id","moodysDetailsDiv") 
moodys_ratings_elements<-moodys_ratings_div[[1]]$findChildElements("tag name","tr")

rating_image<-moodys_ratings_elements[[1]]$findChildElements("tag name", "img")
long_term_moodys_rating<-ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = moodys_ratings) %>% str_remove("\\n")

rating_image<-moodys_ratings_elements[[4]]$findChildElements("tag name", "img")

short_term_moodys_rating<-tryCatch(
  ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = moodys_ratings) %>% str_remove("\\n"),
  error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)

#snp
snp_ratings_div <- remDr_cli$findElements("id","snpDetailsDiv") 

snp_ratings_elements<-snp_ratings_div[[1]]$findChildElements("tag name","tr")

rating_image<-snp_ratings_elements[[1]]$findChildElements("tag name", "img")
long_term_snp_rating<-ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings) %>% str_remove("\\n")

rating_image<-snp_ratings_elements[[4]]$findChildElements("tag name", "img")

short_term_snp_rating<-tryCatch(
  ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = moodys_ratings) %>% str_remove("\\n"),
  error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)


D_ratings_this_cusip <- tibble(fitch_long = long_term_fitch_rating, fitch_short = short_term_fitch_rating,
                               moody_long = long_term_moodys_rating, moody_short = short_term_moodys_rating,
                               snp_long = long_term_snp_rating, snp_short=short_term_snp_rating)

D_results_meta<-rbind(D_results_meta, D_meta_cusip %>% add_column(cusip = cusip_list[1]))
D_results_ratings<-rbind(D_results_ratings, D_ratings_this_cusip %>% add_column(cusip = cusip_list[1]))

Sys.sleep(3)

for(i in 1:length(cusip_list)){
  #click form
  form_select <- remDr_cli$findElements("id","quickSearchText")
  form_select[[1]]$sendKeysToElement(list(cusip_list[i]))
  form_button <- remDr_cli$findElements("id","quickSearchButton")
  form_button[[1]]$clickElement()
  
  Sys.sleep(5+runif(1, 0,1))
  
  # form_button <- remDr_cli$findElements("id","acceptId")
  # form_button[[1]]$clickElement()
  
  # form_button <- remDr_cli$findElements("class","closeOverlay")
  # form_button[[1]]$clickElement()
  # 
  initial_card <-  remDr_cli$findElements("class","card-body")
  
  li_s<-initial_card[[2]]$findChildElements("tag name", "li")
  
  results_col_1<- rep(0, length(li_s)-1)
  results_col_2<- rep(0, length(li_s)-1)
  
  for(j in 1:(length(li_s)-1)){
    actual_results_2<-li_s[[j]]$findChildElements("tag name", "span")
    
    results_col_1[j]<- tryCatch(actual_results_2[[1]]$getElementAttribute("innerHTML"),
                                error = function(e)e)
    results_col_2[j]<-tryCatch(actual_results_2[[2]]$getElementAttribute("innerHTML"),
                               error = function(e)e)
  }
  
  D_meta_cusip<-tibble(names = results_col_1 %>% unlist, vals = results_col_2 %>% unlist)
  
  #now we click to the ratings 
  tabs_div <- remDr_cli$findElements("id","tabs")
  tabs_ul_s<-tabs_div[[1]]$findChildElements("tag name", "ul")
  tabs_li_s<-tabs_ul_s[[1]]$findChildElements("tag name", "li") 
  tabs_li_s[[2]]$clickElement()
  
  #fitch
  fitch_ratings_div <- remDr_cli$findElements("id","fitchRatingsDiv") 
  
  if(length(fitch_ratings_div)>0){
    fitch_ratings_elements<-fitch_ratings_div[[1]]$findChildElements("tag name","tr")
    
    rating_image<-fitch_ratings_elements[[1]]$findChildElements("tag name", "img")
    long_term_fitch_rating<-tryCatch(
      ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings) %>% str_remove("\\n"),
      error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)
    
    rating_image<-fitch_ratings_elements[[3]]$findChildElements("tag name", "img")
    short_term_fitch_rating<-tryCatch(
      ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings) %>% str_remove("\\n"),
      error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)
  }else{
    long_term_fitch_rating<-"none"
    short_term_fitch_rating<-"none"
  }
  
  ##moody
  moodys_ratings_div <- remDr_cli$findElements("id","moodysDetailsDiv") 
  
  if(length(moodys_ratings_div)>0){
    moodys_ratings_elements<-moodys_ratings_div[[1]]$findChildElements("tag name","tr")
    
    rating_image<-moodys_ratings_elements[[1]]$findChildElements("tag name", "img")
    
    long_term_moodys_rating<-tryCatch(
      ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = moodys_ratings) %>% str_remove("\\n"),
      error = function(e)e)%>% ifelse(class(.)[1]=="simpleError", "none",.)
    
    rating_image<-moodys_ratings_elements[[4]]$findChildElements("tag name", "img")
    
    short_term_moodys_rating<-tryCatch(
      ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = moodys_ratings) %>% str_remove("\\n"),
      error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)
  }else{
    long_term_moodys_rating<-"none"
    short_term_moodys_rating<-"none"
  }
  
  
  #snp
  snp_ratings_div <- remDr_cli$findElements("id","snpDetailsDiv") 
  
  if(length(snp_ratings_div)>0){
    snp_ratings_elements<-snp_ratings_div[[1]]$findChildElements("tag name","tr")
    
    rating_image<-snp_ratings_elements[[1]]$findChildElements("tag name", "img")
    long_term_snp_rating<-tryCatch(
      ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings) %>% str_remove("\\n"),
      error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)

    
    rating_image<-snp_ratings_elements[[4]]$findChildElements("tag name", "img")
    
    short_term_snp_rating<-tryCatch(
      ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = moodys_ratings) %>% str_remove("\\n"),
      error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)
    
  }else{
    long_term_snp_rating<-"none"
    short_term_snp_rating<-"none"
  }
  
  D_ratings_this_cusip <- tibble(fitch_long = long_term_fitch_rating, fitch_short = short_term_fitch_rating,
                                 moody_long = long_term_moodys_rating, moody_short = short_term_moodys_rating,
                                 snp_long = long_term_snp_rating, snp_short=short_term_snp_rating)
  
  D_results_meta<-rbind(D_results_meta, D_meta_cusip %>% add_column(cusip = cusip_list[i]))
  D_results_ratings<-rbind(D_results_ratings, D_ratings_this_cusip %>% add_column(cusip = cusip_list[i]))
  print(str_c(i, " of ", length(cusip_list)," completed"))
}
  


