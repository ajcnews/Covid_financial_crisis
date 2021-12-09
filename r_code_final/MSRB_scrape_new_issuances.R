#########################################
###MSRB scrape new issuances from EMMA###
#########################################

#One thing the MSRB doesn't provide us is the data on new issuances. We can just scrape it from their 
#website though. Which we do here.

###scraping MSRB EMMA
library(tidyverse)
library(RSelenium)
library(RMySQL)
library(keyring)
library(tesseract)

db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet")
db_port <- 3306

#waiting funtion. don't want to send too many requests and we want to let the page load beforehand
#so we use a waiting function in our scraping that checks for the prescence of page elements
#before it goes ahead to scrape. It works 2000 out of 2001 times (rough numbers) times, so just be
#around in case it fails (beep() is a useful function here)

waitFor <- function(kind,target, source=driver, timeout=5) {
  nChecks <- 2 * timeout  # repeats a check twice per second
  oneWaitDur <- timeout / nChecks
  
  targetFun <- if (is.function(target)) target else 
    # getEls is my function for getting elements by query
    if (is.character(target)) function() sapply(target, function(x) source$findElements(kind,target)) else
      if (is.call(target)) function() eval(target) else
        stop(sprintf('Not implemented for target class [%s]', class(target)))
  
  for (i in 1:nChecks) {
    res <- suppressWarnings(targetFun())
    if (is.list(res)) {
      if (length(target) == 1) {
        if (length(res)) {
          return(invisible(if (length(res) > 1) res else res[[1]]))
        }
      } else {
        if (length(Filter(length, res)) == 1) {
          res <- Filter(length, res)[[1]]
          return(invisible(if (length(res) > 1) res else res[[1]]))
        }
      }
    } else if (is.logical(res) && res) {
      return(res)
    }
    Sys.sleep(oneWaitDur)
  }
  driver$screenshot(T)
  stop('Could not wait')
}

D_results_ratings <- data.frame( )
D_results_meta_tot <- data.frame( )

##connect to server
remDr <- rsDriver(
  browser ="firefox", 
  port= 4116L,
  extraCapabilities = makeFirefoxProfile(
    list("browser.helperApps.neverAsk.saveToDisk"="application/octet-stream doc xls pdf txt")
  )
)

remDr_cli <- remDr$client

#go to cusip homepage
remDr_cli$navigate("https://emma.msrb.org/Home/Index")

#click form
form_select <- remDr_cli$findElements("class","advSearch")
adv_search_button<-form_select[[1]]$findChildElement("tag name","a")
adv_search_button$clickElement()

accept <- remDr_cli$findElements("id","acceptId")
accept[[1]]$clickElement()

form_button <- remDr_cli$findElements("class","green-button-TOC")
form_button[[1]]$clickElement()

#check GA
input_state<-remDr_cli$findElements("class","fields")
items<-input_state[[1]]$findChildElements("tag name","li")
state_button<-items[[1]]$findChildElements("tag name", "button")
state_button[[1]]$clickElement()
checkboxes<-remDr_cli$findElements("class","ui-multiselect-checkboxes")
actual_checks<-checkboxes[[1]]$findChildElements("tag name", "li") 
actual_checks[[11]]$clickElement()

##add date
dated_date_from<-remDr_cli$findElements("id","datedDateFrom")
dated_date_from[[1]]$clickElement()
dated_date_from[[1]]$sendKeysToElement(list("10/01/2018"))
dated_date_to<-remDr_cli$findElements("id","datedDateTo")
dated_date_to[[1]]$clickElement()
dated_date_to[[1]]$sendKeysToElement(list("10/01/2021"))

##set a minimum interest
interestfrom<-remDr_cli$findElements("id","interestRateFrom")
interestfrom[[1]]$clickElement()
interestfrom[[1]]$sendKeysToElement(list("0"))
interestto<-remDr_cli$findElements("id","interestRateTo")
interestto[[1]]$clickElement()
interestto[[1]]$sendKeysToElement(list("20"))

##rate type
rate_type<-remDr_cli$findElements("id","rateTypeDropdownList")
rate_type[[1]]$clickElement()
webElem1 <- rate_type[[1]]$findElements(using = 'xpath', value = "//option[@value='F']")
webElem1[[1]]$clickElement()

#search
search_button<-remDr_cli$findElements("id","runSearchButton")
search_button[[1]]$clickElement()

##
num_results<-remDr_cli$findElements("class","valid")
webElem1 <- remDr_cli$findElement(using = 'xpath', value = "//option[@value='50']")
webElem1$clickElement()

# D_results_meta_tot<-data.frame()
# D_sales_total <- data.frame()
#D_desc <- data.frame()

ratings <- tesseract()
ratings_a <- tesseract(options = list(tessedit_char_whitelist = "ABCDEFNR"))
moodys_ratings <- tesseract(options = list(tessedit_char_whitelist = "AaBbC"))
D_results_ratings<-data.frame()

#we split the scraping into even and odd elements because that's the page structure

for(k in 1:##insert number of pages here){

  odd_results <-  remDr_cli$findElements("class","odd")
  even_results <-  remDr_cli$findElements("class","even")
  
  #odds
  for(i in 1:length(odd_results)){
    
    meta_info  <- odd_results[[i]]$findChildElements("tag name","td")
    meta_coup<-meta_info[[3]]$getElementText()[[1]] %>% str_c(" %")
    meta_maturity <- meta_info[[4]]$getElementText()[[1]]
    meta_princ <-meta_info[[5]]$getElementText()[[1]] %>% str_c("$",.)
    meta_dated <- meta_info[[6]]$getElementText()[[1]]
    
    cusip_link<-odd_results[[i]]$findChildElement("tag name","a")
    link_to_follow<-cusip_link$getElementAttribute("href")
    
    currWindow <-  remDr_cli$getCurrentWindowHandle()
    remDr_cli$executeScript('window.open("", "windowName", "height=1000,width=800");')
    Sys.sleep(1)
    windows <- remDr_cli$getWindowHandles()
    new_window<-which(unlist(windows)!=currWindow[[1]])
    remDr_cli$switchToWindow(windows[[new_window]])
    remDr_cli$navigate(link_to_follow[[1]])
    
    ##do the scraping
    
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
        ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings_a) %>% str_remove("\\n"),
        error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)
      
      rating_image<-fitch_ratings_elements[[3]]$findChildElements("tag name", "img")
      short_term_fitch_rating<-tryCatch(
        ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings_a) %>% str_remove("\\n"),
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
        ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings_a) %>% str_remove("\\n"),
        error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)
      
      
      rating_image<-snp_ratings_elements[[4]]$findChildElements("tag name", "img")
      
      short_term_snp_rating<-tryCatch(
        ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = moodys_ratings_a) %>% str_remove("\\n"),
        error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)
      
    }else{
      long_term_snp_rating<-"none"
      short_term_snp_rating<-"none"
    }
    
    header<-remDr_cli$findElement("id","sdHeader")
    cusip_label<-header$findChildElement("class","cusipImg")
    
    cusip<-ocr(cusip_label$getElementAttribute("src")[[1]], engine = ratings) %>%
      str_remove("\\n")
    
    D_ratings_this_cusip <- tibble(fitch_long = long_term_fitch_rating, fitch_short = short_term_fitch_rating,
                                   moody_long = long_term_moodys_rating, moody_short = short_term_moodys_rating,
                                   snp_long = long_term_snp_rating, snp_short=short_term_snp_rating)
    
    D_results_ratings<-rbind(D_results_ratings, D_ratings_this_cusip %>% 
                               add_column( coup = meta_coup,
                                          maturity = meta_maturity,
                                          princ = meta_princ,
                                          dated= meta_dated,
                                          cusip = cusip
                                          )
    
    )
    
    initial_card<-waitFor("class", "card-body", remDr_cli, 5)

    li_s<-initial_card[2,][[1]]$findChildElements("tag name", "li")

    results_col_1<- rep(0, length(li_s)-1)
    results_col_2<- rep(0, length(li_s)-1)

    for(j in 1:(length(li_s)-1)){
      actual_results_2<-li_s[[j]]$findChildElements("tag name", "span")

      results_col_1[j]<- actual_results_2[[1]]$getElementAttribute("innerHTML")
      results_col_2[j]<-actual_results_2[[2]]$getElementAttribute("innerHTML")
    }

    header<-remDr_cli$findElement("id","sdHeader")
    cusip_label<-header$findChildElement("class","cusipImg")

    cusip<-ocr(cusip_label$getElementAttribute("src")[[1]], engine = ratings) %>%
      str_remove("\\n")

    D_meta_cusip_tot<-tibble(cusip= cusip, names=results_col_1 %>% unlist, vals = results_col_2 %>% unlist)
    D_results_meta_tot<-rbind(D_results_meta_tot, D_meta_cusip_tot)

    D_results_meta_tot_p<-D_results_meta_tot %>% distinct %>%
      pivot_wider(names_from = names, values_from = vals)
    ##now get the issuer data

    link_tree<-waitFor("class", "breadCrumb", remDr_cli, 5)

    links<-link_tree$findChildElements("tag name","a")

    click<-tryCatch(links[[5]]$clickElement(), error = function(e)e)

    if(is.null(click)){

    }else{
      Sys.sleep(2)

      click_2<-tryCatch(links[[5]]$clickElement(), error = function(e)e)

      if(is.null(click_2)){
      }else{
        print("quit by click")
        remDr_cli$closeWindow()
        remDr_cli$switchToWindow(currWindow[[1]])
        next
      }
    }

    initial_card<-waitFor("class", "card-body", remDr_cli, 5)

    li_s<-initial_card[2,][[1]]$findChildElements("tag name", "li")

    results_col_1<- rep(0, length(li_s)-1)
    results_col_2<- rep(0, length(li_s)-1)

    for(j in 1:(length(li_s)-1)){
      actual_results_2<-li_s[[j]]$findChildElements("tag name", "span")

      results_col_1[j]<- actual_results_2[[1]]$getElementAttribute("innerHTML")
      results_col_2[j]<-actual_results_2[[2]]$getElementAttribute("innerHTML")
    }

    D_sales_type_new<-tibble(cusip= cusip, results_col_1 %>% unlist, vals = results_col_2 %>% unlist)
    D_sales_total<-rbind(D_sales_total, D_sales_type_new)
    ###
    remDr_cli$closeWindow()
    remDr_cli$switchToWindow(currWindow[[1]])
    
    print(str_c(i, " of 25 odds completed"))
  }
  
  #evens
  for(i in 2:length(even_results)){
    cusip_link<-even_results[[i]]$findChildElement("tag name","a")
    
    meta_info  <- even_results[[i]]$findChildElements("tag name","td")
    meta_coup<-meta_info[[3]]$getElementText()[[1]] %>% str_c(" %")
    meta_maturity <- meta_info[[4]]$getElementText()[[1]]
    meta_princ <-meta_info[[5]]$getElementText()[[1]] %>% str_c("$",.)
    meta_dated <- meta_info[[6]]$getElementText()[[1]]
    
    # filt<-D_results_meta_tot_p %>% 
    #   filter(`Coupon:`==meta_coup,`Maturity Date:`==meta_maturity,
    #          `Principal Amount at Issuance:`==meta_princ,
    #          `Dated Date:`==meta_dated)
    # 
    # if(nrow(filt)>0){
    #   print("already have")
    #   
    #   D_desc_curr<-tibble(cusip=filt$cusip[1],
    #                       desc= meta_info[[2]]$getElementText()[[1]]
    #   )
    #   
    #   D_desc<-rbind(D_desc_curr, D_desc)
    #   next
    # }else{
    #   "don't have"
    # }
    
    
    link_to_follow<-cusip_link$getElementAttribute("href")
    
    currWindow <-  remDr_cli$getCurrentWindowHandle()
    remDr_cli$executeScript('window.open("", "windowName", "height=1000,width=800");')
    Sys.sleep(1)
    windows <- remDr_cli$getWindowHandles()
    new_window<-which(unlist(windows)!=currWindow[[1]])
    remDr_cli$switchToWindow(windows[[new_window]])
    remDr_cli$navigate(link_to_follow[[1]])
    

    ##do more scraping
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
        ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings_a) %>% str_remove("\\n"),
        error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)
      
      rating_image<-fitch_ratings_elements[[3]]$findChildElements("tag name", "img")
      short_term_fitch_rating<-tryCatch(
        ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings_a) %>% str_remove("\\n"),
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
        ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = ratings_a) %>% str_remove("\\n"),
        error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)
      
      
      rating_image<-snp_ratings_elements[[4]]$findChildElements("tag name", "img")
      
      short_term_snp_rating<-tryCatch(
        ocr(rating_image[[1]]$getElementAttribute("src")[[1]], engine = moodys_ratings_a) %>% str_remove("\\n"),
        error = function(e)e) %>% ifelse(class(.)[1]=="simpleError", "none",.)
      
    }else{
      long_term_snp_rating<-"none"
      short_term_snp_rating<-"none"
    }
    
    header<-remDr_cli$findElement("id","sdHeader")
    cusip_label<-header$findChildElement("class","cusipImg")
    
    cusip<-ocr(cusip_label$getElementAttribute("src")[[1]], engine = ratings) %>%
      str_remove("\\n")
    
    D_ratings_this_cusip <- tibble(fitch_long = long_term_fitch_rating, fitch_short = short_term_fitch_rating,
                                   moody_long = long_term_moodys_rating, moody_short = short_term_moodys_rating,
                                   snp_long = long_term_snp_rating, snp_short=short_term_snp_rating)
    
    D_results_ratings<-rbind(D_results_ratings, D_ratings_this_cusip%>% 
      add_column( coup = meta_coup,
                  maturity = meta_maturity,
                  princ = meta_princ,
                  dated= meta_dated,
                  cusip = cusip
      )
    )
    initial_card<-waitFor("class", "card-body", remDr_cli, 5)

    li_s<-initial_card[2,][[1]]$findChildElements("tag name", "li")

    results_col_1<- rep(0, length(li_s)-1)
    results_col_2<- rep(0, length(li_s)-1)

    for(j in 1:(length(li_s)-1)){
      actual_results_2<-li_s[[j]]$findChildElements("tag name", "span")

      results_col_1[j]<- actual_results_2[[1]]$getElementAttribute("innerHTML")
      results_col_2[j]<-actual_results_2[[2]]$getElementAttribute("innerHTML")
    }

    header<-remDr_cli$findElement("id","sdHeader")
    cusip_label<-header$findChildElement("class","cusipImg")

    cusip<-ocr(cusip_label$getElementAttribute("src")[[1]], engine = ratings) %>%
      str_remove("\\n")

    D_meta_cusip_tot<-tibble(cusip= cusip, names=results_col_1 %>% unlist, vals = results_col_2 %>% unlist)
    D_results_meta_tot<-rbind(D_results_meta_tot, D_meta_cusip_tot)

    D_results_meta_tot_p<-D_results_meta_tot %>% distinct %>%
      pivot_wider(names_from = names, values_from = vals)

    ##now get the issuer data

    link_tree<-waitFor("class", "breadCrumb", remDr_cli, 5)

    links<-link_tree$findChildElements("tag name","a")

    click<-tryCatch(links[[5]]$clickElement(), error = function(e)e)

    if(is.null(click)){

    }else{
      Sys.sleep(2)

      click_2<-tryCatch(links[[5]]$clickElement(), error = function(e)e)

      if(is.null(click_2)){
      }else{
        print("quit by click")
        remDr_cli$closeWindow()
        remDr_cli$switchToWindow(currWindow[[1]])
        next
      }
    }

    initial_card<-waitFor("class", "card-body", remDr_cli, 5)

    li_s<-initial_card[2,][[1]]$findChildElements("tag name", "li")

    results_col_1<- rep(0, length(li_s)-1)
    results_col_2<- rep(0, length(li_s)-1)

    for(j in 1:(length(li_s)-1)){
      actual_results_2<-li_s[[j]]$findChildElements("tag name", "span")

      results_col_1[j]<- actual_results_2[[1]]$getElementAttribute("innerHTML")
      results_col_2[j]<-actual_results_2[[2]]$getElementAttribute("innerHTML")
    }

    D_sales_type_new<-tibble(cusip= cusip, results_col_1 %>% unlist, vals = results_col_2 %>% unlist)
    D_sales_total<-rbind(D_sales_total, D_sales_type_new)
    ####
    remDr_cli$closeWindow()
    remDr_cli$switchToWindow(currWindow[[1]])
    
    print(str_c(i, " of 25 even completed.","on page ",k," out of 79"))
  }
  
  next_button<-remDr_cli$findElement("id","lvSecurities_next")
  next_button$clickElement()
  
}

D_sales_total_f<-D_sales_total %>%distinct %>%  pivot_wider(names_from =`results_col_1 %>% unlist`, values_from = vals ) %>% 
  mutate(sales_type = case_when(`Underwriting Spread Amount:`=="Disclosed in Official Statement"~"negotiated",
                                `Underwriting Spread Amount:`!="Disclosed in Official Statement"~"competetive")
         ) %>% select(cusip, sales_type)                                


D_full_issuers<-D_desc %>% left_join(D_results_meta_tot_p) %>% left_join(D_sales_total_f)

write_csv(D_full_issuers, "scraped_new_issuances.csv")
