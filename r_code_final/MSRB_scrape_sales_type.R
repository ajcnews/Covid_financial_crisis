###################################
###scraping MSRB EMMA sales type###
###################################

# there are a couple different data sources used in the MSRB analysis. For the yield data that is 
#purchased from the MSRB, the easiest way to get the sales type (whether the initial sale was a 
#negotiated sale or a competetive one) is to scrape them from the MSRB. This script does that.
#it doesn't work perfectly because of the waiting associated with it. It breaks every 500 or so 
#scrapes, so this is often best used in concert with beep()

library(tidyverse)
library(RSelenium)
library(RMySQL)
library(keyring)

db_user <- 'nthieme'
db_password <- key_get("sql")
db_name <- 'project_cfc'
db_host <- key_get("sql_intranet")
db_port <- 3306

#waiting funtion
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
        if (length(res[[1]])==1) {
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

mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

D_spread<-dbReadTable(mydb,"msrb_derived_trades_agg_by_month")

cusip_list <- D_spread$cusip %>% unique

D_results_ratings <- data.frame( )
D_results_meta <- data.frame( )

##connect to server
remDr <- rsDriver(
  browser ="firefox", 
  port= 4123L,
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


link_tree <-  remDr_cli$findElements("class","breadCrumb")

links<-link_tree[[1]]$findChildElements("tag name","a")

links[[5]]$clickElement()

initial_card <-  remDr_cli$findElements("class","card-body")

li_s<-initial_card[[2]]$findChildElements("tag name", "li")

results_col_1<- rep(0, length(li_s)-1)
results_col_2<- rep(0, length(li_s)-1)

for(j in 1:(length(li_s)-1)){
  actual_results_2<-li_s[[j]]$findChildElements("tag name", "span")
  
  results_col_1[j]<- actual_results_2[[1]]$getElementAttribute("innerHTML")
  results_col_2[j]<-actual_results_2[[2]]$getElementAttribute("innerHTML")
}

D_meta_cusip<-tibble(cusip= cusip_list[1], results_col_1 %>% unlist, vals = results_col_2 %>% unlist)
D_results_meta<-rbind(D_results_meta, D_meta_cusip)

for(i in 1:length(cusip_list)){
  Sys.sleep(1)
  #click form
  form_select <- remDr_cli$findElements("id","quickSearchText")
  form_select[[1]]$sendKeysToElement(list(cusip_list[i]))
  form_button <- remDr_cli$findElements("id","quickSearchButton")
  form_button[[1]]$clickElement()
  
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
  
  D_meta_cusip<-tibble(cusip= cusip_list[i], results_col_1 %>% unlist, vals = results_col_2 %>% unlist)
  D_results_meta<-rbind(D_results_meta, D_meta_cusip)
  
  print(str_c(i, " of ", length(cusip_list)," completed"))

}
                
names(D_results_meta) <- c("cusip", "names","vals")

#write out the result
D_results_meta_new_f<-D_results_meta%>% distinct %>%  
  mutate(names = str_remove(names,":") %>% str_replace_all(" ","_") %>% tolower) %>% 
  filter(names=="underwriting_spread_amount") %>%
  pivot_wider(names_from = names, values_from = vals, values_fn = {function(x)return(x[1])}) %>% 
  mutate(sale_type = case_when(underwriting_spread_amount=="Not Disclosed - Competitive Sale"~"competetive", 
                               underwriting_spread_amount!="Not Disclosed - Competitive Sale"~"negotiated"
  )
  )

write_csv(D_results_meta,"~/Desktop/credit_spread_data/D_results_meta.csv")




                        