# Covid_financial_crisis

README

Nick Thieme, AJC

Welcome to the code for the Atlanta Journal-Constitution's investigative series Covid Financial Crisis. 

This is the code underlying a number of stories at the AJC that investigated the effects of the COVID pandemic on Georgia's economy and the financial state of Georgians. The aim of this repo and of these code files is to allow other newsrooms and analysts to reproduce an analysis of this sort for other states and regions without rebuilding the low-level infrastructure that makes it possible and to provide guide posts for higher-level statistical analysis.

This work was made possible by grants from The Pulitzer Center and the Brown Institute for Media Innovation at Columbia University, and by relationships with CUSIP Global Services and Pacer Monitor

## Order of operation

1)	Sample_calculation_PACER.R
  a.	Get the sample size you need
  b.	Calculate a rough budget for how to get there
2)	Pacer_monitor_case_metadata.R
  a.	Get the metadata for all bankruptcy cases in a particular region
  b.	Write that metadata out to an SQL table
3)	Pacer_update_code.R
  a.	Use the metadata and the sample to sample a specified number of bk cases.
  b.	Purchase and download the sample of cases from PACER
  c.	Convert the cases to image files and create local and AWS directories with those case files
4)	Pacer_textract_code_pers.R
  a.	Send the bankruptcy PDFs from AWS S3 to AWS Textract for parsing
  b.	Download the parsed lists from AWS to local drive and organize them into the already existing file directory
  c.	Fix parsed lists that didn’t parse correctly (generally image PDFs)
  d.	Get information about creditors and specific debts, clean.
  e.	Upload SQL tables of parsed bankruptcy filings, secured creditor tables, and unsecured creditor tables with joinable IDs
5)	Naices_join.R
  a.	Subset total U.S. companies to geographic region to save computation time
  b.	Use python-dedup function to cluster creditors in the creditor database
  c.	Link bk creditors with U.S. companies
  d.	Upload linked creditors to SQL table created in 4)
6)	Daily_count_inc_story_code.R (story code)
  a.	Use the metadata to create weekly detrended estimates of bankruptcies relative to 2019.
  b.	Upload tidy dataframes to S3 of those detrended estimates for front-end plotting
  c.	This function is called at the end of Pacer_monitor_case_metadata.R automatically
7)	GA_debts.Rmd (story code)
  a.	Analyze debts owed and creditors to whom debts are owed in a variety of ways
    i.	Histograms of types of debt
    ii.	Multiply-corrected t-tests for kinds of debts across demographic classes
    iii.	Dirichlet regression for proportion of debt type owed by race, income, bk type
    iv.	Analysis of mortgage data
    v.	GAM models and ZIP models on amount of debt owed by demographic class
8)	MSRB_data_upload.R
  a.	Calculate liquidity variables from the MSRB data
  b.	Filter MSRB data to issuances that fit with Schwert (2017)
9)	MSRB_credit_rating_scrape.R
  a.	Scrape credit rating for traded bonds from MSRB website 
  b.	Format scraped data
10)	MSRB_scrape_new_issuances
  a.	Scrape new issuances of debt and credit ratings for those debts from the MSRB website
  b.	Format scraped data
11)	MSRB_scrape_sales_type.R
  a.	Scrape sales type of bonds for both traded and scraped bonds
  b.	Format sales type and join with bond data
12)	Bond_analysis.Rmd
  a.	Analyze bond traded and new issuances to understand the relationship between COVID and coupon prices in various ways
    i.	Daily trends
    ii.	Regression discontinuity
    iii.	Various GAMs 
    iv.	Sensitivity analysis and model checking

