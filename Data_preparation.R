#############################################
# OpenSea Transactions Data Analysis
# data from 2017-06-23 until 2022-07-14
# 99 NFT collections
# ETH-USD price from Etherscan
# collection slug is unique id for collection
# Repeat Sales Index refers to Base day
# Script takes some time to load due to large data
#############################################


install.packages("plyr")
install.packages("dplyr")
install.packages("readr")
install.packages("psych")
install.packages("xtable")
install.packages("ggplot2")
install.packages("stargazer")
install.packages("tidyquant")
install.packages("timetk")
install.packages("imputeTS")

library("psych")
library("plyr")
library("dplyr")
library("readr")
library("xtable")
library("ggplot2")
library(stargazer)
library(tidyquant)
library(timetk)
library(imputeTS)


# Clear variables in Workspace
rm(list = ls())


#############################################
# Load and prepare OpenSea data
#############################################


# Load 99 CSVs and bind rows to one dataset
raw_data <-
  list.files(path = "~/R-Code/OpenSea_sales_analysis/data_05_07_2022",
             pattern = "*.csv",
             full.names = TRUE) %>%
  lapply(read_csv) %>%
  bind_rows

# add up-to-date data
raw_data2 <-
  list.files(path = "~/R-Code/OpenSea_sales_analysis/data_15-07-2022",
             pattern = "*.csv",
             full.names = TRUE) %>%
  lapply(read_csv) %>%
  bind_rows

# append dfs
raw_data <- rbind(raw_data, raw_data2)

#remove duplicate rows from data frame
raw_data <- raw_data %>% distinct(.keep_all = TRUE)

# drop observations with missing identifier information
raw_data <-
  subset(raw_data,!is.na(coll_contract_address) & !is.na(nft_id))

# print number of null values per variable
#colSums(is.na(raw_data))

# construct unique ID variable from col_slug and nft_id
raw_data$nft_unique_id <-
  do.call(paste, c(raw_data[c("collection_slug", "nft_id")]))

# only take transactions until 14th of July 2022
raw_data <- raw_data %>% filter(raw_data$event_date <= "2022-07-14")


#############################################
# Import Ether Daily Price (USD) from Etherscan and join with OpenSea data
#############################################


# Load data
eth_usd_price <-
  read_csv(file = "~/R-Code/OpenSea_sales_analysis/ETH_USD_Price.csv")

# Change date format
eth_usd_price$event_date <-
  format(as.Date(eth_usd_price$`Date(UTC)`, format = "%m/%d/%Y"),
         "%Y-%m-%d")
eth_usd_price$event_date <-
  as.Date(eth_usd_price$event_date, "%Y-%m-%d")

# Delete unnecessary column
eth_usd_price <- subset(eth_usd_price, select = -c(1, 2))

# join two tables: ETH and USD
new_df <- left_join(raw_data, eth_usd_price, by = "event_date")

# compute USD price
new_df <-
  new_df %>% mutate(price_usd = new_df$price_eth * new_df$Value)

# drop transactions with USD price < 1
new_df <- subset(new_df, price_usd >= 1)


#############################################
# Analyse data
#############################################


attach(new_df)

# summarize data by grouping transactions by collection
summary <-
  describeBy(new_df$price_usd,
             group = collection_slug,
             mat = TRUE,
             skew = FALSE)
attach(summary)

# number of unique NFTs per collection (that had transactions)
unique_nfts <-
  aggregate(data = raw_data, nft_id ~ collection_slug, function(nft_id)
    length(unique(nft_id)))

# summarize data in dataframe
collection <- summary$group1
trans_count <- summary$n
nft_count <- unique_nfts$nft_id
collections_overview <-
  data.frame(collection, trans_count, nft_count)
collections_overview$trans_count <-
  as.integer(collections_overview$trans_count)


#############################################
# For single collection analysis
#############################################


# initialize 3 dataframes
#avg_day_rets <- data.frame(matrix(ncol = 2, nrow = 0))
#colnames(avg_day_rets) <- c("collection", "avg_day_ret")

coll_stats <- data.frame(matrix(ncol = 7, nrow = 0))
colnames(coll_stats) <-
  c(
    "collection",
    "total_volume",
    "total_num_sales",
    "avg_price",
    "adj_r_sq",
    "p_value",
    "GMR"
  )


#############################################
# LOOP Start
#############################################


library(stringr)

for (coll in collection) {
  coll_df <- subset(new_df, str_detect(new_df$nft_unique_id, coll))
  
  
  ###########################################################
  # construct df for every transaction with ID, Date and Price (USD)
  ###########################################################
  
  
  # Get NFTs with more than one transaction
  temp <- table(coll_df$nft_unique_id)
  temp <- as.data.frame(temp)
  nft_mehr_als_eine_transaktionen <- subset(temp, Freq >= 2)
  
  # Rename column
  colnames(nft_mehr_als_eine_transaktionen)[colnames(nft_mehr_als_eine_transaktionen) == "Var1"] <-
    "nft_unique_id"
  
  # JOIN: All transactions from NFTs that have at least two transactions
  rsr_transactions <-
    left_join(nft_mehr_als_eine_transaktionen, coll_df, by = "nft_unique_id")
  
  # Drop unnecessary columns
  rsr_transactions <-
    rsr_transactions[, c("nft_unique_id", "event_date", "price_usd")]
  
  # Take avg price for NFTs that got traded more than once per day
  # Now there is at most one transaction per NFT per day
  rsr_transactions_periods = rsr_transactions %>%
    dplyr::group_by(nft_unique_id, event_date) %>%
    dplyr::summarise(price_usd = mean(price_usd),
                     .groups = 'drop')
  
  # Remove transactions that only occur twice, but at the same day
  # i. e. keep NFTs with more than one transaction remaining
  rsr_transactions_periods = rsr_transactions_periods %>%
    group_by(nft_unique_id) %>%
    filter(n() > 1)
  
  # sort dataset by id and date
  rsr_transactions_periods = rsr_transactions_periods %>% arrange(nft_unique_id, event_date)
  
  # remove unused variables from env
  rm(temp, rsr_transactions, nft_mehr_als_eine_transaktionen)
  
  
  ###########################################################
  # Chain all the transactions of a NFT together and group them in pairs
  # Compute log of price difference
  ###########################################################
  
  
  # convert to vectors
  vec_1 <- rsr_transactions_periods$nft_unique_id
  vec_2 <- as.character(rsr_transactions_periods$event_date)
  vec_3 <- rsr_transactions_periods$price_usd
  vec_4 <- rsr_transactions_periods$nft_unique_id
  vec_5 <- as.character(rsr_transactions_periods$event_date)
  vec_6 <- rsr_transactions_periods$price_usd
  
  # Construct pairs of transactions for each NFT
  # Using vectors for computation as it is much more efficient
  for (i in 2:nrow(rsr_transactions_periods)) {
    if (vec_1[i - 1] == vec_1[i]) {
      vec_4[i - 1] <- vec_1[i]
      vec_5[i - 1] <- vec_2[i]
      vec_6[i - 1] <- vec_3[i]
    } else{
      # Delete transaction from previous NFT since it was already paired with the one before (set NA and delete in next step)
      vec_1[i - 1] <- NA
    }
  }
  
  # construct dataframe from vectors
  pairs <-
    as.data.frame(do.call(
      cbind,
      list(
        first_sale_nft_id = vec_1,
        first_sale_date = vec_2,
        first_sale_price_usd = vec_3,
        second_sale_nft_id = vec_4,
        second_sale_date = vec_5,
        second_sale_price_usd = vec_6
      )
    ))
  
  # delete NA-Rows and last row
  pairs <- na.omit(pairs)
  pairs <- pairs[-nrow(pairs),]
  
  # change datatypes of columns
  pairs$first_sale_date <-
    as.Date(pairs$first_sale_date, format = "%Y-%m-%d")
  pairs$second_sale_date <-
    as.Date(pairs$second_sale_date, format = "%Y-%m-%d")
  pairs$first_sale_price_usd <-
    as.numeric(pairs$first_sale_price_usd)
  pairs$second_sale_price_usd <-
    as.numeric(pairs$second_sale_price_usd)
  
  # compute log price difference
  pairs_logged <-
    pairs %>% mutate(log_price_usd = log(pairs$second_sale_price_usd / pairs$first_sale_price_usd))
  
  
  ###########################################################
  # Reduce effect of extreme outliers
  ###########################################################
  
  
  # add weeks
  pairs_logged <-
    pairs_logged %>% mutate(week1 = paste(
      strftime(pairs_logged$first_sale_date, format = "%G") ,
      strftime(pairs_logged$first_sale_date, format = "%V")
    ))
  pairs_logged <-
    pairs_logged %>% mutate(week2 = paste(
      strftime(pairs_logged$second_sale_date, format = "%G") ,
      strftime(pairs_logged$second_sale_date, format = "%V")
    ))
  
  library(DescTools)
  
  #winsorize weekly data at the 99-percent level
  pairs_logged$log_price_usd <- unsplit(
    tapply(pairs_logged$log_price_usd, pairs_logged$week1,
           function(x)
             Winsorize(x,
                       probs = c(0.01, 0.99)))
    ,
    f = pairs_logged$week1
  )
  
  pairs_logged$log_price_usd <- unsplit(
    tapply(pairs_logged$log_price_usd, pairs_logged$week2,
           function(x)
             Winsorize(x,
                       probs = c(0.01, 0.99)))
    ,
    f = pairs_logged$week2
  )
  
  # remove weeks
  pairs_logged <- subset (pairs_logged, select = -week1)
  pairs_logged <- subset (pairs_logged, select = -week2)
  
  
  ###########################################################
  # Create table with dates
  ###########################################################
  
  
  # Date range for daily frequency
  date_range <-
    seq(min(pairs_logged$first_sale_date),
        max(pairs_logged$second_sale_date),
        "days")
  
  
  ###########################################################
  # Set -1 for first_sale day and 1 for second_sale day in matrix
  ###########################################################
  
  mat <-
    matrix(0,
           nrow = nrow(pairs_logged),
           ncol = length(date_range))
  
  for (r in 1:nrow(pairs_logged)) {
    # first sale date
    index <-  match(pairs_logged[r, 2], date_range)
    mat[r, index] <- -1
    # second sale date
    index <-  match(pairs_logged[r, 5], date_range)
    mat[r, index] <- 1
  }
  
  # construct final df
  df_m <- as.data.frame(mat)
  
  names(df_m) <- as.character(date_range)
  finished_df <- cbind(pairs_logged$log_price_usd, df_m)
  
  # rename first column
  names(finished_df)[1] <- "logP"
  
  # Set first Date Column to zero as it is the base day
  finished_df[, 2] <- rep(0, nrow(finished_df))
  
  
  #############################################
  # Run linear model
  #############################################
  
  
  nft_coll_index = lm(formula = logP ~ 0 + ., data = finished_df)
  
  # change first coefficient to 0 as it is the base for the index
  nft_coll_index$coefficients[1] <- 0
  
  # extract adj. r-squared
  adj_r_sq <- summary(nft_coll_index)$adj.r.squared
  
  # get p-value
  get_pvalue <- function (temp) {
    f <- summary(temp)$fstatistic
    p <- pf(f[1], f[2], f[3], lower.tail = F)
    attributes(p) <- NULL
    return(p)
  }
  p_value <- get_pvalue(nft_coll_index)
  
  #summary(nft_coll_index)
  # Linear interpolation of missing values: Value stays the same at days without transactions
  nft_coll_index$coefficients <- na_interpolation(nft_coll_index$coefficients)
  
  # rolling median 3 values
  sm <- nft_coll_index$coefficients
  
  # save plot
  mypath <-
    file.path(
      "~/R-Code/OpenSea_sales_analysis/Index_Results",
      paste("index_", coll, ".png", sep = "")
    )
  png(
    file = mypath,
    res = 300,
    width = 2000,
    height = 1400
  )
  plot(
    date_range,
    exp(sm),
    type = "l",
    main = paste("NFT Collection Index of", coll),
    xlab = "Date",
    ylab = "Index",
    xaxt = "n"
  )
  axis(
    1,
    date_range,
    format(date_range, "%Y-%m-%d"),
    at = seq(min(date_range), max(date_range), length.out = 5),
    labels = seq(min(date_range), max(date_range), length.out = 5)
  )
  dev.off()
  
  # change data format for coefficients
  d <- as.data.frame(exp(sm))
  d <- cbind(Date = date_range, d)
  names(d) <- c("Date", "Coefficient")
  d$Date <- as.Date(d$Date, format = "%Y-%m-%d")
  
  # save index to csv
  write.csv(
    d,
    paste(
      "~/R-Code/OpenSea_sales_analysis/Index_Results/",
      "index_",
      coll,
      ".csv",
      sep = ""
    )
  )
  
  # compute percentage index change
  coll_index_day_ret <- d %>%
    tidyquant::tq_transmute(
      select = Coefficient,
      mutate_fun = periodReturn,
      period = "daily",
      col_rename = "index_daily_return"
    )
  
  # compute GMR
  r <- coll_index_day_ret %>% mutate(index_daily_return = index_daily_return +1)
  GMR <- ((prod(r$index_daily_return)) ^ (1 / nrow(r)) - 1) * 100
  
  # add collection data to stats df
  coll_stats <-
    rbind(coll_stats, c(
      coll,
      sum(coll_df$price_usd),
      nrow(coll_df),
      mean(coll_df$price_usd),
      round(adj_r_sq, digits = 4),
      round(p_value, digits = 6),
      GMR
    ))
}


#############################################
# LOOP End
#############################################


# change colnames
names(coll_stats) <-
  c(
    "collection",
    "total_volume",
    "total_num_sales",
    "avg_price",
    "adj_r_sq",
    "p_value",
    "GMR"
  )

# change data types
coll_stats$total_volume <- as.numeric(coll_stats$total_volume)
coll_stats$total_num_sales <- as.numeric(coll_stats$total_num_sales)
coll_stats$avg_price <- as.numeric(coll_stats$avg_price)
coll_stats$adj_r_sq <- as.numeric(coll_stats$adj_r_sq)
coll_stats$p_value <- as.numeric(coll_stats$p_value)
coll_stats$GMR <- as.numeric(coll_stats$GMR)

# print average daily return for all collections
mypath <-
  file.path(
    "~/R-Code/OpenSea_sales_analysis/Index_Results",
    paste("gmr_collections", ".png", sep = "")
  )

png(
  file = mypath,
  res = 300,
  width = 2000,
  height = 1400
)
plot(
  coll_stats$GMR,
  type = "h",
  main = "Daily Geometric Mean Return for all Collections",
  xlab = "NFT Collections",
  ylab = "Daily GMR in %"
)
dev.off()

# save coll stats
write.csv(coll_stats, "~/R-Code/OpenSea_sales_analysis/analysis_results.csv")


#############################################
# Export data (to Latex)
#############################################


# export overview for Appendix to Latex
collections_overview$adj_r_squ <- coll_stats$adj_r_sq
print(xtable(collections_overview, type = "latex", auto = TRUE, caption = "NFT Collection Index Statistics", label = "tab:index_stats_collections", digits = c(0, 0, 0, 0, 2)), file = "collections_overview_appendix.tex")


#############################################

