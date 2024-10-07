

#### Libraries ####
library(RSQLite)
library(dplyr)
library(lubridate)
library(janitor)
library(readxl)
library(ggplot2)
library(RcppRoll)
library(tidyr)
library(readr)
library(reshape2)
library(gridExtra)
library(bigrquery)



current_date <- Sys.Date()
current_month = month(Sys.Date())
current_year <- year(Sys.Date())



# Ensure unique bidprice per node and hour
make_unique_bidprice <- function(data) {
  data <- data %>% arrange(node, hour, bidprice)
  
  for (i in seq_len(nrow(data))) {
    while (any(duplicated(data$bidprice[data$node == data$node[i] & data$hour == data$hour[i]]))) {
      duplicate_rows <- which(duplicated(data$bidprice[data$node == data$node[i] & data$hour == data$hour[i]]))
      for (j in duplicate_rows) {
        if (data$bidvol[j] > 0) {
          data$bidprice[j] <- data$bidprice[j] + 1
        } else {
          data$bidprice[j] <- data$bidprice[j] - 1
        }
      }
    }
  }
  
  return(data)
}

`%notin%` <- Negate(`%in%`)

#### end ####


# Replace with the path to your client secrets JSON file
CLIENT_SECRETS_FILE <- "F:/google_auth.json"
bq_auth(path = "F:/google_auth.json")

# Set your project ID
project_id <- "iso-data-437618"
current_date <- Sys.Date()


#### SPP Fundamental Data ####

# Construct a SQL query
sql <- sprintf("

SELECT * 
FROM `spp.mtlf` 
WHERE DATE(pricedate) <= CURRENT_DATE() + 1
ORDER BY pricedate DESC, hour
               
               "
)

# Run the query
result <- bq_project_query(project_id, sql)

# Download the query results into a data frame
spp_mtlf_data <- bq_table_download(result) %>% dplyr::rename(load_forecast = MTLF)




# Construct a SQL query
sql <- sprintf("

SELECT * 
FROM `spp.mtrf` 
WHERE DATE(pricedate) <= CURRENT_DATE() + 1
ORDER BY pricedate DESC, hour
               
               "
)

# Run the query
result <- bq_project_query(project_id, sql)

# Download the query results into a data frame
spp_mtrf_data <- bq_table_download(result) 




# Construct a SQL query
sql <- sprintf("

SELECT * 
FROM `spp.rfrz` 
WHERE DATE(pricedate) <= CURRENT_DATE() + 1
ORDER BY pricedate DESC, hour
               
               "
)

# Run the query
result <- bq_project_query(project_id, sql)

# Download the query results into a data frame
spp_rfrz_data <- bq_table_download(result) 





#### end SPP Fundamental Data ####


#### CISO Fundamental Data ####


# Construct a SQL query
sql <- sprintf("

SELECT * 
FROM `ciso.load` 
WHERE DATE(pricedate) <= CURRENT_DATE() + 1
ORDER BY pricedate DESC, hour
               
               "
)

# Run the query
result <- bq_project_query(project_id, sql)

# Download the query results into a data frame
ciso_load_data <- bq_table_download(result) 



# Construct a SQL query
sql <- sprintf("

SELECT * 
FROM `ciso.renewables` 
WHERE DATE(pricedate) <= CURRENT_DATE() + 1
ORDER BY pricedate DESC, hour
               
               "
)

# Run the query
result <- bq_project_query(project_id, sql)

# Download the query results into a data frame
ciso_renewables_data <- bq_table_download(result) 




ciso_fund_data <- left_join(ciso_load_data, ciso_renewables_data, by = c("pricedate", "hour")) %>%
  mutate(net_demand_forecast = two_da - total_renewables_fcast, inverse_net = total_renewables_fcast - two_da)

ciso_fcast_data <- ciso_fund_data %>% 
  dplyr::select(pricedate, hour, two_da, dam,  NP15_Solar_DAM, SP15_Solar_DAM, ZP26_Solar_DAM, 
                total_solar_fcast, total_wind_fcast, total_renewables_fcast,  net_demand_forecast, inverse_net)

ciso_fcast_tomorrow <- ciso_fcast_data %>% filter(pricedate == current_date + 1)
View(ciso_fund_data)

ciso_unique_nodes <- read_csv("E:\\Cincinnatus_dbs\\CISO\\node_info\\ciso_unique_nodes.csv", show_col_types = F)
ciso_peaks <- ciso_fcast_data %>% group_by(pricedate) %>% summarise(max_solar = max(total_solar_fcast), max_load = max(two_da))


#### end CISO Fundamental Data ####



#### CISO node and price data ####



# Construct a SQL query
sql <- sprintf("

SELECT * 
FROM ciso.prices
WHERE DATE(pricedate) >= CURRENT_DATE() - 22
AND node = 'POD_COVERD_2_HCKHY1-APND'
ORDER BY pricedate DESC, hour ASC, node
               
               "
)

# Run the query
result <- bq_project_query(project_id, sql)

# Download the query results into a data frame
ciso_prices_data <- bq_table_download(result) %>% mutate_if(is.numeric, round, digits = 2)

View(ciso_prices_data)




# Construct a SQL query
sql <- sprintf("

SELECT * 
FROM ciso.prices
WHERE DATE(pricedate) = CURRENT_DATE() - 1
               
               "
)

# Run the query
result <- bq_project_query(project_id, sql)

# Download the query results into a data frame
ciso_yday_prices <- bq_table_download(result) %>% mutate_if(is.numeric, round, digits = 2)



# Calculate the value from rtlmp - dalmp
ciso_yday_prices <- ciso_yday_prices %>%
  mutate(rtda = rtlmp - dalmp)

# Reshape the dataframe to have hours as columns
ciso_yday_prices_wide <- ciso_yday_prices %>%
  dplyr::select(node, hour, rtda) %>%
  spread(key = hour, value = rtda)

# Create a sum column
ciso_yday_prices_wide <- ciso_yday_prices_wide %>%
  rowwise() %>%
  mutate(sum = sum(c_across(2:25), na.rm = TRUE), median = median(c_across(2:25)))



#### end price and node data ####
