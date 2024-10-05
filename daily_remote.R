library(bigrquery)
library(dplyr)
# Replace with the path to your client secrets JSON file
CLIENT_SECRETS_FILE <- "F:/oauth_access.json"

# Set your project ID
project_id <- "iso-data-437618"





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


#### end CISO Fundamental Data ####