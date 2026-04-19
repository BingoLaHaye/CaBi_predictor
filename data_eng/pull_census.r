library(tidycensus)
library(tidyverse)
library(purrr)
library(arrow)

census_api_key("API_KEY", install = TRUE, overwrite = TRUE)



years <- 2021:2024
states <- c("DC", "VA", "MD")
# expand grid
combinations <- tidyr::expand_grid(years, states)
# county filters
county_filter <- list(
  VA = c("Arlington", "Fairfax County", "Alexandria city"),
  MD = c("Montgomery", "Prince George's"),
  DC = NULL  # DC has no counties, get all tracts
)
# define func
get_census_stuff <- function(year, state) {
  vars <- c(
    total_pop = "B01001_001",
    pop_male = "B01001_002",
    pop_female = "B01001_026",
    median_age = "B01002_001",
    median_hh_income = "B19013_001"
  )
    boop <- get_acs(
      geography = "tract",
      variables = vars,
      state = state,
      county = county_filter[[state]],
      year = year,
      survey = "acs5",
      output = "wide",
      keep_geo_vars = TRUE
    ) %>% 
      mutate(year = year, state = state)
   
    return(boop)
  }
# map it
results <- purrr::map2(combinations$years, combinations$states, get_census_stuff)

# now lets combine it together

census_results <- data.table::rbindlist(results) 


# now we can write out the results

write_parquet(census_results, "C:/Users/bingo/OneDrive - Georgia Institute of Technology/CSE7643/project/census_tract_demographics.parquet")
