#' Function to identify STRs located in housing
#'
#' \code{strr_housing} takes an STR property file and adds a `housing` field.
#'
#' A function for adding a `housing` field to an STR property file. The function
#' reads the `property_type` fields and categorizes the values into either
#' housing or not housing.
#'
#' @param property An STR property file.
#' @param property_type The name of a field identifying the type of property a
#' listing is located in.
#' @return The output will be the input `property` table, but with an additional
#' `housing` field added, with TRUE or FALSE values indicating if a given
#' listing is located in housing (TRUE) or in a dedicated tourist accommodation
#' facility such as a hotel or B&B (FALSE).
#' @importFrom dplyr %>% if_else
#' @importFrom rlang .data
#' @export

strr_housing <- function(property, property_type = property_type) {

  housing_types <- c(
    "Apartment", "Building", "Bungalow", "Cabin", "Casa particular",
    "Casa particular (cuba)", "Castle", "Cave", "Chalet",
    "Chateau / Country House", "Condo", "Condominium", "Corporate Apartment",
    "Cottage", "Cycladic house", "Cycladic house (greece)", "Dammuso",
    "Dammuso (italy)", "Dome house", "Earth house", "Earth House",
    "Entire apartment", "Entire bungalow", "Entire cabin",
    "Entire casa particular", "Entire castle", "Entire cave", "Entire chalet",
    "Entire condominium", "Entire cottage", "Entire earth house", "Entire flat",
    "Entire floor", "Entire Floor", "Entire guest suite", "Entire guesthouse",
    "Entire holiday home", "Entire house", "Entire hut", "Entire in-law",
    "Entire island", "Entire loft", "Entire place", "Entire serviced apartment",
    "Entire tiny house", "Entire townhouse",  "Entire villa", "Estate",
    "Farm stay", "Farmhouse", "Flat", "Floor", "Guest House", "Guest suite",
    "Guesthouse", "Holiday home", "Home/apt", "House", "Hut",  "In-law", "Island",
    "Loft", "Mas", "Mill", "Other", "Place", "Private room",
    "Private room in apartment", "Private room in bungalow",
    "Private room in cabin", "Private room in casa particular",
    "Private room in casa particular (cuba)", "Private room in castle",
    "Private room in cave", "Private room in chalet",
    "Private room in condominium", "Private room in cottage",
    "Private room in dome house",  "Private room in earth house",
    "Private room in farm stay", "Private room in floor",
    "Private room in guest suite", "Private room in guesthouse",
    "Private room in house",  "Private room in hut", "Private room in in-law",
    "Private room in island", "Private room in loft",
    "Private room in serviced apartment", "Private room in tiny house",
    "Private room in townhouse", "Private room in villa", "Riad",
    "Serviced apartment", "Serviced flat", "Shared room",
    "Shared room in apartment", "Shared room in bungalow", "Shared room in cabin",
    "Shared room in casa particular", "Shared room in casa particular (cuba)",
    "Shared room in castle", "Shared room in cave", "Shared room in chalet",
    "Shared room in condominium", "Shared room in cottage",
    "Shared room in earth house", "Shared room in farm stay",
    "Shared room in floor", "Shared room in guest suite",
    "Shared room in guesthouse", "Shared room in house", "Shared room in hut",
    "Shared room in in-law", "Shared room in island", "Shared room in loft",
    "Shared room in serviced apartment", "Shared room in tiny house",
    "Shared room in townhouse", "Shared room in trullo (italy)",
    "Shared room in villa", "Studio", "Tiny house", "Tower", "Townhome",
    "Townhouse", "Trullo", "Trullo (italy)", "Villa", "", NA
  )

  not_housing_types <- c(
    "Aparthotel", "Barn", "Bed & breakfast", "Bed & Breakfast",
    "Bed &amp; Breakfast", "Bed and breakfast", "Bed And Breakfast", "Boat",
    "Boutique hotel", "Bus", "Camper/rv", "Camper/RV", "Campground", "Campsite",
    "Car", "Caravan", "Condohotel", "Dorm", "Entire aparthotel",
    "Entire barn", "Entire bed & breakfast", "Entire bed and breakfast",
    "Entire boat", "Entire boutique hotel", "Entire camper/RV", "Entire campsite",
    "Entire car", "Entire dorm", "Entire heritage hotel", "Entire hostel",
    "Entire hotel", "Entire houseboat", "Entire igloo",  "Entire lighthouse",
    "Entire minsu", "Entire nature lodge", "Entire pension", "Entire plane",
    "Entire pousada", "Entire ryokan", "Entire tent", "Entire timeshare",
    "Entire tipi", "Entire train", "Entire treehouse", "Entire vacation home",
    "Entire yurt", "Heritage hotel", "Heritage hotel (india)",
    "Heritage hotel (India)", "Hostel", "Hotel", "Hotel Suites", "House Boat",
    "Houseboat", "Igloo", "Lighthouse", "Lodge", "Minsu", "Minsu (taiwan)",
    "Minsu (Taiwan)", "Mobile Home", "Nature lodge", "Parking space",
    "Parking Space", "Pension", "Pension (Korea)", "Pension (south korea)",
    "Pension (South Korea)", "Plane", "Pousada", "Private room in aparthotel",
    "Private room in bed & breakfast", "Private room in bed and breakfast",
    "Private room in boat", "Private room in boutique hotel",
    "Private room in camper/rv", "Private room in car",
    "Private room in condohotel", "Private room in dorm",
    "Private room in heritage hotel (india)", "Private room in hostel",
    "Private room in hotel", "Private room in houseboat", "Private room in igloo",
    "Private room in lighthouse", "Private room in minsu",
    "Private room in minsu (taiwan)", "Private room in nature lodge",
    "Private room in parking space", "Private room in pension",
    "Private room in pension (korea)", "Private room in pension (south korea)",
    "Private room in plane", "Private room in pousada", "Private room in resort",
    "Private room in ryokan (japan)", "Private room in tent",
    "Private room in timeshare", "Private room in tipi", "Private room in train",
    "Private room in treehouse", "Private room in vacation home",
    "Private room in yurt", "Recreational Vehicle", "Resort",
    "Room in aparthotel", "Room in boutique hotel", "Room in heritage hotel",
    "Room in heritage hotel (india)", "Room in hotel", "Ryokan", "Ryokan (japan)",
    "Ryokan (Japan)", "Shared room in aparthotel",
    "Shared room in bed & breakfast", "Shared room in bed and breakfast",
    "Shared room in boat", "Shared room in boutique hotel",
    "Shared room in camper/rv", "Shared room in dorm", "Shared room in hostel",
    "Shared room in hotel", "Shared room in igloo", "Shared room in lighthouse",
    "Shared room in minsu (taiwan)", "Shared room in nature lodge",
    "Shared room in parking space", "Shared room in pension (korea)",
    "Shared room in pension (south korea)", "Shared room in plane",
    "Shared room in pousada", "Shared room in resort",
    "Shared room in ryokan (japan)", "Shared room in tent",
    "Shared room in timeshare", "Shared room in tipi", "Shared room in train",
    "Shared room in treehouse", "Shared room in vacation home",
    "Shared room in yurt", "Shepherd&#39;s hut",
    "Shepherd&#39;s hut (u.k., france)", "Shepherd&#39;s hut (U.K., France)",
    "Tent", "Timeshare", "Tipi", "Train", "Treehouse", "Vacation home", "Van",
    "Windmill", "Yacht", "Yurt"
  )

  property %>%
    dplyr::mutate(
      housing = if_else({{ property_type }} %in% housing_types, TRUE, FALSE),
      # Add extra logic to catch non-housing option with non-ASCII character
      housing = if_else(stringr::str_detect({{ property_type }}, "ara/s"),
                        FALSE, .data$housing),
      housing = if_else(is.na(.data$housing), TRUE, .data$housing))
}


