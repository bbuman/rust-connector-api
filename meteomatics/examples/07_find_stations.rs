//! # Query Stations
//! It is possible to get a list of available weather stations from which you can select the desired
//! station identifiers. Several conditions can be specified in order to filter the list of stations.
//! The output is a sorted list of stations matching the specified conditions. The output format is CSV.
//! 
//!# The Example
//! Find a station for a coordinate that measures temperature at least since Jan 1st, 2018
//! 
//! # The account
//! You can use the provided credentials or your own if you already have them. 
//! Check out <https://www.meteomatics.com/en/request-business-wather-api-package/> to request an 
//! API package.

use chrono::{Utc, TimeZone};
use meteomatics::APIClient;
use meteomatics::errors::ConnectorError;
use polars::frame::DataFrame;

#[tokio::main]
async fn main(){
    // Credentials
    let api: APIClient = APIClient::new("rust-community", "5GhAwL3HCpFB", 10);

    let station_list = example_request(&api).await.unwrap();

    println!(
        "dataframe: {:?}",
        station_list
    );
   
}

/// Query a time series for a single point and two parameters.
async fn example_request(api: &APIClient) -> std::result::Result<DataFrame, ConnectorError>{
    let location = "50.705502,10.467007";
    let elevation = None;
    let parameters = vec!["t_2m:C"];
    let startdate = Utc.ymd(2018, 1, 1).and_hms(0, 0, 0);
    let enddate = None;

    let result = api.query_station_list(
        &Option::from(location),
        &Option::from(parameters),
        &Option::from(elevation),
        &Option::from(startdate),
        &Option::from(enddate)
    ).await.unwrap();

    Ok(result)
}
