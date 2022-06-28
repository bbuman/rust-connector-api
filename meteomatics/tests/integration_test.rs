use rust_connector_api::APIClient;
use chrono::{Duration, Utc, prelude::*};
use dotenv::dotenv;
use std::env;
use rust_connector_api::{Point, BBox, TimeSeries};
use polars::prelude::*;
use std::io::Cursor;
use std::fs;
use std::path::Path;

// Unit testing section
#[tokio::test]
async fn call_query_time_series_with_options() {
    let s = r#"lat;lon;validdate;t_2m:C;precip_1h:mm
    52.520551;13.461804;1989-11-09T18:00:00Z;7.8;0.00
    52.520551;13.461804;1989-11-10T06:00:00Z;3.2;0.00
    52.520551;13.461804;1989-11-10T18:00:00Z;7.6;0.00
    -52.520551;13.461804;1989-11-09T18:00:00Z;-1.7;0.00
    -52.520551;13.461804;1989-11-10T06:00:00Z;-2.2;0.00
    -52.520551;13.461804;1989-11-10T18:00:00Z;-1.4;0.00
    "#;

    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .with_delimiter(b';')
        .finish()
        .unwrap();

    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();

    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time series
    let time_series_start = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let time_series = TimeSeries{ 
        start: time_series_start, 
        end: time_series_start + Duration::days(1), 
        timedelta: Option::from(Duration::hours(12)) 
    };

    // Create Parameters
    let mut parameters = Vec::new();
    parameters.push(String::from("t_2m:C"));
    parameters.push(String::from("precip_1h:mm"));

    // Create Locations
    let p1: Point = Point { lat: 52.520551, lon: 13.461804};
    let p2: Point = Point { lat: -52.520551, lon: 13.461804};
    let coords: Vec<Point> = vec![p1, p2];

    // Create Optionals
    let mut optionals = Vec::new();
    optionals.push(String::from("source=mix"));
    optionals.push(String::from("calibrated=true"));

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series(
            &time_series, &parameters, &coords, &Option::from(optionals)
        )
        .await.unwrap();

    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn call_query_time_series_without_options() {
    let s = r#"lat;lon;validdate;t_2m:C;precip_1h:mm
    52.520551;13.461804;1989-11-09T18:00:00Z;6.8;0.00
    52.520551;13.461804;1989-11-10T06:00:00Z;1.4;0.00
    52.520551;13.461804;1989-11-10T18:00:00Z;5.2;0.00
    -52.520551;13.461804;1989-11-09T18:00:00Z;-1.7;0.00
    -52.520551;13.461804;1989-11-10T06:00:00Z;-2.2;0.00
    -52.520551;13.461804;1989-11-10T18:00:00Z;-1.4;0.00
    "#;

    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .with_delimiter(b';')
        .finish()
        .unwrap();

    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time series
    let time_series_start = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let time_series = TimeSeries{ 
        start: time_series_start, 
        end: time_series_start + Duration::days(1), 
        timedelta: Option::from(Duration::hours(12)) 
    };

    // Create Parameters
    let mut parameters = Vec::new();
    parameters.push(String::from("t_2m:C"));
    parameters.push(String::from("precip_1h:mm"));

    // Create Locations
    let p1: Point = Point { lat: 52.520551, lon: 13.461804};
    let p2: Point = Point { lat: -52.520551, lon: 13.461804};
    let coords: Vec<Point> = vec![p1, p2];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series(&time_series, &parameters, &coords, &None)
        .await.unwrap();

    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_time_series_one_point_one_param() {
    // Reference data from python connector
    let s = r#"lat,lon,validdate,t_2m:C
    52.52,13.405,1989-11-09T18:00:00Z,6.8
    52.52,13.405,1989-11-10T06:00:00Z,1.4
    52.52,13.405,1989-11-10T18:00:00Z,5.3
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time series
    let time_series_start = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let time_series = TimeSeries{ 
        start: time_series_start, 
        end: time_series_start + Duration::days(1), 
        timedelta: Option::from(Duration::hours(12)) 
    };

    // Create Parameters
    let parameters = vec![String::from("t_2m:C")];

    // Create Locations
    let p1 = Point { lat: 52.52, lon: 13.405};
    let coords = vec![p1];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series(&time_series, &parameters, &coords, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_time_series_one_point_two_param() {
    // Reference data from python connector
    let s = r#"lat,lon,validdate,t_2m:C,precip_1h:mm
    52.52,13.405,1989-11-09T18:00:00Z,6.8,0.06
    52.52,13.405,1989-11-10T06:00:00Z,1.4,0.0
    52.52,13.405,1989-11-10T18:00:00Z,5.3,0.0
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time series
    let time_series_start = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let time_series = TimeSeries{ 
        start: time_series_start, 
        end: time_series_start + Duration::days(1), 
        timedelta: Option::from(Duration::hours(12)) 
    };

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Locations
    let p1: Point = Point { lat: 52.52, lon: 13.405};
    let coords: Vec<Point> = vec![p1];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series(&time_series, &parameters, &coords, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_time_series_two_point_two_param() {
    // Reference data from python connector
    let s = r#"lat,lon,validdate,t_2m:C,precip_1h:mm
    52.5,13.4,1989-11-09T18:00:00Z,6.8,0.05
    52.5,13.4,1989-11-10T06:00:00Z,1.4,0.0
    52.5,13.4,1989-11-10T18:00:00Z,5.4,0.0
    52.4,13.5,1989-11-09T18:00:00Z,6.9,0.0
    52.4,13.5,1989-11-10T06:00:00Z,1.5,0.0
    52.4,13.5,1989-11-10T18:00:00Z,5.3,0.0
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time series
    let time_series_start = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let time_series = TimeSeries{ 
        start: time_series_start, 
        end: time_series_start + Duration::days(1), 
        timedelta: Option::from(Duration::hours(12)) 
    };

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Locations
    let p1: Point = Point { lat: 52.50, lon: 13.40};
    let p2: Point = Point { lat: 52.40, lon: 13.50};
    let coords: Vec<Point> = vec![p1, p2];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series(&time_series, &parameters, &coords, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_time_series_one_postal_one_param() {
    // Reference data from python connector
    let s = r#"station_id,validdate,t_2m:C
    postal_CH9000,1989-11-09T18:00:00Z,4.6
    postal_CH9000,1989-11-10T06:00:00Z,0.9
    postal_CH9000,1989-11-10T18:00:00Z,3.1
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time series
    let time_series_start = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let time_series = TimeSeries{ 
        start: time_series_start, 
        end: time_series_start + Duration::days(1), 
        timedelta: Option::from(Duration::hours(12)) 
    };

    // Create Parameters
    let parameters = vec![String::from("t_2m:C")];

    // Create Locations
    let postal1: Vec<String> = vec![String::from("postal_CH9000")];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series_postal(&time_series, &parameters, &postal1, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_time_series_two_postal_two_param() {
    // Reference data from python connector
    let s = r#"station_id,validdate,t_2m:C,precip_1h:mm
    postal_CH8000,1989-11-09T18:00:00Z,5.8,0.0
    postal_CH8000,1989-11-10T06:00:00Z,3.1,0.0
    postal_CH8000,1989-11-10T18:00:00Z,5.5,0.0
    postal_CH9000,1989-11-09T18:00:00Z,4.6,0.0
    postal_CH9000,1989-11-10T06:00:00Z,0.9,0.0
    postal_CH9000,1989-11-10T18:00:00Z,3.1,0.0
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time series
    let time_series_start = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let time_series = TimeSeries{ 
        start: time_series_start, 
        end: time_series_start + Duration::days(1), 
        timedelta: Option::from(Duration::hours(12)) 
    };

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Locations
    let postal1 = vec![String::from("postal_CH8000"), String::from("postal_CH9000")];

    // Call endpoint
    let df_q = meteomatics_connector
        .query_time_series_postal(&time_series, &parameters, &postal1, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_grid_pivoted() {
    // This is a bit hacked, because the python connetor changes 'data' to 'lat'
    let s = r#"data,13.4,13.45,13.5
    52.5,6.8,6.9,6.9
    52.45,6.8,6.8,6.9
    52.4,6.8,6.9,6.9
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time information
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);

    // Create Parameters
    let parameter = String::from("t_2m:C");

    // Create Location
    let bbox = BBox {
        lat_min: 52.40,
        lat_max: 52.50,
        lon_min: 13.40,
        lon_max: 13.50,
        lat_res: 0.05,
        lon_res: 0.05
    };

    // Call endpoint
    let df_q = meteomatics_connector
        .query_grid_pivoted(&start_date, &parameter, &bbox, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_grid_unpivoted() {
    // directly downloaded from the API
    // https://api.meteomatics.com/1989-11-09T18:00:00.000Z/t_2m:C,precip_1h:mm/52.50,13.40_52.40,13.50:0.05,0.05/csv?model=mix
    let s = r#"lat;lon;validdate;t_2m:C;precip_1h:mm
    52.4;13.4;1989-11-09T18:00:00Z;6.8;0.00
    52.4;13.45;1989-11-09T18:00:00Z;6.9;0.00
    52.4;13.5;1989-11-09T18:00:00Z;6.9;0.00
    52.45;13.4;1989-11-09T18:00:00Z;6.8;0.00
    52.45;13.45;1989-11-09T18:00:00Z;6.8;0.00
    52.45;13.5;1989-11-09T18:00:00Z;6.9;0.00
    52.5;13.4;1989-11-09T18:00:00Z;6.8;0.05
    52.5;13.45;1989-11-09T18:00:00Z;6.9;0.00
    52.5;13.5;1989-11-09T18:00:00Z;6.9;0.00
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_delimiter(b';')
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time information
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Location
    let bbox = BBox {
        lat_min: 52.40,
        lat_max: 52.50,
        lon_min: 13.40,
        lon_max: 13.50,
        lat_res: 0.05,
        lon_res: 0.05
    };

    // Call endpoint
    let df_q = meteomatics_connector
        .query_grid_unpivoted(&start_date, &parameters, &bbox, &None)
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_grid_unpivoted_time_series() {
    // directly downloaded from the API
    // https://api.meteomatics.com/1989-11-09T18:00:00.000Z--1989-11-10T18:00:00.000Z:PT12H/t_2m:C,precip_1h:mm/52.50,13.40_52.40,13.50:0.05,0.05/csv?model=mix
    let s = r#"lat;lon;validdate;t_2m:C;precip_1h:mm
    52.4;13.4;1989-11-09T18:00:00Z;6.8;0.00
    52.4;13.4;1989-11-10T06:00:00Z;1.5;0.00
    52.4;13.4;1989-11-10T18:00:00Z;5.4;0.00
    52.4;13.45;1989-11-09T18:00:00Z;6.9;0.00
    52.4;13.45;1989-11-10T06:00:00Z;1.5;0.00
    52.4;13.45;1989-11-10T18:00:00Z;5.3;0.00
    52.4;13.5;1989-11-09T18:00:00Z;6.9;0.00
    52.4;13.5;1989-11-10T06:00:00Z;1.5;0.00
    52.4;13.5;1989-11-10T18:00:00Z;5.3;0.00
    52.45;13.4;1989-11-09T18:00:00Z;6.8;0.00
    52.45;13.4;1989-11-10T06:00:00Z;1.4;0.00
    52.45;13.4;1989-11-10T18:00:00Z;5.4;0.00
    52.45;13.45;1989-11-09T18:00:00Z;6.8;0.00
    52.45;13.45;1989-11-10T06:00:00Z;1.4;0.00
    52.45;13.45;1989-11-10T18:00:00Z;5.3;0.00
    52.45;13.5;1989-11-09T18:00:00Z;6.9;0.00
    52.45;13.5;1989-11-10T06:00:00Z;1.5;0.00
    52.45;13.5;1989-11-10T18:00:00Z;5.3;0.00
    52.5;13.4;1989-11-09T18:00:00Z;6.8;0.05
    52.5;13.4;1989-11-10T06:00:00Z;1.4;0.00
    52.5;13.4;1989-11-10T18:00:00Z;5.4;0.00
    52.5;13.45;1989-11-09T18:00:00Z;6.9;0.00
    52.5;13.45;1989-11-10T06:00:00Z;1.4;0.00
    52.5;13.45;1989-11-10T18:00:00Z;5.3;0.00
    52.5;13.5;1989-11-09T18:00:00Z;6.9;0.00
    52.5;13.5;1989-11-10T06:00:00Z;1.4;0.00
    52.5;13.5;1989-11-10T18:00:00Z;5.3;0.00
    "#;
    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_delimiter(b';')
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time series
    let time_series_start = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let time_series = TimeSeries{ 
        start: time_series_start, 
        end: time_series_start + Duration::days(1), 
        timedelta: Option::from(Duration::hours(12)) 
    };

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm")];

    // Create Location
    let bbox = BBox {
        lat_min: 52.40,
        lat_max: 52.50,
        lon_min: 13.40,
        lon_max: 13.50,
        lat_res: 0.05,
        lon_res: 0.05
    };

    // Call endpoint
    let df_q = meteomatics_connector
        .query_grid_unpivoted_time_series(
            &time_series, &parameters, &bbox, &None
        )
        .await
        .unwrap();
    println!("Rust result: {:?}", df_q);
    println!("Python result: {:?}", df_s);
    assert!(df_s.frame_equal(&df_q));
}

#[tokio::test]
async fn query_netcdf() {
    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time series
    let time_series_start = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let time_series = TimeSeries{ 
        start: time_series_start, 
        end: time_series_start + Duration::days(1), 
        timedelta: Option::from(Duration::hours(12)) 
    };

    // Create Parameters
    let parameter =String::from("t_2m:C");

    // Create Location
    let bbox = BBox {
        lat_min: 52.40,
        lat_max: 52.50,
        lon_min: 13.40,
        lon_max: 13.50,
        lat_res: 0.05,
        lon_res: 0.05
    };

    // Create file name
    let file_name = String::from("tests/netcdf/my_netcdf.nc");

    // Call endpoint
    meteomatics_connector
        .query_netcdf(
            &time_series, &parameter, &bbox, &file_name, &None
        )
        .await
        .unwrap();
    
    assert!(Path::new(&file_name).exists());
    // These tests are disabled because [`netcdf`] depends on HDF5 being installed on the target
    // operating system. Please install HDF5 on your platform before you run these tests. Then also
    // include netcdf = "0.7.0" in the dependencies.
    // // Make some tests
    // let nc_file = netcdf::open(&file_name).unwrap();
    // let temperature = &nc_file.variable("t_2m").expect("Could not find variable 't_2m");

    // // Check value: ds_rust["t_2m"].data[0,0,0]
    // let temp_val_ref: f64 = 6.81058931350708; // extracted in Python 
    // let temp_val_here: f64 = temperature.value(Some(&[0,0,0])).unwrap(); // extracted from file
    // assert_eq!(temp_val_ref, temp_val_here);

    // // Check another value: ds_rust["t_2m"].data[2,2,2]
    // let temp_val_ref: f64 = 5.269172668457031;
    // let temp_val_here: f64 = temperature.value(Some(&[2,2,2])).unwrap();
    // assert_eq!(temp_val_ref, temp_val_here);

    // Remove the file    
    let dir: &Path = Path::new(&file_name).parent().unwrap();
    fs::remove_file(&file_name).unwrap();
    fs::remove_dir_all(&dir).unwrap();
    // Check if the file and the directory were removed.
    assert!(!Path::new(&file_name).exists());
    assert!(!Path::new(&dir).exists());
}

#[tokio::test]
async fn query_png() {
    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time information
    // 1989-11-09 19:00:00 --> 18:00:00 UTC
    let start_date = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);

    // Create Parameters
    let parameter = String::from("t_2m:C");

    // Create Location
    let bbox = BBox {
        lat_min: 45.8179716,
        lat_max: 47.8084648,
        lon_min: 5.9559113,
        lon_max: 10.4922941,
        lat_res: 0.01,
        lon_res: 0.01
    };

    // Create file name
    let file_name = String::from("tests/png/my_png.png");

    // Call endpoint
    meteomatics_connector
        .query_grid_png(
            &start_date, &parameter, &bbox, &file_name, &None
        )
        .await
        .unwrap();

    let decoder = png::Decoder::new(fs::File::open(&file_name).unwrap());
    let reader = decoder.read_info().unwrap();

    // Inspect more details of the last read frame.
    assert_eq!(454, reader.info().width);
    assert_eq!(200, reader.info().height);
    
    // Remove the file    
    let dir: &Path = Path::new(&file_name).parent().unwrap();
    fs::remove_file(&file_name).unwrap();
    fs::remove_dir_all(&dir).unwrap();
    // Check if the file and the directory were removed.
    assert!(!Path::new(&file_name).exists());
    assert!(!Path::new(&dir).exists());
}

#[tokio::test]
async fn query_grid_png_timeseries() {
    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time series
    let time_series_start = Utc.ymd(1989, 11, 9).and_hms_micro(18, 0, 0, 0);
    let time_series = TimeSeries{ 
        start: time_series_start, 
        end: time_series_start + Duration::days(1), 
        timedelta: Option::from(Duration::hours(12)) 
    };

    // Create Parameters
    let parameter = String::from("t_2m:C");

    // Create Location
    let bbox = BBox {
        lat_min: 45.8179716,
        lat_max: 47.8084648,
        lon_min: 5.9559113,
        lon_max: 10.4922941,
        lat_res: 0.01,
        lon_res: 0.01
    };

    // Create file name
    let prefixpath: String = String::from("tests/png_series/test_series");

    // Call endpoint
    meteomatics_connector
        .query_grid_png_timeseries(
            &time_series, &parameter, &bbox, &prefixpath, &None
        )
        .await
        .unwrap();
    
    // Open a single PNG
    let fmt = "%Y%m%d_%H%M%S";
    let file_name = format!("{}_{}.png", prefixpath, time_series.start.format(fmt));
    let decoder = png::Decoder::new(fs::File::open(&file_name).unwrap());
    let reader = decoder.read_info().unwrap();

    // Inspect more details of the last read frame.
    assert_eq!(454, reader.info().width);
    assert_eq!(200, reader.info().height);
        
    // Remove the file    
    let dir: &Path = Path::new(&file_name).parent().unwrap();
    fs::remove_file(&file_name).unwrap();
    fs::remove_dir_all(&dir).unwrap();
    // Check if the file and the directory were removed.
    assert!(!Path::new(&file_name).exists());
    assert!(!Path::new(&dir).exists());
}

#[tokio::test]
async fn query_user_features(){
    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    let ustats = meteomatics_connector.query_user_features().await.unwrap();
    assert_eq!(env::var("METEOMATICS_USER").unwrap(), ustats.stats.username);
}

#[tokio::test]
async fn query_lightning(){
    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time information
    let start_date = Utc.ymd(2022, 5, 20).and_hms_micro(10, 0, 0, 0);
    let time_series = TimeSeries {
        start: start_date,
        end: start_date + Duration::days(1),
        timedelta: None
    };

    // Create Location
    let bbox: BBox = BBox {
        lat_min: 45.8179716,
        lat_max: 47.8084648,
        lon_min: 5.9559113,
        lon_max: 10.4922941,
        lat_res: 0.0,
        lon_res: 0.0
    };

    let df = meteomatics_connector.query_lightning(&time_series, &bbox).await.unwrap();

    println!("{:?}", df);
}

#[tokio::test]
async fn query_route_points(){
    let s = r#"lat;lon;validdate;t_2m:C;precip_1h:mm;sunshine_duration_1h:min
    47.423938;9.372858;2021-05-25T12:00:00Z;11.5;0.00;60.0
    47.499419;8.726517;2021-05-25T13:00:00Z;13.2;0.06;58.6
    47.381967;8.530662;2021-05-25T14:00:00Z;13.4;0.00;24.3
    46.949911;7.430099;2021-05-25T15:00:00Z;12.8;0.00;53.5
    "#;

    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_delimiter(b';')
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time information
    let date1 = Utc.ymd(2021, 5, 25).and_hms_micro(12, 0, 0, 0);
    let date2 = Utc.ymd(2021, 5, 25).and_hms_micro(13, 0, 0, 0);
    let date3 = Utc.ymd(2021, 5, 25).and_hms_micro(14, 0, 0, 0);
    let date4 = Utc.ymd(2021, 5, 25).and_hms_micro(15, 0, 0, 0);
    let dates = vec![date1, date2, date3, date4];

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm"), String::from("sunshine_duration_1h:min")];

    // Create Locations
    let p1: Point = Point { lat: 47.423938, lon: 9.372858};
    let p2: Point = Point { lat: 47.499419, lon: 8.726517};
    let p3: Point = Point { lat: 47.381967, lon: 8.530662};
    let p4: Point = Point { lat: 46.949911, lon: 7.430099};
    let coords = vec![p1, p2, p3, p4];

    let df_r = meteomatics_connector.route_query_points(
        &dates, &coords, &parameters
    ).await.unwrap();

    assert_eq!(df_s, df_r);
}

#[tokio::test]
async fn query_route_postal(){
    let s = r#"station_id;validdate;t_2m:C;precip_1h:mm;sunshine_duration_1h:min
    postal_CH9000;2021-05-25T12:00:00Z;11.4;0.00;60.0
    postal_CH8400;2021-05-25T13:00:00Z;13.2;0.03;56.4
    postal_CH8000;2021-05-25T14:00:00Z;13.4;0.00;21.9
    postal_CH3000;2021-05-25T15:00:00Z;12.6;0.00;20.1
    "#;

    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_delimiter(b';')
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    // Create time information
    let date1 = Utc.ymd(2021, 5, 25).and_hms_micro(12, 0, 0, 0);
    let date2 = Utc.ymd(2021, 5, 25).and_hms_micro(13, 0, 0, 0);
    let date3 = Utc.ymd(2021, 5, 25).and_hms_micro(14, 0, 0, 0);
    let date4 = Utc.ymd(2021, 5, 25).and_hms_micro(15, 0, 0, 0);
    let dates = vec![date1, date2, date3, date4];

    // Create Parameters
    let parameters = vec![String::from("t_2m:C"), String::from("precip_1h:mm"), String::from("sunshine_duration_1h:min")];

    // Create Locations
    let pcode1 = String::from("postal_CH9000");
    let pcode2 = String::from("postal_CH8400");
    let pcode3 = String::from("postal_CH8000");
    let pcode4 = String::from("postal_CH3000");
    let coords = vec![pcode1, pcode2, pcode3, pcode4];

    let df_r = meteomatics_connector.route_query_postal(
        &dates, &coords, &parameters
    ).await.unwrap();

    assert_eq!(df_s, df_r);
}

#[tokio::test]
async fn query_station_list(){
    let s = r#"Station Category;Station Type;ID Hash;WMO ID;Alternative IDs;Name;Location Lat,Lon;Elevation;Start Date;End Date;Horizontal Distance;Vertical Distance;Effective Distance
    SYNOP;DWD_KL;2701611566;;M721;Schmalkalden, Kurort;50.725,10.454;296m;2005-02-01T00:00:00Z;2017-02-28T23:00:00Z;2356.14;-42;17969.5
    SYNOP;SYNA;3672691085;;M520;Waltershausen;50.896,10.548;348m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;21957.9;10;25675.4
    SYNOP;DWD_KL;4168763873;;M520;Waltershausen;50.896,10.548;348m;2006-08-01T00:00:00Z;2020-01-01T00:00:00Z;21957.9;10;25675.4
    SYNOP;SYNA;388815746;;P022;Ostheim v.d. Rhön;50.454,10.221;314m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;32957.4;-24;41879.4
    SYNOP;DWD_KL;306110816;;P022;Ostheim v.d. Rhön;50.454,10.221;314m;2006-03-01T00:00:00Z;2020-01-01T00:00:00Z;32957.4;-24;41879.4
    SYNOP;SYNA;3483966946;;M405;Moorgrund Gräfen-Nitzendorf;50.843,10.252;283m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;21525.5;-55;41971.6
    SYNOP;DWD_KL;3696469660;;M405;Moorgrund Gräfen-Nitzendorf;50.843,10.252;283m;2004-10-01T00:00:00Z;2020-01-01T00:00:00Z;21525.5;-55;41971.6
    SYNOP;SYNA;1838129212;105400;;Eisenach;51.0007,10.3621;312m;2017-01-01T00:00:00Z;2022-06-28T09:30:00Z;33677.4;-26;43342.9
    SYNOP;DWD_KL;2939077217;105400;;Eisenach;51.0007,10.3621;312m;2007-11-01T00:00:00Z;2020-01-01T00:00:00Z;33677.4;-26;43342.9
    SYNOP;SYNA;2743571505;;L592;Tann/Rhön;50.639,10.023;395m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;32186.6;57;53376.1
    SYNOP;DWD_KL;2767035538;;L592;Tann/Rhön;50.639,10.023;395m;2002-11-01T00:00:00Z;2020-01-01T00:00:00Z;32186.6;57;53376.1
    SYNOP;SYNA;2913658643;105540;EDDE;Erfurt-Weimar;50.9829,10.9608;316m;2016-12-31T23:50:00Z;2022-06-28T09:50:00Z;46456.1;-22;54634.5
    SYNOP;DWD_KL;2071148746;105540;;Erfurt-Weimar;50.9829,10.9608;316m;1951-01-01T01:00:00Z;2020-01-01T00:00:00Z;46456.1;-22;54634.5
    SYNOP;SYNO;2765202655;105540;EDDE;Erfurt-Weimar;50.9829,10.9608;316m;2003-05-18T00:00:00Z;2017-11-30T23:20:00Z;46456.1;-22;54634.5
    SYNOP;SYNA;1918809727;105480;;Meiningen;50.5611,10.3771;450m;2017-01-01T00:00:00Z;2022-06-28T09:30:00Z;17282.5;112;58918.2
    SYNOP;DWD_KL;3817792293;105480;;Meiningen;50.5611,10.3771;450m;1979-01-01T00:00:00Z;2020-01-01T00:00:00Z;17282.5;112;58918.2
    SYNOP;SYNO;2906345460;105480;;Meiningen;50.5611,10.3771;450m;1991-11-01T00:00:00Z;2016-01-13T06:00:00Z;17282.5;112;58918.2
    SYNOP;SYNA;3066885913;106710;;Lautertal-Oberlauter;50.3066,10.9679;344m;2017-01-01T00:00:00Z;2022-06-28T09:30:00Z;56826.8;6;59057.3
    SYNOP;DWD_KL;2462918716;106710;;Lautertal-Oberlauter;50.3066,10.9679;344m;1950-01-01T00:00:00Z;2020-01-01T00:00:00Z;56826.8;6;59057.3
    SYNOP;SYNA;784807562;106710;;Lautertal-Oberlauter;50.3066,10.9679;344m;1990-01-01T06:00:00Z;2007-12-31T18:00:00Z;56826.8;6;59057.3
    SYNOP;SYNA;1555901026;;M927;Veilsdorf;50.417,10.816;397m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;40502.2;59;62435.2
    SYNOP;DWD_KL;4044184341;;M927;Veilsdorf;50.417,10.816;397m;2007-05-01T00:00:00Z;2020-01-01T00:00:00Z;40502.2;59;62435.2
    SYNOP;SYNA;714823714;;M732;Martinroda;50.734,10.882;429m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;29418.7;91;63247.7
    SYNOP;DWD_KL;3765251320;;M732;Martinroda;50.734,10.882;429m;1991-04-01T03:00:00Z;2020-01-01T00:00:00Z;29418.7;91;63247.7
    SYNOP;SYNA;4074896074;;P033;Königshofen, Bad;50.284,10.446;289m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;46944;-49;65159.6
    SYNOP;DWD_KL;2160042738;;P033;Königshofen, Bad;50.284,10.446;289m;2005-06-01T00:00:00Z;2020-01-01T00:00:00Z;46944;-49;65159.6
    SYNOP;DWD_KL;3212862851;;M439;Berka, Bad (Flugplatz);50.907,11.267;303m;2017-09-01T00:00:00Z;2020-01-01T00:00:00Z;60581.4;-35;73592.6
    SYNOP;SYNA;1131779060;;M756;Schwarzburg;50.644,11.194;277m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;51739.8;-61;74416.4
    SYNOP;DWD_KL;3373583240;;M756;Schwarzburg;50.644,11.194;277m;1991-08-01T03:00:00Z;2020-01-01T00:00:00Z;51739.8;-61;74416.4
    SYNOP;SYNA;2438569949;;M448;Weimar-Schöndorf;51.018,11.354;328m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;71372.8;-10;75090.3
    SYNOP;DWD_KL;1131955414;;M448;Weimar-Schöndorf;51.018,11.354;328m;2007-06-01T00:00:00Z;2020-01-01T00:00:00Z;71372.8;-10;75090.3
    SYNOP;SYNA;2055210251;105420;;Hersfeld, Bad;50.852,9.7377;272m;2017-01-01T00:00:00Z;2022-06-28T09:30:00Z;53862;-66;78397.3
    SYNOP;DWD_KL;1316479285;105420;;Hersfeld, Bad;50.852,9.7377;272m;1951-01-01T01:00:00Z;2020-01-01T00:00:00Z;53862;-66;78397.3
    SYNOP;SYNO;1186868868;105420;;Hersfeld, Bad;50.852,9.7377;272m;1990-01-01T06:00:00Z;2007-12-31T18:00:00Z;53862;-66;78397.3
    SYNOP;DWD_KL;620676444;105460;;Kaltennordheim;50.6266,10.1455;487m;1951-01-01T01:00:00Z;2003-05-05T15:00:00Z;24325.6;149;79715.9
    SYNOP;SYNA;4108845518;105460;;Kaltennordheim;50.6266,10.1455;487m;1991-05-22T12:00:00Z;1992-12-31T18:00:00Z;24325.6;149;79715.9
    SYNOP;SYNA;528208010;106580;;Kissingen, Bad;50.224,10.0792;282m;2017-01-01T00:00:00Z;2022-06-28T09:30:00Z;60232.9;-56;81050.8
    SYNOP;DWD_KL;1782307044;106580;;Kissingen, Bad;50.224,10.0792;282m;1950-01-01T00:00:00Z;2020-01-01T00:00:00Z;60232.9;-56;81050.8
    SYNOP;SYNO;2024254201;106580;;Kissingen, Bad;50.224,10.0792;282m;1990-01-01T06:00:00Z;2007-12-31T18:00:00Z;60232.9;-56;81050.8
    SYNOP;SYNA;3996622171;;L286;Sontra;51.061,9.927;265m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;54811.3;-73;81948.9
    SYNOP;DWD_KL;2248091821;;L286;Sontra;51.061,9.927;265m;1961-01-01T03:00:00Z;2020-01-01T00:00:00Z;54811.3;-73;81948.9
    SYNOP;SYNA;1412926804;104490;;Leinefelde;51.3933,10.3123;356m;2017-01-01T00:00:00Z;2022-06-28T09:30:00Z;77325.3;18;84016.7
    SYNOP;DWD_KL;2263006289;104490;;Leinefelde;51.3933,10.3123;356m;1956-01-01T01:00:00Z;2020-01-01T00:00:00Z;77325.3;18;84016.7
    SYNOP;SYNO;1015506961;104490;;Leinefelde;51.3933,10.3123;356m;1990-01-01T00:00:00Z;2007-12-31T18:00:00Z;77325.3;18;84016.7
    SYNOP;SYNA;212558672;;P131;Schonungen-Mainberg;50.058,10.297;305m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;73081.2;-33;85348.9
    SYNOP;DWD_KL;2914197341;;P131;Schonungen-Mainberg;50.058,10.297;305m;2005-03-01T00:00:00Z;2020-01-01T00:00:00Z;73081.2;-33;85348.9
    SYNOP;SYNA;3872966869;;P066;Kronach;50.252,11.321;310m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;78791.1;-28;89200
    SYNOP;DWD_KL;1062585615;;P066;Kronach;50.252,11.321;310m;2005-08-01T18:00:00Z;2020-01-01T00:00:00Z;78791.1;-28;89200
    SYNOP;DWD_KL;3331685814;105550;;Weimar;50.9751,11.3076;264m;1950-01-01T00:00:00Z;2007-06-30T23:00:00Z;66273.2;-74;93782.5
    SYNOP;SYNA;142572850;105550;;Weimar;50.9751,11.3076;264m;1997-01-01T00:00:00Z;2004-05-18T00:00:00Z;66273.2;-74;93782.5
    SYNOP;SYNA;4182166783;;L585;Fulda-Horas;50.567,9.653;242m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;59502.2;-96;95190
    SYNOP;DWD_KL;243815700;;L585;Fulda-Horas;50.567,9.653;242m;1951-01-01T03:00:00Z;2020-01-01T00:00:00Z;59502.2;-96;95190
    SYNOP;SYNA;2713637135;;P148;Ebrach;49.852,10.499;346m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;95036.6;8;98010.6
    SYNOP;DWD_KL;4294000053;;P148;Ebrach;49.852,10.499;346m;2004-10-06T18:00:00Z;2020-01-01T00:00:00Z;95036.6;8;98010.6
    SYNOP;SYNA;2826484721;;M225;Mühlhausen/Thüringen-Görmar;51.206,10.498;193m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;55756.4;-145;109660
    SYNOP;DWD_KL;3382604221;;M225;Mühlhausen/Thüringen-Görmar;51.206,10.498;193m;2004-12-01T00:00:00Z;2020-01-01T00:00:00Z;55756.4;-145;109660
    SYNOP;SYNA;2507307426;;M348;Dachwig;51.078,10.862;170m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;49886;-168;112340
    SYNOP;DWD_KL;3444800984;;M348;Dachwig;51.078,10.862;170m;2004-08-01T00:00:00Z;2020-01-01T00:00:00Z;49886;-168;112340
    METAR;AUTO;2859191357;;KQKT;Schweinfurt 7ws;50.05,10.1666;240m;2017-06-28T17:58:00Z;2021-03-17T14:59:00Z;76020.9;-98;112452
    METAR;META;1375004811;;KQKT;Schweinfurt 7ws;50.05,10.1666;240m;1990-01-18T07:00:00Z;1997-08-13T03:00:00Z;76020.9;-98;112452
    SYNOP;SYNA;2961793948;;P113;Lohr/Main-Halsbach;50.013,9.654;288m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;96311;-50;114898
    SYNOP;DWD_KL;3945886763;;P113;Lohr/Main-Halsbach;50.013,9.654;288m;2005-09-28T06:00:00Z;2020-01-01T00:00:00Z;96311;-50;114898
    SYNOP;SYNA;136879664;;L678;Schlüchtern-Herolz;50.345,9.553;230m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;76119.5;-108;116268
    SYNOP;DWD_KL;1228694489;;L678;Schlüchtern-Herolz;50.345,9.553;230m;2004-09-01T00:00:00Z;2020-01-01T00:00:00Z;76119.5;-108;116268
    SYNOP;DWD_KL;2865269964;;;Bamberg (Sternwarte);49.88,10.88;282m;1950-01-01T00:00:00Z;1955-01-01T00:00:00Z;96471.7;-56;117290
    SYNOP;SYNA;2132020976;;P013;Sandberg;50.352,10.003;518m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;51250.2;180;118165
    SYNOP;DWD_KL;1673264954;;P013;Sandberg;50.352,10.003;518m;2005-09-01T00:00:00Z;2020-01-01T00:00:00Z;51250.2;180;118165
    SYNOP;DWD_KL;684141844;;M161;Sondershausen;51.352,10.906;221m;2004-11-01T00:00:00Z;2010-12-31T23:00:00Z;78253.9;-117;121748
    METAR;META;1338848335;;ETEJ;Bamberg/MIL;49.92,10.92;250m;2003-05-18T00:00:00Z;2007-12-31T18:00:00Z;93180.4;-88;125894
    SYNOP;SYNA;2756144759;;L291;Eschwege;51.204,10.014;156m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;63940;-182;131598
    SYNOP;DWD_KL;2129785147;;L291;Eschwege;51.204,10.014;156m;1980-12-01T00:00:00Z;2020-01-01T00:00:00Z;63940;-182;131598
    SYNOP;SYNA;1065662035;106750;;Bamberg;49.8743,10.9206;240m;2017-01-01T00:00:00Z;2022-06-28T09:30:00Z;97989;-98;134420
    SYNOP;DWD_KL;521574635;106750;;Bamberg;49.8743,10.9206;240m;1950-01-01T00:00:00Z;2020-01-01T00:00:00Z;97989;-98;134420
    SYNOP;SYNO;3099885917;106750;;Bamberg;49.8743,10.9206;240m;2003-05-18T00:00:00Z;2016-01-13T06:00:00Z;97989;-98;134420
    SYNOP;SYNA;2852925586;;P125;Arnstein-Müdesheim;49.969,9.911;217m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;91005.8;-121;135987
    SYNOP;DWD_KL;930659309;;P125;Arnstein-Müdesheim;49.969,9.911;217m;2004-11-04T10:00:00Z;2020-01-01T00:00:00Z;91005.8;-121;135987
    SYNOP;SYNA;3661630852;;M299;Olbersleben;51.151,11.332;160m;2017-10-08T08:00:00Z;2022-06-28T09:40:00Z;78373.8;-178;144545
    SYNOP;DWD_KL;2285806159;;M299;Olbersleben;51.151,11.332;160m;2005-09-01T00:00:00Z;2020-01-01T00:00:00Z;78373.8;-178;144545
    SYNOP;SYNA;2519142489;104600;;Artern;51.3744,11.292;164m;2017-01-01T00:00:00Z;2022-06-28T09:30:00Z;94225.4;-174;158909
    SYNOP;DWD_KL;3121080579;104600;;Artern;51.3744,11.292;164m;1958-04-01T03:00:00Z;2020-01-01T00:00:00Z;94225.4;-174;158909
    SYNOP;SYNO;984185596;104600;;Artern;51.3744,11.292;164m;1991-11-01T06:00:00Z;2015-05-26T12:00:00Z;94225.4;-174;158909
    SYNOP;SYNA;3159359959;104440;;Göttingen;51.5002,9.9507;167m;2017-01-01T00:00:00Z;2022-06-28T09:30:00Z;95541.1;-171;159110
    SYNOP;DWD_KL;1653219978;104440;;Göttingen;51.5002,9.9507;167m;1950-01-01T00:00:00Z;2020-01-01T00:00:00Z;95541.1;-171;159110
    SYNOP;SYNO;2113489438;104440;;Göttingen;51.5002,9.9507;167m;1990-01-01T06:00:00Z;2007-12-31T18:00:00Z;95541.1;-171;159110
    "#;

    let file = Cursor::new(s);
    let df_s = CsvReader::new(file)
        .infer_schema(Some(100))
        .has_header(true)
        .with_delimiter(b';')
        .with_ignore_parser_errors(true)
        .finish()
        .unwrap();

    // Query using rust connector
    // Credentials
    dotenv().ok();
    let api_key: String = env::var("METEOMATICS_PW").unwrap();
    let api_user: String = env::var("METEOMATICS_USER").unwrap();
    
    // Create API connector
    let meteomatics_connector = APIClient::new(
        &api_user,
        &api_key,
        10,
    );

    let location: &str = &Point{ lat: 50.705502, lon: 10.467007}.to_string();
    let parameters = vec!["t_2m:C"];
    let elevation = None;
    let startdate = Utc.ymd(2018, 1, 1).and_hms(0, 0, 0);
    let enddate = None;

    let df_r = meteomatics_connector.query_station_list(
        &Option::from(location),
        &Option::from(parameters),
        &Option::from(elevation),
        &Option::from(startdate),
        &Option::from(enddate)
    ).await.unwrap();
    assert_eq!(df_s.select(["ID Hash"]).unwrap().sum(), df_r.select(["ID Hash"]).unwrap().sum());
    assert_eq!(
        df_s.select(["Horizontal Distance"]).unwrap().sum(), 
        df_r.select(["Horizontal Distance"]).unwrap().sum()
    );
}