from dagster import AssetSelection, define_asset_job

raw_stations_blob = AssetSelection.keys(["raw_stations_blob"])
stg_stations_parquet = AssetSelection.keys(["raw_stations_parquet"])

raw_weather_blob = AssetSelection.keys(["raw_weather_blob"])
stg_weather_parquet = AssetSelection.keys(["raw_weather_parquet"])

get_stations_data = define_asset_job(
    name="stations_every_ten_minutes",
    selection=raw_stations_blob,
)

stations_summary = define_asset_job(
    name="stations_monthly",
    selection=stg_stations_parquet,
)

get_weather_data = define_asset_job(
    name="weather_daily",
    selection=raw_weather_blob,
)

weather_summary = define_asset_job(
    name="weather_monthly",
    selection=stg_weather_parquet.downstream(),
)

