---
title: Usage Patterns Analysis
---

In Gothenburg there are **<Value data={stations_metrics} column=station_count/>** stations and there is on average **<Value data={stations_metrics} column=average_bikes/>** bikes per station between
**<Value data={stations_metrics} column=start_date/>** and **<Value data={stations_metrics} column=end_date/>**. The average distance
between stations stands at **<Value data={stations_metrics} column=average_distance/>** km, with the maximum distance between any two stations measuring **<Value data={stations_metrics} column=max_distance/>** km. 
Accompanying this data is a map that comprehensively illustrates all stations in Gothenburg, including information on the closest station to each, along with the respective distances.

*Note - that the station-to-station distances are computed based on coordinates, and occasional inaccuracies may arise.*



<LeafletMap 
    data={stations_data} 
    lat=lat 
    long=long
    name=name
    tooltipFields={['closest_station_name', 'distance']}
    height=500
/>

<br />

This bubble chart visually displays the traffic for each station, where the size of each bubble corresponds to the volume of passengers departing from or arriving at each location; larger bubbles indicate higher user volumes.

<br />

<BubbleChart 
    data={trips_aggregated} 
    x=long 
    y=lat 
    size=trips
    series=trip_type
    xAxisTitle=false 
    yAxisTitle=false
    xGridlines=false
    yGridlines=false
    xTickMarks=false
    yTickMarks=false
    chartAreaHeight=900
    yMin=57.63
    yMax= 57.755
    scaleTo=1.3
    sort=true
    colorPalette={['#003FF9', '#F9BA00']}
/>

<br />

This tables showcase compilations of stations along with the count of departures/arrivals observed within the specified timeframe. Each row contains the station name and the associated quantity of arrivals/departures
during the designated period. 

<br />

### Departures/Arrivals per station:
<DataTable data="{trips_started_ended}" search="true">
    <Column id="station_name" title="Station name" />
    <Column id="started_trips" title="Departures" />
    <Column id="ended_trips" title="Arrivals"/>
    <Column id="trips_delta" title="Difference" contentType=colorscale scaleColor=blue/>
</DataTable>


<br />

This bar chart provides a comprehensive view of the average number of trips for each day of the week over the entire observation period. Each bar represents the mean number of trips across multiple weeks, offering insights into the station's activity patterns on different days.

<br />

<BarChart 
    data={trips_day_sum} 
    x=day_of_week 
    y=average_trips
/>








```sql stations_metrics
select 
    count(distinct fs.station_id) as station_count, 
    AVG(ds.available_bikes) AS average_bikes,
    AVG(fs.distance) as average_distance,
    MAX(fs.distance) as max_distance,
    MIN(date_time) AS start_date, 
    MAX(date_time) AS end_date 
from main_mart.fct_stations fs
join main_mart.dim_stations ds on fs.station_id = ds.station_id
```

```sql stations_data
select *
from main_mart.fct_stations fs
```


```sql trip_details
select * from main_mart.trip_details
```


```sql trips_aggregated
SELECT
    start_station_name as station_name,
    start_lat as lat,
    start_long as long,
    COUNT(*) AS trips,
    'started' AS trip_type
FROM
    main_mart.trip_details
GROUP BY
    start_station_name, start_lat, start_long

UNION

SELECT
    end_station_name AS station_name,
    end_lat AS lat,
    end_long AS long,
    COUNT(*) AS trips,
    'ended' AS trip_type
FROM
    main_mart.trip_details
GROUP BY
    station_name, lat, long
order by station_name

```


```sql trips_started_ended
SELECT
    station_name,
    SUM(CASE WHEN trip_type = 'started' THEN trips ELSE 0 END) AS started_trips,
    SUM(CASE WHEN trip_type = 'ended' THEN trips ELSE 0 END) AS ended_trips,
    started_trips - ended_trips as trips_delta
FROM
    ${trips_aggregated}
GROUP BY
    station_name,
ORDER BY
    trips_delta DESC
```

```sql trips_ended
SELECT
    station_name,
    SUM(CASE WHEN trip_type = 'started' THEN trips ELSE 0 END) AS started_trips,
    SUM(CASE WHEN trip_type = 'ended' THEN trips ELSE 0 END) AS ended_trips,
    ended_trips - started_trips as trips_delta
FROM
    ${trips_aggregated}
GROUP BY
    station_name,
HAVING
    SUM(CASE WHEN trip_type = 'started' THEN trips ELSE 0 END) < SUM(CASE WHEN trip_type = 'ended' THEN trips ELSE 0 END)
ORDER BY
    trips_delta DESC

```

```sql trips_aggregated_by_day
SELECT
    start_station_name AS station_name,
    COUNT(*) AS trips,
    CASE dayofweek 
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        ELSE NULL
    END AS day_of_week,
    'started' AS trip_type,
    start_time AS date
FROM
    main_mart.trip_details
GROUP BY
    start_station_name, day_of_week, date

UNION

SELECT
    end_station_name AS station_name,
    COUNT(*) AS trips,
    CASE dayofweek 
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        ELSE NULL
    END AS day_of_week,
    'ended' AS trip_type,
    start_time AS date
FROM
    main_mart.trip_details
GROUP BY
    end_station_name, day_of_week, date

ORDER BY 
    date, trips DESC
```

```sql trips_day_sum
SELECT
    day_of_week,
    AVG(sum_trips) AS average_trips
FROM (
    SELECT
        day_of_week,
        DAY(date) AS day_of_month,
        SUM(trips) AS sum_trips
    FROM
        ${trips_aggregated_by_day}
    GROUP BY
        day_of_week, DAY(date)
) AS subquery
GROUP BY
    day_of_week
ORDER BY
    AVG(sum_trips);
```


