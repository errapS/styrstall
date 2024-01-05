
```sql trips_agg
SELECT
    long,
    lat,
    SUM(trips) AS total_trips,
FROM (
    SELECT
        start_station_name AS station_name,
        start_lat AS lat,
        start_long AS long,
        COUNT(*) AS trips
    FROM
        main_mart.trip_details
    GROUP BY
        start_station_name, start_lat, start_long

    UNION ALL

    SELECT
        end_station_name AS station_name,
        end_lat AS lat,
        end_long AS long,
        COUNT(*) AS trips
    FROM
        main_mart.trip_details
    GROUP BY
        end_station_name, end_lat, end_long
) AS subquery
GROUP BY
    station_name, lat, long
ORDER BY
    station_name;
```

<script type="module">
   
const longValues = [];
const latValues = [];
const totalTripsValues = [];

const appendValuesToArrays = (obj) => {
  Object.entries(obj).forEach(([key, value]) => {
    const propertyType = typeof value;

    if (propertyType === 'object' && value !== null) {
      appendValuesToArrays(value);
    } else {
      switch (key) {
        case 'long':
          longValues.push(parseFloat(value.toFixed(4)));
          break;
        case 'lat':
          latValues.push(parseFloat(value.toFixed(4)));
          break;
        case 'total_trips':
          totalTripsValues.push(value);
          break;
      }
    }
  });
};

appendValuesToArrays(trips_agg);

longValues.sort();
latValues.sort();
totalTripsValues.sort();


const findMinMax = (arr) => {
  const min = Math.min(...arr);
  const max = Math.max(...arr);
  return { min, max };
};


const latRange = findMinMax(latValues);
const lonRange = findMinMax(longValues);
const gridStep = 0.0001;
const grid = [];

for (let lat = latRange.min; lat <= latRange.max; lat += gridStep) {
  for (let lon = lonRange.min; lon <= lonRange.max; lon += gridStep) {
    grid.push({ lat: parseFloat(lat.toFixed(4)), lon: parseFloat(lon.toFixed(4)), totalTrips: 0 });
  }
}


for (let c = 0; c < totalTripsValues.length; c++){
    const targetIndex = grid.findIndex((point) => point.lat === latValues[c] && point.lon === longValues[c]);

    if (targetIndex !== -1) {
        grid[targetIndex].totalTrips = totalTripsValues[c];
    }
}

function calculateDistance(lat1, lon1, lat2, lon2) {
  const R = 6371; // Radius of the Earth in kilometers
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  const distance = R * c; // Distance in kilometers

  return distance;
}

function toRadians(degrees) {
  return degrees * (Math.PI / 180);
}

const radius = 0.01; // Adjust the radius as needed
const constantFactor = 100; // Adjust the constant factor as needed

// Iterate through the grid
for (let i = 0; i < grid.length; i++) {
  const gridPoint = grid[i];

  // Check if totalTrips is zero
  if (gridPoint.totalTrips === 0) {
    let closestDistance = Number.MAX_VALUE;

    // Iterate through the stations to find the closest one
    for (let j = 0; j < latValues.length; j++) {
        const stationLat = latValues[j];
        const stationLon = longValues[j];
        const trips = totalTripsValues[j]
        const distance = calculateDistance(gridPoint.lat, gridPoint.lon, stationLat, stationLon);

        // Check if the distance is within the radius and closer than the previous closest
        if (distance <= radius && distance < closestDistance) {
            closestDistance = distance;
            // Update the totalTrips based on the distance and a constant factor
            gridPoint.totalTrips = Math.round(distance * constantFactor * trips);
      }
    }
  }
}

const uniqueLatSet = new Set(grid.map(point => point.lat));
const uniqueLonSet = new Set(grid.map(point => point.lon));

const latArray = Array.from(uniqueLatSet);
const lonArray = Array.from(uniqueLonSet);
const tripsArray = grid.map(point => [point.lon, point.lat, point.totalTrips]);

console.log('Lat Array:', latArray);
console.log('Lon Array:', lonArray);
console.log('Total Trips Array:', tripsArray);

// console.log(grid);
// const nonZeroTripsCount = grid.filter((point) => point.totalTrips !== 0).length;
// console.log(`Number of values in grid with non-zero totalTrips: ${nonZeroTripsCount}`);



let options = {
    tooltip: {},
    xAxis: {
        type: 'numeric',
        data: lonArray
    },
    yAxis: {
        type: 'numeric',
        data: latArray
    },
    visualMap: {
        min: 0,
        max: 200,
        calculable: true,
        realtime: false,
        inRange: {
        color: [
            '#313695',
            '#4575b4',
            '#74add1',
            '#abd9e9',
            '#e0f3f8',
            '#ffffbf',
            '#fee090',
            '#fdae61',
            '#f46d43',
            '#d73027',
            '#a50026'
        ]
        }
    },
    series: [
        {
        name: 'Gaussian',
        type: 'heatmap',
        data: tripsArray,
        emphasis: {
            itemStyle: {
            borderColor: '#333',
            borderWidth: 1
            }
        },
        progressive: 1000,
        animation: false
        }
    ]
}
</script>

<ECharts queryID=trips_agg data=trips_agg config={options}/>

