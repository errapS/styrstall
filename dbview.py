import duckdb
import pandas as pd
import folium

db_connection = duckdb.connect(database='styrstall_dbt/dev.duckdb')


query = """SELECT 
                CONCAT(extract(year from start_time), '-',
                extract(month from start_time), '-',
                extract(day from start_time), ' ',
                extract(hour from start_time), ':00:00')::timestamp as time, 
                COUNT(*) as trips 
            FROM 
                main_mart.trips
            GROUP BY
                extract(year from start_time),
                extract(month from start_time),
                extract(day from start_time),
                extract(hour from start_time)
            ORDER BY
                time asc
  """

result = db_connection.execute(query)


columns = [column[0] for column in result.description]

rows = result.fetchall()

db_connection.close()

df = pd.DataFrame(rows, columns=columns)

print(df)