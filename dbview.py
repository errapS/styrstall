import duckdb
import pandas as pd
import folium

db_connection = duckdb.connect(database='styrstall_dbt/dev.duckdb')
query="""
    select * from main.int_stations limit 5;
 """
result = db_connection.execute(query)

columns = [column[0] for column in result.description]

rows = result.fetchall()

df = pd.DataFrame(rows, columns=columns)

print(df)
db_connection.close()
