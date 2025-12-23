import requests
import duckdb
import json

# --- STEP 1: INGESTION ---
print("Fetching data from NREL PVWatts v8...")
url = "https://developer.nrel.gov/api/pvwatts/v8.json"
params = {
    "api_key": "glLYjww0ygV3WVeWTF53rz57q1IJyPy5ynDmhfrG", 
    "lat": 34.05, "lon": -118.24,
    "system_capacity": 5, "azimuth": 180, "tilt": 30,
    "array_type": 1, "module_type": 1, "losses": 10,
    "timeframe": "hourly"
}

response = requests.get(url, params=params)
if response.status_code == 200:
    with open('raw_v8_baseline.json', 'w') as f:
        json.dump(response.json(), f)
else:
    print(f"Error: {response.status_code}")

# --- STEP 2: PROCESSING (The Final Fix) ---
print("Processing data with DuckDB...")
con = duckdb.connect()

# The FIX: Individual unnest() calls for each column.
# DuckDB zips these together automatically.
con.sql("""
    CREATE TABLE solar_silver AS
    WITH unnested_data AS (
        SELECT 
            range(8760) as hour_of_year,
            unnest(outputs.ac::DOUBLE[]) as expected_watts,
            unnest(outputs.poa::DOUBLE[]) as sun_intensity,
            unnest(outputs.tcell::DOUBLE[]) as panel_temp
        FROM read_json_auto('raw_v8_baseline.json')
    )
    SELECT * FROM unnested_data
    WHERE expected_watts >= 0; 
""")

# Alternative FIX: Using generate_series to index into lists.
# con.sql("""
#     CREATE TABLE solar_silver AS
#     WITH base AS (
#         SELECT 
#             outputs.ac::DOUBLE[] as ac_list,
#             outputs.poa::DOUBLE[] as poa_list,
#             outputs.tcell::DOUBLE[] as tcell_list
#         FROM read_json_auto('raw_v8_baseline.json')
#     )
#     SELECT 
#         h as hour_of_year,
#         -- DuckDB lists are 1-indexed, so we use h + 1
#         list_extract(ac_list, h + 1) as expected_watts,
#         list_extract(poa_list, h + 1) as sun_intensity,
#         list_extract(tcell_list, h + 1) as panel_temp
#     FROM base, 
#          generate_series(0, 8759) as t(h)
#     WHERE expected_watts > 0;
# """)

# --- STEP 3: EGRESS ---
print("Exporting to CSV...")
con.sql("COPY solar_silver TO 'solar_soiling_dataset.csv' (HEADER, DELIMITER ',')")

print("Success! Your dataset is ready: 'solar_soiling_dataset.csv'")

# if __name__ == '__main__':
#     start_time = time.time()
#     asyncio.run(main())
#     print(f"Total time taken: {time.time() - start_time} seconds")