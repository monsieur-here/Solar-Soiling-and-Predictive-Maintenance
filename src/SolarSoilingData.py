import duckdb
import asyncio
# import aiohttp

# Unique API key for NREL services
API_KEY = "glLYjww0ygV3WVeWTF53rz57q1IJyPy5ynDmhfrG"

print("Starting Solar Soiling Data Fetch and Process...")
# Aim to perform asynchronous data fetching for solar soiling analysis using periodic fetching
async def solar_fetch_soiling_data(session, url):
    async with session.get(url) as response:
        await asyncio.sleep(1)
        data = await response.json()
        return data
    
async def main():
    #Using API endpoints for solar soiling data
    urls = [
        f"https://developer.nrel.gov/api/pvwatts/v8.json?api_key={API_KEY}&azimuth=180&system_capacity=4&losses=14&array_type=1&module_type=0&gcr=0.4&dc_ac_ratio=1.2&inv_eff=96.0&radius=0&dataset=nsrdb&tilt=10&lat=40&lon=-105&soiling=12|4|45|23|9|99|67|12.54|54|9|0|7.6&albedo=0.3&bifaciality=0.7",
        "https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-download.json"
    ]
    
    # Using PSM3 solar data from NSRDB for soiling analysis for these years below
    years = ["2018", "2019", "2020", "2021", "2022", "2023", "2024"]
    