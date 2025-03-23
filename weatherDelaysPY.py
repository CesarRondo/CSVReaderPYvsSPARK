import pandas as pd
import time

start_time = time.time()

# read_start_time = time.time()
df = pd.read_csv("Flight Delays/2018.csv")
df["WEATHER_DELAY"] = pd.to_numeric(df["WEATHER_DELAY"], errors="coerce").fillna(0)
read_end_time = time.time()

group_start_time = time.time()
weather_delays = df.groupby("ORIGIN")["WEATHER_DELAY"].sum().reset_index()
group_end_time = time.time()

sort_start_time = time.time()
weather_delays_sorted = weather_delays.sort_values(by="WEATHER_DELAY", ascending=False)  #.head(50) to see the 50 airports with most weather delays
sort_end_time = time.time()

end_time = time.time()

read_time = read_end_time - start_time
grouping_time = group_end_time - group_start_time
sorting_time = sort_end_time - sort_start_time
total_time = end_time - start_time

print(f"Time to read the csv fle: {read_time:.4f} seconds")
print(f"Time to group and sum weather delays: {grouping_time:.4f} seconds")
print(f"Time to sort the delays: {sorting_time:.4f} seconds")
print(f"Total execution time: {total_time:.4f} seconds")