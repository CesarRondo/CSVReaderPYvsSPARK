from pyspark.sql import SparkSession

# Initialize Spark context
spark = SparkSession.builder.appName("FlightDelaysRDD").getOrCreate()
sc = spark.sparkContext  

# Read CSV file into RDD
file_path = "Flight Delays/2010.csv"
rdd = sc.textFile(file_path)

# Extract header
header = rdd.first()
rdd = rdd.filter(lambda line: line != header)  # Remove header row

# Split CSV lines into columns
rdd = rdd.map(lambda line: line.strip().split(","))

# Extract ORIGIN and WEATHER_DELAY columns   *add more depending if needed
ORIGIN_INDEX = 3   # Column index for ORIGIN
WEATHER_DELAY_INDEX = 24   # Column index for WEATHER_DELAY

# Convert WEATHER_DELAY to float, if no entry then 0
rdd = rdd.map(lambda row: (row[ORIGIN_INDEX], float(row[WEATHER_DELAY_INDEX]) if row[WEATHER_DELAY_INDEX].strip() else 0))

# Group by ORIGIN and sum the WEATHER_DELAY
weather_delays = rdd.reduceByKey(lambda a, b: a + b)

# Sort by WEATHER_DELAY in descending order
weather_delays_sorted = weather_delays.sortBy(lambda x: x[1], ascending=False)

# Print sorted results
for origin, delay in weather_delays_sorted.collect():
    print(f"Airport: {origin}, Total Weather Delay: {delay}")

# Stop Spark session
spark.stop()
