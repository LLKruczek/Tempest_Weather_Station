# Tempest_Weather_Station
Sample script for Tempest Weather Station users to download data from Tempest website and integrate it into their own databases.
To download data from your weather station, add your credentials, including device id, token, etc. You will also need to create an SQL database. The SQL insert query in weather_station.py simply inserts all data in to a table with the same amount of columns and is for demonstration. You can modify the SQL insert query to fit your own database. 
To run the script, type python3 run_weather_station.py into the command line. 
