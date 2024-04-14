#Author Le Li Kruczek
#driver script to pull data from Tempest websites and to insert into our database. This script needs weather_station.py

from datetime import datetime
from datetime import timezone
from weather_station import weatherStation

current_dt = datetime.now(timezone.utc)
utc_current_time = current_dt.replace(tzinfo=timezone.utc)
utc_current_timestamp = utc_current_time.timestamp()

myWeatherStation_1 = weatherStation(station_name="#Your Station Name#", device_id = "#Your Device ID#", token = "#Your Tempest Token#")
myWeatherStation_1.set_current_time(utc_current_timestamp)
myWeatherStation_1.insert_weather_station_data()

myWeatherStation_2 = weatherStation(station_name="#Your Station Name#", device_id = "#Your Device ID#", token = "#Your Tempest Token#")
myWeatherStation_2.set_current_time(utc_current_timestamp)
myWeatherStation_2.insert_weather_station_data()
