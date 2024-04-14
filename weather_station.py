#Author Le Li Kruczek
#Adapted from original script written by Emilee Edmonds

#import webbrowser as wb
import pandas as pd

import requests
from contextlib import closing
import csv
from codecs import iterdecode
import mysql.connector

#from datetime import datetime
#from datetime import timezone
import pytz
#import time
import logging
import logging.handlers

class weatherStation:
    def __init__(self, station_name, device_id, token):
        self.station_name=station_name
        self.device_id = device_id
        self.token = token
        self.mydb = None
        self.df = None
        self.current_time=None

    def set_current_time(self, utc_current_timestamp):
        self.current_time=utc_current_timestamp

    def insert_weather_station_data(self):
        #current_dt = datetime.now(timezone.utc)
        #utc_current_time = current_dt.replace(tzinfo=timezone.utc)
        #utc_current_timestamp = utc_current_time.timestamp()

        self.get_db_connection()
        print(self.mydb)
    
        end = int(self.current_time)
        start = int(end-86400)

        self.csv_to_df(start,end)
        self.insert_data_sql()
    
        start_previous_day = int(start-86400)
        end_previous_day = int(end-86400)

        self.csv_to_df(start_previous_day, end_previous_day)
        self.insert_data_sql()

        self.close_db_connection();

    def csv_to_df(self, start, end):
        url = "https://swd.weatherflow.com/swd/rest/observations/device/"+str(self.device_id)+"?time_start="+str(start)+"&time_end="+str(end)+"&format=csv&token="+str(self.token)
        weather_columns = []
        weather_rows = []

        i=0
        #Builds the DataFrame from the link info
        with requests.get(url, stream=True) as r:
            lines = (line.decode('utf-8') for line in r.iter_lines())
            for weather in csv.reader(lines):
                if i==0:
                    weather_columns.append(weather)
                    i=1
                else:
                    del weather[-1]
                    weather_rows.append(weather)

        self.df = pd.DataFrame(weather_rows, columns = weather_columns)
        print(self.df)
        return self.df

    def get_db_connection(self):
        self.mydb=mysql.connector.connect(
            host="#Your SQL Host#",
            user="#Your Username#",
            password="#Your Password#",
            database="#Your Database Name#",
        )
    
    def close_db_connection(self):
        self.mydb.close()

    def insert_data_sql(self):
        cursor = self.mydb.cursor()

        table_name="#your table name#"       
 
        create_table_query=f"""
            CREATE TABLE If NOT EXISTS {table_name} (
                `device_id` VARCHAR(50), 
                `type` VARCHAR(50), 
                `bucket_step_minutes` VARCHAR(50), 
                `timestamp` VARCHAR(50) PRIMARY KEY, 
                `wind_lull` VARCHAR(50), 
                `wind_avg` VARCHAR(50), 
                `wind_gust` VARCHAR(50), 
                `wind_dir` VARCHAR(50), 
                `wind_interval` VARCHAR(50), 
                `pressure` VARCHAR(50), 
                `temperature` VARCHAR(50), 
                `humidity` VARCHAR(50), 
                `lux` VARCHAR(50), 
                `uv` VARCHAR(50), 
                `solar_radiation` VARCHAR(50), 
                `precip` VARCHAR(50), 
                `precip_type` VARCHAR(50),
                `strike_distance` VARCHAR(50), 
                `strike_count` VARCHAR(50), 
                `battery` VARCHAR(50), 
                `report_interval` VARCHAR(50), 
                `local_daily_precip` VARCHAR(50), 
                `precip_final` VARCHAR(50), 
                `local_daily_precip_final` VARCHAR(50), 
                `precip_analysis_type` VARCHAR(50),
                UNIQUE KEY (`timestamp`)
            )
        """

        cursor.execute(create_table_query)
        # Insert DataFrame records into the table

        for _, row in self.df.iterrows():
            insert_query = f"""
                INSERT IGNORE INTO {table_name} (
                `device_id`, 
                `type`, 
                `bucket_step_minutes`, 
                `timestamp`, 
                `wind_lull`, 
                `wind_avg`, 
                `wind_gust`, 
                `wind_dir`, 
                `wind_interval`, 
                `pressure`, 
                `temperature`, 
                `humidity`, 
                `lux`, 
                `uv`, 
                `solar_radiation`, 
                `precip`, 
                `precip_type`,
                `strike_distance`, 
                `strike_count`, 
                `battery`, 
                `report_interval`, 
                `local_daily_precip`, 
                `precip_final`, 
                `local_daily_precip_final`, 
                `precip_analysis_type`
                )
                VALUES (
                '{row['device_id']}', 
                '{row['type']}', 
                '{row['bucket_step_minutes']}', 
                '{row['timestamp']}', 
                '{row['wind_lull']}', 
                '{row['wind_avg']}', 
                '{row['wind_gust']}', 
                '{row['wind_dir']}', 
                '{row['wind_interval']}', 
                '{row['pressure']}', 
                '{row['temperature']}', 
                '{row['humidity']}', 
                '{row['lux']}', 
                '{row['uv']}', 
                '{row['solar_radiation']}', 
                '{row['precip']}', 
                '{row['precip_type']}', 
                '{row['strike_distance']}', 
                '{row['strike_count']}', 
                '{row['battery']}', 
                '{row['report_interval']}', 
                '{row['local_daily_precip']}', 
                '{row['precip_final']}', 
                '{row['local_daily_precip_final']}', 
                '{row['precip_analysis_type']}'
                )
            """
            #print(insert_query)
            cursor.execute(insert_query)

        # Commit the changes to the database
        self.mydb.commit()
        print("Data has been successfully written to the MySQL database.")
        cursor.close()

