import pandas as pd
import mysql.connector
import json
import numpy as np

# Citește fișierul JSON
with open(r'D:\AN4\PBD\Global_YouTube_Statistics.json', 'r') as f:
    data = json.load(f)

# Înlocuiește 'nan' cu None (null)
for entry in data:
    for key, value in entry.items():
        if isinstance(value, str) and value.lower() == 'nan':
            entry[key] = None  # Înlocuiește cu None (null)
        elif value == '':
            entry[key] = None  # Înlocuiește șirurile goale cu None

# Crează un DataFrame din datele curate
df = pd.json_normalize(data)

# Conectarea la baza de date
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='tutorial0',
    database='youtubeschema'
)

cursor = connection.cursor()

# Inserarea datelor
for index, row in df.iterrows():
    cursor.execute(""" 
        INSERT INTO youtubeschema.temp_StagingYouTubeStats 
        (`rank`, Youtuber, subscribers, `video views`, category, Title, uploads, Country, Abbreviation, channel_type, video_views_rank, country_rank, channel_type_rank, video_views_for_the_last_30_days, lowest_monthly_earnings, highest_monthly_earnings, lowest_yearly_earnings, highest_yearly_earnings, subscribers_for_last_30_days, created_year, created_month, created_date, `Gross tertiary education enrollment (%)`, Population, `Unemployment rate`, Urban_population, Latitude, Longitude) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))

# Comite modificările și închide conexiunea
connection.commit()
cursor.close()
connection.close()

print("Datele au fost încărcate cu succes!")
