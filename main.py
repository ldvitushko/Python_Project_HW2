from datetime import timezone, datetime, timedelta
import requests
import json
import pandas as pd
import ibm_db_dbi as db


def createConnection():
    conn = db.connect('DATABASE=IBA_EDU;'
                      'HOSTNAME=3d-edu-db.icdc.io;'
                      'PORT=8163;'
                      'PROTOCOL=TCPIP;'
                      'UID=stud06;'
                      'PWD=12345;', '', '')
    return conn


def get_weather_data(current_date):
    url = 'http://api.openweathermap.org/data/2.5/onecall/timemachine'

    town_latitude = 53.89
    town_longitude = 27.56
    token = '74b2ef6b90fab008b89fa81a47a84e80'

    json_list = []

    dt = current_date
    date_time = dt.replace(tzinfo=timezone.utc).timestamp()

    params = {
        'lat': town_latitude,
        'lon': town_longitude,
        'dt': int(date_time),
        'units': 'metric',
        'appid': token
    }

    json_list.append(json.loads(requests.get(url, params=params).text))

    return json_list


def data_to_lst(data, df_lst=[]):
    for item in data:
        df_dict = {}

        df_dict['LAT'] = item['lat']
        df_dict['LON'] = item['lon']
        df_dict['DT'] = str(datetime.fromtimestamp(item['hourly'][9]['dt']))
        df_dict['TEMP'] = item['hourly'][9]['temp']
        df_dict['PRESSURE'] = item['hourly'][9]['pressure']
        df_dict['CLOUDS'] = item['hourly'][9]['clouds']
        df_dict['WIND_SPEED'] = item['hourly'][9]['wind_speed']

        df_lst.append(df_dict)


def add_data_to_db():
    json_data = get_weather_data(datetime.now() - timedelta(days=1))
    json_lst = []
    data_to_lst(json_data, json_lst)

    df = pd.DataFrame(json_lst)

    conn = createConnection()
    cur = conn.cursor()
    s = 'INSERT INTO WEATHER_DATA (' + ', '.join(df.columns) + ') VALUES (' + ('?,' * len(df.columns))[:-1] + ');'

    for i, r in df.iterrows():
        param_lst = []
        for col in df.columns:
            if r[col] and not pd.isnull(r[col]) and r[col] != '':
                param_lst.append(r[col])
            else:
                param_lst.append(None)
        try:
            cur.execute(s, param_lst)
        except Exception as e:
            print(e)
            print(param_lst)
    cur.execute('COMMIT')


def get_data_from_db():
    return pd.read_sql('SELECT * FROM WEATHER_DATA', con=createConnection())