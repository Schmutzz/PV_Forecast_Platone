from os import listdir
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from tqdm import tqdm
import json
import requests

engine = create_engine('sqlite://', echo=False)

folders_path = 'C:/Users/bo112/E.ON/Platone HAW groupwork - raw data'
conn = sqlite3.connect('data/input_data.db')
c = conn.cursor()


def import_baseline():
    baseline_path = '/eIOT-iONS_baseline/PTEI_Baseline_1_Minutes_Mean_2022.csv'
    df_baseline = pd.read_csv(f'{folders_path}{baseline_path}', sep=';', decimal=',', skiprows=[1])
    df_baseline['Timestamp'] = pd.to_datetime(df_baseline['Timestamp'], format='%Y-%m-%df %H:%M:%S')
    df_baseline.set_index('Timestamp', inplace=True)
    df_baseline.rename(columns={'PTEI - Baseline (Calculated) - 1-Minutes Mean': 'Baseline in kW'}, inplace=True)
    df_baseline.to_sql('baseline', con=conn)


def import_slp():
    slp_path = '/standard-load-profile/tshouseholdUTC2022.csv'
    df_slp = pd.read_csv(f'{folders_path}{slp_path}', sep=',', decimal='.')
    df_slp['Timestamp'] = pd.to_datetime(df_slp['RowKey'].str[:-6], format='%Y-%m-%df %H:%M:%S')
    df_slp.drop(columns=['RowKey'], inplace=True)
    df_slp.set_index('Timestamp', inplace=True)
    df_slp.to_sql('slp', con=conn)


def import_households():
    folder = 'household-batteries'
    files = [f'{folders_path}/{folder}/{f}' for f in listdir(f'{folders_path}/{folder}')]
    df_table = pd.DataFrame(columns=['Timestamp'])
    for file in files:
        df = pd.read_csv(file, sep=',', decimal='.', skiprows=1)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%df %H:%M:%S')
        df_table = pd.merge(df_table, df, on='Timestamp', how='outer')
    # only use dates with data for all households
    df_table.dropna(inplace=True)
    df_table.set_index('Timestamp', inplace=True)
    df_table.to_sql('household_batteries', con=conn)


def import_mb_basic():
    folder = 'MB-basic'
    files = [f'{folders_path}/{folder}/{f}' for f in listdir(f'{folders_path}/{folder}')]
    df_table = pd.DataFrame()
    for file in tqdm(files):
        date = file.split(sep='basic_')[1][:10]
        df = pd.read_csv(file, sep=',', decimal='.')
        df = df.loc[df['time'].str.contains(date)]
        df['Timestamp'] = pd.to_datetime(df['time'].str[:16], format='%Y-%m-%dT%H:%M')
        df.drop(columns=['time'], inplace=True)
        df.set_index('Timestamp', inplace=True)
        df_table = pd.concat([df_table, df])
    df_table.to_sql('mb_basic', con=conn)


def import_mb_clouds():
    folder = 'MB-clouds'
    files = [f'{folders_path}/{folder}/{f}' for f in listdir(f'{folders_path}/{folder}')]
    df_table = pd.DataFrame()
    for file in tqdm(files):
        date = file.split(sep='clouds_')[1][:10]
        df = pd.read_csv(file, sep=',', decimal='.')
        df = df.loc[df['time'].str.contains(date)]
        df['Timestamp'] = pd.to_datetime(df['time'].str[:16], format='%Y-%m-%dT%H:%M')
        df.drop(columns=['time'], inplace=True)
        df.set_index('Timestamp', inplace=True)
        df_table = pd.concat([df_table, df])
    df_table.to_sql('mb_clouds', con=conn)


def import_mb_solar():
    folder = 'MB-solar'
    files = [f'{folders_path}/{folder}/{f}' for f in listdir(f'{folders_path}/{folder}')]
    df_table = pd.DataFrame()
    for file in tqdm(files):
        date = file.split(sep='solar_')[1][:10]
        df = pd.read_csv(file, sep=',', decimal='.')
        df = df.loc[df['time'].str.contains(date)]
        df['Timestamp'] = pd.to_datetime(df['time'].str[:16], format='%Y-%m-%dT%H:%M')
        df.drop(columns=['time'], inplace=True)
        df.set_index('Timestamp', inplace=True)
        df_table = pd.concat([df_table, df])
    df_table.to_sql('mb_solar', con=conn)


def import_mb_sunmoon():
    folder = 'MB-sunmoon'
    files = [f'{folders_path}/{folder}/{f}' for f in listdir(f'{folders_path}/{folder}')]
    df_table = pd.DataFrame()
    for file in tqdm(files):
        date = file.split(sep='sunmoon_')[1][:10]
        df = pd.read_csv(file, sep=',', decimal='.')
        df = df.loc[df['time'].str.contains(date)]
        df['Timestamp'] = pd.to_datetime(df['time'].str[:16], format='%Y-%m-%dT%H:%M')
        df.drop(columns=['time'], inplace=True)
        df.set_index('Timestamp', inplace=True)
        df_table = pd.concat([df_table, df])
    df_table.to_sql('mb_sunmoon', con=conn)


def import_mb_pvpro4():
    folder = 'MB-pvpro_4'
    files = [f'{folders_path}/{folder}/{f}' for f in listdir(f'{folders_path}/{folder}')]
    df_table = pd.DataFrame()
    for file in tqdm(files):
        date = file.split(sep='PVpro_4_')[1][:10]
        with open(file) as f:
            data = json.load(f)
        df = pd.DataFrame(data.get('data_1h', data.get('data_xmin')))
        df = df.loc[df['time'].str.contains(date)]
        df['Timestamp'] = pd.to_datetime(df['time'].str[:16], format='%Y-%m-%dT%H:%M')
        df.drop(columns=['time'], inplace=True)
        df.set_index('Timestamp', inplace=True)
        df_table = pd.concat([df_table, df])
    df_table.to_sql('mb_pvpro_1h', con=conn)


def import_mb_pvpro5():
    folder = 'MB-pvpro_5'
    files = [f'{folders_path}/{folder}/{f}' for f in listdir(f'{folders_path}/{folder}')]
    df_table = pd.DataFrame()
    for file in tqdm(files):
        date = file.split(sep='PVpro_5_')[1][:10]
        if date in ['2022-07-14', '2022-07-15']:
            continue
        with open(file) as f:
            data = json.load(f)
        df = pd.DataFrame(data.get('data_xmin'))
        df = df.loc[df['time'].str.contains(date)]
        df['Timestamp'] = pd.to_datetime(df['time'].str[:16], format='%Y-%m-%dT%H:%M')
        df.drop(columns=['time'], inplace=True)
        df.set_index('Timestamp', inplace=True)
        df_table = pd.concat([df_table, df])
    df_table.to_sql('mb_pvpro_15min', con=conn)


def import_wunderground():
    api_key = 'b2d5c5b846d9403595c5b846d99035ee'
    result_df = pd.DataFrame()

    for date in tqdm(pd.date_range(start='2022-05-01', end='2022-11-15').tolist()):
        date = f'{date.year}{date.month:02d}{date.day:02d}'
        url = f'https://api.weather.com/v2/pws/history/all?stationId=ITWIST25&format=json&units=m&date={date}&apiKey={api_key}'
        json_obj = json.loads(requests.get(url=url).text)

        for step in json_obj.get('observations'):
            new_row = {'Date': step.get('obsTimeLocal'),
                       'solar_radiation': step.get('solarRadiationHigh'),
                       'avg_wind_dir': step.get('winddirAvg')}
            new_row.update(step.get('metric'))
            result_df = pd.concat([result_df, pd.DataFrame(new_row, index=[0])], ignore_index=True)

    result_df['Date'] = pd.to_datetime(result_df['Date'], format='%Y-%m-%d %H:%M:%S')
    result_df.to_sql('wunderground_historical_25', con=conn)

import_wunderground()
