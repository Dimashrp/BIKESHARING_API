from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd
app = Flask(__name__) 

##Created Flask App
@app.route('/')
@app.route('/homepage')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trip = get_all_trips(conn)
    return trip.to_json()

@app.route('/trips/<trip_id>')
def route_trip_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id, conn)
    return trip.to_json()

@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result


@app.route('/trips/average_duration') 
def route_avg_duration():
    conn = make_connection()
    avg_dur = get_avg_duration(conn)
    return avg_dur.to_json()

@app.route('/trips/average_duration/<bike_id>')
def route_avg_bike_id(bike_id):
    conn = make_connection()
    trip = get_avg_bike_id(bike_id, conn)
    return trip.to_json()

@app.route('/trips/end_sid')
def route_end_station_id():
    req = request.get_json(force=True)
    end_sid = req['end_station_id']
    conn = make_connection()
    trip = get_end_station_id_info1(end_sid, conn)
    return trip.to_json()


@app.route('/trips/end_sid/<end_sid>')
def route_end_station_id_info(end_sid):
    conn = make_connection()
    trip = get_end_station_id_info2(end_sid, conn)
    return trip.to_json()

@app.route('/json') 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

####function

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection


##Created functionality to read or get specific data from the database
def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_all_trips(conn):
    query_trip = f"""SELECT * FROM trips"""
    result_trip = pd.read_sql_query(query_trip, conn)
    return result_trip

def get_trip_id(trip_id, conn):
    query_trip = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result_trip = pd.read_sql_query(query_trip, conn)
    return result_trip

##Created functionality to input new data into each table for the databases
def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'


def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'


##Created static endpoints which return analytical result (must be different from point 2,3)
def get_avg_duration(conn):
    query = f"""SELECT AVG(DURATION_MINUTES) as Average_Duration_Minutes FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

#Created dynamic endpoints which return analytical result (must be different from point 2,3,4)
def get_avg_bike_id(bike_id, conn):
    query = f"""SELECT AVG(DURATION_MINUTES) Average_Duration_Minutes_for_bike_id_{bike_id} FROM TRIPS WHERE bikeid = {bike_id}"""
    result = pd.read_sql_query(query, conn)
    return result

#Created POST endpoint which receive input data, then utilize it to get analytical result (must be different from point 2,3,4,5)
###fungsi untuk melihat jumlah sepeda dan rata-rata durasi terhadap inputan id statsiun terakhir
def get_end_station_id_info1(end_sid, conn):
    query = f"""SELECT * FROM TRIPS WHERE end_station_id = {end_sid}"""
    bike_sii= pd.read_sql_query(query, conn)
    result = bike_sii.groupby('end_station_id').agg(
        {
            'bikeid' : 'count', 
            'duration_minutes' : 'mean'
        }
    )
    return result

def get_end_station_id_info2(end_sid, conn):
    query = f"""SELECT * FROM TRIPS WHERE end_station_id = {end_sid}"""
    bike_sii= pd.read_sql_query(query, conn)
    result = bike_sii.groupby('end_station_id').agg(
        {
            'bikeid' : 'count', 
            'duration_minutes' : 'mean'
        }
    )
    return result


if __name__ == '__main__':
    app.run(debug=True, port=5000)

