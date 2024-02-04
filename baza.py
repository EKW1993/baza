 
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, DateTime

engine = create_engine('sqlite:///database.db', echo=True)

metadata = MetaData()

stations = Table('stations', metadata, 
                 Column('station', String, primary_key=True), 
                 Column('latitude', Float), 
                 Column('longitude', Float), 
                 Column('elevation', Integer), 
                 Column('name', String), 
                 Column('country', String), 
                 Column('state', String))

measure = Table('measure', metadata, 
                Column('station', String, primary_key=True), 
                Column('date', DateTime), 
                Column('precip', Float), 
                Column('tobs', Float))

metadata.create_all(engine)

with engine.connect() as conn:
    stations_data = pd.read_csv('clean_stations.csv')
    conn.execute(stations.insert(), stations_data.to_dict('records'))
    
    measure_data = pd.read_csv('clean_measure.csv')
    measure_data['date'] = pd.to_datetime(measure_data['date'], format='%Y-%m-%d')
    
    measure_data = measure_data.drop_duplicates()
    
    conn.execute(measure.insert(), measure_data.to_dict('records'))

with engine.connect() as conn:
    result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
    print(result)