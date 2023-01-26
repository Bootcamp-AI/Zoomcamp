import pandas as pd
from sqlalchemy import create_engine
from time import time

df = pd.read_csv("converted.csv", nrows=100)
df.head()


engine = create_engine('postgresql://db:db@localhost:5432/ny_taxi')
engine.connect()



print(pd.io.sql.get_schema(df, name = "yellow_taxi_data", con=engine) )


df.tpep_pickup_datetime


#Cambiar el tipo de datos tpep_pickup_datetime, tpep_dropoff_datetime

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

print(pd.io.sql.get_schema(df, name = "yellow_taxi_data", con=engine) )


from sqlalchemy import create_engine


engine = create_engine('postgresql://db:db@localhost:5432/ny_taxi')
engine.connect()


print(pd.io.sql.get_schema(df, name = "yellow_taxi_data", con=engine) )


## Colocamos el iterador

#Define un iterador para "converted" de 100000 elemenetos cada chunk
df_iter = pd.read_csv('converted.csv', iterator=True, chunksize=10000)


df_iter

df1 = next(df_iter)

df1

#Cambiar el tipo de datos tpep_pickup_datetime, tpep_dropoff_datetime
df1.tpep_pickup_datetime = pd.to_datetime(df1.tpep_pickup_datetime)
df1.tpep_dropoff_datetime = pd.to_datetime(df1.tpep_dropoff_datetime)

df1.head()

len(df1)

df1

get_ipython().run_line_magic('time', 'df1.to_sql(name="yellow_taxi_data", con=engine, if_exists=\'append\')')


while True:
    t_start = time()
    df = next(df_iter)
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    df.to_sql(name="yellow_taxi_data", con=engine, if_exists='append')
    
    t_end = time()
    
    print('chunck inserted. %.3f seconds' % (t_end-t_start))
