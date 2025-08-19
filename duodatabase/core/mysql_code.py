import pymysql
import pandas as pd
from itertools import product
from sqlalchemy import create_engine
from .to_neo4j import *
from .data_preprocess import convert_to_desired_format

def to_database(data, column_structure, mysql_engine_data):
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user=mysql_engine_data[0], pw=mysql_engine_data[1], db=mysql_engine_data[2]))
    selected_df = pd.DataFrame()
    column_list = []
    query_list = []
    if column_structure==[]:
        if data.empty:
            pass
        else:
            table_name = str(data.loc[0]['uuid'])
            data.to_sql(table_name, con=engine, if_exists='replace', chunksize=1000)
        return

    for n in range(len(column_structure[0])):
        column_list.append(list(set(data[column_structure[0][n]])))
    column_combi = product(*column_list)
    for combi in column_combi:
        first = True
        combi_list = list(combi)
        query = ''
        for i in range(len(combi_list)):
            if first:
                first = False
                query = "`{}`=='{}'".format(column_structure[0][i], combi_list[i])
            else:
                query = query + " & `{}`=='{}'".format(column_structure[0][i], combi_list[i])
        query_list.append(query)

    for query in query_list:
        time_exist = False
        selected_df = data.query(query)
        for i in selected_df.columns:
            if (selected_df[i].dtype == 'float64') or (selected_df[i].dtype == 'int64'):  # determine if is number
                pass
            elif i=='TIMESTAMP':
                for index, row in selected_df.iterrows():
                    date_string = row['TIMESTAMP']
                    date_object = convert_to_desired_format(date_string,"%d.%m.%Y %H:%M")
                    selected_df.at[index, 'TIMESTAMP'] = date_object
                    #selected_df.loc[i]['TIMESTAMP']= pd.to_datetime(selected_df.loc[i]['TIMESTAMP'], format="%d.%m.%Y %H:%M")
                #selected_df['TIMESTAMP'] = pd.to_datetime(selected_df['TIMESTAMP'], format="%d.%m.%Y %H:%M")
                pass
            elif i=='VALUE':
                selected_df['VALUE'] = selected_df['VALUE'].str.replace(',', '.').astype(float)
                pass
            elif (check_is_datetime(selected_df[i].reset_index(drop=True), time_mark='.')):  # determine if is date
                if time_exist:  # duplicated time column should be deleted
                    selected_df = selected_df.drop(i, axis=1)
                time_exist = True
                pass
            elif i=='uuid':
                pass
            else:
                selected_df = selected_df.drop(i, axis=1)
        selected_df = selected_df.reset_index(drop=True)
        if selected_df.empty:
            pass
        else:
            table_name = str(selected_df.loc[0]['uuid'])
            selected_df.to_sql(table_name, con=engine, if_exists='replace', chunksize=1000)
    return

def query(id, time, cursor):
    if not id:
        return
    query_result = []
    date_object = convert_to_desired_format(time,"%d.%m.%Y %H:%M")
    cursor.execute("SELECT VALUE From `{id}` WHERE TIMESTAMP='{time}'".format(id=id,time=date_object))
    result = cursor.fetchall()
    for i in result:
        query_result.append(i[0])
    return query_result

def query_timerange(id, time_range, cursor):
    if not id:
        return
    start_time = time_range[0]
    start_time_object = convert_to_desired_format(start_time,"%d.%m.%Y %H:%M")
    end_time = time_range[1]
    end_time_object = convert_to_desired_format(end_time,"%d.%m.%Y %H:%M")
    cursor.execute("SELECT * FROM `{id}` WHERE TIMESTAMP BETWEEN '{start_time}' AND '{end_time}'".format(id=id,start_time=start_time_object,end_time=end_time_object))
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    if df.empty:
        return
    df.columns = [desc[0] for desc in cursor.description]
    df = df[['TIMESTAMP','VALUE']]
    return df

def query_timeaverage(id, time_range, cursor):
    if not id:
        return
    start_time = time_range[0]
    start_time_object = convert_to_desired_format(start_time,"%d.%m.%Y %H:%M")
    end_time = time_range[1]
    end_time_object = convert_to_desired_format(end_time,"%d.%m.%Y %H:%M")
    cursor.execute("SELECT AVG(VALUE) AS average_value FROM `{id}` WHERE TIMESTAMP BETWEEN '{start_time}' AND '{end_time}'".format(id=id,start_time=start_time_object,end_time=end_time_object))
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    if df.empty:
        return
    # Set column names based on cursor description
    df.columns = [desc[0] for desc in cursor.description]
    return df.iloc[0]['average_value']

def count_database_size(cursor, mysql_database_name):
    sql = """
        SELECT ROUND(
            COALESCE(SUM(COALESCE(data_length,0) + COALESCE(index_length,0)), 0) / 1024
        , 1) AS size_kb
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE';
    """
    cursor.execute(sql, (mysql_database_name,))
    row = cursor.fetchone()
    return float(row[0] or 0.0)

