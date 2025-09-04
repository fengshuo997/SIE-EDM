import time
from nltk import tag
import pandas as pd
from .to_neo4j import *
from .mysql_code import *
import rdfizer
import subprocess

def query_experiment_duodatabase(neo4j_g, mysql_conn,data_sample, column_structure, ta, cta, mysql_engine_data, matchname1, matchname2, Timestamp, Timerange, experiment_result, n):
  db_generate_start = time.time()
  neo4j_g.run("MATCH (n) DETACH DELETE n")
  kg = csv_to_neo4j(neo4j_g, data=data_sample, ta=ta, cta=cta, column_structure=column_structure)
  to_mysqldatabase(data_sample, column_structure, mysql_engine_data)
  db_generate_end = time.time()
  experiment_result.loc[n, 'Database Generate time (methode_doubleDatabases) [s]'] = db_generate_end - db_generate_start
  print('Response time for generate database of double databases methode: ', db_generate_end - db_generate_start)
  neo4j_database_size = neo4j_databasesize_count(kg)
  mysql_database_size = count_database_size(conn=mysql_conn, mysql_database_name=mysql_engine_data[2])
  total_database_size = neo4j_database_size + mysql_database_size
  experiment_result.loc[n, 'Database size (methode_doubleDatabases) [KB]'] = total_database_size
  print('Database size of double databases methode: ', total_database_size,'KB')
  with mysql_conn.cursor() as mysql_cursor:
    match_start = time.time()
    match = match_cypher(kg, matchname1=matchname1, matchname2=matchname2)
    neo4j_end = time.time()
    double_tp_value = query(id=match, time=Timestamp, cursor=mysql_cursor)
    match_end = time.time()
    experiment_result.loc[n, 'Query time-point (methode_doubleDatabases)'] = match_end-match_start
    print('The query result of double databases methode for time point: ',double_tp_value,' Response time: ',match_end-neo4j_end)
    print('Response time for neo4j match: ',neo4j_end - match_start)
      # Query time-range (methode_doubleDatabases)
    match_start = time.time()
    match = match_cypher(kg, matchname1=matchname1, matchname2=matchname2)
    double_tr_value = query_timerange(id=match, time_range=Timerange, cursor=mysql_cursor)
    match_end = time.time()
    experiment_result.loc[n, 'Query time-range (methode_doubleDatabases)'] = match_end - match_start
    print('The query result of double databases methode for time range: ')
    print(double_tr_value)
    print('Response time: ', match_end - match_start)
      # Query time-average (methode_doubleDatabases)
    match_start = time.time()
    match = match_cypher(kg, matchname1=matchname1, matchname2=matchname2)
    double_ta_value = query_timeaverage(id=match, time_range=Timerange, cursor=mysql_cursor)
    match_end = time.time()
    experiment_result.loc[n, 'Query time-average (methode_doubleDatabases)'] = match_end - match_start
    print('The query result of double databases methode for average in time range: ')
    print(double_ta_value)
    print('Response time: ',match_end - match_start)
    # Close MySQL connection at the end of each loop
  mysql_cursor.close()
  return experiment_result   

def do_query_experiment(data=pd.DataFrame, neo4j_g=None, mysql_cursor=None, mysql_engine_data = [], mysql_database_name=None, ta=None, cta=None, column_structure=None, matchname=None, query_time=None, datasize=120000,sp=1):
    datasize_sample = [int(datasize/sp * i) for i in range(sp+1)]
    print(datasize_sample)

    experiment_result = pd.DataFrame(columns=['Database Generate time (methode_doubleDatabases) [s]',
                                              'Database Generate time (methode_singleDatabase) [s]',
                                              'Database size (methode_doubleDatabases) [KB]',
                                              'Database size (methode_singleDatabase) [KB]',
                                              'Query time-point (methode_doubleDatabases)',
                                              'Query time-point (methode_singleDatabase)',
                                              'Query time-range (methode_doubleDatabases)',
                                              'Query time-range (methode_singleDatabase)',
                                              'Query time-average (methode_doubleDatabases)',
                                              'Query time-average (methode_singleDatabase)'],
                                     index=datasize_sample)

    for n in datasize_sample:
        print('datasize:',n)
        
        # Reconnect and reset to MySQL at the beginning of each loop
        import pymysql
        mysql_conn = pymysql.connect(port=3306, user=mysql_engine_data[0], password=mysql_engine_data[1], database=mysql_engine_data[2])
        db_name = mysql_engine_data[2]
        with mysql_conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
            cursor.execute(f"CREATE DATABASE {db_name}")
        mysql_conn.commit()
        mysql_conn = pymysql.connect(port=3306, user=mysql_engine_data[0], password=mysql_engine_data[1], database=mysql_engine_data[2])
        
        # select experiment data sample
        matchname1 = matchname[0]
        matchname2 = matchname[1]
        matchname3 = matchname[2]
        Timestamp = query_time['timestamp']
        Timerange = query_time['range']
        tagetrow = data.loc[(data['ENERGY_SOURCE'] == matchname1) & (data['MEASURE'] == matchname2) & (data['TIMESTAMP'] == matchname3)]
        if n==0:
            experiment_result.loc[0] = 0
            # Close connection before continue
            mysql_cursor.close()
            mysql_conn.close()
            continue
        data_sample = pd.concat([data.sample(n-1), tagetrow], ignore_index=True).dropna(axis=0, how='any')

        #  experiment by double databases
        experiment_result = query_experiment_duodatabase(neo4j_g, mysql_conn,data_sample, column_structure, ta, cta, mysql_engine_data, matchname1, matchname2, Timestamp, Timerange, experiment_result, n)

    return experiment_result

