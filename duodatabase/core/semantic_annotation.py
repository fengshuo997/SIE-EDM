# -*- coding: utf-8 -*-
import time
import pandas as pd
import pymysql
import docker
import sys
import os
from neo4j import GraphDatabase
from .ontology import *
from .data_preprocess import *
from .pairs_set_method import *
from .to_neo4j import *
from .data_annonation import *
from .experiment import *
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from example.config_rdf import *



def connect_neo4j(neo4j_container_id, neo4j_ip, neo4j_user, neo4j_password):
    client = docker.from_env()
    container = client.containers.get(neo4j_container_id)
    container.restart()
    time.sleep(10)
    driver = GraphDatabase.driver(neo4j_ip, auth = (neo4j_user, neo4j_password))
    return driver

def connect_mysql(mysql_port, mysql_user, mysql_password, mysql_database_name):
    conn = pymysql.connect(port= mysql_port, user =mysql_user, password=mysql_password )
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS {name}".format(name=mysql_database_name))
    cursor.execute("create database IF NOT EXISTS {name} default charset utf8 collate utf8_general_ci".format(name=mysql_database_name))  # create database
    conn = pymysql.connect(port= mysql_port, user =mysql_user, password=mysql_password, database= mysql_database_name)
    cursor = conn.cursor()
    return cursor


def semantic_annotation(config_path):
    driver = connect_neo4j(neo4j_container_id, neo4j_ip, neo4j_user, neo4j_password)
    mysql_cursor = connect_mysql(mysql_port, mysql_user, mysql_password, mysql_database_name)

    filelist = list(get_all_file(rawdata_source))
    data = get_rawdata(rawdata_path)
    ot = Ontology(ontology_path)
    ot_g = ot.get_ontology()  #generate ontology class
    owl = get_ot_alliri(ontology_path=ontology_path_owl,ontology_alliri_path = ontology_alliri_path)  #get all iri in ontology
    owl_alliri = owl[0]
    owl_classiri = owl[1]
    owl_propertyiri = owl[2]
    namespaces = get_namespaces(owl_alliri)  #get all namespaces of ontology
    entitiesnames_all = get_entities(owl_alliri)
    entitiesnames_class = get_entities(owl_classiri)
    entitiesnames_property = get_entities(owl_propertyiri)

    ontology_alliri = pd.DataFrame(columns=['ot_all', 'ot_class','ot_proerty'])
    ontology_alliri['ot_all'] = pd.Series(entitiesnames_all)
    ontology_alliri['ot_class'] = pd.Series(entitiesnames_class)
    ontology_alliri['ot_proerty'] = pd.Series(entitiesnames_property)
    ontology_alliri.to_csv(ontology_output_path)

    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        for i in filelist:  # generate similarity for all data file
            data, dataname, datatype = get_rawdata(rawdata_source + '/' + i)
            data = data.dropna(axis=1, how='all')
            data = data.dropna(axis=0, how='any')
            data = limit_column_length(data)
            columnlist = data.columns.values
            similarity_column_class_path = similarity_calculation(columnlist, entitiesnames_property, similarity_result_path=similarity_result_path,filename="similarity_result_column_class_'" + dataname + "'.csv")
            similarity_column_property_path = similarity_calculation(columnlist, entitiesnames_property, similarity_result_path=similarity_result_path,filename="similarity_result_column_property_'" + dataname+ "'.csv")

            ta = topic_annonation(ontology_path_owl, similarity_column_property_path = similarity_column_property_path,similarity_column_class_path = similarity_column_class_path)
            ta = ta.index[0] #the most relevant class of the table
            cta = columntype_annotation(similarity_column_property_path = similarity_column_property_path)

            if example_column_structure == []:
                column_structure = generate_column_structure(df=data, select_flag=False)
            else:
                column_structure = example_column_structure
            print(column_structure)
            data = csv_id_generation(df=data, column_structure=column_structure)

            experiment_result = do_query_experiment(data=data, neo4j_g=session, 
                                                    mysql_cursor=mysql_cursor, mysql_engine_data=[mysql_user, mysql_password, mysql_database_name], mysql_database_name=mysql_database_name,
                                                    ta=ta, cta=cta,
                                                    column_structure=column_structure,
                                                    matchname=matchname_example,
                                                    query_time=query_time_example,
                                                    datasize=datasize_sample)
            # experiment_result.to_csv(experiment_result_path)

            # queryname = ["Sensor"]  #set query value
            # kq = psm.generate_query(queryname,namespaces=namespaces) #generate query sentence
            # qs = psm.do_query(kq,ot_g) #implement query
            # neo4j = to_neo4j.to_neo4j(nodename=queryname, queryresult=qs) #generate knowledge graph based on query result in neo4j [only for sample test]
    session.close()

