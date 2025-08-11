
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
import itertools
from itertools import product
import hashlib
from datetime import datetime
from .data_preprocess import check_is_datetime, convert_to_desired_format



def csv_to_neo4j(g, data, ta, cta, column_structure):
    #g.delete_all()
    # build the structure of key column and other columns
    graphcontainer = pd.DataFrame(columns=['Subject', 'Property', 'Object'])
    if column_structure==[]:
        column_list = data.columns.tolist()
        column_list.remove('uuid')
        id_str = ''.join(column_list)
        timeseries_id = hashlib.md5(id_str.encode()).hexdigest()
        for x in column_list:
            new_row = pd.DataFrame({graphcontainer.columns[0]: ta, graphcontainer.columns[1]: cta[x],graphcontainer.columns[2]: x})
            timeseries_id_row = pd.DataFrame({graphcontainer.columns[0]: x, graphcontainer.columns[1]: ['hasTimeseriesId'],graphcontainer.columns[2]: [timeseries_id]})
            graphcontainer = pd.concat([graphcontainer, new_row], ignore_index=True)
            graphcontainer = pd.concat([graphcontainer, timeseries_id_row], ignore_index=True)
        for index, row in graphcontainer.iterrows():  # generate graph
            cypher_generate = """
                MERGE (start_node:Subject {name: $row0})
                MERGE (end_node:Subject {name: $row2})
                """ + """MERGE (start_node)-[r: {relation_type}]->(end_node)""".format(relation_type=row.iloc[1])
            g.run(cypher_generate, row0=row.iloc[0], row2=row.iloc[2])
        return g

    for n in range(len(column_structure[0])):  # generate the knowledge graph for column structure
        if n==0:  # generate the knowledge graph for ta
            for x in set(data[column_structure[0][n]]):
                new_row = pd.DataFrame({graphcontainer.columns[0]: ta, graphcontainer.columns[1]: cta[column_structure[0][n]], graphcontainer.columns[2]: x})
                graphcontainer = pd.concat([graphcontainer, new_row], ignore_index=True)
            if (len(column_structure[0])==1):
                for x in set(data[column_structure[0][0]]):
                    timeseries_id = hashlib.md5(x.encode()).hexdigest()
                    timeseries_id_row = pd.DataFrame(
                        {graphcontainer.columns[0]: x, graphcontainer.columns[1]: ['hasTimeseriesId'],
                         graphcontainer.columns[2]: [timeseries_id]})
                    graphcontainer = pd.concat([graphcontainer, timeseries_id_row], ignore_index=True)
        else:  # generate the knowledge graph for the end column
            cols = column_structure[0]
            timeseries_id_row = []
            for level in range(1, len(cols)):
                prev_col = cols[level - 1]
                curr_col = cols[level]
                uniq = data[cols]
                for _, row in uniq.iterrows():
                    path_values = [row[c] for c in cols]
                    id_str = '||'.join('' if pd.isna(v) else str(v) for v in path_values)
                    timeseries_id = hashlib.md5(id_str.encode()).hexdigest()

                    timeseries_id_row.append({
                        graphcontainer.columns[0]: row[prev_col],
                        graphcontainer.columns[1]: cta[curr_col][0],
                        graphcontainer.columns[2]: row[curr_col]
                    })
                    nodes = list(dict.fromkeys(path_values))
                    for node in nodes:
                        timeseries_id_row.append({
                            graphcontainer.columns[0]: node,
                            graphcontainer.columns[1]: 'hasTimeseriesId',
                            graphcontainer.columns[2]: timeseries_id
                        })
            graphcontainer = pd.concat([graphcontainer, pd.DataFrame(timeseries_id_row)], ignore_index=True)

            # for x in set(data[column_structure[0][n-1]]):
            #     selected_df = data.loc[data[column_structure[0][n-1]] == x]
            #     for i in set(selected_df[column_structure[0][n]]):
            #         new_row = pd.DataFrame({graphcontainer.columns[0]: x, graphcontainer.columns[1]: cta[column_structure[0][n]],graphcontainer.columns[2]: i})
            #         id_str = str(x + i)
            #         timeseries_id = hashlib.md5(id_str.encode()).hexdigest()
            #         timeseries_id_row_lower = pd.DataFrame({graphcontainer.columns[0]: i, graphcontainer.columns[1]: ['hasTimeseriesId'],graphcontainer.columns[2]: [timeseries_id]})
            #         timeseries_id_row_higher = pd.DataFrame({graphcontainer.columns[0]: x, graphcontainer.columns[1]: ['hasTimeseriesId'],graphcontainer.columns[2]: [timeseries_id]})
            #         graphcontainer = pd.concat([graphcontainer, new_row], ignore_index=True)
            #         graphcontainer = pd.concat([graphcontainer, timeseries_id_row_lower], ignore_index=True)
            #         graphcontainer = pd.concat([graphcontainer, timeseries_id_row_higher], ignore_index=True)
    for index,row in graphcontainer.loc[graphcontainer['Property']=='hasTimeseriesId'].iterrows():
        cypher_generate = """
            MERGE (start_node:Subject {name: $row0})
            MERGE (end_node:`Timeseries id` {name: $row2, type:'timeseries id'})
            MERGE (start_node)-[r:hasTimeseriesId]->(end_node)
            """
        g.run(cypher_generate, row0=row.iloc[0], row2=row.iloc[2])
        graphcontainer = graphcontainer.drop(index)
    for index, row in graphcontainer.iterrows():  # generate graph
        cypher_generate = """
            MERGE (start_node:Subject {name: $row0})
            MERGE (end_node:Subject {name: $row2})
            """ + """MERGE (start_node)-[r: {relation_type}]->(end_node)""".format(relation_type=row.iloc[1])
        g.run(cypher_generate, row0=row.iloc[0], row2=row.iloc[2])

    # generate knowledge graph for the rest lower level of column structure
    graphcontainer = pd.DataFrame(columns=['Subject', 'Property', 'Object'])
    for x in set(data[column_structure[0][-1]]):
        selected_df = data.loc[data[column_structure[0][-1]]==x]
        for k in column_structure[1]:
            checkdate = check_is_datetime(selected_df[k].reset_index(drop=True),time_mark='.')  # for different time mark should be changed
            if (k=='VALUE') or (checkdate) or (k=='TIMESTAMP') or (k=='DATE'):  # determine if numerical column
                pass
            else:
                for i in set(selected_df[k]):
                    new_row = pd.DataFrame({graphcontainer.columns[0]: x, graphcontainer.columns[1]: cta[k],graphcontainer.columns[2]: i})
                    graphcontainer = pd.concat([graphcontainer, new_row], ignore_index=True)

    for index, row in graphcontainer.iterrows():  # generate graph
        cypher_generate = """
            MERGE (start_node:Subject {name: $row0})
            MERGE (end_node:Subject {name: $row2})
            """ + """MERGE (start_node)-[r: {relation_type}]->(end_node)""".format(relation_type=row.iloc[1])
        g.run(cypher_generate, row0=row.iloc[0], row2=row.iloc[2])

    return g

def csv_to_neo4j_s(g, data, ta, cta, column_structure):
    """Generate Knowledge graph with single databases"""
    #g.delete_all()
    # build the structure of key column and other columns
    # generate the knowledge graph for column structure
    for x in set(data[column_structure[0][0]]):  # generate the knowledge graph for ta
        cypher_generate = """
            MERGE (start_node:Subject {name: $row0})
            MERGE (end_node:Subject {name: $row2})
            """ + """MERGE (start_node)-[r: {relation_type}]->(end_node)""".format(relation_type=cta[column_structure[0][0]][0])
        g.run(cypher_generate, row0=ta, row2=x)
    # generate the knowledge graph for the rest column
        selected_df = data.loc[data[column_structure[0][0]] == x]
        selected_df = selected_df.dropna(axis=0, how='any')
        if any(isinstance(val, str) and ',' in val for val in selected_df['VALUE']):
            selected_df['VALUE'] = selected_df['VALUE'].str.replace(',', '.').astype(float)
        #existing_node_level1 = matcher.match('Object', name=x).first()
        for i in set(selected_df[column_structure[0][1]]):
            selected_df_2 = selected_df.loc[selected_df[column_structure[0][1]] == i]
            cypher_generate = """
                MATCH (start_node:Subject {name: $row0})
                MERGE (end_node:Subject {name: $row2})
                """ + """MERGE (start_node)-[r: {relation_type}]->(end_node)""".format(relation_type=cta[column_structure[0][1]][0])
            g.run(cypher_generate,row0=x, row2=i)
            for j in set(selected_df_2[column_structure[0][2]]):
                selected_df_3 = selected_df.loc[selected_df[column_structure[0][2]] == j]
                if column_structure[0][2]=='TIMESTAMP':
                    date_string = selected_df_3.iloc[0]['TIMESTAMP']
                    date_neo4j_str = convert_to_desired_format(date_string, "%Y-%m-%dT%H:%M:%S")
                    cypher_generate = """
                        MATCH (existing_node_level1: Subject {name: $x})-[relation]->(existing_node_level2: Subject {name: $i})
                        MERGE (end_node:Subject {name: $j, timestamp: $time_stamp})
                        """ + """MERGE (existing_node_level2)-[r: {relation_type}]->(end_node)""".format(relation_type=cta[column_structure[0][2]][0])
                    g.run(cypher_generate, x=x, i=i, j=j, time_stamp=date_neo4j_str)
                else:
                    cypher_generate = """
                                            MATCH (existing_node_level1: Subject {name: $x})-[relation]->(existing_node_level2: Subject {name: $i})
                                            MERGE (end_node:Subject {name: $j})
                                            """ + """MERGE (existing_node_level2)-[r: {relation_type}]->(end_node)""".format(relation_type=cta[column_structure[0][2]][0])
                    g.run(cypher_generate, x=x, i=i, j=j)
                for q in range(len(column_structure[1])):
                    for k in set(selected_df_3[column_structure[1][q]]):
                        cypher_generate = ("""MATCH (existing_node_level2: Subject {name: $i})-[relation]->(existing_node_level3: Subject {name: $j})
                        """ + """MERGE (end_node:{node_type}""".format(node_type=column_structure[1][q]) + """{name: $k})
                        """
                        + """MERGE (existing_node_level3)-[r: {relation_type}]->(end_node)""".format(relation_type=cta[column_structure[1][q]][0]))
                        g.run(cypher_generate,i=i, j=j, k=k)
    return g

def cypher_generate_process(g, x, i,selected_df, selected_df_2, column_structure, cta):
    print('generate cypher kg')
    cypher_generate = """
                    MATCH (start_node:Subject {name: $row0})
                    MERGE (end_node:Subject {name: $row2})
                    """ + """MERGE (start_node)-[r: {relation_type}]->(end_node)""".format(
        relation_type=cta[column_structure[0][1]][0])
    g.run(cypher_generate, row0=x, row2=i)
    for j in set(selected_df_2[column_structure[0][2]]):
        selected_df_3 = selected_df.loc[selected_df[column_structure[0][2]] == j]
        if column_structure[0][2] == 'TIMESTAMP':
            date_string = selected_df_3.iloc[0]['TIMESTAMP']
            date_object = datetime.strptime(date_string, "%d.%m.%Y %H:%M")
            date_neo4j_str = date_object.strftime("%Y-%m-%dT%H:%M:%S")
            # existing_node_level3 = Node("Object", name=j, timestamp=date_neo4j_str)
            cypher_generate = """
                MATCH (existing_node_level1: Subject {name: $x})-[relation]->(existing_node_level2: Subject {name: $i})
                MERGE (end_node:Subject {name: $j, timestamp: $time_stamp})
                """ + """MERGE (existing_node_level2)-[r: {relation_type}]->(end_node)""".format(
                relation_type=cta[column_structure[0][2]][0])
            g.run(cypher_generate, x=x, i=i, j=j, time_stamp=date_neo4j_str)
        else:
            # existing_node_level3 = Node("Object", name=j)
            cypher_generate = """
                                    MATCH (existing_node_level1: Subject {name: $x})-[relation]->(existing_node_level2: Subject {name: $i})
                                    MERGE (end_node:Subject {name: $j})
                                    """ + """MERGE (existing_node_level2)-[r: {relation_type}]->(end_node)""".format(
                relation_type=cta[column_structure[0][2]][0])
            g.run(cypher_generate, x=x, i=i, j=j)
        # relation = Relationship(existing_node_level2, cta[column_structure[0][2]][0], existing_node_level3)
        # g.create(relation)
        for q in range(len(column_structure[1])):
            for k in set(selected_df_3[column_structure[1][q]]):
                cypher_generate = ("""MATCH (existing_node_level2: Subject {name: $i})-[relation]->(existing_node_level3: Subject {name: $j})
                """ + """MERGE (end_node:{node_type}""".format(node_type=column_structure[1][q]) + """{name: $k})
                """
                                   + """MERGE (existing_node_level3)-[r: {relation_type}]->(end_node)""".format(
                            relation_type=cta[column_structure[1][q]][0]))
                g.run(cypher_generate, i=i, j=j, k=k)
    return g

#def generate_kg(g, ta, cta, data):
#    g.delete_all()
#    graphcontainer = pd.DataFrame(columns=['Subject', 'Property', 'Object'])
#    for i in cta:
#        if len(set(data[i]))>=0.2*len(data[i]): #determine if numerical column
#            new_row = pd.DataFrame({graphcontainer.columns[0]: ta, graphcontainer.columns[1]: cta[i], graphcontainer.columns[2]: 'id***'})
#            graphcontainer = pd.concat([graphcontainer, new_row], ignore_index=True)
#            continue
#        for x in set(data[i]):
#            new_row = pd.DataFrame({graphcontainer.columns[0]: ta, graphcontainer.columns[1]: cta[i], graphcontainer.columns[2]: x})
#            graphcontainer = pd.concat([graphcontainer, new_row], ignore_index=True)
#    for index, row in graphcontainer.iterrows():
#        start_node = Node("Subject", name=row[0])
#        end_node = Node("Object", name=row[2])
#        relation = Relationship(start_node, row[1], end_node)
#        g.merge(relation, "Property", "name")
#    return

def generate_column_structure(df, select_flag=True):
    """Sort the columns by the length of cells in each column
    Input: raw data [dataframe] Output: sort result of columns [list]
    """
    # drop the time and value columns
    df_og = df
    for i in df.columns:
        if (df[i].dtype=='int64') or (df[i].dtype=='float64') or (check_is_datetime(df[i].reset_index(drop=True),time_mark='.') or i=='DATE' or i=='VALUE' or i=='TIMESTAMP'):  # determine if is number or date
            df = df.drop(i, axis=1)
    df = df.drop_duplicates(keep='first').reset_index(drop=True)
    column_structure = []
    if len(df.columns)==0:
        return column_structure
    break_flag = True  # global variable for break the loop
    for i in range(1,len(df.columns)+1):  # generate id by at least one column until all columns
        column_combi = list(itertools.combinations(df.columns, i))
        for combi in column_combi:
            id_list = []
            for n in range(len(df)):
                id = ''
                for column in combi:
                    id = id + str(df.loc[n][column])
                id_list.append(id)
            id_list = list(set(id_list))
            if len(id_list)==len(df):
                column_high = list(combi)
                break_flag = False
                break
        if not break_flag:
            break
    column_lower = list(set(df.columns) - set(column_high))
    if not select_flag:
        column_lower = list(set(df_og.columns) - set(column_high))
    column_dict = {}
    for i in column_high:
        column_dict[i] = len(set(df[i]))
    column_sorted = sorted(column_dict.items(), key=lambda item: item[1])
    column_high = []
    for i in column_sorted:
        column_high.append(i[0])
    column_dict = {}
    for i in column_lower:
        column_dict[i] = len(set(df_og[i]))
    column_sorted = sorted(column_dict.items(), key=lambda item: item[1])
    column_lower = []
    for i in column_sorted:
        column_lower.append(i[0])
    column_structure.append(column_high)
    column_structure.append(column_lower)
    return column_structure

#def match(g, matchname1='HVAC KN4',matchname2='Supply temp'):  #只考虑了两个变量的情况
#    matcher = py2neo.NodeMatcher(g)
#    match_node_1 = matcher.match(name=matchname1).first()
#    match_node_2 = matcher.match(name=matchname2).first()
#    match1 = list(r.end_node['name'] for r in g.match(nodes=(match_node_1,),r_type='hasTimeseriesId'))
#    match2 = list(r.end_node['name'] for r in g.match(nodes=(match_node_2,),r_type='hasTimeseriesId'))
#    result = [i for i in match1 if i in match2]
#    if not result:
#        return
#    return result[0]

def match_cypher(g, matchname1='HVAC KN4',matchname2='Supply temp'):  #只考虑了两个变量的情况
    cypher_query = """
    MATCH (n1:Subject {name: $matchname1})-[r1:hasTimeseriesId]->(common)
    MATCH (n2:Subject {name: $matchname2})-[r2:hasTimeseriesId]->(common)
    RETURN common.name AS TimeseriesId
    """
    result = g.run(cypher_query, matchname1=matchname1, matchname2=matchname2)  #.to_data_frame()
    timeseriesId = []
    for i in result:
        timeseriesId.append(i['TimeseriesId'])
    if timeseriesId==[]:
        return "Result Not Found"
    else:
        return timeseriesId[0]


#def match_s(g, matchname1='Supply temp', matchname2='HVAC KN4', timestamp='16.08.2021 07:43'):  #只考虑了两个变量的情况
#    matcher = py2neo.NodeMatcher(g)
#    match_node_1 = matcher.match(name=matchname1).first()
#    match1 = list(r.end_node for r in g.match(nodes=(match_node_1,)))
#    for n in match1:
#        if n['name']==matchname2:
#            match2 = list(r.end_node for r in g.match(nodes=(n,)))
#            for ts in match2:
#                if ts['name']==timestamp:
#                    match3 = list(r.end_node for r in g.match(nodes=(ts,)))
#                    for end_node in match3:
#                        if end_node.has_label('VALUE'):
#                            return end_node['name']
#    return

def match_s_cypher_timepoint(g, matchname1='Supply temp', matchname2='HVAC KN4', timestamp='16.08.2021 07:43'):  #只考虑了两个变量的情况
    cypher_query = """
    MATCH (n:Subject {name: $matchname1})-[*]->(n2:Subject {name: $matchname2})
    MATCH (n2)-[*]->(ts:Subject {name: $timestamp})
    MATCH (ts)-[*]->(value:VALUE)
    RETURN value.name
    """
    result = g.run(cypher_query, matchname1=matchname1, matchname2=matchname2, timestamp=timestamp)
    value = []
    for i in result:
        value.append(i['value.name'])
    if value==[]:
        return []
    return value[0]

def match_s_cypher_timerange(g, matchname1='Supply temp', matchname2='HVAC KN4', timerange=['02.01.2021  00:00','31.03.2021  23:00']):  #只考虑了两个变量的情况
    start_time_string = timerange[0]
    start_time_neo4j_str = convert_to_desired_format(start_time_string,"%Y-%m-%dT%H:%M:%S")
    end_time_string = timerange[1]
    end_time_neo4j_str = convert_to_desired_format(end_time_string,"%Y-%m-%dT%H:%M:%S")
    cypher_query = """
    MATCH (n:Subject {name: $matchname1})-[*]->(n2:Subject {name: $matchname2})
    MATCH (n2)-[hasTimestamp]->(ts)
    MATCH (ts)-[*]->(value:VALUE)
    WHERE ts.timestamp >= $start_time AND ts.timestamp <= $end_time
    RETURN ts.timestamp AS Timestamp, value.name AS VALUE
    """
    match_result = g.run(cypher_query, matchname1=matchname1, matchname2=matchname2, start_time=start_time_neo4j_str, end_time=end_time_neo4j_str)
    result = pd.DataFrame([dict(record) for record in match_result])
    return result

def match_s_cypher_timeaverage(g, matchname1='Supply temp', matchname2='HVAC KN4', timerange=['02.01.2021  00:00','31.03.2021  23:00']):  #只考虑了两个变量的情况
    start_time_string = timerange[0]
    start_time_neo4j_str = convert_to_desired_format(start_time_string, "%Y-%m-%dT%H:%M:%S")
    end_time_string = timerange[1]
    end_time_neo4j_str = convert_to_desired_format(end_time_string, "%Y-%m-%dT%H:%M:%S")
    cypher_query = """
    MATCH (n:Subject {name: $matchname1})-[*]->(n2:Subject {name: $matchname2})
    MATCH (n2)-[hasTimestamp]->(ts)
    MATCH (ts)-[*]->(value:VALUE)
    WHERE ts.timestamp >= $start_time AND ts.timestamp <= $end_time
    RETURN AVG(value.name) AS averageValue
    """
    match_result = g.run(cypher_query, matchname1=matchname1, matchname2=matchname2, start_time=start_time_neo4j_str, end_time=end_time_neo4j_str)
    result = pd.DataFrame([dict(record) for record in match_result])
    return result

def neo4j_databasesize_count(g):
        node_c = g.run("MATCH (n) RETURN COUNT(n) AS nodeCount")
        relation_c = g.run("MATCH ()-[r]->() RETURN COUNT(r) AS relationshipCount")
        property_c = g.run("MATCH (n) RETURN SIZE(keys(n)) AS propertyCount")
        nc_int = node_c.single()['nodeCount']
        rc_int = relation_c.single()['relationshipCount']
        pc_int = property_c.single()['propertyCount']
        database_size = nc_int*12 + rc_int*34 + pc_int*41
        return database_size/1024

