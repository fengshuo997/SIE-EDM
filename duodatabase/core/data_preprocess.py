import os
import csv
import pandas as pd
from .wordembedding import *
import hashlib
from multiprocessing.dummy import Pool
from datetime import datetime
import time

def get_all_file(base):
    filelist = []
    for root, ds, fs in os.walk(base):
        for f in fs:
            yield f
            for i in f:
                filelist.append(i)
    return filelist

def get_rawdata(rawdata_path):
    """Get the raw data"""
    file = os.path.splitext(rawdata_path)
    filename, type = file
    filename = os.path.basename(filename)
    if type=='.xlsx':
        data = pd.read_excel(rawdata_path)
    elif type=='.xls':
        data = pd.read_excel(rawdata_path)
    elif type=='.csv':
        data = pd.read_csv(rawdata_path, on_bad_lines='skip', sep=',')
    else:
        return
    return data, filename, type

def reform(data,reformdata_path,splitdata_path):
    """reform the structure of the raw data"""
    #get the columns of data
    column_og = list(data.columns.values)
    column_sp = []
    for i in range(0,len(column_og)):
        split = column_og[i].split(';')
        for element in split:
            column_sp.append(element)
    # add the value of index to cell
    dataindex = []
    for i in range(1,len(data)+1):
        dataindex.append(list(data.head(i).index.values[i-1]))
    # write the new cell with index value to csv
    with open(reformdata_path, 'w') as f:
        csv_write = csv.writer(f)
        for x in range(1,len(dataindex)+1):
            csv_head = dataindex[x-1]
            csv_write.writerow(csv_head)
    # split the cell by semicolon
    reformdata = pd.read_csv(reformdata_path, encoding='latin-1', header=None)
    splitdata = [column_sp]
    for row in range(0, len(reformdata)):
        cell_sp = []
        for cell in range(0, len(reformdata.loc[row])):
            if type(reformdata.loc[row][cell]) == str:
                split = reformdata.loc[row][cell].split(';')
                for element in split:
                    cell_sp.append(element)
            else:
                cell_sp.append(reformdata.loc[row][cell])
        splitdata.append(cell_sp)
    splitdata = pd.DataFrame(splitdata)
    splitdata.to_csv(splitdata_path)
    return


def csv_id_generation(df, column_structure):
    """Generate id for each row in csv data
    Input: csv file [dataframe] Output: dataframe with id [dataframe]"""
    df = df.dropna(axis=0, how='any')
    if column_structure==[]:
        column_sum = ''.join(df.columns.tolist())
        id = hashlib.md5(column_sum.encode()).hexdigest()
        df['uuid'] = id
        return df
    else:
        df_t = df[column_structure[0]].sum(axis=1).map(lambda x:hashlib.md5(str(x).encode()).hexdigest())
        df_t.name = 'uuid'
        df = df.join(df_t)
        return df


def similarity_calculation(data, ontologydata,similarity_result_path, filename='similarity_result_property_class.csv'):
    """calculate the similarity between raw data and ontology
    Input: raw data name list [list] and ontology name list [list] """
    #get the columns of data
    similarity_result_path = similarity_result_path + filename
    print(similarity_result_path)
    if not os.path.exists(similarity_result_path):
        scorefile = pd.DataFrame(columns=['Dataname', 'Ontologyname', 'score'])
        for i in data:
            compare_with_args = lambda item: compare(i, item)
            with Pool(processes=1) as pool:
                scorelist = pool.map(compare_with_args, ontologydata)
            for x in range(len(scorelist)):
                scorefile.loc[len(scorefile)] = {'Dataname': i, 'Ontologyname': ontologydata[x], 'score': round(scorelist[x].tolist()[0][0], 3)}
                print(scorefile.iloc[-1])
        scorefile.to_csv(similarity_result_path)
    return similarity_result_path

def compare(a='time',b='energy'):
    bert = bert_Energy_tsdae()
    score = bert.sim(sentences_a=[a], sentences_b=[b])
    return score

def limit_column_length(df):
    for i in df.columns:
        i_short = i[:64]
        i_rstrip = i_short.rstrip()  # drop the last whitespace
        df = df.rename(columns={i: i_rstrip})
    df.columns = df.columns.str.replace('©', 'C')
    df.columns = df.columns.str.replace('Ã', 'A')
    return df

def check_is_datetime(series, time_mark='.') -> bool:
    if series.dtype == 'object':
        for k in series:
            try:
                if ":" in k:
                    time.strptime(k, "%d.%m.%Y %H:%M")
                elif "." in k:
                    time.strptime(k, "%d.%m.%Y")
                else:
                    time.strptime(k, "%m-%d-%Y")
                    time.strptime(k, "%m:%d:%Y")
                return True
            except:
                return False
        for sp in series.str.split(time_mark):
            if len(sp) != 3:
                return False
        return True
    elif series.dtypes == 'datetime64[ns]':
        return True
    else:
        return False

def convert_to_desired_format(time_string, targetformat="%d.%m.%Y %H:%M"):
    formats_to_check = [
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y.%m.%d %H:%M:%S",
        "%Y.%m.%d %H:%M",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M"
    ]

    formatted_time = None
    for time_format in formats_to_check:
        try:
            parsed_time = datetime.strptime(time_string, time_format)
            formatted_time = parsed_time.strftime(targetformat)
            break
        except ValueError:
            pass

    return formatted_time