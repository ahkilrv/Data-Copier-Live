import pandas as pd

def get_tables(path):

    tables=pd.read_csv(path,sep=':')
    return tables[tables['to_be_loaded']=='yes']
    # for idx,row in q.iterrows():
    #     print(row)


