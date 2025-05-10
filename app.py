import sys
from util import get_tables,load_db_details
from read import read_table
from write import write_table

def main():

    env = sys.argv[1]
    db_details=load_db_details(env)
    tables=get_tables('table_list.txt')
    for idx,table in tables.iterrows():
        print(f'Reading for table {table.table_name}')
        data,column_names,total_rows=read_table(db_details,table.table_name,0)
        print(f'loading data for {table.table_name}')
        rows_inserted=write_table(db_details,table.table_name,column_names,data)
        print(f"Total Number of Rows:{total_rows} \n Number of Rows Inserted:{rows_inserted} \n {(rows_inserted/total_rows)*100} Percent Successful Loading" )




if __name__=='__main__':
    main()
