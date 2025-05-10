from util import get_connection

def read_table(db_details,table_name,limit):
    source_db=db_details['SOURCE_DB']
    connection=get_connection(db_type=source_db['DB_TYPE'],db_user=source_db['DB_USER'],db_pass=source_db['DB_PASS'],db_host=source_db['DB_HOST'],db_name=source_db['DB_NAME'])
    cursor=connection.cursor()
    if limit==0:
        query=f'select * from {table_name}'

    else:
        query = f'select * from {table_name} limit {limit}'

    cursor.execute(query)
    data=cursor.fetchall()
    column_names=cursor.column_names
    rows=len(data)
    connection.close()
    return data,column_names,rows