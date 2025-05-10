from util import get_connection


def write_table(db_details,table_name,column_names,data):
    target_db=db_details['TARGET_DB']
    connection=get_connection(db_type=target_db['DB_TYPE'],db_user=target_db['DB_USER'],db_pass=target_db['DB_PASS'],db_host=target_db['DB_HOST'],db_name=target_db['DB_NAME'])
    cursor=connection.cursor()
    query=build_insert_query(table_name,column_names)
    rows_inserted=insert_data(connection,cursor,query,data)
    return rows_inserted


def build_insert_query(table_name,column_names):
    colum_names_string=", ".join(column_names)
    column_values=", ".join(tuple(map(lambda column :column.replace(column,'%s'),column_names)))
    query=f"insert into {table_name} ({colum_names_string}) values ({column_values})"
    return query

def insert_data(connection,cursor,query,data,batch_size=100):
    recs=[]
    count=1
    for rec in data:
        recs.append(rec)
        if count % batch_size==0:
            cursor.executemany(query,recs)
            connection.commit()
            recs=[]
        count+=1
    cursor.executemany(query, recs)
    connection.commit()
    return count-1