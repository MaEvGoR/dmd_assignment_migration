import psycopg2
import numpy as np

import pymongo
import dns

client = pymongo.MongoClient("mongodb+srv://maximevgrafov:maximevgrafov@dmdassignmentspring-qvs0c.mongodb.net/test?retryWrites=true&w=majority")
mongo_db = client.test

conn = psycopg2.connect(dbname='dvdrental', user='postgres', password='smth', host='localhost', port='5433')
postgres_cursor = conn.cursor()


def get_table_names(cursor):
    cursor.execute('''
        SELECT table_name
                FROM information_schema.tables
                        WHERE table_type='BASE TABLE'
                            AND table_schema='public';
    ''')
    result = list(np.array(cursor.fetchall())[:, 0])
    return result


def get_json_info(cursor, table_name):
    cursor.execute('SELECT row_to_json(t) FROM {} t;'.format(table_name))

    result = list(np.array(cursor.fetchall())[:, 0])

    return result



names = get_table_names(postgres_cursor)
for name in names:
    rows_json = get_json_info(postgres_cursor, name)
    current_collection = mongo_db[name]
    for current_item in rows_json:
        current_collection.insert_one(current_item)