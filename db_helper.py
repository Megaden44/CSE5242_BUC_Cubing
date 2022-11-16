import sqlite3
from sqlite3 import Error
import data_helper

#Code for anything that reads, updates, and deletes data.

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        connection = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return connection


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    c = conn.cursor()
    try:
        c.execute(create_table_sql)
    except Error as e:
        print(e)




def drop_table(conn, table):
    """ drop a table from the connected database statement
    :param conn: Connection object
    :param table: name of table to drop
    :return:
    """
    c = conn.cursor()
    try:
        query_drop = f"""DROP TABLE {table}"""
        c.execute(query_drop)
        conn.commit()
    except Error as e:
        print(e)



def insert_into_table(conn, temp_table, parent_table, node, values):
    """ insert into table specified number of rows
      :param node: dimensions
      :param conn: Connection object
      :param temp_table: table to insert into
      :param values: values to insert into table
      :param parent_table: table to select from
      :return:
      """
    c = conn.cursor()
    try:
        query_insert_into = f"""INSERT INTO {temp_table} SELECT * FROM {parent_table}"""
        if len(node) > 0:
            query_insert_into += " WHERE "
            for index in range(len(node)):
                query_insert_into += str(node[index]) + " = " + str(values[index + 1]) + " AND "
            query_insert_into = query_insert_into[0:-5]
        c.execute(query_insert_into)
        conn.commit()
    except Error as e:
        print(e)


def generate_table(conn, table, num_rows):
    """ insert into table specified number of rows
    :param num_rows: number of rows to insert
    :param conn: Connection object
    :param table: a CREATE TABLE statement
    :return:
    """
    c = conn.cursor()
    try:
        data = data_helper.generate_data(num_rows)
        query_insert = f"""INSERT INTO {table} (A,B,C,D,E) VALUES (?,?,?,?,?)"""
        query_select = f"""SELECT * FROM {table}"""
        c.executemany(query_insert, data)
        conn.commit()
        c.execute(query_select)
        records = c.fetchall()
        return len(records)
    except Error as e:
        print(e)
