import sqlite3
from sqlite3 import Error
import random


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


def check_table(connection, table):
    c = connection.cursor()
    try:
        query_check = f"""select COUNT(*) from {table}"""
        c.execute(query_check)
<<<<<<< HEAD
        return c
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()

def insert_table(conn, table):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    c = conn.cursor()
    data = generate_data(10000)
    try:
        data = generate_data(10000)
        query_insert = f"""INSERT INTO {table} (A,B,C,D,E) VALUES (?,?,?,?,?)"""
        query_select = f"""SELECT * FROM {table}"""
        c.executemany(query_insert, data)
        conn.commit()
        c.execute(query_select)
        records = c.fetchall()
        return len(records)
=======
        return c.rowcount
>>>>>>> 2641f4dd6e226ae1db6c93846c0eed052b229072
    except Error as e:
        print(e)


def insert_table(connection, table):
    """
    Inserts generated data into specified table
    :param connection:
    :param table:
    :return:
    """
    c = connection.cursor()
    data = generate_data(10000)
    try:
        query_insert = f"""INSERT INTO {table} (A,B,C,D,E) VALUES (?,?,?,?,?)"""
        c.executemany(query_insert, data)
        connection.commit()
    except Error as e:
        print(e)


def generate_data(rows):
    arr = []
    for i in range(rows):
        A = random.randint(0, 1)
        B = random.randint(0, 1)
        C = random.randint(0, 1)
        D = random.randint(0, 1)
        E = random.randint(0, 1)
        arr.append((A, B, C, D, E))
    return arr


def main():
    db_name = r"data-cubing.db"
    table_name = "cubing_data"
    conn = create_connection(db_name)

    sql_create_projects_table = f""" CREATE TABLE IF NOT EXISTS {table_name} (
                                            id integer PRIMARY KEY,
                                            A integer NOT NULL,
                                            B integer NOT NULL,
                                            C integer NOT NULL,
                                            D integer NOT NULL,
                                            E integer NOT NULL
                                        ); """

    if conn is not None:
        # create projects table
        create_table(conn, sql_create_projects_table)

    # insert_table(conn, table_name)
    print(check_table(conn, table_name))
    conn.close()


if __name__ == '__main__':
    main()
