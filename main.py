import sqlite3
from sqlite3 import Error
import random

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

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
    finally:
        if c:
            c.close()

def check_table(conn, table):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    c = conn.cursor()
    try:

        query_check = f"""select COUNT(*) from {table}""".format(table=table)
        c.execute(query_check)
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

    try:

        return c.rowcount
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()


def generate_data(rows):
    arr = []
    for i in range(rows):
        A = random.randint(0,1)
        B = random.randint(0, 1)
        C = random.randint(0, 1)
        D = random.randint(0, 1)
        E = random.randint(0, 1)
        arr.append((A,B,C,D,E))
    return arr





# Press the green button in the g
# utter to run the script.
if __name__ == '__main__':
    db_name = r"datacubing.db"
    table_name = "cubing_data"
    conn = create_connection(db_name)

    sql_create_projects_table = f""" CREATE TABLE IF NOT EXISTS {table_name} (
                                            id integer PRIMARY KEY,
                                            A integer NOT NULL,
                                            B integer NOT NULL,
                                            C integer NOT NULL,
                                            D integer NOT NULL,
                                            E integer NOT NULL
                                        ); """.format(table_name=table_name)

    if conn is not None:
        # create projects table
        create_table(conn, sql_create_projects_table)


    print(insert_table(conn, table_name))






# See PyCharm help at https://www.jetbrains.com/help/pycharm/
# @Test Commit From Peter