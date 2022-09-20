import random
import sqlite3
from itertools import combinations
from math import comb
from sqlite3 import Error


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
        return c.rowcount
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()


def insert_table(conn, table, num_rows):
    """ insert into table specified number of rows
    :param num_rows: number of rows to insert
    :param conn: Connection object
    :param table: a CREATE TABLE statement
    :return:
    """
    c = conn.cursor()
    try:
        data = generate_data(num_rows)
        query_insert = f"""INSERT INTO {table} (A,B,C,D,E) VALUES (?,?,?,?,?)"""
        query_select = f"""SELECT * FROM {table}"""
        c.executemany(query_insert, data)
        conn.commit()
        c.execute(query_select)
        records = c.fetchall()
        return len(records)
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


def generate_data(rows):
    """ generate uniform data
        :param rows: specifies number of rows to generate
        :return: array of cardinality rows with generated data
    """
    arr = []
    for i in range(rows):
        a = random.randint(0, 1)
        b = random.randint(0, 1)
        c = random.randint(0, 1)
        d = random.randint(0, 1)
        e = random.randint(0, 1)
        arr.append((a, b, c, d, e))
    return arr


def partition(conn, table_name, group_by_tuple):
    """ partitions a table by a collection of dimensions by COUNT aggregate
        :param conn: Connection object
        :param table_name: name of table with partitions
        :param group_by_tuple: tuple that defines the dimensions to partition by
        :return: array of groups and values
    """
    group_list = []
    string_tuple = ""
    # makes a string like "A, C, D"
    # TODO: smarter way to do this??
    for dem in group_by_tuple:
        string_tuple += dem + ", "
    formatted_tuple = string_tuple[0: -2]

    c = conn.cursor()
    try:
        if formatted_tuple:
            query_check = \
                f"""select COUNT(*), {formatted_tuple} from {table_name}
                 group by {formatted_tuple}"""
        else:
            query_check = f"""select COUNT(*), A from {table_name}"""
        c.execute(query_check)
        print("COUNT   |   DIMENSIONS  |  VALUES")
        for row in c:
            values = ""
            for i in range(len(row)):
                if i > 0:
                    values += str(row[i]) + ", "
            values = values[0: -2]
            print("%-7d" % row[0], "|", "%-13s" % formatted_tuple, "|", "%-7s" % values)
            group_list.append(row)
        print("---------------------------------------")
        return group_list
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()
    return 0


def buc_cubing(conn, table_name, filter_level, dems, cur_level):
    """ cubes table_name using a BUC Iceberg approach
            :param conn: Connection object
            :param table_name: name of table with partitions
            :param filter_level: how deep the iceberg goes
            :param dems: how deep the iceberg goes
            :param cur_level: tracks the lattice layer for recursion
            :return:
    """
    if cur_level == len(dems):
        return
    else:
        num_dem = len(dems)
        dimensions = comb(num_dem, cur_level)
        # the nodes on a given level
        lattice_nodes = list(combinations(dems, cur_level))
        # for each permutation at a given lattice level
        for d in range(dimensions):
            # check of the aggregation sum for each group meets the filter level
            # if it does stop, otherwise cube with only those values
            result = partition(conn, table_name, lattice_nodes[d])
            for row in result:
                if row[0] > filter_level:
                    buc_cubing(conn, table_name, filter_level, dems, cur_level + 1)
                else:
                    print("Number of groups is too sparse for node: " + str(lattice_nodes[d]) + ", group: " + str(row))


def main():
    db_name = r"data-cubing.db"
    table_name = "cubing_data"
    conn = create_connection(db_name)
    dems = ["A", "B", "C", "D", "E"]
    num_rows = 100000
    output_arr = []
    sql_create_projects_table = f""" CREATE TABLE IF NOT EXISTS {table_name} (
                                            id integer PRIMARY KEY,
                                            A integer NOT NULL,
                                            B integer NOT NULL,
                                            C integer NOT NULL,
                                            D integer NOT NULL,
                                            E integer NOT NULL
                                        ); """

    if conn is not None:
        # drop tables
        drop_table(conn, table_name)

        # create projects table
        create_table(conn, sql_create_projects_table)

        # insert_table(conn, table_name)
        insert_table(conn, table_name, num_rows)

        # run naive BUC cubing on data
        buc_cubing(conn, table_name, 3, dems, 0)

        # close connection
        conn.close()


if __name__ == '__main__':
    main()
