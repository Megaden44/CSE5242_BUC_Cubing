import random
import sqlite3
from sqlite3 import Error
import tracemalloc
import time
from memory_profiler import profile


class Node(object):
    def __init__(self, data):
        self.data = data
        self.children = []

    def add_child(self, obj):
        self.children.append(obj)


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


def generate_table(conn, table, num_rows):
    """ insert into table specified number of rows
    :param num_rows: number of rows to insert
    :param conn: Connection object
    :param table: a CREATE TABLE statement
    :return:
    """
    c = conn.cursor()
    try:
        data = generate_data(num_rows)
        query_insert = f"""INSERT INTO {table} (A,B,C,D,E, aggregate_column) VALUES (?,?,?,?,?,?)"""
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
        aggregate_column = random.randint(0, 100)
        arr.append((a, b, c, d, e, aggregate_column))
    return arr


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


def count_partition(conn, table_name, group_by_tuple, count_result):
    """ partitions a table by a collection of dimensions by COUNT aggregate
        :param conn: Connection object
        :param table_name: name of table with partitions
        :param group_by_tuple: tuple that defines the dimensions to partition by
        :param count_result: running total of counts at each node
        :return: array of groups and values
    """
    group_list = []
    string_tuple = ""
    total_count = 0
    for dem in group_by_tuple:
        string_tuple += dem + ", "
    formatted_tuple = string_tuple[0: -2]
    c = conn.cursor()
    try:
        if formatted_tuple:
            query_check = f"""select COUNT(*), {formatted_tuple} from {table_name} group by {formatted_tuple}"""
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
            #print("%-7d" % row[0], "|", "%-13s" % formatted_tuple, "|", "%-7s" % values)
            group_list.append(row)
            total_count += row[0]
        print("---------------------------------------")
        count_result[group_by_tuple] = total_count
        return group_list
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()


def count_buc_cubing(conn, parent_table, filter_level, buc_root, cur_level, count_result):
    """ cubes table_name using a BUC Iceberg approach
            :param conn: Connection object
            :param parent_table: name of table with partitions
            :param filter_level: how deep the iceberg goes
            :param buc_root: root of processing tree
            :param cur_level: current lattice level
            :param count_result: running total of counts at each node
            :return:
    """
    # for each permutation at a given lattice level
    temp_table = "filtered_data_node_" + str(cur_level)
    sql_create_temp_table = f""" CREATE TABLE IF NOT EXISTS {temp_table} (
                                                        id integer PRIMARY KEY,
                                                        A integer NOT NULL,
                                                        B integer NOT NULL,
                                                        C integer NOT NULL,
                                                        D integer NOT NULL,
                                                        E integer NOT NULL,
                                                        aggregate_column NOT NULL
                                                    ); """
    create_table(conn, sql_create_temp_table)
    # check of the aggregation sum for each group meets the filter level
    # if it does stop, otherwise cube with only those values
    result = count_partition(conn, parent_table, buc_root.data, count_result)
    for row in result:
        if row[0] < filter_level:  # only add values to table that meet criteria
            print("Number of groups is too sparse for node: " + str(buc_root.data) + ", group: " + str(row))
        else:
            insert_into_table(conn, temp_table, parent_table, buc_root.data, row)
    for child_node in buc_root.children:
        count_buc_cubing(conn, temp_table, filter_level, child_node, cur_level + 1, count_result)
    drop_table(conn, temp_table)


def avg_partition(conn, table_name, group_by_tuple):
    """ partitions a table by a collection of dimensions by COUNT aggregate
        :param conn: Connection object
        :param table_name: name of table with partitions
        :param group_by_tuple: tuple that defines the dimensions to partition by
        :return: array of groups and values
    """
    group_list = []
    string_tuple = ""
    for dem in group_by_tuple:
        string_tuple += dem + ", "
    formatted_tuple = string_tuple[0: -2]
    c = conn.cursor()
    try:
        if formatted_tuple:
            query_check = f"""select AVG(aggregate_column), {formatted_tuple} from {table_name} 
            group by {formatted_tuple}"""
        else:
            query_check = f"""select AVG(aggregate_column), A from {table_name}"""
        c.execute(query_check)
        print("AVERAGE   |   DIMENSIONS  |  VALUES")
        for row in c:
            values = ""
            for i in range(len(row)):
                if i > 0:
                    values += str(row[i]) + ", "
            values = values[0: -2]
            #print("%-7d" % row[0], "|", "%-13s" % formatted_tuple, "|", "%-7s" % values)
            group_list.append(row)
        print("---------------------------------------")
        return group_list
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()


def avg_buc_cubing(conn, parent_table, buc_root, cur_level):
    """ cubes table_name using a BUC Iceberg approach
            :param conn: Connection object
            :param parent_table: name of table with partitions
            :param cur_level: current lattice level
            :param buc_root: root of processing tree
            :return:
    """
    # for each permutation at a given lattice level
    temp_table = "filtered_data_node_" + str(cur_level)
    sql_create_temp_table = f""" CREATE TABLE IF NOT EXISTS {temp_table} (
                                                        id integer PRIMARY KEY,
                                                        A integer NOT NULL,
                                                        B integer NOT NULL,
                                                        C integer NOT NULL,
                                                        D integer NOT NULL,
                                                        E integer NOT NULL,
                                                        aggregate_column NOT NULL
                                                    ); """
    create_table(conn, sql_create_temp_table)
    # check of the aggregation sum for each group meets the filter level
    # if it does stop, otherwise cube with only those values
    result = avg_partition(conn, parent_table, buc_root.data)
    for row in result:
        insert_into_table(conn, temp_table, parent_table, buc_root.data, row)
    for child_node in buc_root.children:
        avg_buc_cubing(conn, temp_table, child_node, cur_level + 1)
    drop_table(conn, temp_table)


def statistical_avg_partition(conn, table_name, group_by_tuple, count_result_value):
    """ partitions a table by a collection of dimensions by COUNT aggregate
        :param conn: Connection object
        :param table_name: name of table with partitions
        :param group_by_tuple: tuple that defines the dimensions to partition by
        :param count_result_value: population sizes for each combination
        :return: array of groups and values
    """
    group_list = []
    string_tuple = ""
    for dem in group_by_tuple:
        string_tuple += dem + ", "
    formatted_tuple = string_tuple[0: -2]
    c = conn.cursor()
    try:
        population_size = count_result_value
        sample_size = int(inf_pop_sample_size/(1+inf_pop_sample_size/population_size)) + 1
        if formatted_tuple:
            query_check = f"""select AVG(aggregate_column), {formatted_tuple} from 
            (select aggregate_column, {formatted_tuple} from {table_name} limit {sample_size}) 
            group by {formatted_tuple}"""
        else:
            query_check = f"""select AVG(aggregate_column), A from 
            (select aggregate_column, A from {table_name} limit {sample_size})"""
        c.execute(query_check)
        print("AVERAGE   |   DIMENSIONS  |  VALUES")
        for row in c:
            values = ""
            for i in range(len(row)):
                if i > 0:
                    values += str(row[i]) + ", "
            values = values[0: -2]
            #print("%-7d" % row[0], "|", "%-13s" % formatted_tuple, "|", "%-7s" % values)
            group_list.append(row)
        print("---------------------------------------")
        return group_list
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()


def statistical_avg_buc_cubing(conn, parent_table, buc_root, cur_level, count_result):
    """ cubes table_name using a BUC Iceberg approach
            :param conn: Connection object
            :param parent_table: name of table with partitions
            :param buc_root: root of processing tree
            :param cur_level: current lattice level
            :param count_result: running total of counts at each node
            :return:
    """
    # for each permutation at a given lattice level
    temp_table = "filtered_data_node_" + str(cur_level)
    sql_create_temp_table = f""" CREATE TABLE IF NOT EXISTS {temp_table} (
                                                        id integer PRIMARY KEY,
                                                        A integer NOT NULL,
                                                        B integer NOT NULL,
                                                        C integer NOT NULL,
                                                        D integer NOT NULL,
                                                        E integer NOT NULL,
                                                        aggregate_column NOT NULL
                                                    ); """
    create_table(conn, sql_create_temp_table)
    # check of the aggregation sum for each group meets the filter level
    # if it does stop, otherwise cube with only those values
    if count_result.get(buc_root.data) > 0:  # this checks if the count filter eliminated all groups
        result = statistical_avg_partition(conn, parent_table, buc_root.data, count_result.get(buc_root.data))
        for row in result:
            insert_into_table(conn, temp_table, parent_table, buc_root.data, row)
        for child_node in buc_root.children:
            statistical_avg_buc_cubing(conn, temp_table, child_node, cur_level + 1, count_result)
    drop_table(conn, temp_table)


def buc_processing_tree(dems, root):
    test = dems.copy()
    while len(test) > 0:
        d = test.pop(0)
        child_node = Node(root.data + (d,))
        root.add_child(child_node)
        buc_processing_tree(test, child_node)


def tracing_start():
    tracemalloc.stop()
    tracemalloc.start()


def tracing_mem():
    first_size, first_peak = tracemalloc.get_traced_memory()
    peak = first_peak / (1024 * 1024)
    print("Peak Size in MB - ", peak)


z_score = 1.96
error_margin = 0.05
population_proportion = 0.5
inf_pop_sample_size = int(z_score*z_score*population_proportion*(1-population_proportion)/
                          (error_margin*error_margin)) + 1


def main():
    db_name = r"data-cubing.db"
    table_name = "cubing_data"
    conn = create_connection(db_name)
    dems = ["A", "B", "C", "D", "E"]
    num_rows = 100000
    sql_create_projects_table = f""" CREATE TABLE IF NOT EXISTS {table_name} (
                                            id integer PRIMARY KEY,
                                            A integer NOT NULL,
                                            B integer NOT NULL,
                                            C integer NOT NULL,
                                            D integer NOT NULL,
                                            E integer NOT NULL,
                                            aggregate_column NOT NULL
                                        ); """

    if conn is not None:
        # drop tables
        drop_table(conn, table_name)

        # create projects table
        create_table(conn, sql_create_projects_table)

        # add data to table
        generate_table(conn, table_name, num_rows)

        # create processing tree
        buc_root = Node(())
        buc_processing_tree(dems, buc_root)

        # start analytics
        tracing_start()
        start = time.time()

        # run naive BUC cubing on data
        count_result = {}
        count_buc_cubing(conn, table_name, 12500, buc_root, 0, count_result)

        # print analytics
        end = time.time()
        print("Results for count buc")
        print("time elapsed {} milli seconds".format((end - start) * 1000))
        tracing_mem()

        # start analytics
        tracing_start()
        start = time.time()

        # run statistical avg BUC cubing on data
        avg_buc_cubing(conn, table_name, buc_root, 0)

        # print analytics
        end = time.time()
        print("Results for avg buc")
        print("time elapsed {} milli seconds".format((end - start) * 1000))
        tracing_mem()

        # start analytics
        tracing_start()
        start = time.time()

        # run statistical avg BUC cubing on data
        statistical_avg_buc_cubing(conn, table_name, buc_root, 0, count_result)

        # print analytics
        end = time.time()
        print("Results for statistical avg buc")
        print("time elapsed {} milli seconds".format((end - start) * 1000))
        tracing_mem()

        # close connection
        conn.close()


if __name__ == '__main__':
    main()
