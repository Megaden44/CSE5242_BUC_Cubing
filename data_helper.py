from sqlite3 import Error
import random


class Node(object):
    def __init__(self, data):
        self.data = data
        self.children = []

    def add_child(self, obj):
        self.children.append(obj)


def generate_data(rows, skew):
    """ generate uniform data
        :param rows: specifies number of rows to generate
        :param skew: how much skew in the database
        :return: array of cardinality rows with generated data
    """
    arr = []
    for i in range(rows):
        if random.randint(0, 100) > skew:
            a = 0
        else:
            a = 1
        if random.randint(0, 100) > skew:
            b = 0
        else:
            b = 1
        if random.randint(0, 100) > skew:
            c = 0
        else:
            c = 1
        if random.randint(0, 100) > skew:
            d = 0
        else:
            d = 1
        if random.randint(0, 100) > skew:
            e = 0
        else:
            e = 1
        aggregate_column = random.randint(0, 100)
        arr.append((a, b, c, d, e, aggregate_column))
    return arr


def processing_tree(dems, root):
    test = dems.copy()
    while len(test) > 0:
        d = test.pop(0)
        child_node = Node(root.data + (d,))
        root.add_child(child_node)
        processing_tree(test, child_node)


def count_partition(conn, table_name, group_by_tuple, count_result, verbose):
    """ partitions a table by a collection of dimensions by COUNT aggregate
        :param conn: Connection object
        :param table_name: name of table with partitions
        :param group_by_tuple: tuple that defines the dimensions to partition by
        :param count_result: running total of counts at each node
        :param verbose: print out status
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
        if verbose:
            print("COUNT   |   DIMENSIONS  |  VALUES")
        for row in c:
            values = ""
            for i in range(len(row)):
                if i > 0:
                    values += str(row[i]) + ", "
            values = values[0: -2]
            if verbose:
                print("%-7d" % row[0], "|", "%-13s" % formatted_tuple, "|", "%-7s" % values)
            group_list.append(row)
            total_count += row[0]
        if verbose:
            print("---------------------------------------")
        count_result[group_by_tuple] = total_count
        return group_list
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()


def avg_partition(conn, table_name, group_by_tuple, verbose):
    """ partitions a table by a collection of dimensions by COUNT aggregate
        :param conn: Connection object
        :param table_name: name of table with partitions
        :param group_by_tuple: tuple that defines the dimensions to partition by
        :param verbose: print out status
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
        if verbose:
            print("AVERAGE   |   DIMENSIONS  |  VALUES")
        for row in c:
            values = ""
            for i in range(len(row)):
                if i > 0:
                    values += str(row[i]) + ", "
            values = values[0: -2]
            if verbose:
                print("%-7d" % row[0], "|", "%-13s" % formatted_tuple, "|", "%-7s" % values)
            group_list.append(row)
        if verbose:
            print("---------------------------------------")
        return group_list
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()


z_score = 1.96
error_margin = 0.05
population_proportion = 0.5
inf_pop_sample_size = int(z_score*z_score*population_proportion*(1 - population_proportion) /
                          (error_margin*error_margin)) + 1


def statistical_avg_partition(conn, table_name, group_by_tuple, count_result_value, verbose):
    """ partitions a table by a collection of dimensions by COUNT aggregate
        :param conn: Connection object
        :param table_name: name of table with partitions
        :param group_by_tuple: tuple that defines the dimensions to partition by
        :param count_result_value: population sizes for each combination
        :param verbose: print out status
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
        # sample size is individual sample size times the number of dimensions
        sample_size = \
            (int(inf_pop_sample_size/(1+inf_pop_sample_size/population_size)) + 1) * pow(2, len(group_by_tuple))
        if formatted_tuple:
            query_check = f"""select AVG(aggregate_column), {formatted_tuple} from 
            (select aggregate_column, {formatted_tuple} from {table_name} limit {sample_size}) 
            group by {formatted_tuple}"""
        else:
            query_check = f"""select AVG(aggregate_column), A from 
            (select aggregate_column, A from {table_name} limit {sample_size})"""
        c.execute(query_check)
        if verbose:
            print("AVERAGE   |   DIMENSIONS  |  VALUES")
        for row in c:
            values = ""
            for i in range(len(row)):
                if i > 0:
                    values += str(row[i]) + ", "
            values = values[0: -2]
            if verbose:
                print("%-7d" % row[0], "|", "%-13s" % formatted_tuple, "|", "%-7s" % values)
            group_list.append(row)
        if verbose:
            print("---------------------------------------")
        return group_list
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()
