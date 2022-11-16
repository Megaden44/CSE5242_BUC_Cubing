import sqlite3
from sqlite3 import Error
import random

#Code for anything that manipulates or calculates data from database.

class Node(object):
    def __init__(self, data):
        self.data = data
        self.children = []

    def add_child(self, obj):
        self.children.append(obj)

class ReverseNode(object):
    def __init__(self, data):
        self.data = data
        self.children = []

    def add_child(self, obj):
        self.children.append(obj)

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

def processing_tree(dems, root):
    test = dems.copy()
    while len(test) > 0:
        d = test.pop(0)
        child_node = Node(root.data + (d,))
        root.add_child(child_node)
        processing_tree(test, child_node)

def reverse_processing_tree(dims, root):

    for dim in dims:
        test = dims.copy()
        test.remove(dim)
        child_node = Node(test)
        root.add_child(child_node)
        reverse_processing_tree(test, child_node)

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
            print("%-7d" % row[0], "|", "%-13s" % formatted_tuple, "|", "%-7s" % values)
            group_list.append(list(row))
        print("---------------------------------------")
        return group_list
    except Error as e:
        print(e)
    finally:
        if c:
            c.close()

def amalgamate(results, dim_index):

    new_results = []
    for result in results:
        new_tuple = [str(dim_entry) for i, dim_entry in enumerate(result[1:]) if i != dim_index]
        if len(new_tuple) > 0:
            new_index = int(''.join(new_tuple),2)
        else:
            new_index = 0
        if new_index < len(new_results):
            new_results[new_index][0] += result[0]
        else:
            new_results.append([result[0]] + new_tuple)
    return new_results