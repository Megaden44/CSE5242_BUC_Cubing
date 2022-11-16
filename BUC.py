import data_helper
import db_helper

def buc_cubing(conn, parent_table, filter_level, buc_root, cur_level):
    """ cubes table_name using a BUC Iceberg approach
            :param conn: Connection object
            :param parent_table: name of table with partitions
            :param filter_level: how deep the iceberg goes
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
                                                        E integer NOT NULL
                                                    ); """
    db_helper.create_table(conn, sql_create_temp_table)
    # check of the aggregation sum for each group meets the filter level
    # if it does stop, otherwise cube with only those values
    result = data_helper.partition(conn, parent_table, buc_root.data)
    for row in result:
        if row[0] < filter_level:  # only add values to table that meet criteria
            print("Number of groups is too sparse for node: " + str(buc_root.data) + ", group: " + str(row))
        else:
            db_helper.insert_into_table(conn, temp_table, parent_table, buc_root.data, row)
    for child_node in buc_root.children:
        buc_cubing(conn, temp_table, filter_level, child_node, cur_level + 1)
    db_helper.drop_table(conn, temp_table)




