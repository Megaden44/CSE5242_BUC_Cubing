import data_helper
import db_helper


def count_buc(conn, parent_table, filter_level, buc_root, cur_level, count_result, verbose):
    """ cubes table_name using a BUC Iceberg approach to get count cube
            :param conn: Connection object
            :param parent_table: name of table with partitions
            :param filter_level: how deep the iceberg goes
            :param buc_root: root of processing tree
            :param cur_level: current lattice level
            :param count_result: running total of counts at each node
            :param verbose: print out status
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
    db_helper.create_table(conn, sql_create_temp_table)
    # check of the aggregation sum for each group meets the filter level
    # if it does stop, otherwise cube with only those values
    result = data_helper.count_partition(conn, parent_table, buc_root.data, count_result, verbose)
    for row in result:
        if row[0] < filter_level:  # only add values to table that meet criteria
            if verbose:
                print("Number of groups is too sparse for node: " + str(buc_root.data) + ", group: " + str(row))
        else:
            db_helper.insert_into_table(conn, temp_table, parent_table, buc_root.data, row)
    for child_node in buc_root.children:
        count_buc(conn, temp_table, filter_level, child_node, cur_level + 1, count_result, verbose)
    db_helper.drop_table(conn, temp_table)


def ref_count_buc(conn, parent_table, filter_level, buc_root, count_result, verbose):
    """ cubes table_name using a BUC Iceberg approach
            :param conn: Connection object
            :param parent_table: name of table with partitions
            :param filter_level: how deep the iceberg goes
            :param buc_root: root of processing tree
            :param count_result: running total of counts at each node
            :param verbose: print out status
            :return:
    """
    filtered_values = []
    # for each permutation at a given lattice level
    # check of the aggregation sum for each group meets the filter level
    # if it does stop, otherwise cube with only those values
    result = data_helper.count_partition(conn, parent_table, buc_root.data, count_result, verbose)
    for row in result:
        if row[0] < filter_level:  # only add values to table that meet criteria
            if verbose:
                print("Number of groups is too sparse for node: " + str(buc_root.data) + ", group: " + str(row))
            # filtered_values.append(row)
    # remove filtered out elements from table
    db_helper.delete_from_table(conn, parent_table, buc_root.data, filtered_values)
    for child_node in buc_root.children:
        ref_count_buc(conn, parent_table, filter_level, child_node, count_result, verbose)
