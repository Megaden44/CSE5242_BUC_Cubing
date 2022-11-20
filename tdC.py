import data_helper
import db_helper


def tdc_cubing(conn, parent_table, filter_level, buc_root, cur_level, last_dropped, verbose):
    """ cubes table_name using a BUC Iceberg approach
            :param conn: Connection object
            :param parent_table: name of table with partitions
            :param filter_level: how deep the iceberg goes
            :param buc_root: root of processing tree
            :param cur_level: current level of node tree
            :param last_dropped: last dropped table
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
    results = data_helper.tdc_partition(conn, parent_table, buc_root.data, verbose)
    node_data = buc_root.data.copy()
    index = len(buc_root.data)
    while index > last_dropped:
        dim_index = len(node_data)
        results = data_helper.amalgamate(results, dim_index)
        for row in results:
            if row[0] < filter_level:  # only add values to table that meet criteria
                if verbose:
                    print("Number of groups is too sparse for node: " + str(node_data) + ", group: " + str(row))
            else:
                db_helper.insert_into_table(conn, temp_table, parent_table, node_data, row)
                if verbose:
                    print("Good on: " + str(node_data) + ", group: " + str(row))

        index -= 1
        if len(node_data) > 0:
            node_data.pop()
    for i in range(last_dropped, len(buc_root.children[:-1])):
        tdc_cubing(conn, temp_table, filter_level, buc_root.children[i], cur_level + 1, i, verbose)
    db_helper.drop_table(conn, temp_table)
