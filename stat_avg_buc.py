import data_helper
import db_helper


def stat_avg_buc(conn, parent_table, buc_root, cur_level, count_result, verbose):
    """ cubes table_name using a BUC Iceberg approach with statistics to get avg cube
            :param conn: Connection object
            :param parent_table: name of table with partitions
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
    if count_result.get(buc_root.data) > 0:  # this checks if the count filter eliminated all groups
        result = data_helper.\
            statistical_avg_partition(conn, parent_table, buc_root.data, count_result.get(buc_root.data), verbose)
        for row in result:
            db_helper.insert_into_table(conn, temp_table, parent_table, buc_root.data, row)
        for child_node in buc_root.children:
            stat_avg_buc(conn, temp_table, child_node, cur_level + 1, count_result, verbose)
    db_helper.drop_table(conn, temp_table)
