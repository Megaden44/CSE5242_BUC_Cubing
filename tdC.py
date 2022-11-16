import data_helper
import db_helper


def tdc_cubing(conn, parent_table, filter_level, buc_root, cur_level, last_dropped):
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

    if len(buc_root.data) == 5 and buc_root.data[0] == 'A' and buc_root.data[1] == 'B':
        x = 1

    results = data_helper.partition(conn, parent_table, buc_root.data)
    node_data = buc_root.data.copy()
    index = len(buc_root.data)
    while index > last_dropped:
        dim_index = len(node_data)
        results = data_helper.amalgamate(results, dim_index)
        for row in results:
            if row[0] < filter_level:  # only add values to table that meet criteria
                print("Number of groups is too sparse for node: " + str(node_data) + ", group: " + str(row))
            else:
                db_helper.insert_into_table(conn, temp_table, parent_table, node_data, row)
                print("Good on: " + str(node_data) + ", group: " + str(row))

        index -= 1
        if len(node_data) > 0:
            node_data.pop()

    if len(buc_root.data) == 4 and buc_root.data[0] == 'A' and buc_root.data[1] == 'B' and buc_root.data[2] == 'D' and \
            buc_root.data[3] == 'E':
        x = 1
        y = len(buc_root.children[last_dropped:-1])


    for i in range(last_dropped, len(buc_root.children[:-1])):

        tdc_cubing(conn, temp_table, filter_level, buc_root.children[i], cur_level + 1,i)
    db_helper.drop_table(conn, temp_table)



