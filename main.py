import data_helper
import db_helper
import BUC
import tdC





# def main():
#     db_name = r"data-cubing.db"
#     table_name = "cubing_data"
#     conn = db_helper.create_connection(db_name)
#     dim = list(map(chr, range(65, 70)))
#     num_rows = 100000
#     sql_create_projects_table = f""" CREATE TABLE IF NOT EXISTS {table_name} (
#                                             id integer PRIMARY KEY,
#                                             A integer NOT NULL,
#                                             B integer NOT NULL,
#                                             C integer NOT NULL,
#                                             D integer NOT NULL,
#                                             E integer NOT NULL
#                                         ); """
#
#     if conn is not None:
#         # drop tables
#         db_helper.drop_table(conn, table_name)
#
#         # create projects table
#         db_helper.create_table(conn, sql_create_projects_table)
#
#         # add data to table
#         db_helper.generate_table(conn, table_name, num_rows)
#
#         # create processing tree
#
#         # buc_root = data_helper.ReverseNode(dim)
#         # data_helper.reverse_processing_tree(dim, buc_root)
#         #
#         # # run naive BUC cubing on data
#         # tdC.tdc_cubing(conn, table_name, 12500, buc_root, 0, -1)
#
#         buc_root = data_helper.Node(())
#         data_helper.processing_tree(dim, buc_root)
#
#         # run naive BUC cubing on data
#         BUC.buc_cubing(conn, table_name, 12500, buc_root, 0)
#
#         # close connection
#         conn.close()

def main():
    db_name = r"data-cubing.db"
    table_name = "cubing_data"
    conn = db_helper.create_connection(db_name)
    dim = list(map(chr, range(65, 70)))
    num_rows = 100000
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
        db_helper.drop_table(conn, table_name)

        # create projects table
        db_helper.create_table(conn, sql_create_projects_table)

        # add data to table
        db_helper.generate_table(conn, table_name, num_rows)

        # create processing tree

        buc_root = data_helper.ReverseNode(dim)
        data_helper.reverse_processing_tree(dim, buc_root)

        # run naive BUC cubing on data
        tdC.tdc_cubing(conn, table_name, 12500, buc_root, 0, 0)

        # buc_root = data_helper.Node(())
        # data_helper.processing_tree(dim, buc_root)
        #
        # # run naive BUC cubing on data
        # BUC.buc_cubing(conn, table_name, 12500, buc_root, 0)

        # close connection
        conn.close()

if __name__ == '__main__':
    main()
