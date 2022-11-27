import tracemalloc

import avg_buc
import data_helper
import db_helper
import count_buc
import stat_avg_buc
import time


def tracing_start():
    tracemalloc.stop()
    tracemalloc.start()


def tracing_mem():
    first_size, first_peak = tracemalloc.get_traced_memory()
    peak = first_peak / (1024 * 1024)
    print("Peak Size in MB - " + str(peak) + "\n")


def main():
    db_name = r"data-cubing.db"
    table_name = "cubing_data"
    conn = db_helper.create_connection(db_name)
    dim = list(map(chr, range(65, 70)))
    num_rows = int(input("Enter number of rows to generate (integer): "))
    filter_level = int(input("Enter filter level to use (integer): "))
    verbose_input = input("Verbose output (T/F): ")
    verbose = (verbose_input == "T") or (verbose_input == "t")
    count_result = {}
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
        db_helper.drop_table(conn, table_name)

        # create projects table
        db_helper.create_table(conn, sql_create_projects_table)

        # add data to table
        db_helper.generate_table(conn, table_name, num_rows)

        # create BUC processing tree
        buc_root = data_helper.Node(())
        data_helper.processing_tree(dim, buc_root)

        # start analytics
        tracing_start()
        start = time.time()

        # run naive count BUC cubing on data
        count_buc.count_buc(conn, table_name, filter_level, buc_root, 0, count_result, verbose)

        # print analytics
        end = time.time()
        print("Results for count BUC")
        print("time elapsed {} milli seconds".format((end - start) * 1000))
        tracing_mem()

        # start analytics
        tracing_start()
        start = time.time()

        # run avg BUC cubing on data no filter
        avg_buc.avg_buc(conn, table_name, buc_root, 0, verbose)

        # print analytics
        end = time.time()
        print("Results for avg BUC")
        print("time elapsed {} milli seconds".format((end - start) * 1000))
        tracing_mem()

        # start analytics
        tracing_start()
        start = time.time()

        # run statistical avg BUC cubing on data
        stat_avg_buc.stat_avg_buc(conn, table_name, buc_root, 0, count_result, verbose)

        # print analytics
        end = time.time()
        print("Results for stat avg BUC")
        print("time elapsed {} milli seconds".format((end - start) * 1000))
        tracing_mem()

        # start analytics
        tracing_start()
        start = time.time()

        # run refined count BUC cubing on data
        count_buc.ref_count_buc(conn, table_name, filter_level, buc_root, count_result, verbose)

        # print analytics
        end = time.time()
        print("Results for refinec count BUC")
        print("time elapsed {} milli seconds".format((end - start) * 1000))
        tracing_mem()

        # start analytics
        tracing_start()
        start = time.time()

        # run refined avg BUC cubing on data no filter
        avg_buc.ref_avg_buc(conn, table_name, buc_root, verbose)

        # print analytics
        end = time.time()
        print("Results for refined avg BUC")
        print("time elapsed {} milli seconds".format((end - start) * 1000))
        tracing_mem()

        # start analytics
        tracing_start()
        start = time.time()

        # run refined statistical avg BUC cubing on data
        stat_avg_buc.ref_stat_avg_buc(conn, table_name, buc_root, verbose)

        # print analytics
        end = time.time()
        print("Results for refined stat avg BUC")
        print("time elapsed {} milli seconds".format((end - start) * 1000))
        tracing_mem()

        # close connection
        conn.close()


if __name__ == '__main__':
    main()
