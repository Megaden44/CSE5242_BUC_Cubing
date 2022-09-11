import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    create_connection("...")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
# @Test Commit From Peter