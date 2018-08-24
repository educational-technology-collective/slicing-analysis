from morf_slice_utils import *
import argparse


COURSE = "sna"
SESSION = "002"


def main(course=COURSE, session=SESSION):
    unzip_sql_dumps(course, session)
    load_data(course, session)
    execute_mysql_query_into_csv("SELECT * FROM hash_mapping", "/input/hash_mapping.csv")
    return


if __name__ == "__main__":
    ## TODO: argparse here to get commands
    main()