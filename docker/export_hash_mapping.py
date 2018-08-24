"""
A script to load the hash happing tables from coursera spark data exports and export it to a csv file.
"""

from morf_slice_utils import *
import argparse
import pandas as pd


COURSE = "sna"
SESSION = "002"

def main(course=COURSE, session=SESSION):
    unzip_sql_dumps(course, session)
    load_data(course, session)
    execute_mysql_query_into_csv("SELECT * FROM hash_mapping", "/input/hash_mapping_{}_{}.csv".format(course, session))
    return



if __name__ == "__main__":
    ## TODO: argparse here to get commands
    main()