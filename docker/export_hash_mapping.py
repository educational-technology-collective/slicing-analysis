"""
A script to load the hash happing tables from coursera spark data exports and export it to a csv file in output_dir.
"""

from morf_slice_utils import *
import argparse
import pandas as pd
import os


def main(course, session, output_dir="/output"):
    outfilename = "/hash_mapping_{}_{}.csv".format(course, session)
    unzip_sql_dumps(course, session)
    load_data(course, session)
    execute_mysql_query_into_csv("SELECT * FROM hash_mapping", os.path.join(output_dir, outfilename))
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="execute feature extraction, training, or testing.")
    parser.add_argument("-c", "--course", required=True, help="an s3 pointer to a course")
    parser.add_argument("-r", "--session", required=False, help="3-digit course run number")
    args = parser.parse_args()
    main(args.course, args.session)

