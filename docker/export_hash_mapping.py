"""
A script to load the hash happing tables from coursera spark data exports and export it to a csv file in output_dir.
"""

from morf_slice_utils import *
import argparse
import os


def main(course, session, output_dir="/output"):
    outfilename = "hash_mapping_{}_{}.csv".format(course, session)
    unzip_sql_dumps(course, session)
    load_data(course, session)
    execute_mysql_query_into_csv("SELECT * FROM hash_mapping", os.path.join(output_dir, outfilename))
    print("[INFO] files in output_dir: ".format(os.listdir(output_dir)))
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="execute feature extraction, training, or testing.")
    parser.add_argument("-c", "--course", required=True, help="an s3 pointer to a course")
    parser.add_argument("-r", "--session", required=True, help="3-digit course run number")
    parser.add_argument("--mode", required=False, help="mode; not used but automatically passed to docker by most MORF API functions")
    args = parser.parse_args()
    main(args.course, args.session)

