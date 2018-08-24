from morf_slice_utils import *
import argparse
import pandas as pd


COURSE = "sna"
SESSION = "002"
GENDER_CSV_FP = "/Users/joshgardner/Documents/Github/slicing-analysis/data/names_for_josh.csv"
GENDER_VALUES_TO_KEEP = ("male", "female")

def main(course=COURSE, session=SESSION):
    unzip_sql_dumps(course, session)
    load_data(course, session)
    execute_mysql_query_into_csv("SELECT * FROM hash_mapping", "/input/hash_mapping.csv")
    return

gender = pd.read_csv(GENDER_CSV_FP).drop_duplicates()
gender = gender.loc[gender['gender'].isin(GENDER_VALUES_TO_KEEP)]

if __name__ == "__main__":
    ## TODO: argparse here to get commands
    main()