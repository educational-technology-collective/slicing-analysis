"""
Utility functions for extracting data for slicing analysis from MORF.

Eventually these functions should be incoprorated into MORF API or another public utilities package.
"""

import os
import subprocess


DATABASE_NAME = "course"


def extract_id_lookup_table(outfile = "id_lookup.csv"):
    """
    Extract lookup table of various identifiers from course sql export and write to outfile.
    :return: pd.DataFrame identical to csv written to outfile.
    """
    return


def generate_morf_id_lookup():
    """
    Iterate over all MORf data, extracting id lookup from each session, and generate a single csv file for all users.
    :return: pd.DataFrame identical to csv written to outfile.
    """

def execute_mysql_query_into_csv(query, file, database_name=DATABASE_NAME):
    """
    Execute a mysql query into a file.
    :param query: valid mySQL query as string.
    :param file: csv filename to write to.
    :return: none
    """
    mysql_to_csv_cmd = """ | tr '\t' ',' """  # string to properly format result of mysql query
    command = '''mysql -u root -proot {} -e"{}"'''.format(database_name, query)
    command += """{} > {}""".format(mysql_to_csv_cmd, file)
    subprocess.call(command, shell=True)
    return


def load_dump(dump_file, dbname = DATABASE_NAME):
    print("[INFO] loading dump from {}".format(dump_file))
    command = '''mysql -u root -proot {} < {}'''.format(dbname, dump_file)
    res = subprocess.call(command, shell=True)
    print("[INFO] result: {}".format(res))
    return


def load_data(course, session, dbname = DATABASE_NAME, data_dir = "/input"):
    """
    Loads data into mySQL database from database dump files.
    :param course: shortname of course.
    :param session: 3-digit session id (string).
    :return:
    """
    password = 'root'
    user = 'root'
    mysql_binary_location = '/usr/bin/mysql'
    mysql_admin_binary_location = '/usr/bin/mysqladmin'
    session_dir = os.path.join(data_dir, course, session)
    hash_mapping_sql_dump = [x for x in session_dir if 'hash_mapping' in x and session in x][0]
    forum_sql_dump = [x for x in session_dir if 'anonymized_forum' in x and session in x][0]
    anon_general_sql_dump = [x for x in session_dir if 'anonymized_general' in x and session in x][0]
    # start mysql server
    subprocess.call('service mysql start', shell=True)
    # create a database
    print("[INFO] creating database")
    res = subprocess.call('''mysql -u root -proot -e "CREATE DATABASE {}"'''.format(dbname), shell=True)
    print("RES: {}".format(res))
    # load all data dumps needed
    load_dump(os.path.join(session_dir, forum_sql_dump))
    load_dump(os.path.join(session_dir, hash_mapping_sql_dump))
    load_dump(os.path.join(session_dir, anon_general_sql_dump))
    return
