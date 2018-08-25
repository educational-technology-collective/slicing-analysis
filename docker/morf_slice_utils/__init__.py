"""
Utility functions for extracting data for slicing analysis from MORF.

Eventually these functions should be incoprorated into MORF API or another public utilities package.
"""

import os
import subprocess
import tarfile
import shutil
import gzip
import re




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


def unarchive_file(src, dest):
    """
    Untar or un-gzip a file from src into dest. Supports file extensions: .zip, .tgz, .gz. Ported from MORF API (morf.utils).
    :param src: path to source file to unarchive (string).
    :param dest: directory to unarchive result into (string).
    :return: None
    """
    if src.endswith(".zip") or src.endswith(".tgz"):
        tar = tarfile.open(src)
        tar.extractall(dest)
        tar.close()
        os.remove(src)
        outpath = os.path.join(dest, os.path.basename(src))
    elif src.endswith(".gz"):
        with gzip.open(src, "rb") as f_in:
            destfile = os.path.basename(src)[:-3] # source file without '.gz' extension
            destpath = os.path.join(dest, destfile)
            with open(destpath, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(src)
        outpath = destpath
    else:
        raise NotImplementedError("Passed in a file with an extension not supported by unarchive_file: {}".format(src))
    return outpath


def clean_filename(src):
    """
    Rename file, removing any non-alphanumeric characters. Ported from MORF API (morf.utils).
    :param src: file to rename.
    :return: None
    """
    src_dir, src_file = os.path.split(src)
    clean_src_file = re.sub('[\(\)\s&]+', '', src_file)
    clean_src_path = os.path.join(src_dir, clean_src_file)
    try:
        os.rename(src, clean_src_path)
    except Exception as e:
        print("[ERROR] error renaming file: {}".format(e))
    return


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
    print("[INFO] executing {}".format(command))
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
    session_input_dir = os.path.join(data_dir, course, session)
    hash_mapping_sql_dump = [x for x in os.listdir(session_input_dir) if 'hash_mapping' in x and session in x][0]
    forum_sql_dump = [x for x in os.listdir(session_input_dir) if 'anonymized_forum' in x and session in x][0]
    anon_general_sql_dump = [x for x in os.listdir(session_input_dir) if 'anonymized_general' in x and session in x][0]
    # start mysql server
    subprocess.call('service mysql start', shell=True)
    # create a database
    print("[INFO] creating database")
    res = subprocess.call('''mysql -u root -proot -e "CREATE DATABASE {}"'''.format(dbname), shell=True)
    print("RES: {}".format(res))
    # load all data dumps needed
    load_dump(os.path.join(session_input_dir, forum_sql_dump))
    load_dump(os.path.join(session_input_dir, hash_mapping_sql_dump))
    load_dump(os.path.join(session_input_dir, anon_general_sql_dump))
    return


def unzip_sql_dumps(course, session, data_dir = "/input"):
    """
    unzip all of the sql files and remove any parens from filename.
    :param course:
    :param session:
    :param data_dir:
    :return:
    """
    session_input_dir = os.path.join(data_dir, course, session)
    for item in os.listdir(session_input_dir):
        if item.endswith(".sql.gz"):
            item_path = os.path.join(session_input_dir, item)
            unarchive_res = unarchive_file(item_path, session_input_dir)
            clean_filename(unarchive_res)
    return

