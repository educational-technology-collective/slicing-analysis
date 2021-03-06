"""
A script to fetch the hash mapping data for each course session in MORF buckets and build a master hash mapping.
"""
from morf.utils.docker import load_docker_image, make_docker_run_command
from morf.utils.config import MorfJobConfig
from morf.utils import fetch_complete_courses, fetch_sessions, fetch_raw_course_data
from morf.utils.log import set_logger_handlers, execute_and_log_output
import logging
import os
import tempfile
import re
import pandas as pd

GENDER_CSV_FP = os.path.join(os.getcwd(), "data/names_for_josh.csv")  # docker doesn't like relative file paths
GENDER_VALUES_TO_KEEP = ("male", "female")
MORF_DATA_DIR = "morf-data/"
MYSQL_DOCKER_DIR = os.path.join(os.getcwd(), "docker")
MYSQL_DOCKER_IMG_NAME = "mysql-docker.tar"
OUTPUT_DIR = os.path.join(os.getcwd(), "data/hash-mapping-exports")
OUTPUT_FILENAME = "coursera_user_hash_gender_lookup.csv"
GENDER_COL_NAME = "gender"

module_logger = logging.getLogger(__name__)
job_config = MorfJobConfig("config.properties")
logger = set_logger_handlers(module_logger, job_config)

for raw_data_bucket in job_config.raw_data_buckets:
    for course in fetch_complete_courses(job_config, raw_data_bucket):
        for session in fetch_sessions(job_config, raw_data_bucket, data_dir=MORF_DATA_DIR, course=course,
                                      fetch_all_sessions=True):
            with tempfile.TemporaryDirectory(dir=os.getcwd()) as working_dir:
                print("[INFO] processing course {} session {}".format(course, session))
                # download the data exports
                fetch_raw_course_data(job_config, raw_data_bucket, course, session, input_dir=working_dir)
                # download_raw_course_data(job_config, raw_data_bucket, course=course, session=session, input_dir=working_dir,
                #                          data_dir=MORF_DATA_DIR[:-1]) # drop trailing slash on data dir
                # create docker run command and load image
                image_uuid = load_docker_image(MYSQL_DOCKER_DIR, job_config, logger, image_name=MYSQL_DOCKER_IMG_NAME)
                cmd = make_docker_run_command(job_config.docker_exec, working_dir, OUTPUT_DIR, image_uuid,
                                              course=course, session=session, mode=None,
                                              client_args=None)
                # run the docker image, make sure to pass params for course and session
                execute_and_log_output(cmd, logger)

# concatenate into single file in OUTPUT_DIR
df_list = []
for f in os.listdir(OUTPUT_DIR):
    if f.endswith(".csv") and not f.startswith("."):  # only use csv files, ignore system files
        print("[INFO] reading file {}".format(f))
        # pull course name and session from filename
        regex = re.compile("^hash_mapping_(\S+)_([0-9\-]+).*")
        res = re.search(regex, f)
        course = res.group(1)
        session = res.group(2)
        # read dataframe and add columns for course and session
        df = pd.read_csv(os.path.join(OUTPUT_DIR, f))
        df["course"] = course
        df["session"] = session
        df_list.append(df)
hash_mapping_df = pd.concat(df_list)
print("[INFO] concatenated hash mapping table contains {} rows".format(hash_mapping_df.shape[0]))

# read in gender
gender_df = pd.read_csv(GENDER_CSV_FP).drop_duplicates()
print("[INFO] original gender_df file contains {} rows".format(gender_df.shape[0]))
gender_df = gender_df.loc[gender_df[GENDER_COL_NAME].isin(GENDER_VALUES_TO_KEEP)]
print("[INFO] after dropping low-confidence gender guesses gender_df contains {} rows".format(gender_df.shape[0]))

# merge results and write to OUTPUT_DIR
df_out = pd.merge(hash_mapping_df, gender_df, left_on="user_id", right_on="coursera_user_id", how="inner")
print("[INFO] gender balance in final output dataset:")
print(df_out[GENDER_COL_NAME].value_counts())
df_out.to_csv(os.path.join(OUTPUT_DIR, OUTPUT_FILENAME))
