import argparse, math, datetime, os, bisect
import pandas as pd
from functools import reduce

MILLISECONDS_IN_SECOND = 1000

def fetch_start_end_date(course_name, run, date_csv = "coursera_course_dates.csv"):
    """
    Fetch course start end end date (so user doesn't have to specify them directly).
    :param course_name: Short name of course.
    :param run: run number
    :param date_csv: Path to csv of course start/end dates.
    :return: tuple of datetime objects (course_start, course_end)
    """
    full_course_name = '{0}-{1}'.format(course_name, run)
    date_df = pd.read_csv(date_csv, usecols=[0, 2, 3]).set_index('course')
    course_start = datetime.datetime.strptime(date_df.loc[full_course_name].start_date, '%m/%d/%y')
    course_end = datetime.datetime.strptime(date_df.loc[full_course_name].end_date, '%m/%d/%y')
    return (course_start, course_end)


def course_len(course_start, course_end):
    '''
    Return the duration of a course, in number of whole weeks.
    Note: Final week may be less than 7 days, depending on course start and end dates.
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: integer of course duration in number of weeks (rounded up if necessary)
    '''
    course_start, course_end = course_start, course_end
    n_days = (course_end - course_start).days
    n_weeks = math.ceil(n_days / 7)
    return int(n_weeks)


def timestamp_week(timestamp, course_start, course_end):
    '''
    Get (zero-indexed) week number for a given timestamp.
    :param timestamp: UTC timestamp, in seconds.
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: integer week number of timestamp. If week not in range of course dates provided, return None.
    '''
    timestamp = datetime.datetime.fromtimestamp(timestamp / MILLISECONDS_IN_SECOND)
    n_weeks = course_len(course_start, course_end)
    week_starts = [course_start + datetime.timedelta(days=x) for x in range(0, n_weeks * 7, 7)]
    week_number = bisect.bisect_left(week_starts, timestamp) - 1
    if week_number >= 0 and week_number <= n_weeks:
        return week_number
    return None


def aggregate_and_remove_feature_files(input_dir, output_dir="/output", result_filename = "feats.csv", match_substring=None, drop_cols = ["dropout_current_week", "week"]):
    """
    Read in all feature files in input_dir, merge them, and write the results to output_dir, removing the files after merging.
    :param input_dir: directory containing feature files to be merged
    :param output_dir: directory to write results in
    :param result_filename: name of file to write in output_dir
    :param match_substring: optional, only match files containg this substring
    :return:
    """
    user_id_colname = "userID" # key column used for joining
    session_user_id_colname = "session_user_id" # another name for userid which will be renamed to user_id_colname
    result_fp = os.path.join(output_dir, result_filename)
    df_list = []
    # append all feature files to list
    for dirpath, _, filenames in os.walk(input_dir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if (not match_substring) or (match_substring in f):
                try:
                    # read the file and remove it
                    df = pd.read_csv(fp, dtype=object)
                    df_list.append(df)
                    os.remove(fp)
                except Exception as e:
                    print("[ERROR] {}".format(e))
    # rename columns as necessary, from https://stackoverflow.com/questions/37221147/how-do-i-apply-transformations-to-list-of-pandas-dataframes
    for i in range(len(df_list)):
        # drop columns in drop_cols
        temp = df_list[i].rename(columns={session_user_id_colname: user_id_colname})
        temp.drop(drop_cols, axis=1, inplace=True, errors="ignore")
        df_list[i] = temp
    # merge dataframes on userID
    df_out = reduce(lambda df1, df2: df1.merge(df2, on=user_id_colname), df_list)
    # check to ensure number of columns/users has not changed via merging
    assert(all(df.shape[0] == df_out.shape[0] for df in df_list))
    # remove any additional files in output_dir
    for dirpath, _, filenames in os.walk(output_dir):
        for f in filenames:
            try:
                fp = os.path.join(dirpath, f)
                os.remove(fp)
            except Exception as e:
                print("[INFO] exception when attempting to remove file {}: {}".format(fp, e))
    # write results to file in output_dir
    df_out.to_csv(result_fp, index=False)






