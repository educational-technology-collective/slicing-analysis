# Copyright (C) 2016  The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see [http://www.gnu.org/licenses/].

"""

Takes CSVs of Coursera quiz scores as input (from joined Coursera quiz_submission_metadata and quiz_metadata tables) and outputs a set of quiz features.

For the SQL queries used to generate the CSV files of raw input data, see ./sql/quiz_sql_query.txt


Usage: python3 quiz_feature_extractor.py\
-i /path/to/raw_data_directory\
-d /path/to/course_date_file
-o /path/to/output_directory
-n course_name [must match name in coursera_course_dates.csv; ex. "introfinance"]

on JG local:
python3 quiz_feature_extractor.py -i raw_data/thermo/ -d coursera_course_dates.csv -o proc_data/thermo/ -n introthermodynamics
"""


import argparse, datetime, re, os
import pandas as pd
import numpy as np
import itertools
from extraction_utils import course_len, timestamp_week, fetch_start_end_date

MILLISECONDS_IN_SECOND = 1000
MILLISECONDS_IN_DAY = 86400000

def fetch_course_runs(input_dir):
    """
    Fetch numbers of course runs; this looks for any 3-digit numbers in filenames in input_dir.
    :param input_dir: input directory with CSV files of quiz data.
    :return: list of strings of course run numbers as they appear in filenames.
    """
    runs = [re.match('.*(\d{3})_quiz\.csv', f).group(1) for f in os.listdir(input_dir) if re.match('.*(\d{3})_quiz\.csv', f)]
    return runs


def read_quiz_data(dir, run):
    """
    Read quiz data for a given run.
    :param dir: input directory with CSV files of quiz data.
    :param run: run number; must match number in filename exactly (i.e., '006' not '6').
    :return: pd.DataFrame of quiz data for run.
    """
    quiz_file =  [x for x in os.listdir(dir) if x.endswith('{0}_quiz.csv'.format(run))][0]
    quiz_df = pd.read_csv(os.path.join(dir, quiz_file))
    return quiz_df


def read_quiz_metadata(dir, run):
    """
    Read quiz metadata for a given run.
    :param dir: input directory with CSV files of quiz data.
    :param run: run number; must match number in filename exactly (i.e., '006' not '6').
    :return: pd.DataFrame of quiz metadata for run.
    """
    quiz_meta_file =  [x for x in os.listdir(dir) if x.endswith('{0}_quiz_metadata.csv'.format(run))][0]
    quiz_meta_df = pd.read_csv(os.path.join(dir, quiz_meta_file))
    return quiz_meta_df


def get_users_and_weeks(df, dropout_fp, df_user_col = 'session_user_id', dropout_user_col = 'userID', week_col = 'assignment_week'):
    """
    Helper function to fetch all unique users and weeks in a course
    :param df: pd.DataFrame of course quiz data; needs columns for session_user_id and assignment_week
    :param dropout_fp: path to droput csv from clickstream_feature_extractor
    :param df_user_col: name of column containing unique user IDs in df.
    :param dropout_user_col: name of column containing user IDs in droput df (user_dropout_weeks.csv, in output_dir). This will be set in xing_feature_extractor.py.
    :param week_col: name of column containing weeks.
    :return: series containing all unique session_user_ids in df, and zero-indexed list of all week numbers in course as integers.
    """
    dropout_df = pd.read_csv(dropout_fp)
    users = dropout_df[dropout_user_col].unique()
    weeks = [x for x in range(int(max(df[week_col].dropna().unique())) + 1)]
    return users,weeks


def gen_user_week_df(users, weeks):
    """
    Create dataframe with all unique combinations of users and weeks.
    :param users: array or iterable of user ids.
    :param weeks: array or iterable of weeks.
    :return: pd.DataFrame with columns 'session_user_id', 'week'
    """
    user_df = pd.DataFrame({'key': 1, 'session_user_id': users})
    week_df = pd.DataFrame({'key': 1, 'week': weeks})
    df_out = pd.merge(user_df, week_df, on='key')[['session_user_id', 'week']]
    return df_out


def merge_feat_df(df, feat_temp_df, zero_fill_cols = None, zero_fill_prefix = None):
    """
    Helper function to perform joining between output dataframe and temporary feature dataframes.
    :param df_out: pd.DataFrame of user-week level features
    :param feat_temp_df: pd.DataFrame of new features to be appended, column-wise, to df_out.
    :return: df_out with feat_temp_df merged on, using left merge to retain all user-week records even if not present in feat_temp_df.
    """
    df_out = df.merge(feat_temp_df, how='left', left_on=['session_user_id', 'week'], right_on=['session_user_id', 'assignment_week']).drop('assignment_week', axis=1)
    if zero_fill_cols:
        for col in zero_fill_cols:
            df_out[col].fillna(0, inplace = True)
    if zero_fill_prefix:
        for col in [x for x in df_out.columns if x.startswith(zero_fill_prefix)]:
            df_out[col].fillna(0, inplace=True)
    return df_out


def gen_quiz_expanding_mean(df_in, users, weeks, quiz_types = ('video', 'quiz', 'homework')):
    """
    Generate columns with expanging mean--i.e., cumulative or rolling mean--for each user across all previous weeks for quiz_type.
    :param df_in: dataframe of user-submission level quiz data and features.
    :param quiz_types: tuple of quiz types to consider or ('AGG') to aggregate all quiz types into single feat; default is all Coursera Spark quiz types documented here https://wiki.illinois.edu/wiki/display/coursera/quiz_metadata.
    :return: pd.DataFrame with user-week level features.
    """
    # initialize dataframe with all user, week combinations
    df_out = pd.DataFrame([x for x in itertools.product(users, weeks)], columns=['session_user_id', 'assignment_week'])
    for qt in quiz_types: # for each quiz type, compute expanding mean for user by week and merge as new column onto df_out
        new_col_name = 'prior_avg_quiz_score_{0}'.format(qt)
        if qt != 'AGG':
            if not qt in df_in.quiz_type.unique():  # quiz type not used in course; set column to na and continue to next quiz type
                df_out[new_col_name] = np.nan
                continue
            df = df_in[df_in.quiz_type == qt][['session_user_id', 'assignment_week', 'raw_score']]
        else:  # for AGG; get a subset of columns but keep all rows
            df = df_in[['session_user_id', 'assignment_week', 'raw_score']]
        # get expanding sums of raw scores and counts of quizzes at user-week level
        df_feat = df.groupby(['session_user_id', 'assignment_week'])['raw_score'] \
            .agg(('sum', 'count')) \
            .reindex(pd.MultiIndex.from_product([users, weeks], names=['session_user_id', 'assignment_week'])) \
            .groupby(level=0) \
            .cumsum() \
            .groupby(level=0) \
            .shift(1)
        df_feat[new_col_name] = df_feat['sum'] / df_feat['count']
        df_feat = df_feat.reset_index().drop(['count', 'sum'], axis=1)
        user_id_ixs = df_feat['session_user_id']
        df_feat = df_feat.groupby('session_user_id').fillna(method='ffill')
        df_feat['session_user_id'] = user_id_ixs
        df_feat = df_feat[['session_user_id', 'assignment_week', new_col_name]]
        df_out = df_out.merge(df_feat, how = 'left')
    return df_out


def pct_max_weekly_submissions(quiz_df, quiz_meta_df):
    """
    Helper function to compute student weekly submissions as a percentage of max # of submissions, and as a percentage of the highest number of student submissions that week.
    :param quiz_df: pd.DataFrame of quiz submission data.
    :param quiz_meta_df: pd.DataFrame of quiz metadata.
    :return: df_out; pd.DataFrame with session_user_id, assignment_week, total_user_submissions_week, and weekly_pct_max_submissions
    """
    # submissions as percentage of maximum instructor-allowed submissions that week
    max_submission_df = quiz_meta_df.groupby('assignment_week')['maximum_submissions'].agg('sum').rename('max_allowed_submissions_week').reset_index()
    total_submission_df = quiz_df[['session_user_id', 'assignment_week']].groupby(['session_user_id', 'assignment_week']).size().rename('total_user_submissions_week').reset_index()
    df_out = total_submission_df.merge(max_submission_df)
    df_out['weekly_pct_max_allowed_submissions'] = df_out['total_user_submissions_week']/df_out['max_allowed_submissions_week']
    df_out.drop('max_allowed_submissions_week', axis = 1, inplace = True)
    # submissions as a percentage of maximum/highest number of student submissions that week
    max_student_submission_df = df_out.groupby('assignment_week')['total_user_submissions_week'].agg('max').rename('max_student_submissions_week').reset_index()
    df_out = df_out.merge(max_student_submission_df)
    df_out['weekly_pct_max_student_submissions'] = df_out['total_user_submissions_week']/df_out['max_student_submissions_week']
    df_out.drop('max_student_submissions_week', axis = 1, inplace = True)
    return df_out


def raw_points_per_submission(quiz_df):
    total_submission_df = quiz_df[['session_user_id', 'assignment_week']]\
        .groupby(['session_user_id', 'assignment_week'])\
        .size()\
        .rename('total_user_submissions_week')\
        .reset_index()
    total_raw_points_df = quiz_df\
        .groupby(['session_user_id', 'assignment_week'])['raw_score']\
        .agg('sum')\
        .rename('total_raw_points_week')\
        .reset_index()
    df_out = total_submission_df.merge(total_raw_points_df)
    df_out['raw_points_per_submission'] = df_out['total_raw_points_week'] / df_out['total_user_submissions_week']
    df_out.drop('total_user_submissions_week', axis = 1, inplace = True)
    return df_out


def pre_dl_submissions(quiz_df, submission_bins = [-np.inf, 0, MILLISECONDS_IN_DAY, 3*MILLISECONDS_IN_DAY, 7*MILLISECONDS_IN_DAY, np.inf], bin_labels = ['pre_dl_submission_count_late', 'pre_dl_submission_count_0_1_day', 'pre_dl_submission_count_1_3_day', 'pre_dl_submission_count_3_7_day', 'pre_dl_submission_count_greater_7_day']):
    """
    Create dataframe with counts of submissions within bins defined by submission_bins by user and week.
    :param quiz_df:
    :param submission_bins:
    :param bin_labels:
    :return:
    """
    # create categorical based on cut points of (1 week; 3 days; > 1 day; < 1 day)
    quiz_submissions_binned = pd.cut(quiz_df['pre_dl_submission_time'], bins = submission_bins, labels = bin_labels)
    temp = pd.concat([quiz_df[['session_user_id', 'assignment_week']], pd.get_dummies(quiz_submissions_binned)], axis = 1).groupby(['session_user_id', 'assignment_week']).agg('sum').reset_index()
    return temp


def gen_quiz_features(quiz_df, quiz_meta_df, course_start, course_end, quiz_types = ('video', 'quiz', 'homework'), dropout_fp = "/output/user_dropout_weeks.csv"):
    """
    Generates derived features for quiz_df.
    :param quiz_df: raw pd.DataFrame of submission-level quiz data as pd.DataFrame; this is also used to append any new columns needed for deriving complex features.
    :param quiz_meta_df: pd.DataFrame of quiz-level metadata
    :param course_start:
    :param course_end:
    :quiz_types: list of quiz types to consider; other quiz types are excluded (quiz types are video, quiz, homework, exam, survey; see documentation here for more info on quiz types: https://wiki.illinois.edu/wiki/display/coursera/quiz_metadata
    :return: df_out, user-week level pd.DataFrame of quiz data with derived features (one entry per user per week).
    """
    # add columns with submission and assignment week using timestamp and course start/end dates
    # note that pre-multiplying by 1000 is necessary because timestamp fomat for these submissions is different from clickstream timestamp format
    quiz_df['submission_week'] = (quiz_df['submission_time']*1000).apply(timestamp_week, args = (course_start, course_end))
    quiz_df['assignment_week'] = (quiz_df['soft_close_time']*1000).apply(timestamp_week, args = (course_start, course_end))
    quiz_meta_df['assignment_week'] = (quiz_meta_df['soft_close_time']*1000).apply(timestamp_week, args = (course_start, course_end))
    quiz_df['pre_dl_submission_time'] = quiz_df['soft_close_time'] - quiz_df['submission_time']
    # fetch users and weeks from df
    users, weeks = get_users_and_weeks(quiz_df, dropout_fp)
    # create dataframe of users and weeks; this is user-week level dataframe for output.
    df_out = gen_user_week_df(users, weeks)
    # compute feature: average pre_dl_submission_time by user/week
    feat_temp = pre_dl_submissions(quiz_df)
    df_out = merge_feat_df(df_out, feat_temp, zero_fill_prefix="pre_dl_submission_count")
    # compute feature: average grade across all submissions within quiz_types by user/week
    feat_temp = quiz_df[quiz_df.quiz_type.isin(quiz_types)].groupby(['assignment_week', 'session_user_id']) \
        .mean()['raw_score'] \
        .rename('avg_raw_score_week') \
        .reset_index()
    df_out = merge_feat_df(df_out, feat_temp, zero_fill_cols=['avg_raw_score_week'])
    # compute feature: average grade by quiz type by user/week
    feat_temp = quiz_df[quiz_df.quiz_type.isin(quiz_types)].groupby(['assignment_week', 'session_user_id', 'quiz_type'])['raw_score'].mean().unstack(
        level=-1).rename(columns=lambda x: 'weekly_avg_score_' + x + '_quiz_type').reset_index()
    missing_quiz_types = [x for x in quiz_types if x not in quiz_df.quiz_type.unique()]
    # # create column of NAN values for any quiz types not used in course
    for qt in missing_quiz_types:
        feat_temp['weekly_avg_score_{0}_quiz_type'.format(qt)] = np.nan
    df_out = merge_feat_df(df_out, feat_temp, zero_fill_prefix="weekly_avg_score_")
    # compute feature: difference between weekly quiz avg and prior quiz avg
    feat_temp = gen_quiz_expanding_mean(quiz_df, users, weeks)
    df_out = merge_feat_df(df_out, feat_temp)
    for qt in quiz_types:
        # subtract week average from prior week expanding average to get change
        df_out['week_avg_change_{0}_quiz_type'.format(qt)] = df_out['weekly_avg_score_{0}_quiz_type'.format(qt)] - df_out['prior_avg_quiz_score_{0}'.format(qt)]
        # fill zeros if no change
        df_out['week_avg_change_{0}_quiz_type'.format(qt)].fillna(0, inplace = True)
    # drop prior_average; not needed
    df_out.drop([x for x in df_out.columns if 'prior_avg_quiz_score' in x], axis = 1, inplace = True)
    # compute feature: avg number of submissions as percent of maximum number of allowed submissions;
    # compute feature: avg number of submissions as percent of maximum number of actual student submissions
    feat_temp = pct_max_weekly_submissions(quiz_df, quiz_meta_df)
    df_out = merge_feat_df(df_out, feat_temp)
    # compute feature: Avg quiz grade/number of submissions (raw_points_per_submission)
    feat_temp = raw_points_per_submission(quiz_df)
    df_out = merge_feat_df(df_out, feat_temp, zero_fill_cols=['raw_points_per_submission', 'total_raw_points_week', 'weekly_pct_max_allowed_submissions', 'weekly_pct_max_student_submissions', 'total_user_submissions_week'])
    return df_out


def generate_appended_csv(df_in, week):
    """
    Helper function to generate 'wide' appended dataframe from 'long' feature set.
    :param df_in: Full pandas.DataFrame of userID, week, and additional features.
    :param week: Week to create appended feature set for (starting at zero, inclusive)
    :return: pandas.DataFrame of appended ('wide') features for weeks in interval [0, week].
    """
    #initialize output data using week 0; additional weeks will be merged on session_user_id
    df_app = df_in[df_in.week == 0].set_index(['session_user_id', 'week']).rename(columns = lambda x: 'week_0_{0}'.format(str(x))).reset_index()
    if week == 0: # nothing to append; first week of course
        return df_app
    for i in range(1, week+1): # append data from weeks 0-current week
        df_to_append = df_in[df_in.week == i].drop('week', axis=1).set_index('session_user_id')
        df_to_append = df_to_append\
            .rename(columns = lambda x: 'week_{0}_{1}'.format(str(i), str(x)))\
            .reset_index()
        # merge with df_app
        df_app = df_app.merge(df_to_append)
    return df_app


def write_quiz_output(quiz_feature_df, output_dir, appended = True, week_only = False, week = 2):
    week_df = quiz_feature_df[quiz_feature_df.week == week]
    if week_df.shape[0] == 0:
        return  # no data for this week
    app_week_df = generate_appended_csv(quiz_feature_df, week)
    # write output; fill NaN values with NA so R will be happy :-D
    if week_only:
        week_df.set_index('session_user_id').fillna('NA').to_csv(os.path.join(output_dir, 'week_{0}_quiz_only_feats.csv'.format(week)))
    if appended:
        app_week_df.set_index('session_user_id').fillna('NA').to_csv(os.path.join(output_dir, 'week_{0}_quiz_appended_feats.csv'.format(week)))
    return


def main(course_name, run,  output_dir = '/output', date_file = 'coursera_course_dates.csv'):
    """
    Main workhorse function; builds full quiz datasets (appended and week-only) for course_name and writes as CSVs to ouput_dir.
    :param course_name: course short name; should match name in coursera_course_dates.csv
    :param date_file: course dates CSV file
    :param output_dir: output directory
    :return: None; writes output to output_dir
    """
    input_dir = os.path.join('/input', course_name, run)
    print('fetching data for run {0}'.format(run))
    # fetch start/end dates
    date_file_path = os.path.join(input_dir, date_file)
    course_start, course_end = fetch_start_end_date(course_name, run, date_file_path)
    # n_weeks = course_len(course_start, course_end)
    # read in quiz data
    quiz_df = read_quiz_data(output_dir, run)
    quiz_meta_df = read_quiz_metadata(output_dir, run)
    # generate derived features
    quiz_feature_df = gen_quiz_features(quiz_df, quiz_meta_df, course_start, course_end)
    assert quiz_feature_df.isnull().sum().sum() == 0
    # write features to output_dir, by course week; note that many courses won't have any data for week zero (no quizzes due in first week)
    write_quiz_output(quiz_feature_df, output_dir)
    return


if __name__ == '__main__':
    # build parser
    parser = argparse.ArgumentParser(description='Create quiz features from CSV files.')
    parser.add_argument('-i', metavar="input directory; should be /raw_data/shortname",
                        nargs=1, type=str, required=True, dest='input_dir')
    parser.add_argument('-d', metavar="Course dates CSV",
                        nargs=1, type=str, required=True, dest='date_file')
    parser.add_argument('-o',
                        metavar="output directory; should be /proc_data/shortname (will be created if does not exist)",
                        nargs=1, type=str,
                        required=True, dest='out_dir')
    parser.add_argument('-n', metavar="course short name; should match name in coursera_course_dates.csv", nargs=1,
                        type=str,
                        required=True, dest='course_name')
    # collect input from parser
    args = parser.parse_args()
    INPUT_DIR = args.input_dir[0]
    date_file = args.date_file[0]
    course_name = args.course_name[0]
    OUTPUT_DIR = args.out_dir[0]
    # fetch list of run numbers from input_dir
    runs = fetch_course_runs(INPUT_DIR)
    # generate features for each run and export as CSVs into OUTPUT_DIR
    main(course_name, date_file, input_dir=INPUT_DIR, output_dir=OUTPUT_DIR, runs=runs)

