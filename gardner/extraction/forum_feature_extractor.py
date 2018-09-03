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


Usage: python3 forum_feature_extractor.py\
-i /path/to/raw_data_directory\
-d /path/to/course_date_file
-o /path/to/output_directory
-n course_name [must match name in coursera_course_dates.csv; ex. "introfinance"]

on JG local:
python3 forum_feature_extractor.py -i raw_data/thermo/ -d coursera_course_dates.csv -o proc_data/thermo/ -n introthermodynamics
"""

import argparse, datetime, re, os
import pandas as pd
import numpy as np
from extraction_utils import course_len, timestamp_week, fetch_start_end_date
from quiz_feature_extractor import fetch_course_runs, get_users_and_weeks, gen_user_week_df, generate_appended_csv
# from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textstat.textstat import textstat
import nltk

MILLISECONDS_IN_SECOND = 1000


def read_forum_and_comment_data(dir, run):
    """
    Read forum and comments data for a given run; combine into single dataframe.
    :param dir: input directory with CSV files of forum data.
    :param run: run number; must match number in filename exactly (i.e., '006' not '6').
    :return: pd.DataFrame of forum data (including both posts and comments) for run.
    """
    forum_file =  [x for x in os.listdir(dir) if x.endswith('{0}_forum_text.csv'.format(run))][0]
    forum_df = pd.read_csv(os.path.join(dir, forum_file))
    # read in universal newline mode; this is due to pandas issue documented here: https://github.com/pandas-dev/pandas/issues/11166
    # forum_df = pd.read_csv(open(os.path.join(input_dir, forum_file), 'rU'), encoding='utf-8', engine='c')
    return forum_df


def gen_thread_order(df):
    """
    Add column with order of post within each thread, by timestamp.
    :param df: pd.DataFrame of forum post data.
    :return: pd.DataFrame of forum data with thread_order column.
    """
    df.sort_values(by = ['thread_id', 'post_time'], inplace = True)
    df['thread_order'] = df.groupby(['thread_id'])['thread_id'].rank(method='first')
    return df


def gen_threads_started(df):
    """
    Generate counts of threads started, by user and week.
    :param df: pd.DataFrame of forum post data.
    :return: pd.DataFrame of 'session_user_id', 'week', and threads_started.
    """
    df_starts = df[df.thread_order == 1].groupby(['session_user_id', 'week']).size().rename('threads_started').reset_index()
    return df_starts


def gen_num_replies(df):
    """
    Generate feature with number of posts by user which were replies to other users (i.e., not to themselves, and not first post in thread).
    :param df: pd.DataFrame of forum post data.
    :return: pd.DataFrame of 'session_user_id', 'week', and num_replies.
    """
    df.sort_values(by=['thread_id', 'post_time'], inplace=True)
    df['previous_post_user_id'] = df.groupby('thread_id')['session_user_id'].shift()
    df_reply = df[(df.thread_order != 1) & (df.session_user_id != df.previous_post_user_id)]
    df_out = df_reply.groupby(['session_user_id', 'week']).size().rename('num_replies').reset_index()
    return df_out


def gen_sentiment_feats(df):
    """
    Generate features based on sentiment: post_net_sentiment and net_sentiment_diff_from_thread_avg. See https://github.com/cjhutto/vaderSentiment for vader sentiment details.
    :param df: pd.DataFrame of forum post data.
    :return: pd.DataFrame of 'session_user_id', 'week', post_net_sentiment, and net_sentiment_diff_from_thread_avg. Note that users who do not post should have NaNs, not zeros.
    """
    import nltk
    nltk.download('vader_lexicon')
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    sid = SentimentIntensityAnalyzer()
    # compute post net sentiment and initialize df_out
    df['post_net_sentiment'] = df['post_text'].apply(lambda x: sid.polarity_scores(x).get('compound'))
    df_out = df.groupby(['session_user_id', 'week'])['post_net_sentiment'].mean().rename('avg_net_sentiment').reset_index()
    # compute net_sentiment_diff_from_thread_avg and merge onto df_out
    thread_avg_sentiment = df.groupby('thread_id')['post_net_sentiment'].mean().rename('thread_avg_net_sentiment').reset_index()
    df = df.merge(thread_avg_sentiment, how = 'left')
    df['net_sentiment_diff_from_thread_avg'] = df['post_net_sentiment'] - df['thread_avg_net_sentiment']
    thread_sentiment_sd = df.groupby('thread_id')['post_net_sentiment'].std().rename('thread_sentiment_sd').reset_index()
    df = df.merge(thread_sentiment_sd, how = 'left')
    df['positive_post'] = (df['post_net_sentiment'] - df['thread_avg_net_sentiment'])/df['thread_avg_net_sentiment'] >= 1
    df['negative_post'] = (df['post_net_sentiment'] - df['thread_avg_net_sentiment']) / df['thread_avg_net_sentiment'] <= -1
    df['neutral_post'] = abs((df['post_net_sentiment'] - df['thread_avg_net_sentiment']) / df['thread_avg_net_sentiment']) < 1
    df_post_sentiment_counts = df.groupby(['session_user_id', 'week'])[['positive_post', 'negative_post', 'neutral_post']]\
        .sum()\
        .rename(columns = lambda x: x + '_count')\
        .reset_index()
    df_out = df_out.merge(df_post_sentiment_counts)
    df_avg_diff = df.groupby(['session_user_id', 'week'])['net_sentiment_diff_from_thread_avg'].mean().rename('avg_net_sentiment_diff_from_thread_avg').reset_index()
    df_out = df_out.merge(df_avg_diff).drop('avg_net_sentiment', axis = 1)
    return df_out


def flesch_reading_ease(text):
    try:
        return textstat.flesch_reading_ease(text)
    except:
        return np.nan


def flesch_kincaid_grade(text):
    try:
        return textstat.flesch_kincaid_grade(text)
    except:
        return np.nan


def gen_flesch_scores(df, reading_ease_bins = [-np.inf] + [x for x in range(10, 100, 10)] + [np.inf], grade_level_bins = [-np.inf] + [x for x in range(20)] + [np.inf]):
    """
    Generate features based on flesch readability scores: 'flesch_reading_ease' and 'flesch_kincaid_grade'.
    :param df: pd.DataFrame of forum post data.
    :return: pd.DataFrame of 'session_user_id', 'week', 'avg_flesch_reading_ease' and 'avg_flesch_kincaid_grade'. Note that users who do not post should have NaNs, not zeros.
    """
    reading_ease_bin_labs = ["_".join(["reading_ease_bin", str(reading_ease_bins[x]), str(reading_ease_bins[x+1])]) for x in range(len(reading_ease_bins)-1)]
    grade_level_bin_labs = ["_".join(["grade_level_bin", str(grade_level_bins[x]), str(grade_level_bins[x + 1])]) for x in range(len(grade_level_bins) - 1)]
    reading_ease_scores = df['post_text'].apply(flesch_reading_ease)
    reading_ease_bin_scores = pd.cut(reading_ease_scores, bins = reading_ease_bins, labels = reading_ease_bin_labs)
    temp = pd.concat([df[['session_user_id', 'week']], pd.get_dummies(reading_ease_bin_scores)], axis = 1)
    flesch_kinkaid_scores = df['post_text'].apply(flesch_kincaid_grade)
    grade_level_bin_scores = pd.cut(flesch_kinkaid_scores, bins = grade_level_bins, labels = grade_level_bin_labs)
    df_out = pd.concat([temp, pd.get_dummies(grade_level_bin_scores)], axis = 1).groupby(['session_user_id', 'week']).sum().reset_index()
    return df_out


def gen_bigram_counts(df):
    """
    Generate counts of unique bigrams used per user per week.
    :param df: pd.DataFrame of forum post data.
    :return: pd.DataFrame of 'session_user_id', 'week', 'unique_bigrams_week'.
    """
    import nltk
    nltk.download('punkt')
    # TODO: look at cleaning text (removing tags, html, etc.)
    # TODO: is there a more efficient way to do this?
    df['post_bigrams'] = df['post_text'].apply(lambda x: [bg for bg in nltk.bigrams(nltk.word_tokenize(x))])
    df_out = df.groupby(['session_user_id', 'week'])['post_bigrams'].apply(sum).rename('all_bigrams_week').reset_index()
    df_out['unique_bigrams_week'] = df_out['all_bigrams_week'].apply(lambda x: len(set(x)))
    df_out.drop('all_bigrams_week', axis = 1, inplace = True)
    return df_out


def gen_forum_features(forum_df, course_start, course_end, dropout_fp = "/output/user_dropout_weeks.csv"):
    forum_df['week'] = (forum_df['post_time']*1000).apply(timestamp_week, args = (course_start, course_end))
    forum_df['post_text'] = forum_df['post_text'].apply(str)
    users, weeks = get_users_and_weeks(forum_df, dropout_fp, week_col='week')
    forum_df = gen_thread_order(forum_df)
    # initialize output dataframe with one entry per user per week
    df_out = gen_user_week_df(users, weeks)
    # compute feature: threads_started
    feat_temp = gen_threads_started(forum_df)
    df_out = df_out.merge(feat_temp, how = 'left')
    df_out['threads_started'].fillna(0, inplace = True)
    # compute feature: avg post length in characters
    forum_df['post_len_char'] = forum_df['post_text'].apply(len)
    feat_temp = forum_df.groupby(['session_user_id', 'week'])['post_len_char'].agg('sum').rename('week_post_len_char').reset_index()
    df_out = df_out.merge(feat_temp, how = 'left')
    df_out['week_post_len_char'].fillna(0, inplace = True)
    # compute feature: number of posts
    feat_temp = forum_df.groupby(['session_user_id', 'week']).size().rename('num_posts').reset_index()
    df_out = df_out.merge(feat_temp, how='left')
    df_out['num_posts'].fillna(0, inplace = True)
    # compute feature: num_replies: count of posts which were responses to other users (i.e., not first post and not self-response)
    feat_temp = gen_num_replies(forum_df)
    df_out = df_out.merge(feat_temp, how='left')
    df_out['num_replies'].fillna(0, inplace = True)
    #compute feature: votes_net : sum of upvotes minus downvotes (this is what 'votes' field is) for all posts that week
    feat_temp = forum_df.groupby(['session_user_id', 'week'])['votes'].sum().rename('votes_net').reset_index()
    df_out = df_out.merge(feat_temp, how='left')
    df_out['votes_net'].fillna(0, inplace=True)
    # compute feature: avg_sentiment
    # compute feature: net_sentiment_diff_from_thread_avg
    feat_temp = gen_sentiment_feats(forum_df)
    df_out = df_out.merge(feat_temp, how='left')
    df_out['positive_post_count'].fillna(0, inplace = True)
    df_out['negative_post_count'].fillna(0, inplace=True)
    df_out['neutral_post_count'].fillna(0, inplace=True)
    df_out['avg_net_sentiment_diff_from_thread_avg'].fillna(0, inplace = True)
    # compute feature: flesch reading ease score and grade level score
    feat_temp = gen_flesch_scores(forum_df)
    df_out = df_out.merge(feat_temp, how='left')
    for c in [x for x in df_out.columns if x.startswith("reading_ease_bin") or x.startswith("grade_level_bin")]:
        df_out[c].fillna(0, inplace = True)
    # compute feature: number of unique bigrams
    feat_temp = gen_bigram_counts(forum_df)
    df_out = df_out.merge(feat_temp, how='left')
    df_out['unique_bigrams_week'].fillna(0, inplace = True)
    return df_out


def write_forum_output(forum_feature_df, output_dir, run, appended = True, week_only = False, week = 2):
    week_df = forum_feature_df[forum_feature_df.week == week]
    if week_df.shape[0] == 0:
        return  # no data for this week
    app_week_df = generate_appended_csv(forum_feature_df, week)
    # write output; fill NaN values with NA so R will be happy :-D
    if week_only:
        week_df.set_index('session_user_id').fillna('NA').to_csv(os.path.join(output_dir, 'week_{0}_forum_only_feats.csv'.format(week)))
    if appended:
        app_week_df.set_index('session_user_id').fillna('NA').to_csv(os.path.join(output_dir, 'week_{0}_forum_appended_feats.csv'.format(week)))
    return


def main(course_name, run, output_dir = '/output', date_file = 'coursera_course_dates.csv'):
    """
    Main workhorse function; builds full forum dataset for course_name.
    :param course_name: course short name; should match name in coursera_course_dates.csv
    :param date_file: course dates CSV file
    :param output_dir: output directory; should be /proc_data/shortname
    :param run: run numbers in 3-digit string format
    :return: None; writes output to output_dir subdirectories
    """
    input_dir = os.path.join('/input', course_name, run)
    date_file_path = os.path.join(input_dir, date_file)
    print('fetching data for run {0}'.format(run))
    # fetch start/end dates
    course_start, course_end = fetch_start_end_date(course_name, run, date_file_path)
    # read in forum data; this combines comments and posts
    forum_df = read_forum_and_comment_data(output_dir, run)
    # generate derived features
    forum_feature_df = gen_forum_features(forum_df, course_start, course_end)
    assert forum_feature_df.isnull().sum().sum() == 0
    # write features to output_dir, by course week
    write_forum_output(forum_feature_df, output_dir, run)
    return None


if __name__ == '__main__':
    # build parser
    parser = argparse.ArgumentParser(description='Create quiz features from CSV files.')
    parser.add_argument('-i', metavar="Input data file path",
                        nargs=1, type=str, required=True, dest='input_dir')
    parser.add_argument('-d', metavar="Course dates CSV",
                        nargs=1, type=str, required=True, dest='date_file')
    parser.add_argument('-o', metavar="output directory path (will be created if does not exist)", nargs=1, type=str,
                        required=True, dest='out_dir')
    parser.add_argument('-n', metavar="course short name; should match name in coursera_course_dates.csv", nargs=1, type=str,
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
    main(course_name, date_file, input_dir=INPUT_DIR, output_dir=OUTPUT_DIR, runs = runs)


