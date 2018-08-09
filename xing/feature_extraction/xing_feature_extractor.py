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


# note: this file needs to be modified to work on remote data files; currently implemented only for reading/writing locally

'''
Takes gzipped Coursera clickstream/log files as input and returns a set of csvs into current working directory.

Each weekly csv is a list of users (rows), with columns corresponding to the features for that week. 
Features come from table 1, pg 123, of the paper ("Temporal predication of dropouts in MOOCs: 
Reaching the low hanging fruit through stacking generalization", Xing et al. 2016)

Usage: python3 xing_feature_extractor.py \
-i /raw_data/path/to/clickstream_export.gz\
 -f raw_data/path/to/forum_posts.csv\
 -c raw_data/path/to/forum_comments.csv\
 -d /path/to/course_date_file
 -o proc_data/shortname/run_number\
 -n course_name [must match name in coursera_course_dates.csv; ex. "introfinance"]
 -r run_number


 On JG local: python3 xing_feature_extractor.py \
-i raw_data/qd/questionnairedesign-002_clickstream_export.gz\
 -f raw_data/qd/questionnairedesign_002_forum_posts.csv\
 -c raw_data/qd/questionnairedesign_002_forum_comments.csv\
 -d coursera_course_dates.csv\
 -o proc_data/qd/002\
 -n questionnairedesign\
 -r 002

'''

import gzip, argparse, json, re, math, datetime, os, bisect, csv, itertools
import pandas as pd
from collections import defaultdict, Counter

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
    return n_weeks

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
    week_starts = [course_start + datetime.timedelta(days=x)
                   for x in range(0, n_weeks * 7, 7)]
    week_number = bisect.bisect_left(week_starts, timestamp) - 1

    if week_number >= 0 and week_number <= n_weeks:
        return week_number
    return None


def extract_users_dropouts(coursera_clickstream_file, course_start, course_end):
    '''
    Assemble list of all users, and dictionary of their dropout weeks.

    :param coursera_clickstream_file: gzipped Coursera clickstream file; see ./sampledata for example
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: tuple of (users, dropout_dict):
        users: Python set of all unique user IDs that registered any activity in clickstream log
        df_dropout: pandas.DataFrame of userID, dropout_week for each user (dropout_week = 0 if no valid activity)

    '''
    users = set()
    user_dropout_weeks = {}
    linecount = 0
    with(gzip.open(coursera_clickstream_file, 'r')) as f:
        for line in f:
            try:
                log_entry = json.loads(line.decode("utf-8"))
                user = log_entry.get('username')
                timestamp = log_entry.get('timestamp', 0)
                week = timestamp_week(timestamp, course_start, course_end)
                users.add(user)
            except ValueError as e1:
                print('Warning: invalid log line {0}: {1}'.format(linecount, e1))
            except Exception as e:
                print('Warning: invalid log line {0}: {1}\n{2}'.format(linecount, e, line))
            if user not in user_dropout_weeks.keys(): #no entry for user
                if not week: #current entry is outside valid course dates; initialize entry with dropout_week = 0
                    user_dropout_weeks[user] = 0
                else: #current entry is within valid course dates; initialize entry with dropout_week = week
                    user_dropout_weeks[user] = week

            else: #entry already exists for user; check and update if necessary
                # update entry for user if week is valid and more recent than current entry
                if week and user_dropout_weeks[user] < week:
                    user_dropout_weeks[user] = week
            linecount += 1
    df_dropout = pd.DataFrame.from_dict(user_dropout_weeks, orient='index')
    #rename columns; handled this way because DataFrame.from_dict doesn't support column naming directly
    df_dropout.index.names = ['userID']
    df_dropout.columns = ['dropout_week']
    output = (users, df_dropout)
    return output


def forum_views_from_clickstream(coursera_clickstream_file, users, course_start, course_end):
    '''
    Extract forum views from Coursera clickstream file.

    :param coursera_clickstream_file: gzipped Coursera clickstream file; see ./sampledata for example
    :param users: list of all user IDs, from extract_users_dropouts(), to count forum views for
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: a pandas.DataFrame with columns for userID, week, n_forumviews.
    '''
    # call course_week_dates to get week start dates
    n_weeks = course_len(course_start, course_end)
    # initialize empty data structure
    # nested dict in format {user: {week: [url1, url2, url3...]}}
    output = {user: {n: [] for n in range(n_weeks + 1)} for user in users}
    # assemble list tuples of forumview records in log
    fre = re.compile('/forum/')
    forumviews = []
    linecount = 1
    with gzip.open(coursera_clickstream_file, 'r') as f:
        for line in f:
            try:
                l = json.loads(line.decode("utf-8"))
                if l['key'] == 'pageview' and fre.search(l['page_url']):
                    forumviews.append(l)
            except ValueError as e1:
                print('Warning: invalid log line {0}: {1}'.format(linecount, e1))
            except Exception as e:
                print('Warning: invalid log line {0}: {1}\n{2}'.format(linecount, e, line))
            linecount += 1
    # extract relevant data for each entry, find week number, and
    # add each forumview URL accessed to (user, week) entry in output
    for p in forumviews:
        user = p.get('username')
        url = p.get('page_url')
        timestamp = p.get('timestamp')
        week = timestamp_week(timestamp, course_start, course_end)
        if week: #if week falls within active dates of course, add to user entry
            output[user][week].append(url)
    output_list = [(k, week, len(views))
                   for k, v in output.items()
                   for week, views in v.items()]

    df_out = pd.DataFrame(data=output_list, columns=['userID', 'week', 'n_forum_views']).set_index('userID')
    return df_out


def active_days_from_clickstream(coursera_clickstream_file, users, course_start, course_end):
    '''
    Extracts count of number of days user registered any course activity in Coursera clickstream file.

    :param coursera_clickstream_file: gzipped Coursera clickstream file; see ./sampledata for example
    :param users: list of all user IDs, from extract_users_dropouts(), to count forum views for
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: a pandas.DataFrame with columns for userID, week, userID, week, n_active_days.
    '''
    # call course_week_dates to get week start dates
    n_weeks = course_len(course_start, course_end)
    linecount = 1
    # initialize empty data structure
    # nested dict in format {user: {week: set(dates_active)}}
    output = {user: {n: set() for n in range(n_weeks + 1)} for user in users}
    with gzip.open(coursera_clickstream_file, 'r') as f:
        for line in f:
            try:
                j = json.loads(line.decode("utf-8"))
                user = j.get('username')
                timestamp = j.get('timestamp')
                week = timestamp_week(timestamp, course_start, course_end)
                access_date = str(datetime.datetime.fromtimestamp(timestamp / MILLISECONDS_IN_SECOND).date())
                try:
                    output[user][week].add(access_date)
                except KeyError:
                    pass
            except ValueError as e1:
                print('Warning: invalid log line {0}: {1}'.format(linecount, e1))
            except Exception as e:
                print('Warning: invalid log line {0}: {1}\n{2}'.format(linecount, e, line))
            linecount += 1
    active_list = [(k, week, len(dates))
                   for k, v in output.items()
                   for week, dates in v.items()]
    df_active = pd.DataFrame(active_list, columns = ['userID', 'week', 'n_active_days']).set_index('userID')
    return df_active


def quiz_views_from_clickstream(coursera_clickstream_file, users, course_start, course_end):
    '''
    Extracts count of quiz views for each of Coursera's assessment types from Coursera clickstream file.

    Note: assessment types include: exams, human-graded, quizzes, peer-graded, in-video quiz.
    Most courses only include a subset of these assessment types.
    These can be added later downstream, or used as separate features.

    :param coursera_clickstream_file: gzipped Coursera clickstream file; see ./sampledata for example
    :param users: list of all user IDs, from extract_users_dropouts(), to count forum views for
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: a pandas.DataFrame with columns userID, week, quizzes_quiz_attempt, quizzes_exam, quizzes_human_graded
    '''
    # call course_week_dates to get week start dates
    n_weeks = course_len(course_start, course_end)
    # compile regex for assessment types
    ere = re.compile('/quiz?quiz_type=exam')  # in 'url'
    qre = re.compile('/quiz/attempt')  # in 'url';avoids counting /quiz/feedback
    hre = re.compile('hg.hg.pageview')  # stored as value for 'key', not in url
    linecount = 1
    # possibly only count one quiz per quiz ID?
    # initialize empty data structure
    # nested dict in format {user: {week: [accessType, accessType...]}}
    output = {user: {n: [] for n in range(n_weeks + 1)} for user in users}
    with gzip.open(coursera_clickstream_file, 'r') as f:
        for line in f:
            try:
                j = json.loads(line.decode("utf-8"))
                user = j.get('username')
                timestamp = j.get('timestamp')
                url = j.get('page_url')
                week = timestamp_week(timestamp, course_start, course_end)
                if week:
                    # check if access_type is one of an assessment type, and if it is
                    # then append entry of that type to output[user][week]
                    if j.get('key') == 'pageview' and qre.search(j.get('page_url')):
                        output[user][week].append('quizzes_quiz_attempt')
                    elif j.get('key') == 'pageview' and ere.search(j.get('page_url')):
                        output[user][week].append('quizzes_exam')
                    elif hre.search(j.get('key')):
                        output[user][week].append('quizzes_human_graded')
            except ValueError as e1:
                print('Warning: invalid log line {0}: {1}'.format(linecount, e1))
            except Exception as e:
                print('Warning: invalid log line {0}: {1}\n{2}'.format(linecount, e, line))
            linecount += 1
    # compute counts of access type by user and week
    quiz_view_list = [(user, week,
                       access.count('quizzes_quiz_attempt'),
                       access.count('quizzes_exam'),
                       access.count('quizzes_human_graded'))
                      for user, user_data in output.items()
                      for week, access in user_data.items()]
    df_quiz = pd.DataFrame(quiz_view_list,
                           columns = ['userID', 'week', 'quizzes_quiz_attempt',
                                      'quizzes_exam', 'quizzes_human_graded'])\
                           .set_index('userID')
    return df_quiz

def forum_line_proc(line, fre, forumviews, linecount):
    try:
        l = json.loads(line.decode("utf-8"))
        if l['key'] == 'pageview' and fre.search(l['page_url']):
            forumviews.append(l)
    except ValueError as e1:
        print('Warning: invalid log line {0}: {1}'.format(linecount, e1))
    except Exception as e:
        print('Warning: invalid log line {0}: {1}\n{2}'.format(linecount, e, line))
    return forumviews


def activeday_line_proc(line, course_start, course_end, active_days_output, linecount):
    try:
        j = json.loads(line.decode("utf-8"))
        user = j.get('username')
        timestamp = j.get('timestamp')
        week = timestamp_week(timestamp, course_start, course_end)
        access_date = str(datetime.datetime.fromtimestamp(timestamp / MILLISECONDS_IN_SECOND).date())
        try:
            active_days_output[user][week].add(access_date)
        except KeyError:
            pass
    except ValueError as e1:
        print('Warning: invalid log line {0}: {1}'.format(linecount, e1))
    except Exception as e:
        print('Warning: invalid log line {0}: {1}\n{2}'.format(linecount, e, line))
    return active_days_output


def quiz_line_proc(line, course_start, course_end, quiz_output, qre, ere, hre, linecount):
    try:
        j = json.loads(line.decode("utf-8"))
        user = j.get('username')
        timestamp = j.get('timestamp')
        url = j.get('page_url')
        week = timestamp_week(timestamp, course_start, course_end)
        if week:
            # check if access_type is one of an assessment type, and if it is
            # then append entry of that type to quiz_output[user][week]
            if j.get('key') == 'pageview' and qre.search(j.get('page_url')):
                quiz_output[user][week].append('quizzes_quiz_attempt')
            elif j.get('key') == 'pageview' and ere.search(j.get('page_url')):
                quiz_output[user][week].append('quizzes_exam')
            elif hre.search(j.get('key')):
                quiz_output[user][week].append('quizzes_human_graded')
    except ValueError as e1:
        print('Warning: invalid log line {0}: {1}'.format(linecount, e1))
    except Exception as e:
        print('Warning: invalid log line {0}: {1}\n{2}'.format(linecount, e, line))
    return quiz_output


def extract_all_clickstream_features(coursera_clickstream_file, users, course_start, course_end):
    """
    Extract active days, forum views, and quiz views in a single pass.
    :return: 
    """
    # initialize all data structures
    n_weeks = course_len(course_start, course_end)
    forum_output = {user: {n: [] for n in range(n_weeks + 1)} for user in users} # nested dict in format {user: {week: [url1, url2, url3...]}}
    fre = re.compile('/forum/')
    forumviews = []
    linecount = 1
    active_days_output = {user: {n: set() for n in range(n_weeks + 1)} for user in users} # nested dict in format {user: {week: set(dates_active)}}
    # compile regex for assessment types
    ere = re.compile('/quiz?quiz_type=exam')  # in 'url'
    qre = re.compile('/quiz/attempt')  # in 'url';avoids counting /quiz/feedback
    hre = re.compile('hg.hg.pageview')  # stored as value for 'key', not in url
    quiz_output = {user: {n: [] for n in range(n_weeks + 1)} for user in users}  # nested dict in format {user: {week: [accessType, accessType...]}}
    # process each clickstream line, extracting any forum views, active days, or quiz views
    with gzip.open(coursera_clickstream_file, 'r') as f:
        for line in f:
            forumviews = forum_line_proc(line, fre, forumviews, linecount)
            active_days_output = activeday_line_proc(line, course_start, course_end, active_days_output, linecount)
            quiz_output = quiz_line_proc(line, course_start, course_end, quiz_output, qre, ere, hre, linecount)
            linecount += 1
    # post-process data from each
    # forum
    # add each forumview URL accessed to (user, week) entry in forum_output
    for p in forumviews:
        user = p.get('username')
        url = p.get('page_url')
        timestamp = p.get('timestamp')
        week = timestamp_week(timestamp, course_start, course_end)
        if week:  # if week falls within active dates of course, add to user entry
            forum_output[user][week].append(url)
    forum_output_list = [(k, week, len(views)) for k, v in forum_output.items() for week, views in v.items()]
    df_forum = pd.DataFrame(data=forum_output_list, columns=['userID', 'week', 'n_forum_views']).set_index('userID')
    # active
    active_list = [(k, week, len(dates)) for k, v in active_days_output.items() for week, dates in v.items()]
    df_active = pd.DataFrame(active_list, columns=['userID', 'week', 'n_active_days']).set_index('userID')
    # Quiz
    quiz_view_list = [(user, week, access.count('quizzes_quiz_attempt'), access.count('quizzes_exam'), access.count('quizzes_human_graded')) for user, user_data in quiz_output.items() for week, access in user_data.items()]
    df_quiz = pd.DataFrame(quiz_view_list, columns=['userID', 'week', 'quizzes_quiz_attempt', 'quizzes_exam', 'quizzes_human_graded']).set_index('userID')
    return (df_forum, df_active, df_quiz)


def extract_forum_posts(forumposts, forumcomments, users, course_start, course_end):
    '''
    Extract counts of forum posts and comments by user/week.

    This script treats forum posts and comments as identical actions.
    See forum_post_sql_query.txt for sample SQL query used to generate a file with this format.
    Note: this function could be modified to extract data directly
        from database using the SQL queries included in /sampledata.

    :param forumposts: csv generated by forum_post_sql_query
        columns: id, thread_id, post_time, user_id,
        public_user_id, session_user_id, eventing_user_id; see ./sampledata for example
    :param forumcomments: csv generated by forum_comment_sql_query
        columns: thread_id, post_time, session_user_id; see ./sampledata for example
    :param users: list of all user IDs, from extract_users_dropouts(), to count forum views for
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: a pandas.DataFrame with columns userID, week, forum_posts
    '''
    n_weeks = course_len(course_start, course_end)
    output = {user: {n: 0 for n in range(n_weeks + 1)} for user in users}
    # postreader = csv.DictReader(forumposts, fieldnames = fieldnames)
    with open(forumposts) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = int(row.get('post_time')) * MILLISECONDS_IN_SECOND
            week = timestamp_week(timestamp, course_start, course_end)
            if week:
                suid = row.get('session_user_id')
                try:
                    output[suid][week] += 1
                except KeyError: # this means user posted in forums but somehow registered no clickstream activity
                    print('Warning: user {0} posted in forum but not in course users list.'.format(suid))
                    output[suid] = {n: 0 for n in range(n_weeks + 1)}
                    output[suid][week] = 1
    with open(forumcomments) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = int(row.get('post_time')) * MILLISECONDS_IN_SECOND
            week = timestamp_week(timestamp, course_start, course_end)
            if week:
                suid = row.get('session_user_id')
                try:
                    output[suid][week] += 1
                except KeyError: # this means user posted in forums but somehow registered no clickstream activity
                    print('Warning: user {0} commented in forum but not in course users list.'.format(suid))
                    output[suid] = {n: 0 for n in range(n_weeks + 1)}
                    output[suid][week] = 1
    post_list = [(user, week, posts)
                 for user, user_data in output.items()
                 for week, posts in user_data.items()]
    df_post = pd.DataFrame(post_list,
                           columns=['userID', 'week', 'forum_posts'])\
                           .set_index('userID')
    return df_post


def extract_forum_social_networks(forumposts, forumcomments, users, course_start, course_end):
    '''
    Extract weekly counts of direct-reply and thread-reply nodes for each user.

    From Xing et al: each student is considered as a node and a comment from one student to another
        in the forum is regarded as an edge between these two students. The degree value is calculated
        as the number of edges the student has.
    This function uses both "comments" and "posts" from Coursera, and creates two types of nodes:
        "direct-reply," which is between students who post adjacent to one another in a given week,
        and "thread-reply," which is between two students who post in the same forum in a given week.
        Note that every post only creates one "direct-reply" edge, but can create several "thread-reply"
        edges if there are multiple posts in a thread in a given week.

    :param forumposts: csv generated by forum_post_sql_query
        columns: id, thread_id, post_time, user_id,
        public_user_id, session_user_id, eventing_user_id; see ./sampledata for example
    :param forumcomments: csv generated by forum_comment_sql_query
        columns: thread_id, post_time, session_user_id; see ./sampledata for example
    :param users: list of all user IDs, from extract_users_dropouts(), to count forum views for
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: a pandas.DataFrame with columns userID, week, direct_nodes, thread_nodes
    '''
    # initialize data structures
    n_weeks = course_len(course_start, course_end)
    # dict of week: thread: [(timestamp, user), (timestamp, user)]
    week_thread_posts = {n: defaultdict(list) for n in range(n_weeks + 1)}
    # temporary containers for direct_reply and thread_reply tuples
    direct_reply = {n: set() for n in range(n_weeks + 1)}
    thread_reply = {n: set() for n in range(n_weeks + 1)}
    # final output structures for direct_reply and thread_reply
    output_direct = {user: {n: 0 for n in range(n_weeks + 1)} for user in users}
    output_thread = {user: {n: 0 for n in range(n_weeks + 1)} for user in users}
    # read forum post and comment files, insert records into week_thread_posts
    with open(forumposts) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = int(row.get('post_time')) * MILLISECONDS_IN_SECOND
            week = timestamp_week(timestamp, course_start, course_end)
            if week:
                suid = row.get('session_user_id')
                thread_id = row.get('thread_id')
                week_thread_posts[week][thread_id].append((timestamp, suid))
    with open(forumcomments) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            timestamp = int(row.get('post_time')) * MILLISECONDS_IN_SECOND
            week = timestamp_week(timestamp, course_start, course_end)
            if week:
                suid = row.get('session_user_id')
                thread_id = row.get('thread_id')
                week_thread_posts[week][thread_id].append((timestamp, suid))
    # collect direct-reply and thread-reply nodes for each week
    for week, threads in week_thread_posts.items():
        for thread_id, posts in threads.items():
            # create direct_reply nodes between consecutive posters
            posts = sorted(posts, key=lambda x: x[0])
            direct_nodes = [(posts[i][1], posts[i + 1][1])
                            for i in range(0, len(posts) - 1)]
            direct_reply[week].update(direct_nodes)
            # create thread_reply nodes between all users posting in thread
            thread_posters = set([post[1] for post in posts])
            thread_reply_nodes = itertools.combinations(thread_posters, 2)
            thread_reply[week].update(thread_reply_nodes)
    # collect count of direct_reply nodes for each user in each week
    for week, nodes in direct_reply.items():
        if nodes:
            userlist = [user for node in nodes for user in node]
            nodecounts = Counter(userlist)
            for user, direct_nodes in nodecounts.items():
                try:
                    output_direct[user][week] = direct_nodes
                except KeyError: # user posted in forums but somehow registered no clickstream activity; create entry and insert value
                    output_direct[user] = {n: 0 for n in range(n_weeks + 1)}
                    output_direct[user][week] = direct_nodes
    # collect count of thread_reply nodes for each user in each week
    for week, nodes in thread_reply.items():
        if nodes:
            userlist = [user for node in nodes for user in node]
            nodecounts = Counter(userlist)
            for user, thread_nodes in nodecounts.items():
                try:
                    output_thread[user][week] = thread_nodes
                except KeyError:  # user posted in forums but somehow registered no clickstream activity; create entry and insert value
                    output_thread[user] = {n: 0 for n in range(n_weeks + 1)}
                    output_thread[user][week] = thread_nodes
    full_node_list = [(user, week, nodes, output_thread[user][week])
                      for user, user_data in output_direct.items()
                      for week, nodes in user_data.items()
                      if user in users]
    df_snd = pd.DataFrame(full_node_list,
                           columns = ['userID', 'week', 'direct_nodes', 'thread_nodes'])\
                           .set_index('userID')
    return df_snd

def generate_appended_xing_csv(df_in, dropout_weeks, week):
    """
    Create appended feature set for week from df_in.

    :param dropout_weeks:
    :param df_in: Full pandas.DataFrame of userID, week, and additional features.
    :param week: Week to create appended feature set for (starting at zero, inclusive)
    :return: pandas.DataFrame of appended ('wide') features for weeks in interval [0, week], plus dropout column.
    """
    #initialize output data using week 0; additional weeks will be merged on userID

    for i in range(0, week+1): # append data from weeks 0-current week
        df_to_append = df_in[df_in.week == i].drop('week', axis=1)
        df_to_append = df_to_append\
            .rename(columns = lambda x: 'week_{0}_{1}'.format(str(i), str(x)))\
            .reset_index()
        if i ==0: #nothing to merge on yet; initialize df_app using week 0 features
            df_app = df_to_append.set_index('userID')
        else: #append features by merging to current feature set
            df_app = df_app.reset_index()\
            .merge(df_to_append)\
            .set_index('userID')
    #add final binary dropout column
    df_app = df_app.join(dropout_weeks, how='left')
    df_app['dropout_current_week'] = df_app.apply(
        lambda row: (1 if row['dropout_week']==week else 0), axis=1)
    df_app = df_app.drop('dropout_week', axis=1)
    return df_app


def generate_weekly_csv(df_in, dropout_weeks, out_dir):
    """
    Create a series of csv files containing all entries for each week in df_in

    :param df_in: pandas.DataFrame of weekly features to write output for
    :param dropout_weeks: pandas.DataFrame of dropout week number by userID
    :return: Nothing returned; writes csv files to /xing_extractor_output
    """
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    startwk, endwk = min(df_in['week']), max(df_in['week']) + 1
    for i in range(startwk, endwk):
        week_folder = os.path.join(out_dir, ('week_%s') % (i))
        if not os.path.exists(week_folder):
            os.makedirs(week_folder)
        df_out = df_in.join(dropout_weeks, how='left')
        df_out['dropout_current_week'] = df_out.apply(
            lambda row: (1 if row['week'] == row['dropout_week'] else 0), axis=1)
        wk_only_df = df_out[df_out.week == i]\
            .drop(['dropout_week', 'week'], axis=1)
        wk_sum_df = df_out[df_out.week <= i]\
            .reset_index()\
            .groupby('userID')\
            .sum()\
            .drop(['dropout_week', 'week'], axis=1)
        wk_appended_df = generate_appended_xing_csv(df_in, dropout_weeks, i)
        wk_only_destfile = os.path.join(week_folder, "week_%s_only_feats.csv" % i)
        wk_only_df.to_csv(wk_only_destfile)
        wk_sum_destfile = os.path.join(week_folder, "week_%s_sum_feats.csv" % i)
        wk_sum_df.to_csv(wk_sum_destfile)
        wk_app_destfile = os.path.join(week_folder, "week_%s_appended_feats.csv" % i)
        wk_appended_df.to_csv(wk_app_destfile)


def extract_features(coursera_clickstream_file, forumfile, commentfile, users, course_start, course_end):
    """
    Extract full set of features from clickstream, forum, and comment file.

    :param coursera_clickstream_file: gzipped Coursera clickstream file; see ./sampledata for example
    :param forumfile: csv generated by forum_post_sql_query
        columns: id, thread_id, post_time, user_id,
        public_user_id, session_user_id, eventing_user_id; see ./sampledata for example
    :param commentfile: csv generated by forum_comment_sql_query
        columns: thread_id, post_time, session_user_id; see ./sampledata for example
    :param users: list of all user IDs, from extract_users_dropouts(), to count forum views for
    :param course_start: datetime object for first day of course (generated from user input)
    :param course_end: datetime object for last day of course (generated from user input)
    :return: pandas.DataFrame of features by user id and week
    """
    # print("Extracting forum views...")
    # forumviews = forum_views_from_clickstream(coursera_clickstream_file, users, course_start, course_end)
    # print("Forum view extraction complete. \nExtracting active days...")
    # activedays = active_days_from_clickstream(coursera_clickstream_file, users, course_start, course_end)
    # print("Active days extraction complete. \nExtracting quiz views...")
    # quizviews = quiz_views_from_clickstream(coursera_clickstream_file, users, course_start, course_end)
    # print("Quiz view extraction complete. \nExtracting forum posts...")
    print("Extracting all clickstream-based features: forum views, active days, quiz views...")
    forumviews, activedays, quizviews = extract_all_clickstream_features(coursera_clickstream_file, users, course_start, course_end)
    print("Clickstream feature extraction complete.")
    forumposts = extract_forum_posts(forumfile, commentfile, users, course_start, course_end)
    print("Forum post extraction complete. \nExtracting social networks...")
    snd = extract_forum_social_networks(forumfile, commentfile, users, course_start, course_end)
    # merge into single data frame
    features_df = forumviews.reset_index().merge(
        activedays.reset_index()).merge(
        quizviews.reset_index()).merge(
        forumposts.reset_index()).merge(
        snd.reset_index()).set_index('userID')
    return features_df


def main(course_name, run_number):
    session_dir = '/input/{0}/{1}/'.format(course_name, run_number)
    clickstream = [x for x in os.listdir(session_dir) if x.endswith('clickstream_export.gz')][0]
    coursera_clickstream_file = session_dir + clickstream
    forumfile = session_dir + 'forum_posts.csv'
    commentfile = session_dir + 'forum_comments.csv'
    OUTPUT_DIRECTORY = '/output'
    course_start, course_end = fetch_start_end_date(course_name, run_number, session_dir + 'coursera_course_dates.csv')
    # build features
    print("Extracting users...")
    users, dropout_weeks = extract_users_dropouts(coursera_clickstream_file, course_start, course_end)
    print("Complete. Extracting features...")
    feats_df = extract_features(coursera_clickstream_file, forumfile, commentfile, users, course_start, course_end)
    # write output
    generate_weekly_csv(feats_df, dropout_weeks, out_dir=OUTPUT_DIRECTORY)
    dropout_file_path = "%s/user_dropout_weeks.csv" % (OUTPUT_DIRECTORY)
    dropout_weeks.to_csv(dropout_file_path)
    print("Output written to {}".format(OUTPUT_DIRECTORY))


if __name__ == '__main__':
    # build parser
    parser = argparse.ArgumentParser(description='Create features from Coursera clickstream file.')
    parser.add_argument('-n', '--course_name',
                        metavar="course short name [must match name in coursera_course_dates.csv; ex. 'introfinance'",
                        type=str,
                        required=True)
    parser.add_argument('-r', '--run_number', metavar="3-digit run number", type=str, required=True)
    # collect input from parser and assign variables
    args = parser.parse_args()
    main(course_name=args.course_name, run_number=args.run_number)
