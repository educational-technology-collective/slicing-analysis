import argparse, math, datetime, os, bisect
import pandas as pd

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
