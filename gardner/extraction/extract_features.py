"""
Utility script to extract clickstream, forum, and assignment features
"""

from sql_utils import initialize_sql_db, load_sql_dumps, extract_forum_text_csv_from_sql, extract_quiz_csv_from_sql
import argparse
import pandas as pd
from forum_feature_extractor import main as extract_forum_feats
from quiz_feature_extractor import main as extract_quiz_feats
from clickstream_feature_extractor import main as extract_clickstream_feats

parser = argparse.ArgumentParser(description='execute feature extraction, training, or testing.')
parser.add_argument('-c', '--course_id', required=True, help='an s3 pointer to a course')
parser.add_argument('-r', '--run_number', required=False, help='3-digit course run number')
parser.add_argument('--mode', required=False, help='mode')
args = parser.parse_args()

extract_clickstream_feats(args.course_id, args.run_number)
initialize_sql_db()
load_sql_dumps(args.course_id, args.run_number)
extract_forum_text_csv_from_sql(course = args.course_id, session = args.run_number, outdir='/output')
extract_quiz_csv_from_sql(args.course_id, args.run_number, outdir='/output')
extract_forum_feats(args.course_id, args.run_number)
extract_quiz_feats(args.course_id, args.run_number)