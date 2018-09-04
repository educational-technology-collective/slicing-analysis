"""
Utility script to extract clickstream, forum, and assignment features
"""

from sql_utils import initialize_sql_db, load_sql_dumps, extract_forum_text_csv_from_sql, extract_quiz_csv_from_sql
import argparse
from forum_feature_extractor import main as extract_forum_feats
from quiz_feature_extractor import main as extract_quiz_feats
from clickstream_feature_extractor import main as extract_clickstream_feats

def main(course_id, run_number):
    extract_clickstream_feats(course_id, run_number)
    initialize_sql_db()
    load_sql_dumps(course_id, run_number)
    extract_forum_text_csv_from_sql(course = course_id, session = run_number, outdir='/output')
    extract_quiz_csv_from_sql(course_id, run_number, outdir='/output')
    extract_forum_feats(course_id, run_number)
    extract_quiz_feats(course_id, run_number)
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='execute feature extraction, training, or testing.')
    parser.add_argument('-c', '--course_id', required=True, help='an s3 pointer to a course', default=None)
    parser.add_argument('-r', '--run_number', required=False, help='3-digit course run number', default=None)
    parser.add_argument('--mode', required=False, help='mode')
    args = parser.parse_args()
    main(args.course_id, args.run_number)

