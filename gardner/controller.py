"""
Controller script to replicate Gardner and Brooks (2018) "best" model using the MORF platform.
"""

from morf.workflow.extract import extract_session, extract_holdout_session
from morf.workflow.train import train_course
from morf.workflow.test import test_course

extract_session(multithread=False)
extract_holdout_session(multithread=False)
train_course(label_type='dropout', multithread=False)
test_course(label_type='dropout')
evaluate_course(label_type='dropout')
