"""
Main script to replicate "best" model from Gardner and Brooks (2018)
"""

import argparse
import subprocess

INPUT_DIR = "/input"
OUTPUT_DIR = "/output"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="execute feature extraction, training, or testing.")
    parser.add_argument("-c", "--course", required=True, help="an s3 pointer to a course")
    parser.add_argument("-r", "--session", required=False, help="3-digit course run number")
    parser.add_argument("-m", "--mode", required=True, help="mode to run image in; {extract, train, test}")
    parser.add_argument("--model_type", required = True, help="type of model to use for training/testing")

    args = parser.parse_args()
    if args.mode == "extract":
        from extraction.extract_features import main as extract_features
        from extraction.extraction_utils import aggregate_and_remove_feature_files
        extract_features(args.course, args.session)
        aggregate_and_remove_feature_files(input_dir="/output", match_substring="feats")
    elif args.mode == "train":
        cmd = "Rscript modeling/train.R --course {} --input_dir /input --output_dir /output --model_type {}".format(args.course, args.model_type)
        subprocess.call(cmd, shell=True)
    elif args.mode == "test":
        cmd = "Rscript modeling/test.R --course {} --input_dir /input --output_dir /output".format(
            args.course, args.model_type)
        subprocess.call(cmd, shell=True)

