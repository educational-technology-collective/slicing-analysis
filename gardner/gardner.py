"""
Main script to replicate "best" model from Gardner and Brooks (2018)
"""

import argparse
import subprocess

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="execute feature extraction, training, or testing.")
    parser.add_argument("-c", "--course", required=True, help="an s3 pointer to a course")
    parser.add_argument("-r", "--session", required=False, help="3-digit course run number")
    parser.add_argument("-m", "--mode", required=True, help="mode to run image in; {extract, train, test}")

    args = parser.parse_args()
    if args.mode == "extract":
        from extraction.extract_features import main as extract_features
        extract_features(args.course, args.session)
    elif args.mode == "train":
        cmd = "Rscript build_models.R --course {} --session {} --working_dir /input --output_dir /output".format(args.course, args.session)
        subprocess.call(cmd, shell=True)
        # todo: archive results and shift somewhere if needed
    elif args.mode == "test":
        #todo
        pass




