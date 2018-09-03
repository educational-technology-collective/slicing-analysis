import argparse
import subprocess

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="execute feature extraction, training, or testing.")
    parser.add_argument("-c", "--course", required=True, help="an s3 pointer to a course")
    parser.add_argument("-r", "--session", required=False, help="3-digit course run number")
    parser.add_argument("-m", "--mode", required=True, help="mode to run image in; {extract, train, test}")

    args = parser.parse_args()
    if args.mode == "extract":
        subprocess.call("python3 extraction/extract_features.py", shell=True)
    elif args.mode == "train":
        subprocess.call("python3 modeling/build_models.py", shell=True)
    elif args.mode == "test":
        #todo
        pass




