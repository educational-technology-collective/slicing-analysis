import tarfile
import subprocess

parser = argparse.ArgumentParser(description='execute feature extraction, training, or testing.')
parser.add_argument('-c', '--course_id', required=True, help='an s3 pointer to a course')
parser.add_argument('-r', '--run_number', required=False, help='3-digit course run number')
args = parser.parse_args()
cmd = "Rscript build_models.R --course {} --session {}".format(args.course_id, args.run_number)
subprocess.call(cmd, shell = True)

# todo: archive results and shift somewhere