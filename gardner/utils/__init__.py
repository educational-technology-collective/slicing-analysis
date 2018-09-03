import os
import subprocess
import shutil
import re
import tarfile
import tempfile
import configparser
from utils.model_evaluation import calculate_simple_average, make_pub_simple_avg_df, generate_frequentist_comparison, generate_posterior_comparison

_properties = None

def get_properties(config_file = "config.properties"):
    '''
    Returns the list of properties as a dict of key/value pairs in the file config.properties.

    :return:
    '''
    global _properties

    if _properties is None:
        cf = configparser.ConfigParser()
        cf.read(config_file)
        _properties = {}
        for section in cf.sections():
            for item in cf.items(section):
                _properties[item[0]] = item[1]
    return _properties


def fetch_courses_and_sessions(dir):
    """
    Fetch list of (course_name, session) tuples in dir.
    :param dir: directory to search within; should have course/session subdirectories.
    :return: list of (course_name, session) tuples.
    """
    outlist = []
    courses = [x for x in os.listdir(dir) if not '.' in x and x != 'dockerfile']
    for course in courses:
        sessions = [x for x in os.listdir('/'.join([dir, course])) if not '.' in x ]
        for session in sessions:
            outlist.append((course, session))
    return outlist


def make_tarfile(course, session, source_dir, dest_dir):
    tarname = "{}-{}-data.tar".format(course, session)
    with tarfile.open(tarname, "w") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
    shutil.move(tarname, dest_dir)
    return


def load_run_cleanup_image(course, session, working_data_dir, output_dir, image_url, docker_exec):
    # load image
    image_name = image_url.split('/')[-1]
    print("extracting features for course {} session {}".format(course, session))
    cmd = '{} load -i {}'.format(docker_exec, image_name)
    res = subprocess.check_output(cmd, shell=True)
    print(str(res))
    image_uuid = str(res).split('sha256:')[-1].replace("\\n", "").replace("'", "")
    # run image
    cmd = '''{} run --rm=true --volume={}:/input --volume={}:/output {} --course_id {} --run_number {}'''.format(
        docker_exec, working_data_dir, output_dir, image_uuid, course, session)
    print("running {}".format(cmd))
    res = subprocess.check_output(cmd, shell=True)
    # cleanup
    cmd = '''{} rmi --force {}'''.format(docker_exec, image_uuid)
    print("running {}".format(cmd))
    res = subprocess.check_output(cmd, shell=True)
    print("result: {}".format(res))
    return


def run_extraction_image(dir, course, session, data_dir, proc_data_dir, docker_exec = get_properties()['docker_exec'], image_url = get_properties()['extraction_image']):
    with tempfile.TemporaryDirectory(dir=dir) as working_dir:
        output_dir = os.path.join(working_dir, 'output')
        os.makedirs(output_dir)
        print("[INFO] initializing docker image and data course {} session {}".format(course, session))
        shutil.copy(image_url, working_dir)
        source_data_dir = os.path.join(data_dir, course, session)
        working_data_dir = working_dir + '/input'
        session_data_dir = os.path.join(working_data_dir, course, session)
        shutil.copytree(source_data_dir, session_data_dir)
        # move files into course data dir; unzip all of the sql files
        for f in os.listdir(session_data_dir):
            fp_raw = os.path.join(session_data_dir, f)
            fp = re.sub('[\s\(\)":!&]', "", fp_raw)
            # remove bad characters from filename
            shutil.move(fp_raw, fp)
        datefile = 'coursera_course_dates.csv'
        shutil.copy(os.path.join(data_dir, datefile), os.path.join(session_data_dir, datefile))
        unzip_sql_cmd = """for i in `find {} -name "*.sql.gz"`; do gunzip "$i" ; done""".format(session_data_dir)
        subprocess.call(unzip_sql_cmd, shell=True, stdout=open(os.devnull, 'wb'), stderr=open(os.devnull, 'wb'))
        load_run_cleanup_image(course, session, working_data_dir, output_dir, image_url, docker_exec)
        make_tarfile(course, session, output_dir, proc_data_dir)
    return


def run_modeling_job(dir, course, session, proc_data_dir, modeling_script = "build_models.R", log_file = "modeling_log.txt"):
    with tempfile.TemporaryDirectory(dir=dir) as working_dir:
        output_dir = os.path.join(working_dir, 'output')
        print("[INFO] initializing modeling data course {} session {}".format(course, session))
        # initialize preprocessed data inside working_dir
        tarname = "{}-{}-data.tar".format(course, session)
        preprocessed_data_fp = os.path.join(proc_data_dir, tarname)
        if os.path.exists(preprocessed_data_fp):
            shutil.copy(preprocessed_data_fp, working_dir)
            tar = tarfile.open(os.path.join(working_dir, tarname))
            tar.extractall(working_dir)
            tar.close()
            # move files out out 'output' directory packaged in archive file into top-level working dir
            for file in os.listdir(output_dir):
                shutil.move(os.path.join(output_dir, file), working_dir)
            # remove old 'outout' directory from tar file; if necessary, should be replaced with a new one (to erase any permissions)
            os.rmdir(output_dir)
            # copy R scripts into working_dir
            modeling_dir = get_properties()['modeling_dir']
            for file in os.listdir(modeling_dir):
                if file.endswith(".R"):
                    shutil.copy(os.path.join(modeling_dir, file), working_dir)
            # run modeling script
            modeling_script_fp = os.path.join(working_dir, modeling_script)
            cmd = "Rscript {} --course {} --session {} --working_dir {} --output_dir {}".format(modeling_script_fp, course, session, working_dir, get_properties()['results_dir'])
            print("[INFO] running {}".format(cmd))
            subprocess.call(cmd, shell = True)
            print("[INFO] modeling complete course {} session {}".format(course, session))
        else:
            msg = "[WARNING] no data exists for course {} session {}; skipping".format(course, session)
            print(msg)
            with open(os.path.join(dir, log_file), "a") as f:
                f.write(msg + '\n')
        return


def evaluate_results(methods = ['simple_average', 'frequentist', 'bayesian'], exp_results_dir = get_properties()['results_dir'], analysis_dir = get_properties()['analysis_dir'], summary_csvname = 'complete_comparison_results.csv'):
    """
    Conduct naive average, frequentist/nemenyi, and bayesian evaluation of results, writing summary output (including graphics) to files.
    :param methods:
    :param exp_results_dir:
    :return:
    """

    if 'simple_average' in methods: # generate csv output of simple averages by model_id
        simple_avg_df = calculate_simple_average(exp_results_dir, outfile = os.path.join(analysis_dir, 'simple_avg_results.csv'))
        make_pub_simple_avg_df(simple_avg_df, dest_dir = os.path.join(analysis_dir))
    if 'frequentist' in methods: # generate CD diagrams
        frequentist_results_df = generate_frequentist_comparison(exp_results_dir, outdir = analysis_dir)
    if 'bayesian' in methods:
        bt_results_df = generate_posterior_comparison(exp_results_dir, analysis_dir)
    complete_results_df = frequentist_results_df.merge(bt_results_df, left_on = ['model_id_x', 'model_id_y'], right_on = ['model_id_1', 'model_id_2']).drop(['model_id_1', 'model_id_2'], axis = 1)
    assert complete_results_df.shape[0] == frequentist_results_df.shape[0] == bt_results_df.shape[0]
    complete_results_df.to_csv(os.path.join(analysis_dir, summary_csvname), index = False)
    return