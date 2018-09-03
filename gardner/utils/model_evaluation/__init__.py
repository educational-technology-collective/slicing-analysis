import pandas as pd
import os
import Orange
import matplotlib.pyplot as plt
import itertools
from utils.model_evaluation.bayesiantests import signtest, signtest_MC, plot_posterior
from multiprocessing import Pool
from functools import partial

def read_experiment_data(dir, use_cols = ['ROC', 'Resample', 'model', 'feat_type', 'course', 'session', 'model_id'], drop_incompletes = True):
    """
    Utility function to read and compile individual course/feature results into single dataframe.
    :param dir:
    :param outfile: just needed to check and make sure previous simple average results aren't read in as "new" data
    :return:
    """
    results_df_list = []
    for f in os.listdir(dir):
        if f.endswith(".csv"):
            resultsfile = os.path.join(dir, f)
            df = pd.read_csv(resultsfile)
            try:
                results_df_list.append(df[use_cols])
            except:
                print("error reading file {}".format(resultsfile))
    results_df = pd.concat(results_df_list, axis=0)
    if drop_incompletes:
        # only keep courses and sessions where all models successfully trained/tested
        course_obs = results_df.groupby(['course', 'session']).size()
        max_cs_obs = course_obs.max()
        temp = results_df.merge(course_obs.reset_index().rename(columns={0: 'num_obs'}), on = ['course', 'session'])
        temp = temp[temp.num_obs == max_cs_obs].drop(['num_obs'], axis = 1)
        results_df = temp
    return results_df


def calculate_simple_average(dir, outfile, avg_by = ['model_id'], avg_metric ='ROC'):
    """
    Read experiment results data in dir and return dataframe with averages of avg_metric by column avg_by.
    :param dir:
    :param avg_by:
    :param avg_metric:
    :param use_cols:
    :param outfile:
    :return:
    """
    results_df = read_experiment_data(dir)
    simple_average_df = results_df.groupby(avg_by)[avg_metric].mean().reset_index().rename(columns = {avg_metric: 'simple_avg_' + avg_metric})
    simple_average_df.to_csv(outfile, header = True, index = False)
    return simple_average_df


def make_pub_simple_avg_df(df, dest_dir, csvname = "simple_avg_feats_pubversion.csv"):
    """
    Makes a prettier version, separating out model_id, hyperparams, features, and ordering by performance.
    :param df:
    :return:
    """
    # creates feat_type
    temp_df = df['model_id'].str.rsplit('_', 1, expand=True).rename(columns={0: 'algorithm_hyperparam', 1: 'feat_type'})
    # creates algorithm and hyperparam
    temp_df_2 = temp_df['algorithm_hyperparam'].str.split('_', 1, expand=True).rename(columns={0: 'algorithm', 1: 'hyperparams'})
    df_out = pd.concat([df, temp_df, temp_df_2], axis = 1)
    df_out['algorithm'] = df_out.algorithm.apply(lambda x: str(x).title())
    df_out['feat_type'] = df_out.feat_type.apply(lambda x: str(x).title())
    df_out.sort_values(by='simple_avg_ROC').to_csv(os.path.join(dest_dir, csvname), index = False)
    return

def compute_avg_ranks(df):
    """
    :param df:
    :return:
    """
    df.drop(['feat_type', 'model'], inplace = True, axis = 1)
    def generate_ranks(group):
        frame=group
        frame = frame.set_index(["model_id"])
        rks = frame["ROC"].rank(ascending = False)
        return(rks)
    rank_df = df.groupby(['course', 'session', 'Resample']).apply(generate_ranks)
    temp = pd.melt(rank_df.reset_index(), id_vars = ['course', 'session', 'Resample'], value_vars = df.model_id.unique().tolist()).rename(columns={'value':'rank'})
    avg_ranks = temp.groupby(['model_id']).mean().rename(columns = {"rank": "ROC_rank"})
    return avg_ranks.sort_values("ROC_rank", ascending=False)


def output_cd_results(avranks_df, results_df, cd, outdir, csvname = 'nemenyi_avgranks_differences.csv', round_output_places = 3):
    """
    Output a CSV file with the pairwise difference in average ranks between each set of models into outdir.
    :param avranks_df:
    :param cd:
    :param outdir:
    :return:
    """
    avresults_df = results_df.groupby(['model_id'])['ROC'].mean().reset_index().rename(columns = {'ROC':'avg_auc'})
    avranks_df.reset_index(inplace=True)
    avranks_df['join_key'] = '1'
    avranks_df = avranks_df.merge(avresults_df, on = 'model_id')
    diff_df = avranks_df.merge(avranks_df, on = 'join_key').drop(['join_key'], axis=1)
    diff_df = diff_df[diff_df.model_id_x > diff_df.model_id_y]
    diff_df['rank_x-rank_y'] = diff_df['ROC_rank_x'] - diff_df['ROC_rank_y']
    diff_df['avg_auc_x=avg_auc_y'] = diff_df['avg_auc_x'] - diff_df['avg_auc_y']
    diff_df['rank_x-rank_y_greater_cd_{}'.format(round(cd, 4))] = abs(diff_df['rank_x-rank_y']) > cd
    df_out = diff_df.sort_values(by=['ROC_rank_x','rank_x-rank_y'], ascending = [True, False]).round(round_output_places)
    df_out.to_csv(os.path.join(outdir, csvname), index = False)
    return df_out


def generate_frequentist_comparison(dir, outdir):
    """
    Creates critical difference diagram and dataframe of nemenyi test results for data in dir and writes it to outdir.
    :param dir:
    :param outdir:
    :return:
    """
    img_dir = os.path.join(outdir, 'img')
    if not os.path.exists(img_dir):
        os.makedirs(img_dir)
    results_df = read_experiment_data(dir)
    avranks_df = compute_avg_ranks(results_df)
    names = avranks_df.index.tolist()
    avranks = avranks_df["ROC_rank"]
    N = results_df[['course', 'session']].drop_duplicates().shape[0]
    #cd = Orange.evaluation.compute_CD(avranks, N)
    if N == 85:
        k = len(avranks)
        q = 4.23165649
        cd = q * (k * (k + 1) / (6.0 * N)) ** 0.5
    if N == 33:
        k = len(avranks)
        q = 3.795178566
        cd = q * (k * (k + 1) / (6.0 * N)) ** 0.5
    if N == 48:
        k = len(avranks)
        q = 3.973379375
        cd = q * (k * (k + 1) / (6.0 * N)) ** 0.5
    else:
        cd = Orange.evaluation.compute_CD(avranks, N)
    cd_results_df = output_cd_results(avranks_df, results_df, cd, outdir)
    Orange.evaluation.graph_ranks(avranks, names, cd=cd, width=36, textspace=1.5)
    plt.savefig(os.path.join(img_dir, 'cd.eps'), bbox_inches='tight')
    return cd_results_df


def posterior_compare_models(models, results_df, outdir, plot_thresh = 0.8):
    """
    Conduct a posterior comparison of models in results_df; only generate a posterior plot if at least one posterior probability is below plot_thresh.
    :param models:
    :param results_df:
    :param outdir:
    :param plot_thresh:
    :return:
    """
    print('Conducting comparison for {0}'.format(models))
    temp = results_df[results_df['model_id'].isin(models)][['course', 'session', 'Resample', 'model_id', 'ROC']]
    temp['rep_id'] = temp["course"] + temp["session"].map(str) + temp["Resample"]
    temp = temp.pivot(index='rep_id', columns='model_id', values='ROC')
    scores = temp.loc[:, models].as_matrix()
    left, within, right = signtest(scores, rope=0.01)
    # The first value (left) is the probability that the first classifier (the left column of x) has a higher score than the second (or that the differences are negative, if x is given as a vector).
    # If we add arguments verbose and names, the function also prints out the probabilities.
    left, within, right = signtest(scores, rope=0.01, verbose=True, names=models)
    ## The posterior distribution can be plotted out:
    ## using the function signtest_MC(x, rope, prior_strength=1, prior_place=ROPE, nsamples=50000) we generate the samples of the posterior
    ## using the function plot_posterior(samples,names=('C1', 'C2')) we then plot the posterior in the probability simplex
    # if sum(x < plot_thresh for x in [left, within, right]) > 0:  # don't create plots when there is a clear decision
    #     try:
    #         samples = signtest_MC(scores, rope=0.01)
    #         fig = plot_posterior(samples, models)
    #         # NOTE that in order to get positioning on plots right, I adjusted these lines in bayesiantests:
    #         # fig.gca().set_xlim(-0.5, 1.5)
    #         # fig.gca().set_ylim(-0.5, 1.5)
    #         png_name = '{0}_{1}_posterior_plot.png'.format(models[0], models[1])
    #         fig.savefig(os.path.join(outdir, png_name), pad_inches=0.25)
    #     except:
    #         print("[WARINING] exception when writing posterior plot for {}".format(models))
    #         pass
    return [models[0], models[1], left, within, right]


def generate_posterior_comparison(dir, outdir, outfile = "posterior_results.csv"):
    """
    Generate output file with tripartite posterior probability estimates, and average AUC.
    :param dir:
    :param outdir:
    :return:
    """
    results_df = read_experiment_data(dir)
    models = sorted(results_df.model_id.unique(), reverse=True)
    model_combos = itertools.combinations(models, 2)
    with Pool() as pool:
        bt_results = pool.map_async(partial(posterior_compare_models, results_df = results_df, outdir = outdir), model_combos)
        pool.close()
        pool.join()
    bt_results_df = pd.DataFrame.from_records([x for x in bt_results.get()])
    bt_results_df.columns = ['model_id_1', 'model_id_2', 'left', 'rope', 'right']
    outpath = os.path.join(outdir, outfile)
    bt_results_df.to_csv(outpath, index = False)
    return bt_results_df


