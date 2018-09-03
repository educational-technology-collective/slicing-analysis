# Copyright (C) 2017  The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see [http://www.gnu.org/licenses/].
# =========================================================================
# Utility script to (1) read processed data, (2) build models with different feature groups individually and in combination,
# (3) evaluate the performance of model/featureset combinations with the Friedman + Nemenyi testing procedure,
# and (4) produce critical difference diagrams from those comparisons. For more information on this implementation, see:
#     Gardner and Brooks, 2017, Statistical Approaches to Model Performance Evaluation in MOOCs.
# 
#     
#     
#     
# =========================================================================

#TODO: drop forum_posts feature from appended feats (redundant)
#TODO: drop any students who were inactive for > 1wk (not just those that are currently inactive)

# function to build single concatenated dataset for same week of all runs of a given course
aggregate_dataset <- function(course_shortname, run_numbers, course_week, feature_types = c('forum_appended', 'quiz_appended', 'appended'), data_dir = '../proc_data', dropout_filename = 'user_dropout_weeks.csv', drop_zero_var_cols = TRUE, drop_stopouts = TRUE) {
    #check input
    if (class(course_shortname) != 'character') print('Course name must be character.')
    if (!(class(run_numbers) == 'numeric') & !(class(run_numbers) == 'integer')) print('Run numbers must be numeric or integer vector.')
    if (!(class(course_week) == 'numeric')|length(course_week) != 1) print('Course week must be single numeric value.')
    
    join_key = 'session_user_id'
    # initialize list to hold output for each run; joined together below
    run_df_list = list()
    for (i in run_numbers) {
        # initialize data frame of all data for run
        run_df = data.frame()
        # read dropout data
        run_number = str_pad(i, 3, side = "left", pad = 0)
        run_dir = paste0(data_dir, '/', course_shortname, '/', run_number)
        dropout_fp = paste0(run_dir, '/', dropout_filename)
        dropout_wk_df = read.csv(dropout_fp)
        names(dropout_wk_df)[1] <- join_key
        # read all features and merge into single df
        feat_dfs = list()
        feat_dfs[[1]] = dropout_wk_df
        for (ix in seq(length(feature_types))) {
            feature_type = feature_types[ix]
            feat_fp = paste0(run_dir, '/', 'week_', course_week, '/', 'week_', course_week, '_', feature_type, '_feats.csv')
            feat_df = read.csv(feat_fp)
            if (feature_type == 'appended') names(feat_df)[1] <- join_key # set correct name for engagement features; consistent naming is needed for join_all()
            feat_dfs[[ix+1]] = feat_df
        }
        df_run = plyr::join_all(feat_dfs, by = c(join_key), type = 'left')
        # set join_key to be row name and remove column
        row.names(df_run) = df_run[,join_key]
        df_run[,join_key] = NULL
        run_df_list[[i]] = df_run
    }
    df_out = dplyr::bind_rows(run_df_list) # TODO: if we want to have categorical identifiers for runs, set .id parameter here
    # drop any zero-variance columns; this is useful for SVM and certain other models
    if (drop_zero_var_cols == TRUE) {
        zero_var_cols <- unlist(lapply(df_out, function(x) 0 == var(if (is.factor(x)) as.integer(x) else x)))
        df_out = df_out[,!zero_var_cols]
    }
    # drop students who stopped out in previous weeks; this means they have shown at least one full week of inactivity prior to this week
    # TODO: more advanced filtering here; drop any student with ZERO active days in any week prior (this only drops students inactive in weeks directly preceding course week)
    if (drop_stopouts == TRUE) df_out %<>% filter(dropout_week > course_week-1)
    #set dropout_current_week to factor
    df_out$dropout_current_week = factor(df_out$dropout_current_week)
    
    return(df_out)
}

# function to take variable name and return as string; used to build named list of datasets for easier evaluation of modeling results
var_name_string <- function(variable) {
    return(deparse(substitute(variable)))
}



fetch_model_data <- function(target_week) {    
    # fetch data for courses, by run
    proc_data = list()
    #intro thermodynamics, runs 2-6
    for (i in seq(2,6)){
        course = "INTROTHERMO"
        run = str_pad(i, 3, side = 'left', pad = '0')
        agg_df = aggregate_dataset('thermo', run_numbers = i, course_week = target_week, data_dir = '../proc_data', drop_zero_var_cols = FALSE, drop_stopouts = TRUE)
        proc_data[[paste(course, run, sep = '_')]] = agg_df[,3:ncol(agg_df)]
    }
    
    # inside the internet, runs 3-7
    for (i in seq(3,7)){
        course = "INSIDETHEINTERNET"
        run = str_pad(i, 3, side = 'left', pad = '0')
        agg_df = aggregate_dataset('iti', run_numbers = i, course_week = target_week, data_dir = '../proc_data', drop_zero_var_cols = FALSE, drop_stopouts = TRUE)
        proc_data[[paste(course, run, sep = '_')]] = agg_df[,3:ncol(agg_df)]
    }
    
    # instruct. meth. hpe, runs 2-6
    for (i in seq(2,6)){
        course = "INSTRUCTMETHODSHPE"
        run = str_pad(i, 3, side = 'left', pad = '0')
        agg_df = aggregate_dataset('imhpe', run_numbers = i, course_week = target_week, data_dir = '../proc_data', drop_zero_var_cols = FALSE, drop_stopouts = TRUE)
        proc_data[[paste(course, run, sep = '_')]] = agg_df[,3:ncol(agg_df)]
    }
    
    # # fsf runs 2-9
    # fsf = list()
    # for (i in seq(2,9)){
    #     course = "FANTASYSF"
    #     run = str_pad(i, 3, side = 'left', pad = '0')
    #     agg_df =  aggregate_dataset('fsf', run_numbers = i, course_week = target_week, data_dir = '../proc_data', drop_zero_var_cols = FALSE, drop_stopouts = TRUE)
    #     proc_data[[paste(course, run, sep = '_')]] = agg_df[,3:ncol(agg_df)]
    # }
    
    # if runs 2-9
    i_f = list()
    for (i in seq(2,9)){
        course = "INTROFINANCE"
        run = str_pad(i, 3, side = 'left', pad = '0')
        agg_df =  aggregate_dataset('if', run_numbers = i, course_week = target_week, data_dir = '../proc_data', drop_zero_var_cols = FALSE, drop_stopouts = TRUE)
        proc_data[[paste(course, run, sep = '_')]] = agg_df[,3:ncol(agg_df)]
    }
    
    # mt runs 3-10
    i_f = list()
    for (i in seq(3,10)){
        course = "MODELTHINKING"
        run = str_pad(i, 3, side = 'left', pad = '0')
        agg_df =  aggregate_dataset('mt', run_numbers = i, course_week = target_week, data_dir = '../proc_data', drop_zero_var_cols = FALSE, drop_stopouts = TRUE)
        proc_data[[paste(course, run, sep = '_')]] = agg_df[,3:ncol(agg_df)]
    }
    
    return(proc_data)
}



# function to build feature subset using naming convention from feature extraction scripts; keeps any columns in ..._FEAT_STEMS and in keep_cols
make_feature_subset <- function(df, feat_type, keep_cols = c('dropout_current_week')){
    # define feature stems; these are the full names of features for 'only' feats and are the end of the feats for 'appended' feats (i.e., 'week_0_threads_started', 'week_1_threads_started', etc.)
    FORUM_FEAT_STEMS = c("threads_started", "week_post_len_char", "num_posts", "num_replies", "votes_net", "avg_net_sentiment", "positive_post_count", "negative_post_count", "neutral_post_count", "avg_net_sentiment_diff_from_thread_avg", "avg_flesch_reading_ease", "avg_flesch_kincaid_grade", "unique_bigrams_week","direct_nodes", "thread_nodes")
    QUIZ_FEAT_STEMS = c("avg_pre_dl_submission_time_week", "avg_raw_score_week", "weekly_avg_score_quiz_quiz_type", "weekly_avg_score_video_quiz_type", "weekly_avg_score_homework_quiz_type", "week_avg_change_video_quiz_type", "week_avg_change_quiz_quiz_type", "week_avg_change_homework_quiz_type", "total_user_submissions_week", "weekly_pct_max_submissions", "total_raw_points_week", "raw_points_per_submission")
    ENGAGEMENT_FEAT_STEMS = c("n_forum_views", "n_active_days", "quizzes_quiz_attempt", "quizzes_exam", "quizzes_human_graded")
    # get feature stems for specified feature type
    if(feat_type == 'forum') FEAT_STEMS = c(keep_cols, FORUM_FEAT_STEMS)
    else if (feat_type == 'quiz') FEAT_STEMS = c(keep_cols, QUIZ_FEAT_STEMS)
    else if (feat_type == 'engagement') FEAT_STEMS = c(keep_cols, ENGAGEMENT_FEAT_STEMS)
    # fetch matching columns and subset dataframe
    col_ix = lapply(names(df), function(x) any(endsWith(x, FEAT_STEMS)))
    feat_subset_df = df[,unlist(col_ix)]
    return(feat_subset_df)
}