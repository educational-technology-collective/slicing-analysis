# functions for building models
library(dplyr)
library(caret)

WEEK=2

## read in dataframe and drop non-active students (those who have shown >=1 week of inactivity prior to target week)
## param ft: feature type; one of {"clickstream", "forum", "quiz", "all"}
## para id_col_ix: positional index of id column; since naming can be inconsistent in feature extraction this is (re)set manually when reading in data.
read_data <- function(dir, week, ft, dropout_file = "user_dropout_weeks.csv", id_col_ix = 1, id_col_name = "session_user_id", drop_cols = c("week", "dropout_current_week")){
    dropout_df = read.csv(paste(dir, dropout_file, sep = "/"))
    names(dropout_df)[id_col_ix] <- id_col_name
    all_feat_types = c("clickstream", "forum", "quiz")
    if (ft != "all"){
        # read individual feature df
        feat_filename = paste0("week_", week, "_", ft, "_appended_feats.csv")
        feat_fp = paste(dir, feat_filename, sep = "/")
        feat_df = read.csv(feat_fp)
        names(feat_df)[id_col_ix] <- id_col_name
    }
    if (ft == "all"){
        feat_df_list = list()
        for (ix in seq_along(all_feat_types)){ # read each feature type and add to feat_df_list
            ft = all_feat_types[ix]
            # read individual feature df
            feat_filename = paste0("week_", week, "_", ft, "_appended_feats.csv")
            feat_fp = paste(dir, feat_filename, sep = "/")
            feat_df = read.csv(feat_fp)
            names(feat_df)[id_col_ix] <- id_col_name
            feat_df_list[[ix]] = feat_df
        }
        # merge all dataframes; https://stackoverflow.com/questions/8091303/simultaneously-merge-multiple-data-frames-in-a-list
        feat_df = feat_df_list %>% Reduce(function(dtf1,dtf2) left_join(dtf1,dtf2,by="session_user_id"), .)
    }
    # join with dropout_df to get labels; 
    active_feat_df = inner_join(feat_df, dropout_df, by = id_col_name) 
    # set row names and drop any unneeded column; this includes dropping "dropout_current_week" so that label can be generated and dropping "week" column
    row.names(active_feat_df) <- active_feat_df[,id_col_name]
    suppressWarnings(active_feat_df <- dplyr::select(active_feat_df, -one_of(c(id_col_name, drop_cols)))) # suppress warnings about column names not in data
    # drop any students who have shown >=1 week of inactivity prior to this week
    active_feat_df = filter(active_feat_df, dropout_week > week - 1)
    active_feat_df$dropout_current_week = factor(active_feat_df$dropout_week == week, labels = "dropout")
    # drop dropout_week column
    active_feat_df = dplyr::select(active_feat_df, -one_of("dropout_week"))
    return(active_feat_df)
}

## create a blank resample df identical to those produced by R for when training fails; this helps differentiate between "bad features" (cases where model training fails because of data problems) and bad models (cases where the modeling process itself fails but data is otherwise ok)
initialize_blank_resample_df <- function(model_type, n_reps = 5, n_folds = 2, data_cols = c("Sens", "Spec", "ROC")){
    reps = paste0("Rep", seq(n_reps))
    folds = paste0("Fold", seq(n_folds))
    repfolds = apply(expand.grid(folds, reps), 1, paste, collapse=".")
    df = data.frame("Resample" = repfolds)
    for (c in data_cols){
        df[,c] <- NA
    }
    return(df)
}


# writes resampled results to csv file, making a unique identifier column by concatenating model parameters and tune_params into a model_id column.
## resample_df: dataframe returned as the $resample attribute of a caret::train object when the fitControl parameter returnResample = "all".
## tune_params: name of columns which are turning parameters; together these should make a unique identifier for each model.
## model_name: a short name for the model; ideally, use the $method attribute of the caret::train object.
## ft: feature type used.
write_resample_df <- function(mod, model_name, tune_params, tune_grid, ft, course, session, out_dir = "/Users/joshgardner/Documents/UM-Graduate/UMSI/LED_Lab/jla-model-eval/experiment-results-data"){
    if (!(is.null(mod))){
        resample_df = mod$resample
    } else{
        resample_df = initialize_blank_resample_df()
        resample_df = merge(resample_df, tune_grid, by=NULL)
    }
    resample_df$model = model_name
    resample_df$feat_type = ft
    resample_df$course = course
    resample_df$session = session
    # this is a hack but it works; puts underscore separation between multiple params
    resample_df$fill = "_"
    temp = sapply(tune_params, function(x) c(x, "fill"), simplify = "vector")
    tune_params_filled = as.vector(matrix(temp, nrow = 1))
    resample_df$model_id <- do.call(paste0, resample_df[c("model", "fill", tune_params_filled, "feat_type")])
    resample_df = dplyr::select(resample_df, -one_of(c("fill")))
    out_file = paste(course, session, model_name, ft, "results.csv", sep = "_")
    out_path = paste(out_dir, out_file, sep = "/")
    message(paste0("[INFO] writing ", out_path))
    write.csv(resample_df, file = out_path, row.names = F)
}


model_training_message <- function(course, session, ft, mt){
    msg = paste("[INFO] training", mt, "model for course", course, "session", session, "feature type", ft, sep = " ")
    message(msg)
}

missing_data_message <- function(course, session, ft, mt){
    msg = paste("[WARNING] no valid model data for course", course, "session", session, "feature type", ft, "model type", mt, sep = " ")
    message(msg)
}

## train model(s) of model_type and dump fold-level results to output_dir
build_models <- function(course, session, data_dir, output_dir, model_types = c("glmnet", "svmLinear2", "rpart", "adaboost", "nb"), feat_types = c("clickstream", "forum", "quiz", "all")){
    # create seeds; need to be a list of length 11 with 10 integer vectors of size 6 and the last list element having at least a single integer
    d <- seq(from=5000, to=5061)
    max <- 6
    x <- seq_along(d)
    seed_list = split(d, ceiling(x/max))
    fitControl <- trainControl(method = "repeatedcv", number = 2, repeats = 5, summaryFunction=twoClassSummary, classProbs=T, savePredictions = T, returnResamp = "all", seeds =  seed_list)
    # iterate over each feature type
    for (feat_type in feat_types){
        # read data and drop near-zero variance columns; this creates a common baseline dataset for each method
        data = read_data(data_dir, WEEK, feat_type)
        mod_data = data[,-caret::nearZeroVar(data, freqCut = 1000/1, uniqueCut = 2)]
        zero_var_mod_data = data[,-caret::checkConditionalX(data[,-ncol(data)], data[,ncol(data)])]
        # within feature type, iterate over each model type
        for (model_type in model_types){
            if (model_type == "glmnet"){ # penalized logistic regression
                glmGrid = expand.grid(alpha=0, lambda = c(10^c(0, -1, -2, -3), 0))
                model_training_message(course, session, feat_type, model_type)
                if (is.data.frame(mod_data)){
                    mod = caret::train(dropout_current_week ~ ., data = mod_data, method = model_type, metric = "ROC", trControl = fitControl, tuneGrid = glmGrid)
                } else{
                    missing_data_message(course, session, feat_type, model_type)
                    mod = NULL
                }
                #TODO: update all other calls to write_resample_df to have correct parameters in correct order as below
                write_resample_df(mod, model_type, c("alpha", "lambda"), glmGrid, feat_type, course, session, output_dir)
            }
            if (model_type == "svmLinear2"){ # linear svm; need to do some feature selection
                svmGrid = expand.grid(cost = 10^c(1, 0, -1, -2, -3))
                model_training_message(course, session, feat_type, model_type)
                library(doMC);registerDoMC()
                if (is.data.frame(mod_data)){
                    mod = caret::train(dropout_current_week ~ ., data = zero_var_mod_data, method = model_type, metric = "ROC", trControl = fitControl, tuneGrid = svmGrid, preProc = c("center", "scale"))
                } else{
                    missing_data_message(course, session, feat_type, model_type)
                    mod = NULL
                }
                write_resample_df(mod, model_type, c("cost"), svmGrid, feat_type, course, session, output_dir)
            }
            if (model_type == "rpart"){ # simple classification tree; don't use C4.5/J48 because these require java.
                # https://stackoverflow.com/questions/31138751/roc-curve-from-training-data-in-caret to plot AUC
                rpartGrid = expand.grid(cp = c(0.001, 0.01, 0.1, 1))
                model_training_message(course, session, feat_type, model_type)
                if (is.data.frame(mod_data)){
                    mod = caret::train(dropout_current_week ~ ., data = mod_data, method = model_type, metric = "ROC", trControl = fitControl, tuneGrid = rpartGrid)
                } else{
                    missing_data_message(course, session, feat_type, model_type)
                    mod = NULL
                }
                write_resample_df(mod, model_type, c("cp"), rpartGrid, feat_type, course, session, output_dir)
            }
            if (model_type == "adaboost"){ # adaboost; chose not to use random forest because tuning the mtry parameter across datasets with highly-varying numbers of predictors was not practical
                adaGrid = expand.grid(nIter = c(50, 100, 500), method = c("Adaboost.M1", "Real adaboost"))
                model_training_message(course, session, feat_type, model_type)
                if (is.data.frame(mod_data)){
                    mod = caret::train(dropout_current_week ~ ., data = mod_data, method = model_type, metric = "ROC", trControl = fitControl, tuneGrid = adaGrid)
                } else {
                    missing_data_message(course, session, feat_type, model_type)
                    mod = NULL
                }
                write_resample_df(mod, model_type, c("nIter", "method"), adaGrid, feat_type, course, session, output_dir)
            }
            if (model_type == "nb"){ # naive bayes; remove any predictors with empty conditional distributions within each level of outcome variable
                nbGrid = expand.grid(fL = c(0,1), usekernel = c(T,F), adjust = c(1)) # note that only the laplacian smoothing parameter and use of kernel is tuned; adjust is set to default value.
                model_training_message(course, session, feat_type, model_type)
                if (is.data.frame(mod_data)){
                    mod = caret::train(dropout_current_week ~ ., data = zero_var_mod_data, method = "nb", metric = "ROC", trControl = fitControl, tuneGrid = nbGrid)
                }
                else{
                    missing_data_message(course, session, feat_type, model_type)
                    mod = NULL
                }
                write_resample_df(mod, model_type, c("fL", "usekernel", "adjust"), nbGrid, feat_type, course, session, output_dir)
            }
        }
    }
}
