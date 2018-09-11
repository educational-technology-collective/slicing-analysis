# functions for building models
library(dplyr)
library(caret)
library(glue)
library(magrittr)

WEEK=2 #todo: verify this is correct week number

## read in data for every session of course
## para id_col_ix: positional index of id column; since naming can be inconsistent in feature extraction this is (re)set manually when reading in data.
read_session_data <- function(course, session, input_dir = "/input", label_csv_suffix = "_labels", feature_csv_suffix = "_features", id_col_name = "userID", drop_cols = c("week", "dropout_current_week")){
    course_session_dir = file.path(input_dir, course, session)
    feature_filename = glue("{course}_{session}{feature_csv_suffix}.csv")
    label_filename = glue("{course}_{session}{label_csv_suffix}.csv")
    feature_fp = file.path(course_session_dir, feature_filename)
    label_fp = file.path(course_session_dir, label_filename)
    feature_df = read.csv(feature_fp)
    label_df = read.csv(label_fp)
    # join with dropout_df to get labels; set user_id_col to rowname
    feature_label_df = inner_join(feature_df, label_df, by = id_col_name)
    message("[INFO] setting userID to row name and dropping userID column...")
    feature_label_df %<>%
        dplyr::mutate(label_value = factor(label_value, labels = c("non_dropout", "dropout"))) %>%
        dplyr::rename(label = label_value) %>%
        tibble::column_to_rownames(var = "userID") %>%
        dplyr::select(-one_of(c(id_col_name, drop_cols)))
    return(feature_label_df)
}

read_course_data <- function(course, input_dir = "/input") {
    # iteratively call read_session_data; concatenate and return results
    session_df_list = list()
    course_dir = file.path(input_dir, course)
    for (session in list.dirs(course_dir, full.names = FALSE, recursive = FALSE)){
        session_df = read_session_data(course, session)
        session_df_list[[session]] <- session_df
    }
    course_df <- dplyr::bind_rows(session_df_list)
    return(course_df)
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

model_training_message <- function(course, mt){
    msg = paste("[INFO] training", mt, "model for course", course, sep = " ")
    message(msg)
}

missing_data_message <- function(course, session, mt){
    msg = paste("[WARNING] no valid model data for course", course, "session", session, "model type", mt, sep = " ")
    message(msg)
}

## train model(s) of model_type and dump fold-level results to output_dir
build_model <- function(course, data_dir, model_type = NULL){
    # create seeds; need to be a list of length 11 with 10 integer vectors of size 6 and the last list element having at least a single integer
    d <- seq(from=5000, to=5061)
    max <- 6
    x <- seq_along(d)
    seed_list = split(d, ceiling(x/max))
    fitControl <- trainControl(method = "repeatedcv", number = 2, repeats = 5, summaryFunction=twoClassSummary, classProbs=T, savePredictions = T, returnResamp = "all", seeds =  seed_list)

    # read data and drop near-zero variance columns; this creates a common baseline dataset for each method
    data = read_course_data(course, input_dir = data_dir)
    mod_data = data[,-caret::nearZeroVar(data, freqCut = 1000/1, uniqueCut = 2)]
    zero_var_mod_data = data[,-caret::checkConditionalX(data[,-ncol(data)], data[,ncol(data)])]
    # build model
    if (model_type == "glmnet"){ # penalized logistic regression
        glmGrid = expand.grid(alpha=0, lambda = c(10^c(0, -1, -2, -3), 0))
        model_training_message(course, model_type)
        if (is.data.frame(mod_data)){
            mod = caret::train(label ~ ., data = mod_data, method = model_type, metric = "ROC", trControl = fitControl, tuneGrid = glmGrid)
        } else{
            missing_data_message(course, session, model_type)
            mod = NULL
        }
    }
    if (model_type == "svmLinear2"){ # linear svm; need to do some feature selection
        svmGrid = expand.grid(cost = 10^c(1, 0, -1, -2, -3))
        model_training_message(course, model_type)
        library(doMC);registerDoMC()
        if (is.data.frame(mod_data)){
            mod = caret::train(label ~ ., data = zero_var_mod_data, method = model_type, metric = "ROC", trControl = fitControl, tuneGrid = svmGrid, preProc = c("center", "scale"))
        } else{
            missing_data_message(course, session, model_type)
            mod = NULL
        }
    }
    if (model_type == "rpart"){ # simple classification tree; don't use C4.5/J48 because these require java.
        # https://stackoverflow.com/questions/31138751/roc-curve-from-training-data-in-caret to plot AUC
        rpartGrid = expand.grid(cp = c(0.001, 0.01, 0.1, 1))
        model_training_message(course, model_type)
        if (is.data.frame(mod_data)){
            mod = caret::train(label ~ ., data = mod_data, method = model_type, metric = "ROC", trControl = fitControl, tuneGrid = rpartGrid)
        } else{
            missing_data_message(course, session, model_type)
            mod = NULL
        }
    }
    if (model_type == "adaboost"){ # adaboost; chose not to use random forest because tuning the mtry parameter across datasets with highly-varying numbers of predictors was not practical
        adaGrid = expand.grid(nIter = c(50, 100, 500), method = c("Adaboost.M1", "Real adaboost"))
        model_training_message(course, model_type)
        if (is.data.frame(mod_data)){
            mod = caret::train(label ~ ., data = mod_data, method = model_type, metric = "ROC", trControl = fitControl, tuneGrid = adaGrid)
        } else {
            missing_data_message(course, session, model_type)
            mod = NULL
        }
    }
    if (model_type == "nb"){ # naive bayes; remove any predictors with empty conditional distributions within each level of outcome variable
        nbGrid = expand.grid(fL = c(0,1), usekernel = c(T,F), adjust = c(1)) # note that only the laplacian smoothing parameter and use of kernel is tuned; adjust is set to default value.
        model_training_message(course, model_type)
        if (is.data.frame(mod_data)){
            mod = caret::train(label ~ ., data = zero_var_mod_data, method = "nb", metric = "ROC", trControl = fitControl, tuneGrid = nbGrid)
        }
        else{
            missing_data_message(course, session, model_type)
            mod = NULL
        }
    }
    return(mod)
}
