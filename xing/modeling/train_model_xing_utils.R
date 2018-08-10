# utility functions for use in train_model_xing.R
#Sys.setenv(JAVA_HOME="/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home/bin/java")
library(rJava, quietly = TRUE, warn.conflicts = FALSE)
options( java.parameters = c("-Xss2560k", "-Xmx2g") ) # see https://github.com/s-u/rJava/issues/110
library(RWeka, quietly = TRUE, warn.conflicts = FALSE) #note: install.packages("rJava",,"http://rforge.net/",type="source")) before installingrweka
library(bnlearn, quietly = TRUE, warn.conflicts = FALSE) # http://www.bnlearn.com/documentation/bnlearn-manual.pdf

#createFolds; source code copied directly from caret package (so the entire caret package doesn't need to be installed/loaded)
createFolds <- function (y, k = 10, list = TRUE, returnTrain = FALSE){
    if (class(y)[1] == "Surv") 
        y <- y[, "time"]
    if (is.numeric(y)) {
        cuts <- floor(length(y)/k)
        if (cuts < 2) 
            cuts <- 2
        if (cuts > 5) 
            cuts <- 5
        breaks <- unique(quantile(y, probs = seq(0, 1, length = cuts)))
        y <- cut(y, breaks, include.lowest = TRUE)
    }
    if (k < length(y)) {
        y <- factor(as.character(y))
        numInClass <- table(y)
        foldVector <- vector(mode = "integer", length(y))
        for (i in 1:length(numInClass)) {
            min_reps <- numInClass[i]%/%k
            if (min_reps > 0) {
                spares <- numInClass[i]%%k
                seqVector <- rep(1:k, min_reps)
                if (spares > 0) 
                    seqVector <- c(seqVector, sample(1:k, spares))
                foldVector[which(y == names(numInClass)[i])] <- sample(seqVector)
            }
            else {
                foldVector[which(y == names(numInClass)[i])] <- sample(1:k, 
                                                                       size = numInClass[i])
            }
        }
    }
    else foldVector <- seq(along = y)
    if (list) {
        out <- split(seq(along = y), foldVector)
        names(out) <- paste("Fold", gsub(" ", "0", format(seq(along = out))), 
                            sep = "")
        if (returnTrain) 
            out <- lapply(out, function(data, y) y[-data], y = seq(along = y))
    }
    else out <- foldVector
    out
}


read_and_preproc_xing_data <- function(fp, target_week, drop_cols = c("userID", "dropout_week")) {
    data = read.csv(fp, header = TRUE) %>% 
        filter(dropout_week >= target_week) %>%
        select(-one_of(drop_cols)) %>% 
        mutate_all(funs(as.numeric)) %>% 
        mutate(dropout_current_week = as.factor(dropout_current_week))
    # apply log transformation
    X <- data.frame(apply(data[,-ncol(data)], 2, function(x) log(x + 1)))
    data <- cbind(X, dropout_current_week = data$dropout_current_week)
    return(data)
}

# train_bn
predict_bn <- function(bn, newdata, method = "ls", type = "probability"){
    label_ix = match("dropout_current_week", names(newdata))
    # initialize data structure
    preds = list()
    for (ix in seq(nrow(newdata))){
        row = newdata[ix,]
        if (method == "ls") { # logic sampling
            str = paste("(", names(row)[-label_ix], "=='",
                    sapply(row[,-label_ix], as.character), "')",
                    sep = "", collapse = " & ")
            dropout_prob = cpquery(bn, event = (dropout_current_week == "1"), eval(parse(text = str)))
        }
        else if (method == "lw") { # likelihood weighting
            row_evidence = as.list(select(row, -one_of("dropout_current_week")))
            dropout_prob = cpquery(bn, event = (dropout_current_week == "1"), evidence = row_evidence, method = "lw")
        }
        if (type == "class") preds[[ix]] <- ifelse(dropout_prob >= 0.5, 1, 0)
        if (type == "probability") preds[[ix]] <- c("0" = 1-dropout_prob, "1" = dropout_prob)
    }
    if (type == "class") return(factor(preds))
    if (type == "probability") return(do.call(rbind, preds))
}

# convert factor columns to numeric
factor_to_numeric <- function(col) {
    return(as.numeric(as.character(col)))
}

## bn_preds: matrix of bn predictions, labeled 0 and 1
## tree_preds: matrix of tree predictions, labeled 0 and 1
## labels: vector of labels
make_oof_df <- function(bn_preds, tree_preds, labels = NULL){
    bn_pred_df = data.frame(bn_preds)
    names(bn_pred_df) <- c("bn_pred_0", "bn_pred_1")
    tree_pred_df = data.frame(tree_preds)
    names(tree_pred_df) <- c("tree_pred_0", "tree_pred_1")
    if (!is.null(labels)) {
        labels_df = data.frame("label" = labels)
        df_out = cbind(bn_pred_df, tree_pred_df, labels_df)
    }
    if (is.null(labels)) {
        df_out = cbind(bn_pred_df, tree_pred_df)
    }
    return(df_out)
}


# train_xing_model
train_xing_model <- function(df, nfolds = 3, label_col = 'dropout_current_week'){
    X = select(df, -one_of(label_col))
    Y = df[,label_col]
    folds = createFolds(Y, nfolds)
    oof_df = list()
    for (ix in seq_along(folds)) {
        message(paste0("Fold ", ix))
        fold_ix = folds[[ix]]
        # train classifiers on in-fold data
        fold_test_df = df[fold_ix,]
        fold_train_df = df[-fold_ix,]
        # tree model
        mod.tree = RWeka::J48(dropout_current_week ~ ., data = fold_train_df)
        # bayesian network model
        bnw = bnlearn::hc(fold_train_df) 
        mod.bn = bnlearn::bn.fit(bnw, fold_train_df)
        # predict on oof data
        preds.tree = predict(mod.tree, fold_test_df, type = "probability")
        preds.bn = predict_bn(mod.bn, fold_test_df, method = "lw", type = "probability")
        labs = fold_test_df$dropout_current_week
        fold_oof_df = make_oof_df(bn_preds = preds.bn, tree_preds = preds.tree, labels = labs)
        oof_df[[ix]] <- fold_oof_df
    }
    base_df = bind_rows(oof_df)
    #base_df$preds.tree <- factor_to_numeric(base_df$preds.tree)
    #base_df$preds.bn <- factor_to_numeric(base_df$preds.bn)
    #base_df$labs <- factor(base_df$labs)
    # train base models on full dataset for future predictions (no data leakage issue w/test data)
    mods = list()
    mod.tree.base = RWeka::J48(dropout_current_week ~ ., data = df)
    mod.bn.base = bnlearn::bn.fit(bnw, df)
    #  train meta-learner
    mod.meta = glm(label ~ ., family = "binomial", data = base_df) # todo: should interaction term be included here?
    # return all learners in data structure
    mods[['tree_base']] = mod.tree.base
    mods[['bn_base']] = mod.bn.base
    mods[['meta']] = mod.meta
    return(mods)
}

# get ensemble predictions from data
xing_two_stage_predict <- function(df, mod){
    X_test = subset(df, select = -c(dropout_current_week))
    preds.tree = predict(mod[['tree_base']], newdata = X_test, type = "probability")
    preds.bn = predict_bn(mod[['bn_base']], newdata = df, method = "lw", type = "probability")
    base_df = make_oof_df(bn_preds = preds.bn, tree_preds = preds.tree)
    # base_df = data.frame(preds.bn = bn_preds, preds.tree = tree_preds) %>% mutate_all(factor_to_numeric)
    meta_pred_probs = predict(mod[['meta']], newdata = base_df, type = "response")
    meta_preds = factor(ifelse(meta_pred_probs >= 0.5, 1, 0), ordered = TRUE)
    df_out = cbind(base_df, data.frame(preds.ensemble = meta_preds))
    return(df_out)
}

## transform dataframe from xing_two_stage_predict into dataframe of predictions only
make_pred_df <- function(df, ensemble_col = "preds.ensemble"){
    bn = factor(ifelse(df$bn_pred_1 > 0.5, 1, 0))
    tree = factor(ifelse(df$tree_pred_1 > 0.5, 1, 0))
    ens = df[,ensemble_col]
    df_out = data.frame("bn" = bn, "tree" = tree, "ensemble" = ens)
    return(df_out)
}

## pull model metrics from dataframe of class predictions and labels
## df: dataframe containing one column per model, plus a column of the true class labels (label_col)
## label_col: name of column containing true class label
fetch_model_metrics <- function(df, label_col = "label"){
    model_col_ixs = which(!(names(df) %in% label_col))
    results = vector(mode = "list", length = length(model_col_ixs)) #preallocate results entries with NULL
    labs = factor_to_numeric(df$label)
    n = nrow(df)
    n_p = sum(labs == 1)
    n_n = sum(labs == 0)
    for (ix in model_col_ixs){
        model = names(df)[ix]
        preds = factor_to_numeric(df[,ix])
        auc = pROC::auc(response = labs, predictor = preds)
        tp = sum(preds == labs & labs == 1) # todo: check results
        fp = sum(preds != labs & preds == 1)
        precision = tp/(tp + fp)
        acc = sum(preds == labs)/length(preds)
        results[[ix]] <- data.frame("model" = model, "auc" = auc, "precision" = precision, "accuracy" = acc)
    }
    df_out = do.call(rbind, results)
    # add columns for n, n_p and n_n; these are needed for comparison using Fogarty, Baker and Hudson method
    df_out$n <- n
    df_out$n_p <- n_p
    df_out$n_n <- n_n
    return(df_out)
}


