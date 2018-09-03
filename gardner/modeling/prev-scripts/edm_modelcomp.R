# model building

library(dplyr)
library(tidyr)
library(tibble)
library(performanceEstimation)
# library(e1071) # needed for naiveBayes, SVM classifiers
# library(randomForest) # needed for randomForest classifier
library(rpart) # needed for rpart method
# library(penalized) # penalized GLM with function penaliz
library(DMwR) # rpartXse
library(ada)
# paralelization; make sure to load these libraries last; cluster = TRUE setting in performanceEstimation() fails without it.
library(parallel)
library(parallelMap)



# custom workflow to use only forum feats in rpartXse tree
forumFeatXse <- function(form,train,test, se_val,...) {
    # reduce test and train data to only include relevant variables
    forumTrain = make_feature_subset(train, 'forum')
    forumTest = make_feature_subset(test, 'forum')
    ## obtain the model
    forumTree <- rpartXse(form,forumTrain, se = se_val)
    ## obtain the predictions
    forumPreds <- predict(forumTree, forumTest, type="class")
    names(forumPreds) <- rownames(forumTest)
    ## finally produce the list containing the output of the workflow
    res <- list(trues=responseValues(form,forumTest),preds=forumPreds)
    return(res)
}

# custom workflow to use only forum feats in tree
forumFeatAda <- function(form,train,test, iter_val,...) {
    # reduce test and train data to only include relevant variables
    forumTrain = make_feature_subset(train, 'forum')
    forumTest = make_feature_subset(test, 'forum')
    ## obtain the model
    forumAda <- ada(form,forumTrain, iter = iter_val)
    ## obtain the predictions
    forumPreds <- predict(forumAda, forumTest, type="class")
    names(forumPreds) <- rownames(forumTest)
    ## finally produce the list containing the output of the workflow
    res <- list(trues=responseValues(form,forumTest),preds=forumPreds)
    return(res)
}

# custom workflow to use only quiz feats in tree
quizFeatXse <- function(form,train,test, se_val,...) {
    # reduce test and train data to only include relevant variables
    quizTrain = make_feature_subset(train, 'quiz')
    quizTest =  make_feature_subset(test, 'quiz')
    ## obtain the model
    quizTree <- rpartXse(form,quizTrain, se = se_val)
    ## obtain the predictions
    quizPreds <- predict(quizTree, quizTest, type="class")
    names(quizPreds) <- rownames(quizTest)
    ## finally produce the list containing the output of the workflow
    res <- list(trues=responseValues(form,quizTest),preds=quizPreds)
    return(res)
}

# custom workflow to use only quiz feats in adaboost
quizFeatAda <- function(form,train,test, iter_val,...) {
    # reduce test and train data to only include relevant variables
    quizTrain = make_feature_subset(train, 'quiz')
    quizTest =  make_feature_subset(test, 'quiz')
    ## obtain the model
    quizAda <- ada(form,quizTrain, iter = iter_val)
    ## obtain the predictions
    quizPreds <- predict(quizAda, quizTest, type="class")
    names(quizPreds) <- rownames(quizTest)
    ## finally produce the list containing the output of the workflow
    res <- list(trues=responseValues(form,quizTest),preds=quizPreds)
    return(res)
}

# custom workflow to use only engagement feats in tree
engagementFeatXse <- function(form,train,test, se = se_val,...) {
    # reduce test and train data to only include relevant variables
    engagementTrain = make_feature_subset(train, 'engagement')
    engagementTest = make_feature_subset(test, 'engagement')
    ## obtain the model
    engagementTree <- rpartXse(form,engagementTrain)
    ## obtain the predictions
    engagementPreds <- predict(engagementTree, engagementTest, type="class")
    names(engagementPreds) <- rownames(engagementTest)
    ## finally produce the list containing the output of the workflow
    res <- list(trues=responseValues(form,engagementTest),preds=engagementPreds)
    return(res)
}

# custom workflow to use only engagement feats in tree
engagementFeatAda <- function(form,train,test, iter_val,...) {
    # reduce test and train data to only include relevant variables
    engagementTrain = make_feature_subset(train, 'engagement')
    engagementTest = make_feature_subset(test, 'engagement')
    ## obtain the model
    engagementAda <- ada(form,engagementTrain, iter = iter_val)
    ## obtain the predictions
    engagementPreds <- predict(engagementAda, engagementTest, type="class")
    names(engagementPreds) <- rownames(engagementTest)
    ## finally produce the list containing the output of the workflow
    res <- list(trues=responseValues(form,engagementTest),preds=engagementPreds)
    return(res)
}


# build list PredTasks from each element in proc_data and remove dropout_week and week columns (columns 1 and 2)
# TODO: resolve memory issues here; something is causing this to not fully copy the underlying data
# proc_data_tasks = lapply(proc_data, function(x) PredTask(dropout_current_week ~ ., x[,3:ncol(x)], copy = TRUE))
conduct_performanceEstimation <- function(proc_data, week, save_result = TRUE, filename = 'performanceEstimation_results.Rdata') {
    pts = c(
        PredTask(dropout_current_week ~ ., proc_data[[1]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[2]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[3]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[4]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[5]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[6]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[7]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[8]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[9]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[10]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[11]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[12]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[13]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[13]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[15]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[16]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[17]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[18]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[19]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[20]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[21]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[22]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[23]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[24]], copy = TRUE),
        PredTask(dropout_current_week ~ ., proc_data[[25]], copy = TRUE)
    )
    # week 1 settings
    if (week == 1) {
        x =  performanceEstimation(
            pts,
            c(Workflow('forumFeatXse', se_val = 2)
              ,Workflow('forumFeatAda', iter_val = 20)
              ,Workflow('quizFeatXse', se_val = 2)
              ,Workflow('quizFeatAda', iter_val = 20)
              ,Workflow('engagementFeatXse', se_val = 0.1)
              ,Workflow('engagementFeatAda', iter_val = 200)
              ,workflowVariants(learner= "ada", learner.pars = list(iter = 500))
              ,Workflow(learner="rpartXse", learner.pars=list(se=0), predictor.pars=list(type="class"))
            ), 
            EstimationTask(metrics=c("acc", "err"), method = CV(nFolds = 10)),
            cluster = TRUE)
        
    }
    # week 2 settings
    if (week == 2) {
        x =  performanceEstimation(
            pts,
            c(Workflow('forumFeatXse', se_val = 2)
              ,Workflow('forumFeatAda', iter_val = 20)
              ,Workflow('quizFeatXse', se_val = 2)
              ,Workflow('quizFeatAda', iter_val = 200)
              ,Workflow('engagementFeatXse', se_val = 0.1)
              ,Workflow('engagementFeatAda', iter_val = 50)
              ,workflowVariants(learner= "ada", learner.pars = list(iter = 200))
              ,Workflow(learner="rpartXse", learner.pars=list(se=0), predictor.pars=list(type="class"))
            ), 
            EstimationTask(metrics=c("acc", "err"), method = CV(nFolds = 10)), 
            cluster = TRUE)
        
    }
    # week 3 settings
    if (week == 3) {
        x =  performanceEstimation(
            pts,
            c(Workflow('forumFeatXse', se_val = 2)
              ,Workflow('forumFeatAda', iter_val = 20)
              ,Workflow('quizFeatXse', se_val = 2)
              ,Workflow('quizFeatAda', iter_val = 500)
              ,Workflow('engagementFeatXse', se_val = 0.5)
              ,Workflow('engagementFeatAda', iter_val = 200)
              ,workflowVariants(learner= "ada", learner.pars = list(iter = 20))
              ,Workflow(learner="rpartXse", learner.pars=list(se=0.1), predictor.pars=list(type="class"))
            ), 
            EstimationTask(metrics=c("acc", "err"), method = CV(nFolds = 10)), 
            cluster = TRUE)
        
    }
    # week 4 settings
    if (week == 4) {
        x =  performanceEstimation(
            pts,
            c(Workflow('forumFeatXse', se_val = 2)
              ,Workflow('forumFeatAda', iter_val = 20)
              ,Workflow('quizFeatXse', se_val = 2)
              ,Workflow('quizFeatAda', iter_val = 20)
              ,Workflow('engagementFeatXse', se_val = 0.5)
              ,Workflow('engagementFeatAda', iter_val = 100)
              ,workflowVariants(learner= "ada", learner.pars = list(iter = 20))
              ,Workflow(learner="rpartXse", learner.pars=list(se=0.1), predictor.pars=list(type="class"))
            ), 
            EstimationTask(metrics=c("acc", "err"), method = CV(nFolds = 10)), 
            cluster = TRUE)
        
    }
    # save results if specified; return result
    if (save_result == TRUE) save(x, file = filename)
    return(x)
}

compare_hyperparam_by_feature <- function(proc_data, feature_sub, dataset_ix, learner_type) {
    if (feature_sub != 'all') {
        sample_data = lapply(proc_data[dataset_ix], function(x) make_feature_subset(x, feat_type = feature_sub))
    }
    if (feature_sub == 'all') {
        sample_data = proc_data[dataset_ix]
    }
    # create workflows based on type of learner
    if (learner_type == 'rpartXse'){
        wfs = c(
            workflowVariants(learner = "rpartXse",
                             learner.pars=list(se=c(0,0.1, 0.5 , 1, 1.5, 2)),
                             predictor.pars=list(type="class")
            )
        )
    }
    
    if (learner_type == 'ada'){
        wfs = c(
            workflowVariants(learner = "ada",
                             learner.pars=list(iter=c(20, 50, 100, 200, 500)),
            )
        )
    }
    print("SETTING PREDS")
    print(dataset_ix)
    print(lapply(sample_data, names))
    print(names(sample_data))
    pts = c(
        PredTask(dropout_current_week ~ ., sample_data[[1]], copy = TRUE),
        PredTask(dropout_current_week ~ ., sample_data[[2]], copy = TRUE),
        PredTask(dropout_current_week ~ ., sample_data[[3]], copy = TRUE),
        PredTask(dropout_current_week ~ ., sample_data[[4]], copy = TRUE),
        PredTask(dropout_current_week ~ ., sample_data[[5]], copy = TRUE),
        PredTask(dropout_current_week ~ ., sample_data[[6]], copy = TRUE)
    )
    print("DONE SETTING PREDS")
    print(lapply(sample_data, names))
    res = performanceEstimation(
        pts,
        wfs,
        EstimationTask(metrics=c("acc", "err"), method = CV(nFolds = 10)),
        cluster = TRUE)
    # create pairedComparisons object
    pc = pairedComparisons(res, maxs=c(TRUE, FALSE), p.value=0.05)
    # write output to csvs
    write.csv(pc[['acc']][['avgScores']], file = paste0('output/week-', TARGET_WEEK,'/hyperparam_comparison/', learner_type, '/', feature_sub, '/avg_scores.csv'))
    write.csv(pc[['acc']][['rks']], file = paste0('output/week-', TARGET_WEEK,'/hyperparam_comparison/',learner_type, '/', feature_sub,'/ranks.csv'))
    write.csv(pc[['acc']][['avgRksWFs']], file = paste0('output/week-', TARGET_WEEK,'/hyperparam_comparison/',learner_type, '/', feature_sub,'/avg_ranks.csv'))
}