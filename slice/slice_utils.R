# utility functions for conducting slicing analysis
# in future, these functions should be directly incoroporated into auctestr package

library(ROCR)
library(magrittr)
library(dplyr)

####################
##### BEGIN Example from ROCR documentation https://cran.r-project.org/web/packages/ROCR/ROCR.pdf
####################

## computing a simple ROC curve (x-axis: fpr, y-axis: tpr)
library(ROCR)
data(ROCR.simple)
pred <- prediction( ROCR.simple$predictions, ROCR.simple$labels)
perf <- performance(pred,"tpr","fpr")
plot(perf)
## precision/recall curve (x-axis: recall, y-axis: precision)
perf1 <- performance(pred, "prec", "rec")
plot(perf1)
## sensitivity/specificity curve (x-axis: specificity,
## y-axis: sensitivity)
perf1 <- performance(pred, "sens", "spec")
plot(perf1)

####################
##### END Example
####################



## preds: vector of predicted probability
## labs: vector of labels
compute_roc <- function(preds, labs){
    # create prediction object
    pred <- ROCR::prediction(preds, labs)
    perf <- ROCR::performance(pred,"tpr","fpr")
    return(perf)
}

## df: dataframe containing colnames matching pred_col, label_col, and protected_attr_col
## pred_col: name of column containing predicted probabilities
## label_col: name of column containing true labels (should be 0,1 only)
## protected_attr_col: name of column containing protected attr
## majority_protected_attr_val: name of "majority" group wrt protected attribute
## returns: value of slice statistic, absolute value of area between ROC curves for protected_attr_col
compute_slice_statistic <- function(df, pred_col, label_col, protected_attr_col, majority_protected_attr_val){
    # todo: input checking
        # pred_col should be in interval [0,1]
        # label_col should be strictly 0 or 1
        # majority_protected_attr_col should be in protected_attr_col values
        # protected_attr_col must be factor, otherwise convert and warn
    # initialize data structures
    ss = 0
    protected_attr_vals = unique(df[,protected_attr_col])
    roc_list = list()
    # compute roc within each group of protected_attr_vals
    for (protected_attr_val in protected_attr_vals){
        protected_attr_df = df[df[,protected_attr_col] == protected_attr_val,]
        roc_list[[protected_attr_val]] = compute_roc(protected_attr_df[,pred_col], protected_attr_df[,label_col])
    }
    # compare each non-majority class to majority class; accumulate absolute difference between ROC curves to slicing statistic
    for (protected_attr_val in protected_attr_vals[protected_attr_vals != majority_protected_attr_val]){
        
    }
}




