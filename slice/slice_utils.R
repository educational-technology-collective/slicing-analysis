# utility functions for conducting slicing analysis
# in future, these functions should be directly incoroporated into auctestr package

library(ROCR)

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





