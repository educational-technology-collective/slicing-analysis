# utility functions for conducting slicing analysis
# in future, these functions should be directly incoroporated into auctestr package

library(ROCR)
library(magrittr)
library(dplyr)
library(glue)

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

## use interpolation to make approximate curve along n_grid equally-spaced values
interpolate_roc_fun <- function(perf_in, n_grid = 10000){
    x_vals = unlist(perf_in@x.values)
    y_vals = unlist(perf_in@y.values)
    stopifnot(length(x_vals) == length(y_vals))
    roc_approx = approx(x_vals, y_vals, n = n_grid)
    return(roc_approx)
}

## creates a plot of two roc curves w area between shaded
## majority_roc: list with attributes "x" and "y" defining points of roc curve
## minority_roc: list with attributes "x" and "y" defining points of roc curve
slice_plot <- function(majority_roc, minority_roc, majority_group_name = NULL, minority_group_name = NULL, fout = NULL) {
    # check that number of points are the same
    stopifnot(length(majority_roc$x) == length(majority_roc$y), 
              length(majority_roc$x) == length(minority_roc$x),
              length(majority_roc$x) == length(minority_roc$y))
    pdf(fout, width = 7, height = 7)
    # set some graph parameters
    majority_color = "red"
    minority_color = "blue"
    majority_group_label = "Majority Group"
    minority_group_label = "Minority Group"
    plot_title = "ROC Slice Plot"
    if (!is.null(majority_group_name)){
        majority_group_label = glue("{majority_group_label} ({majority_group_name})")
    }
    if (!is.null(minority_group_name)){
        minority_group_label = glue("{minority_group_label} ({minority_group_name})")
    }
    # add labels, if given
    plot(majority_roc$x, 
         majority_roc$y, 
         col = majority_color, 
         type = "l", 
         lwd = 1.5, 
         main = plot_title,
         xlab = "False Positive Rate", 
         ylab = "True Positive Rate")
    polygon(x = c(majority_roc$x, rev(minority_roc$x)), # reverse ordering used to close polygon by ending near start point
            y = c(majority_roc$y, rev(minority_roc$y)),
            col = "grey",
            border = NA
    )
    lines(majority_roc$x, majority_roc$y, col = majority_color, type = "l", lwd = 1.5)
    #segments(majority_roc$x, majority_roc$y, minority_roc$x, minority_roc$y)
    lines(minority_roc$x, minority_roc$y, col = minority_color, type = "l", lwd = 1.5)
    legend("bottomright", legend = c(majority_group_label, minority_group_label), col = c(majority_color, minority_color), lty = 1)
    dev.off()
}

## df: dataframe containing colnames matching pred_col, label_col, and protected_attr_col
## pred_col: name of column containing predicted probabilities
## label_col: name of column containing true labels (should be 0,1 only)
## protected_attr_col: name of column containing protected attr
## majority_protected_attr_val: name of "majority" group wrt protected attribute
## n_grid: number of grid points to use in approximation
## plot_slices: if true, ROC slice plots are generated and saved
## img_dir: directory to save images to
## course: course name, used for filenames if plot_slices is set to TRUE
## returns: value of slice statistic, absolute value of area between ROC curves for protected_attr_col
compute_slice_statistic <- function(df, pred_col, label_col, protected_attr_col, majority_protected_attr_val, n_grid = 10000, plot_slices = TRUE, image_dir = NULL, course = NULL){
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
    majority_roc_fun = interpolate_roc_fun(roc_list[[majority_protected_attr_val]])
    for (protected_attr_val in protected_attr_vals[protected_attr_vals != majority_protected_attr_val]){
        minority_roc_fun = interpolate_roc_fun(roc_list[[protected_attr_val]])
        # use function approximation to compute slice statistic, cf. https://stat.ethz.ch/pipermail/r-help/2010-September/251756.html
        stopifnot(identical(majority_roc_fun$x, minority_roc_fun$x))
        f1 <- approxfun(majority_roc_fun$x, majority_roc_fun$y - minority_roc_fun$y)     # piecewise linear function
        f2 <- function(x) abs(f1(x))                 # take the positive value
        slice = integrate(f2, 0, 1)$value
        ss <- ss + slice
        # todo: plot these or write to file
        if (plot_slices == TRUE) {
            output_filename = file.path(image_dir, glue('slice_plot_{course}_{majority_protected_attr_val}_{protected_attr_val}.pdf'))
            slice_plot(majority_roc_fun, minority_roc_fun, majority_protected_attr_val, protected_attr_val, fout = output_filename)
            }
    }
    return(ss)
}




