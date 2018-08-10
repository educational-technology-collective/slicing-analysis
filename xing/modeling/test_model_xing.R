## script to replicate [Xing 2016] by loading saved model and predicting on new data; saves file of predictions
# usage: Rscript test_model_xing.R --file ./modeling/data/1497282771-josh_gardner-clinicalskills-003-extract/week_3/week_3_sum_feats.csv --model /Users/joshgardner/Documents/UM-Graduate/UMSI/LED_Lab/s17/model_build_infrastructure/job_runner/xing_mod.Rdata

# read command line argument for input data path; see https://www.r-bloggers.com/passing-arguments-to-an-r-script-from-command-lines/
library(optparse, quietly = TRUE, warn.conflicts = FALSE)
option_list = list(
    make_option(c("-f", "--file"), type="character", default=NULL, 
                help="dataset file name", metavar="character"),
    make_option(c("-m", "--model"), type="character", default=NULL, 
                help="model file name", metavar="character"),
    make_option(c("-o", "--pred"), type="character", default="./modeling/data/output/preds.csv",
                help="prediction output file name [default= %default]", metavar="character"),
    make_option(c("-s", "--summary"), type="character", default="./modeling/data/output/preds.csv",
                help="performance summary output file name [default= %default]", metavar="character"),
    make_option(c("-w", "--week"), type="character", default="./modeling/data/output/preds.csv",
                help="week [default= %default]", metavar="character"),
    make_option(c("-ft", "--feat_type"), type="character", default="./modeling/data/output/preds.csv",
                help="feature type [default= %default]", metavar="character")
)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)
if (is.null(opt$file)){
    print_help(opt_parser)
    stop("At least one argument must be supplied (input file).n", call.=FALSE)
}
if (is.null(opt$model)){
    print_help(opt_parser)
    stop("Model must be supplied.", call.=FALSE)
}

# todo: accept multiple input files, or a directory of input files?
test_data = opt$file
model = opt$model
pred_path = opt$pred
summary_path = opt$summary
week = opt$week
feat_type = opt$feat_type

# read data
# load libraries, etc.
source('./modeling/train_model_xing_utils.R')
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)

# read input data and preprocess; load model
data = read_and_preproc_xing_data(test_data, target_week = week)
message(paste0("[INFO] loading model from ", model))
load(model)
# generate predictions and save to pred_path
preds = xing_two_stage_predict(data, xing_mod)
pred_df = make_pred_df(preds)
pred_df = cbind(pred_df, data.frame(label = data$dropout_current_week))
message(paste0("[INFO] writing predictions to ", pred_path))
write.csv(pred_df, file = pred_path, row.names = FALSE)
# generate summary performance metrics and save to summary_path
summary_df = fetch_model_metrics(pred_df)
summary_df$week = week
summary_df$feat_type = feat_type
message(paste0("[INFO] writing model summary to ", summary_path))
write.csv(summary_df, file = summary_path, row.names = FALSE)

