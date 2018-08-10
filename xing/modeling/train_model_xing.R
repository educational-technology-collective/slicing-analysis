## script to replicate [Xing 2016] on training data and save output model
# usage: $ Rscript ./modeling/train_model_xing.R --file' /path/to/train_data --out /path/to/out.csv --week 4

# read command line argument for input data path; see https://www.r-bloggers.com/passing-arguments-to-an-r-script-from-command-lines/
library(optparse, quietly = TRUE, warn.conflicts = FALSE)
option_list = list(
    make_option(c("-f", "--file"), type="character", default=NULL, 
                help="dataset file name", metavar="character"),
    make_option(c("-o", "--out"), type="character", default="./modeling/data/output/xing_mod.Rdata", #TODO: make default "./data/output/xing_mod.Rdata"
                help="output file name [default= %default]", metavar="character"),
    make_option(c("-w", "--week"), type="character", default="./modeling/data/output/preds.csv",
                help="week [default= %default]", metavar="character")
    )

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)
if (is.null(opt$file)){
    print_help(opt_parser)
    stop("At least one argument must be supplied (input file).n", call.=FALSE)
}

train_data = opt$file
output_path = opt$out
week = opt$week
N_FOLDS = 3

# load libraries, etc.
source('./modeling/train_model_xing_utils.R')
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)

# read input data and preprocess
message(paste0("Reading ", train_data))
data = read_and_preproc_xing_data(train_data, target_week = week)

if (sum(table(data$dropout_current_week) < N_FOLDS) > 0) stop(paste0("Fewer than", N_FOLDS, " observations for at least one outcome class; cannot build models"))

# train ensemble model (C4.5, bayesian network --> logistic regression meta-learner).
xing_mod = train_xing_model(data, nfolds = 3)

# save models (base learner and level 1 learner) to output path
message(paste0("Saving output to ", output_path))
.jcache(xing_mod[['tree_base']]$classifier)
save(xing_mod, file = output_path)
