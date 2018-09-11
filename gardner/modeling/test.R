# loads a model, reads test data, and outputs predictions (labels and probabilities) for a replication of Gardner and Brooks (2018)
# read command line argument for input data path; see https://www.r-bloggers.com/passing-arguments-to-an-r-script-from-command-lines/
library(optparse, quietly = TRUE, warn.conflicts = FALSE)
option_list = list(
    make_option(c("-c", "--course"), type="character", default=NULL, 
                help="course", metavar="character"),
    make_option(c("-i", "--input_dir"), type="character", default=NULL,
                help="current working dir", metavar="character"),
    make_option(c("-o", "--output_dir"), type="character", default=NULL,
                help="output dir", metavar="character"),
    # make_option(c("-m", "--model_type"), type="character", default=NULL,
    #             help="model type", metavar="character")
)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

course = opt$course
input_dir = opt$input_dir
output_dir = opt$output_dir

source("modeling/modeling_utils.R")
test_df = read_course_data(course, testdata = TRUE) %>% tibble::column_to_rownames(var = "userID") # for some reason, column_to_rownames does not work inside function
load(file.path(input_dir, "model.Rdata"))
raw_preds = predict(mod, newdata = test_df, type = "prob")
df_out <- raw_preds %>% tibble::rownames_to_column(var = "userID") %>%
    dplyr::mutate(pred = as.numeric(dropout > 0.5)) %>% # predict 1 if dropout probability > 0.5, otherwise zero
    dplyr::rename(prob = dropout) %>% # rename "dropout probability" column to simply prob; this matches column naming convention in MORF
    dplyr::select(c("userID", "prob", "pred")) # select to ensure columns in correct order
write.csv(file.path(output_dir, "predictions.csv"), row.names = FALSE, col.names = TRUE)    

