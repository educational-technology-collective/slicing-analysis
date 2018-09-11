# read command line argument for input data path; see https://www.r-bloggers.com/passing-arguments-to-an-r-script-from-command-lines/
library(optparse, quietly = TRUE, warn.conflicts = FALSE)
option_list = list(
    make_option(c("-c", "--course"), type="character", default=NULL, 
                help="course", metavar="character"),
    make_option(c("-i", "--input_dir"), type="character", default=NULL,
                help="current working dir", metavar="character"),
    make_option(c("-o", "--output_dir"), type="character", default=NULL,
                help="output dir", metavar="character"),
    make_option(c("-m", "--model_type"), type="character", default=NULL,
                help="model type", metavar="character")
)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

course = opt$course
input_dir = opt$input_dir
output_dir = opt$output_dir
mt = opt$model_type

setwd(working_dir)
source("modeling/modeling_utils.R")
mod = build_model(course, input_dir, model_type = mt)
# save mod to file
save(mod, file = file.path(output_dir, "model.Rdata"))
