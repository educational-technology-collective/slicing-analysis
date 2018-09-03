# read command line argument for input data path; see https://www.r-bloggers.com/passing-arguments-to-an-r-script-from-command-lines/
library(optparse, quietly = TRUE, warn.conflicts = FALSE)
option_list = list(
    make_option(c("-c", "--course"), type="character", default=NULL, 
                help="course", metavar="character"),
    make_option(c("-s", "--session"), type="character", default=NULL,
                help="3-digit session number", metavar="character"),
    make_option(c("-w", "--working_dir"), type="character", default=NULL,
                help="current working dir", metavar="character"),
    make_option(c("-o", "--output_dir"), type="character", default=NULL,
                help="output dir", metavar="character")
)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

course = opt$course
session = opt$session
working_dir = opt$working_dir
output_dir = opt$output_dir

setwd(working_dir)
# source("install_packages.R")
source("modeling_utils.R")
build_models(course, session, working_dir, output_dir)