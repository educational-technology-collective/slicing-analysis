# Copyright (C) 2017  The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see [http://www.gnu.org/licenses/].
# =========================================================================
# Utility script to (1) read processed data, (2) build models with different feature groups individually and in combination,
# (3) evaluate the performance of model/featureset combinations with the Friedman + Nemenyi testing procedure,
# and (4) produce critical difference diagrams from those comparisons. For more information on this implementation, see:
#     Gardner and Brooks, 2017, Statistical Approaches to Model Performance Evaluation in MOOCs.
# 
#     
#     
#     
# =========================================================================
library(plyr)
library(dplyr)
library(magrittr)
library(stringr)

# load preprocessing functions from edm_preproc.R
source('edm_preproc.R')
source('edm_modelcomp.R')

# find best model by algorithm and feature subset by week
TARGET_WEEK = 4

model_data = fetch_model_data(target_week = TARGET_WEEK)
set.seed(48125)
n_sample_datasets = round(0.2*length(model_data))
sample_dataset_ix = sample.int(length(model_data), n_sample_datasets)
# remove data used to do hyperparameter tuning
proc_data = model_data[-sample_dataset_ix]
x = conduct_performanceEstimation(proc_data, week = TARGET_WEEK)

# create pairedComparisons object
x_pc = pairedComparisons(x, maxs=c(TRUE, FALSE), p.value=0.05)
# write output to csvs
write.csv(x_pc[['acc']][['avgScores']], file = paste0('output/week-', TARGET_WEEK,'/avg_scores.csv'))
write.csv(x_pc[['acc']][['rks']], file = paste0('output/week-', TARGET_WEEK,'/ranks.csv'))
write.csv(x_pc[['acc']][['avgRksWFs']], file = paste0('output/week-', TARGET_WEEK,'/avg_ranks.csv'))

# # print CDdiagram
# CDdiagram.Nemenyi(x_pc, metric = "acc")
# # save CDdiagram to pdf and png
# pdf(paste0('output/week-', TARGET_WEEK,'/cd_diagram_nemenyi.pdf'), paper ='a4r', width = 5.5, height = 3.4)
# CDdiagram.Nemenyi(x_pc, metric = "acc")
# dev.off()
# png(paste0('output/week-', TARGET_WEEK,'/cd_diagram_nemenyi.png'), width =5.5, height = 3.4, units = 'in', res = 100)
# CDdiagram.Nemenyi(x_pc, metric = "acc")
# dev.off()


