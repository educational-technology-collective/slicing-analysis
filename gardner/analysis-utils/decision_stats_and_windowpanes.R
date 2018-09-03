results = "/Users/joshgardner/Documents/UM-Graduate/UMSI/LED_Lab/jla-model-eval/experiment-results-analysis-11-4/complete_comparison_results.csv"
results = read.csv(results, stringsAsFactors = F)
OUTDIR = "/Users/joshgardner/Documents/UM-Graduate/UMSI/LED_Lab/jla-model-eval/experiment-results-analysis-11-4/"
# preprocessing
cd_col = names(results)[grep("rank_x.rank_y_greater_cd", names(results))]
results[cd_col] = ifelse(results[cd_col] == "False", F, T) # indicator for whether difference is statistically significant/whether NHST makes a decision
BDT = 0.95 # bayesian decision threshold
results$bayes_decision = factor(ifelse(results$left > BDT, "LEFT", ifelse(results$rope > BDT, "ROPE", ifelse(results$right > BDT, "RIGHT", "NONE")))) # indicator for whether Bayesian makes a decision and what that decision is

library(dplyr)
library(ggplot2)
library(magrittr)
# fetch best model by AUC; family of best models is defined as those with performance statistically indistinguishable from this model
best_model_id = results %>% select(one_of(c("model_id_x", "avg_auc_x"))) %>% top_n(1, avg_auc_x)
best_model_id = best_model_id$model_id_x[1]
# percentage of comparisons where NHST makes a decision
nhst_decisions = mean(results[,cd_col])
print(paste0("NHST procedure decides in ", round(nhst_decisions, 4), " of ", nrow(results), " cases"))
best_model_df_nhst = results[(results["model_id_x"] == best_model_id) & (results[cd_col] == F),]
write.csv(best_model_df_nhst, file=paste0(OUTDIR, "best_model_family_nhst.csv"))

# percentage of comparisons where bayesian model eval makes a decision
bayesian_decisions = mean(results$bayes_decision %in% c("LEFT", "RIGHT", "ROPE"))
print(paste0("Bayesian procedure decides in ", round(bayesian_decisions, 4), " of ", nrow(results), " cases"))
best_model_df_bayesian = results[(results["model_id_x"] == best_model_id) & (results["bayes_decision"] == "ROPE"),]
write.csv(best_model_df_bayesian, file=paste0(OUTDIR, "best_model_family_bayesian.csv"))

## method: "frequentist" or "bayesian"
preprocess_results_df <- function(df, method){
    if (method == "frequentist") decision_col = cd_col
    if (method == "bayesian") decision_col = "bayes_decision"
    use_cols = c("model_id_x", "model_id_y","ROC_rank_x", "ROC_rank_y", decision_col)
    temp = dplyr::select(df, one_of(use_cols)) 
    # create a second dataframe with same model_ids, but permuted (also permute ranks)
    temp2 = data.frame("model_id_x" = temp$model_id_y, "model_id_y" = temp$model_id_x, "ROC_rank_x" = temp$ROC_rank_y, "ROC_rank_y" = temp$ROC_rank_x, temp[decision_col])
    # for bayesian method, need to switch "left" and "right" decisions in permuted df
    if (method == "bayesian"){
        #TODO: see if we can avoid hard-coding bayes_decision value here
       temp2 %<>% mutate(bayes_decision = plyr::revalue(bayes_decision, c("RIGHT" = "LEFT", "LEFT" = "RIGHT")))
    }
    # join into single dataframe; now we have all permutations of both models
    complete_comparison_mx = rbind(temp, temp2)
    # if frequentist method, generate categorical variable for decision
    if (method == "frequentist"){
        complete_comparison_mx$ind = ifelse(complete_comparison_mx$rank_x.rank_y_greater_cd_22.5936 == F, "ND", ifelse(complete_comparison_mx$ROC_rank_x > complete_comparison_mx$ROC_rank_y, "GT", "LT"))
    }
    # create dataframe of model_ids
    model_number_df = complete_comparison_mx %>% 
        dplyr::select(one_of(c("model_id_x", "ROC_rank_x"))) %>% 
        dplyr::rename(model_id=model_id_x) %>%
        unique() %>% 
        arrange(ROC_rank_x)
    model_number_df$model_num = seq(nrow(model_number_df))
    # model id for x
    complete_comparison_mx = merge(complete_comparison_mx, model_number_df, by.x = "model_id_x", by.y = "model_id")
    names(complete_comparison_mx)[names(complete_comparison_mx) == "model_num"] <- "model_num_x"
    # model id for y
    complete_comparison_mx = merge(complete_comparison_mx, model_number_df, by.x = "model_id_y", by.y = "model_id")
    names(complete_comparison_mx)[names(complete_comparison_mx) == "model_num"] <- "model_num_y"
    # create nice names for plotting; only needed for model_id_y (model_id_x names not shown in plot)
    complete_comparison_mx$model_id_y_withnum = factor(paste0(complete_comparison_mx$model_id_y, " (", complete_comparison_mx$model_num_y, ")"))
    return(complete_comparison_mx)
}


frequentist_windowpane_plot <- function(df){
    complete_comparison_mx = preprocess_results_df(df, method = "frequentist")
    #plot
    ggplot(complete_comparison_mx, aes(x = reorder(model_id_x, model_num_x), y = reorder(model_id_y_withnum, -model_num_y), fill = factor(ind))) + 
        geom_tile() + 
        geom_text(aes(label = model_num_x), size=1.75) + 
        scale_fill_manual(labels = c("Y > X", "Y < X", "No Decision"), values=c("#E69F00", "#56B4E9", "#FFFFFF")) + 
        theme(panel.background = element_rect(fill="white"), 
              axis.text = element_text(size=rel(0.85)), 
              plot.title=element_text(hjust = 0.5, size = rel(2)), 
              axis.text.x = element_blank(), 
              axis.ticks = element_blank(),
              axis.title.x = element_blank(),
              axis.title.y = element_blank()) + 
        ggtitle("Frequentist Model Decisions") +
        labs(fill = "Frequentist Decision")
}

frequentist_windowpane_plot(results)
ggsave("frequentist_decision_plot.pdf", device = "pdf", path = paste0(OUTDIR, "img"), width = 15, height = 12, units = "in")

## this will be bayesian_windowpane_plot function
bayesian_windowpane_plot <- function(df){
    df = results
    complete_comparison_mx = preprocess_results_df(df, method = "bayesian")
    #plot
    ggplot(complete_comparison_mx, aes(x = reorder(model_id_x, model_num_x), y = reorder(model_id_y_withnum, -model_num_y), fill = bayes_decision)) + 
        geom_tile() + 
        geom_text(aes(label = model_num_x), size=1.75) + 
        scale_fill_manual(labels = c("Y < X", "No Decision", "Y > X", "ROPE"), values=c("#56B4E9", "#FFFFFF", "#E69F00", "#999999")) +
        theme(panel.background = element_rect(fill="white"), 
              axis.text = element_text(size=rel(0.85)), 
              plot.title=element_text(hjust = 0.5, size = rel(2)), 
              axis.text.x = element_blank(), 
              axis.ticks = element_blank(),
              axis.title.x = element_blank(),
              axis.title.y = element_blank()) + 
        ggtitle("Bayesian Model Decisions") +
        labs(fill = "Bayesian Decision")
}

bayesian_windowpane_plot(results)
ggsave("bayesian_decision_plot.pdf", device = "pdf", path = paste0(OUTDIR, "img"), width = 15, height = 12, units = "in")
