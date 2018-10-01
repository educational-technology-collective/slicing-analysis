
library(magrittr)
library(gmodels)
library(ggplot2)
library(ggthemes)
library(ggrepel)


lr <- read.csv("/Users/joshgardner/Documents/Github/slicing-analysis/img/fei-lr/slice_results.csv")
lstm <- read.csv("/Users/joshgardner/Documents/Github/slicing-analysis/img/fei-lstm/slice_results.csv")
svm <- read.csv("/Users/joshgardner/Documents/Github/slicing-analysis/img/fei-svm/slice_results.csv")
cart <- read.csv("/Users/joshgardner/Documents/Github/slicing-analysis/img/gardner-rpart/slice_results.csv")
nb <- read.csv("/Users/joshgardner/Documents/Github/slicing-analysis/img/gardner-nb/slice_results.csv")

color_scheme = c("#a6cee3", "#1f78b4", "#b2df8a", "#33a02c")
fy_models = c("Logistic Regression", "LSTM", "SVM") # used for statistical testing of FY vs. Gardner models
output_dir = "/Users/joshgardner/Documents/Github/slicing-analysis/img/slice_summary"

df <- dplyr::bind_rows(list("Logistic Regression" = lr, "LSTM" = lstm, "SVM" = svm, "CART" = cart, "Naive Bayes" = nb), .id = "model")

# read and clean course category data, producing single df with only (course_shortname, curricular_area)
course_cats = read.csv("/Users/joshgardner/Documents/Github/slicing-analysis/data/course-data/coursera_course_dates_categorizations.csv", stringsAsFactors = F)
course_cats$course = gsub("2012-001", "001", course_cats$course, fixed = T)
course_cats$course = substr(course_cats$course, 1, nchar(course_cats$course)-4)
course_cats %<>% 
    dplyr::select(c("course", "curricular_area")) %>% unique()

curricular_area_desc_df <- data.frame("curricular_area" = c("CS", "GHSS", "HHRDE", "STEM"), 
                                   "curricular_area_desc" = c("Computer Science", "Government, Health, and Social Science",
                                                              "Humanities, History, Design, Religion, and Education",
                                                              "Science, Technology, Engineering, and Mathematics"))

df %<>% dplyr::inner_join(course_cats, by = c("Course" = "course")) %>% dplyr::left_join(curricular_area_desc_df, by = "curricular_area")
df$model <- factor(df$model)
df$Course <- factor(df$Course)
df$curricular_area <- factor(df$curricular_area)


##################################
##### STATISTICAL TESTING
##################################

# Krustkal-Wallis rank sum test; nonparametric test for multiple samples;
# this is similar to Wilcoxon rank sum test but for more than two groups

# test for differences in models
kruskal.test(df$Slice.Statistic, df$model)

# test for differences in courses
kruskal.test(df$Slice.Statistic, df$Course)

# test for differences in subjects
kruskal.test(df$Slice.Statistic, df$curricular_area)

# test for differences by feature sets, FY 2015 models vs. Gardner 2018 models
kruskal.test(df$Slice.Statistic, factor(df$model %in% fy_models))

# simple linear regression model; evaluate association of ...
## course size
summary(lm(Slice.Statistic ~ model + n, data = df))
## course subject
summary(lm(Slice.Statistic ~ model + curricular_area, data = df))
## gender balance
gender_linear = lm(Slice.Statistic ~ model + freq, data = df)
gender_quadratic = lm(Slice.Statistic ~ model + poly(freq,2), data = df)
summary(gender_linear)
summary(gender_quadratic)
anova(gender_linear, gender_quadratic)

cor.test(df$Slice.Statistic, df$AUC, method = "pearson")

##################################
##### SUMMARY STATISTICS
##################################

# summary/CIs for abroca
df %>%
    dplyr::group_by(model) %>%
    dplyr::summarise(mean = gmodels::ci(Slice.Statistic)[1], 
                  lowCI = gmodels::ci(Slice.Statistic)[2],
                  hiCI = gmodels::ci(Slice.Statistic)[3], 
                  sd = gmodels::ci (Slice.Statistic)[4])

# summary/CIs for auc
df %>%
    dplyr::group_by(model) %>%
    dplyr::summarise(mean = gmodels::ci(AUC)[1], 
                     lowCI = gmodels::ci(AUC)[2],
                     hiCI = gmodels::ci(AUC)[3], 
                     sd = gmodels::ci (AUC)[4])
    
##################################
##### EXPLORATORY PLOTS
##################################

## compare by model
df %>% ggplot(aes(x = model, y = Slice.Statistic, color = model)) + 
    geom_jitter(width = 0.2) + 
    geom_boxplot(width = 0.1) +
    guides(color=F) + 
    theme_base()

## compare by course
df %>% ggplot(aes(x = Course, y = Slice.Statistic, color = curricular_area, label = Course)) + 
    geom_boxplot(width = 0.5) +
    geom_text(check_overlap = T, nudge_y = 0.05) + 
    guides(color=F) + 
    coord_flip() +
    facet_wrap(curricular_area ~ .) +
    theme_base() + 
    theme(axis.text.y = element_blank())

## compare by curricular area
df %>% ggplot(aes(x = curricular_area, y = Slice.Statistic, color = curricular_area)) + 
    geom_boxplot(width = 0.5) +
    coord_flip() +
    theme_base() + 
    theme(axis.text.y = element_blank())

## compare by size and gender balance
df %>%
    ggplot(aes(x = freq, y = Slice.Statistic, size = n, color = curricular_area)) + 
    geom_point() + 
    facet_grid(model ~ ., labeller = label_wrap_gen(width=10)) +
    theme_base() + 
    xlab("Gender Balance (% male)") +
    ylab("ABROCA") + 
    scale_color_manual(values = color_scheme) +
    labs(color = "Curricular Area", size = "Course Size") + 
    theme(legend.position="bottom", 
          legend.box = "vertical", 
          plot.background=element_blank() # removes frame around plotting area
          ) + 
    guides(colour = guide_legend(override.aes = list(size=5)))
ggsave(filename = file.path(output_dir, "abroca_summary_by_model.pdf"), device = "pdf", width = 7, height = 11)

## compare by size and gender balance, single frame w/fitted quadratic lines

labelInfo <- #https://stackoverflow.com/questions/37995552/create-dynamic-labels-for-geom-smooth-lines/37996394#37996394
    split(df, df$model) %>%
    lapply(function(x){
        data.frame(
            predAtMax = lm(Slice.Statistic ~ poly(freq, 2), data=x) %>%
                predict(newdata = data.frame(freq = max(x$freq))), 
            max = max(x$freq)
        )}) %>%
    dplyr::bind_rows()
labelInfo$label = levels(df$model)
labelInfo$label <- forcats::lvls_revalue(levels(df$model), c("CART", "LR", "LSTM", "NB", "SVM"))


pub_plot_1 <-df %>%
    ggplot(aes(x = freq, y = Slice.Statistic)) + 
    geom_point(aes(size = n, color = curricular_area_desc)) + 
    geom_smooth(aes(group = model), method = "lm", formula = y ~ poly(x, 2), color = "black", alpha = 0.05, size = 0.5) +
    theme_base() + 
    xlab("Gender Balance (% male)") +
    ylab("ABROCA") + 
    scale_color_manual(values = color_scheme) +
    labs(color = "Curricular Area", size = "Course Size") + 
    theme(legend.position="bottom", 
          legend.box = "horizontal", # update in other plot
          plot.background=element_blank(), # removes frame around plotting area
          plot.title = element_text(hjust = 0.5), 
          plot.subtitle = element_text(hjust = 0.5)
    ) + 
    guides(colour = guide_legend(override.aes = list(size=5), nrow = 4, title.position = "top"), 
           size = guide_legend(nrow = 4, title.position = "top")) + 
    xlim(0.4,0.875) + 
    geom_label_repel(data = labelInfo
                     , aes(x= max
                           , y = predAtMax
                           , label = label),
                     xlim = c(0.8,0.9)) + 
    ggtitle("ABROCA By Gender Balance", subtitle = "Fitted Quadratic Regression Lines Shown")
pub_plot_1
ggsave(filename = file.path(output_dir, "abroca_summary_singleframe_fitted.pdf"), device = "pdf", width = 7, height = 7)

## compare by size and gender balance, one model only
df %>%
    dplyr::filter(model == "LSTM") %>%
    ggplot(aes(x = freq, y = Slice.Statistic)) + 
    geom_point(aes(size = n, color = curricular_area)) + 
    geom_smooth(method = "lm", formula = y ~ poly(x, 2), color = "black", alpha = 0.2, size = 0.5) +
    theme_base() + 
    xlab("Gender Balance (% male)") +
    ylab("ABROCA") + 
    scale_color_manual(values = color_scheme) +
    labs(color = "Curricular Area", size = "Course Size") + 
    theme(legend.position="bottom", 
          legend.box = "vertical", 
          plot.background=element_blank(), # removes frame around plotting area
          plot.title = element_text(hjust = 0.5), 
          plot.subtitle = element_text(hjust = 0.5)
    ) + 
    guides(colour = guide_legend(override.aes = list(size=5))) +
    ggtitle("ABROCA By Gender Balance", subtitle = "LSTM Model Shown")
ggsave(filename = file.path(output_dir, "abroca_summary_lstm_only.pdf"), device = "pdf", width = 7, height = 7)



df %>%
    ggplot(aes(x = freq, y = Slice.Statistic, size = n, color = curricular_area)) + 
    geom_point() + 
    facet_grid(model ~ curricular_area) +
    theme_base() + 
    xlab("Gender Balance (% male)") +
    ylab("ABROCA") + 
    scale_color_manual(values = color_scheme) +
    labs(color = "Curricular Area", size = "Course Size") + 
    theme(legend.position="bottom", legend.box = "vertical") + 
    guides(colour = guide_legend(override.aes = list(size=5)))
ggsave(filename = file.path(output_dir, "abroca_summary_by_model_curricular.pdf"), device = "pdf", width = 11, height = 11)
ggsave(filename = file.path(output_dir, "abroca_summary_by_model_curricular.jpg"), device = "jpg", width = 11, height = 11)

# explore correlation between slice statistic and performance
df %>%
    ggplot(aes(x = AUC, y = Slice.Statistic)) +
    geom_point() +
    facet_wrap(model ~ .)

auc_abroca_cor = round(cor(df$Slice.Statistic, df$AUC), 3)

#exploratory plot, lines by model
lineLabelInfo <- #https://stackoverflow.com/questions/37995552/create-dynamic-labels-for-geom-smooth-lines/37996394#37996394
    split(df, df$model) %>%
    lapply(function(x){
        data.frame(
            predAtMax = lm(Slice.Statistic ~ AUC, data=x) %>%
                predict(newdata = data.frame(AUC = max(x$AUC))), 
            max = max(x$AUC)
        )}) %>%
    dplyr::bind_rows()
lineLabelInfo$label = levels(df$model)
lineLabelInfo$label <- forcats::lvls_revalue(levels(df$model), c("CART", "LR", "LSTM", "NB", "SVM"))


pub_plot_2 <- df %>%
    ggplot(aes(x = AUC, y = Slice.Statistic)) +
    geom_point(aes(color = curricular_area, size = n)) +
    scale_color_manual(values = color_scheme) +
    geom_smooth(aes(group = model), method = "lm", color = "black", alpha = 0.1, size = 0.5) +
    theme_base() + 
    ylab("ABROCA") + 
    xlab("AUC (Area Under Receiver Operating Characteristic Curve)") + 
    labs(color = "Curricular Area", size = "Course Size") + 
    geom_label_repel(data = lineLabelInfo
                     , aes(x= max
                           , y = predAtMax
                           , label = label)) + 
    theme(legend.position="bottom", 
          legend.box = "horizontal",
          plot.background=element_blank() # removes frame around plotting area
    ) + 
    guides(colour = guide_legend(override.aes = list(size=5), nrow = 4, title.position = "top"), 
           size = guide_legend(nrow = 4, title.position = "top")) + 
    ggtitle(label = "Performance - Unfairness Correlation", subtitle = glue::glue("Correlation = {auc_abroca_cor}")) + 
    theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

pub_plot_2
ggsave(filename = file.path(output_dir, "abroca_vs_auc_lines.pdf"), device = "pdf", width = 7, height = 7)


#same as second publication graphic, but no lines
 df %>%
    ggplot(aes(x = AUC, y = Slice.Statistic, color = curricular_area, size = n)) +
    geom_point() +
    scale_color_manual(values = color_scheme) +
    theme_base() + 
    ylab("ABROCA") + 
    xlab("AUC (Area Under Receiver Operating Characteristic Curve)") + 
    labs(color = "Curricular Area", size = "Course Size") + 
    theme(legend.position="bottom", 
          legend.box = "vertical",
          plot.background=element_blank() # removes frame around plotting area
          ) + 
    guides(colour = guide_legend(override.aes = list(size=5))) + 
    ggtitle(label = "Performance - Unfairness Correlation", subtitle = glue::glue("All Models Shown; Correlation = {auc_abroca_cor}")) + 
    theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
ggsave(filename = file.path(output_dir, "abroca_vs_auc_nolines.pdf"), device = "pdf", width = 7, height = 7)

ggpubr::ggarrange(pub_plot_1 + theme(axis.title = element_text(size = rel(0.75))), 
                  pub_plot_2 + theme(axis.title = element_text(size = rel(0.75))), 
                  ncol = 1, nrow = 2, common.legend = T, legend = "bottom")
ggsave(filename = file.path(output_dir, "pub_plot_exploratory.pdf"), device = "pdf", width = 7, height = 12)

