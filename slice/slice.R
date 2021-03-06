# script to conduct sample slicing analysis
library(optparse) #, quietly = TRUE, warn.conflicts = FALSE
option_list = list(
    make_option("--pred_csv", type="character", default=NULL,
                help="csv file of predictions", metavar="character"),
    make_option("--label_csv", type="character", default=NULL,
                help="csv file of labels", metavar="character"),
    make_option("--protected_attr_csv", type="character", default=NULL,
                help="csv file of protected attribute memberships", metavar="character"),
    make_option("--img_dir", type="character", default=NULL,
                help="directory to write images to", metavar="character")
)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)
pred_csv = opt$pred_csv
label_csv = opt$label_csv
protected_attr_csv = opt$protected_attr_csv
img_dir = opt$img_dir
pred_df = read.csv(pred_csv, stringsAsFactors = FALSE)
label_df = read.csv(label_csv, stringsAsFactors = FALSE)
protected_attr_df = read.csv(protected_attr_csv, stringsAsFactors = FALSE)


source("slice/slice_utils.R")

# create single dataframe with user-course level prediction, label, and protected attributes
user_course_df <- label_df %>%
    dplyr::filter(label_type == "dropout") %>%
    dplyr::inner_join(pred_df) %>%
    dplyr::inner_join(protected_attr_df, by = c("userID" = "session_user_id", "course" = "course")) %>%
    dplyr::select(c("userID", "course", "prob", "label_value", "gender")) %>%
    dplyr::mutate(gender = forcats::as_factor(gender))
# compute slice statistic for each course and write slice plots to img_dir
ss_list = list()
auc_list = list()
for (course_name in unique(user_course_df$course)){ 
    message(glue('processing course {course_name}'))
    course_df <- user_course_df %>%
        dplyr::filter(course == course_name) %>%
        tibble::column_to_rownames("userID") %>%
        select(c("prob", "label_value", "gender"))
    if (length(unique(course_df$label_value)) <= 1){
        message(glue::glue("warning: skipping course {course_name}; must be at least 2 unique label values"))
    } else{
        ss_list[[course_name]] = compute_slice_statistic(course_df, 
                                     pred_col = "prob", 
                                     label_col = "label_value", 
                                     protected_attr_col = "gender",  
                                     majority_protected_attr_val = "male",
                                     image_dir = img_dir,
                                     course = course_name)
        auc_list[[course_name]] <- compute_auc(course_df[,"prob"], course_df[,"label_value"])
        
    }
} # end iteration over courses

######################################################
### compute other summary statistics and write to csv
######################################################

# compute gender balance in courses
course_gender_balance_df <- user_course_df %>%
    dplyr::group_by(course, gender) %>%
    dplyr::summarise (n = n()) %>%
    dplyr::mutate(freq = n / sum(n)) %>%
    dplyr::filter(gender == "male")

# compute size of courses
course_size_df <- user_course_df %>%
    dplyr::group_by(course) %>% # get total course size
    dplyr::summarise (total_students_gender_known = n()) %>%
    dplyr::ungroup()

library(ggplot2)
slice_results = data.frame("Slice.Statistic" = unlist(ss_list)) %>% 
    tibble::rownames_to_column(var = "Course") %>%
    dplyr::inner_join(course_gender_balance_df, by = c("Course" = "course")) %>%
    dplyr::inner_join(course_size_df, by = c("Course" = "course"))
auc_results = data.frame("AUC" = unlist(auc_list)) %>%
    tibble::rownames_to_column(var = "Course")
full_results = dplyr::left_join(slice_results, auc_results, by = "Course")

write.csv(full_results, file.path(img_dir, "slice_results.csv"))

######################################################
### exploratory visualizations
######################################################

# barplot of slice statistics by course
full_results  %>%
    ggplot(aes(x  = reorder(full_results$Course, full_results$Slice.Statistic), y = Slice.Statistic)) + 
      geom_bar(stat = "identity") + 
      coord_flip()
ggsave(file.path(img_dir, "summary_slice_by_course.pdf"), device = "pdf")

# scatter plot, gender imbalance vs. slice statistc
full_results %>%
    ggplot(aes(x  = freq, y = Slice.Statistic, size = total_students_gender_known)) + 
    geom_point() +
    geom_smooth(method = "lm", formula = y ~ poly(x, 2)) +
    theme_bw() + 
    # ylim(0,0.2) + 
    xlab("Proportion of Male Students") + 
    ylab("Slice Statistic") + 
    ggtitle("Slice Statistic By Gender Imbalance") + 
    guides(size = FALSE)
ggsave(file.path(img_dir, "summary_gender_ratio_vs_slice.pdf"), device = "pdf")

# scatter plot auc vs. slice statistic
full_results %>%
    ggplot(aes(x  = AUC, y = Slice.Statistic, size = total_students_gender_known)) + 
    geom_point()
ggsave(file.path(img_dir, "summary_auc_vs_slice.pdf"), device = "pdf")





    