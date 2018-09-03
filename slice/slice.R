# script to conduct sample slicing analysis
source("slice_utils.R")

# module-level variables
data_dir = "../data"
pred_csv = "josh_gardner-dl-replication-week3-lstm-test.csv"
label_csv = "labels-test-michigan.csv"
protected_attr_csv = "hash-mapping-exports/coursera_user_hash_gender_lookup.csv"
img_dir = "../img/fei-lstm/michigan"

pred_df = read.csv(file.path(data_dir, pred_csv), stringsAsFactors = FALSE)
label_df = read.csv(file.path(data_dir, label_csv), stringsAsFactors = FALSE)
protected_attr_df = read.csv(file.path(data_dir, protected_attr_csv), stringsAsFactors = FALSE)


# create single dataframe with user-course level prediction, label, and protected attributes
user_course_df <- label_df %>%
    dplyr::filter(label_type == "dropout") %>%
    dplyr::inner_join(pred_df) %>%
    dplyr::inner_join(protected_attr_df, by = c("userID" = "session_user_id", "course" = "course")) %>%
    dplyr::select(c("userID", "course", "prob", "label_value", "gender")) %>%
    dplyr::mutate(gender = forcats::as_factor(gender))


ss_list = list()
for (course_name in unique(user_course_df$course)){ 
    message(glue('processing course {course_name}'))
    course_df <- user_course_df %>%
        dplyr::filter(course == course_name) %>%
        tibble::column_to_rownames("userID") %>%
        select(c("prob", "label_value", "gender"))
    ss = compute_slice_statistic(course_df, 
                                 pred_col = "prob", 
                                 label_col = "label_value", 
                                 protected_attr_col = "gender",  
                                 majority_protected_attr_val = "male",
                                 image_dir = img_dir,
                                 course = course_name)
    ss_list[[course_name]] <- ss
} # end iteration over courses

## exploratory visualization of results

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
slice_results = data.frame("Slice.Statistic" = unlist(ss_list)) %>% tibble::rownames_to_column(var = "Course")

# barplot of slice statistics by course
slice_results %>%
    dplyr::inner_join(course_gender_balance_df, by = c("Course" = "course")) %>%
    dplyr::inner_join(course_size_df, by = c("Course" = "course")) %>%
    ggplot(aes(x  = reorder(slice_results$Course, slice_results$Slice.Statistic), y = Slice.Statistic)) + geom_bar(stat = "identity") + coord_flip()

# scatter plot, gender imbalance vs. slice statistc
slice_results %>%
    dplyr::inner_join(course_gender_balance_df, by = c("Course" = "course")) %>%
    dplyr::inner_join(course_size_df, by = c("Course" = "course")) %>%
    ggplot(aes(x  = freq, y = Slice.Statistic, size = total_students_gender_known)) + 
    geom_point() +
    geom_smooth(method = "lm", formula = y ~ poly(x, 2)) +
    theme_bw() + 
    # ylim(0,0.2) + 
    xlab("Proportion of Male Students") + 
    ylab("Slice Statistic") + 
    ggtitle("Slice Statistic By Gender Imbalance") + 
    guides(size = FALSE)







    