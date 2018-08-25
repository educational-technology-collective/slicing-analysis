# script to conduct sample slicing analysis
source("slice_utils.R")

# module-level variables
data_dir = "../data"
pred_csv = "josh_gardner-dl-replication-week3-lstm-test.csv"
label_csv = "labels-test-michigan.csv"
protected_attr_csv = "hash-mapping-exports/coursera_user_hash_gender_lookup.csv"

#temporary; remove this later as script iterates over courses
course_name = "pythonlearn"

#setwd(".")
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


# for (course_name in unique(user_course_df$course)){    
    course_df <- user_course_df %>%
        dplyr::filter(course == course_name) %>%
        tibble::column_to_rownames("userID") %>%
        select(c("prob", "label_value", "gender"))
    ss = compute_slice_statistic(course_df, pred_col = "prob", label_col = "label_value", protected_attr_col = "gender",  majority_protected_attr_val = "male")
    #todo: store ss in some named list/array
#} # end iteration over courses
    
    