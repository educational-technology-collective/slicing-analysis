# script to conduct sample slicing analysis

# module-level variables
data_dir = "../data"
pred_csv = "josh_gardner-dl-replication-week3-lstm-test.csv"
label_csv = "labels-test-michigan.csv"
protected_attr_csv = "names_for_josh.csv"
course = "sna" # eventually, script should iterate over courses in "course" column of pred_csv

pred_df = read.csv(file.path(data_dir, pred_csv))
label_df = read.csv(file.path(data_dir, label_csv))
protected_attr_df = read.csv(file.path(data_dir, label_csv))

