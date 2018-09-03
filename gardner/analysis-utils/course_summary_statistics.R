model_courses = read.csv("/Users/joshgardner/Documents/UM-Graduate/UMSI/LED_Lab/jla-model-eval/experiment-results-analysis-11-4/model_courses.csv")
course_data = read.csv("/Users/joshgardner/Documents/UM-Graduate/UMSI/LED_Lab/s17/model_build_infrastructure/data-validation/josh_gardner-data_validation-course-validation-statistics.csv")

library(dplyr)
complete_data = merge(model_courses, course_data, by.x = c("course", "session"), by.y = c("course", "run"))
write.csv(complete_data, file = "/Users/joshgardner/Documents/UM-Graduate/UMSI/LED_Lab/jla-model-eval/experiment-results-analysis-11-4/model_courses_detailed_statistics.csv")

colSums(complete_data[,6:16])
colMeans(complete_data[,6:16])
apply(complete_data, 2, sd)
