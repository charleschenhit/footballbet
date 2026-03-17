library(DBI)
library(RPostgres)
library(dplyr)
library(tibble)
library(nnet)
library(caret)

# =========================
# 1) PostgreSQL connection
# =========================
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "postgres",
  host = "localhost",
  port = 5432,
  user = "postgres",
  password = "1q2w3e"
)

schema_name <- "footballbet"
feature_version <- "v1_basic"
split_date <- as.Date("2024-01-01")

# =========================
# 2) read modeling data
# =========================
sql <- sprintf("
  SELECT
    p.match_id,
    m.match_date,
    p.feature_date,
    p.home_team_id,
    p.away_team_id,

    p.last5_points_diff,
    p.last10_points_diff,
    p.last5_goal_diff_diff,
    p.last10_goal_diff_diff,
    p.rest_days_diff_home_away,
    p.neutral_flag,
    p.same_confederation_flag,

    p.target_result_90,
    p.feature_version
  FROM %s.team_pair_pre_match_features p
  JOIN %s.matches m
    ON p.match_id = m.match_id
  WHERE p.feature_version = '%s'
    AND p.target_result_90 IS NOT NULL
  ORDER BY m.match_date, p.match_id
", schema_name, schema_name, feature_version)

df <- dbGetQuery(con, sql)
dbDisconnect(con)

if (nrow(df) == 0) {
  stop("No modeling rows found. Check feature_version and source tables.")
}

message("Total rows loaded: ", nrow(df))

# =========================
# 3) basic cleaning
# =========================
df <- df %>%
  mutate(
    match_date = as.Date(match_date),
    feature_date = as.Date(feature_date),

    target_result_90 = factor(target_result_90, levels = c("H", "D", "A")),

    neutral_flag = ifelse(is.na(neutral_flag), FALSE, neutral_flag),
    same_confederation_flag = ifelse(is.na(same_confederation_flag), FALSE, same_confederation_flag),

    neutral_flag = as.integer(neutral_flag),
    same_confederation_flag = as.integer(same_confederation_flag)
  )

# 只保留当前 baseline 需要的列
model_df <- df %>%
  select(
    match_id,
    match_date,
    target_result_90,
    last5_points_diff,
    last10_points_diff,
    last5_goal_diff_diff,
    last10_goal_diff_diff,
    rest_days_diff_home_away,
    neutral_flag,
    same_confederation_flag
  )

# 去掉目标为空
model_df <- model_df %>%
  filter(!is.na(target_result_90))

# 对数值特征做简单缺失填充
numeric_cols <- c(
  "last5_points_diff",
  "last10_points_diff",
  "last5_goal_diff_diff",
  "last10_goal_diff_diff",
  "rest_days_diff_home_away"
)

for (col in numeric_cols) {
  median_value <- median(model_df[[col]], na.rm = TRUE)

  if (is.na(median_value)) {
    median_value <- 0
  }

  model_df[[col]][is.na(model_df[[col]])] <- median_value
}

message("Rows after cleaning: ", nrow(model_df))

# =========================
# 4) train / test split by time
# =========================
train_df <- model_df %>%
  filter(match_date < split_date)

test_df <- model_df %>%
  filter(match_date >= split_date)

message("Train rows: ", nrow(train_df))
message("Test rows: ", nrow(test_df))

if (nrow(train_df) == 0) stop("Train set is empty.")
if (nrow(test_df) == 0) stop("Test set is empty. Change split_date.")

message("Train target distribution:")
print(table(train_df$target_result_90))

message("Test target distribution:")
print(table(test_df$target_result_90))

# =========================
# 5) fit multinomial logistic regression
# =========================
formula_baseline <- target_result_90 ~
  last5_points_diff +
  last10_points_diff +
  last5_goal_diff_diff +
  last10_goal_diff_diff +
  rest_days_diff_home_away +
  neutral_flag +
  same_confederation_flag

model <- multinom(
  formula_baseline,
  data = train_df,
  trace = FALSE
)

message("Model training finished.")

# =========================
# 6) predict on test set
# =========================
pred_class <- predict(model, newdata = test_df, type = "class")
pred_prob  <- predict(model, newdata = test_df, type = "probs")

# multinom 在某些情况下二分类和多分类返回结构不同，这里统一成 data.frame
pred_prob <- as.data.frame(pred_prob)

# 确保三列都存在
for (cls in c("H", "D", "A")) {
  if (!cls %in% names(pred_prob)) {
    pred_prob[[cls]] <- 0
  }
}
pred_prob <- pred_prob[, c("H", "D", "A")]

# =========================
# 7) evaluation
# =========================
conf_mat <- confusionMatrix(
  data = factor(pred_class, levels = c("H", "D", "A")),
  reference = factor(test_df$target_result_90, levels = c("H", "D", "A"))
)

accuracy <- as.numeric(mean(pred_class == test_df$target_result_90))

calc_multiclass_logloss <- function(actual, probs, eps = 1e-15) {
  actual_chr <- as.character(actual)
  probs <- as.matrix(probs)

  probs[probs < eps] <- eps
  probs[probs > 1 - eps] <- 1 - eps

  row_index <- seq_along(actual_chr)
  col_index <- match(actual_chr, colnames(probs))

  mean(-log(probs[cbind(row_index, col_index)]))
}

logloss <- calc_multiclass_logloss(test_df$target_result_90, pred_prob)

# =========================
# 8) output
# =========================
message("========== Baseline Result ==========")
message("Accuracy: ", round(accuracy, 4))
message("Log Loss: ", round(logloss, 6))

print(conf_mat)

# =========================
# 9) optional: inspect predictions
# =========================
result_preview <- test_df %>%
  transmute(
    match_id,
    match_date,
    actual = target_result_90,
    pred = pred_class,
    prob_H = pred_prob$H,
    prob_D = pred_prob$D,
    prob_A = pred_prob$A
  )

print(head(result_preview, 20))

# =========================
# 10) coefficients
# =========================
message("========== Model Coefficients ==========")
print(summary(model))
