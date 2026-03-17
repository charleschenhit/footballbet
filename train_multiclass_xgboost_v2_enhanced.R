library(DBI)
library(RPostgres)
library(dplyr)
library(tibble)
library(xgboost)
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
feature_version <- "v2_enhanced"
split_date <- as.Date("2024-01-01")

# =========================
# 2) read modeling data
# =========================
sql <- sprintf("
  SELECT
    p.match_id,
    m.match_date,

    p.last5_points_diff,
    p.last10_points_diff,
    p.last5_goal_diff_diff,
    p.last10_goal_diff_diff,
    p.rest_days_diff_home_away,

    p.home_last5_points_avg,
    p.away_last5_points_avg,
    p.home_last10_points_avg,
    p.away_last10_points_avg,

    p.home_last5_goal_diff_avg,
    p.away_last5_goal_diff_avg,
    p.home_last10_goal_diff_avg,
    p.away_last10_goal_diff_avg,

    p.home_clean_sheet_rate_10,
    p.away_clean_sheet_rate_10,
    p.home_scoring_rate_10,
    p.away_scoring_rate_10,

    p.home_days_since_last_match,
    p.away_days_since_last_match,

    p.abs_last5_points_diff,
    p.abs_last10_points_diff,
    p.abs_last5_goal_diff_diff,
    p.abs_last10_goal_diff_diff,
    p.abs_rest_days_diff_home_away,

    p.neutral_flag,
    p.same_confederation_flag,
    p.friendly_flag,
    p.tournament_match_flag,
    p.home_world_cup_host_flag,
    p.away_world_cup_host_flag,

    p.target_result_90
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
  stop("No modeling rows found.")
}

message("Total rows loaded: ", nrow(df))

# =========================
# 3) cleaning
# =========================
df <- df %>%
  mutate(
    match_date = as.Date(match_date),

    neutral_flag = ifelse(is.na(neutral_flag), FALSE, neutral_flag),
    same_confederation_flag = ifelse(is.na(same_confederation_flag), FALSE, same_confederation_flag),
    friendly_flag = ifelse(is.na(friendly_flag), FALSE, friendly_flag),
    tournament_match_flag = ifelse(is.na(tournament_match_flag), FALSE, tournament_match_flag),
    home_world_cup_host_flag = ifelse(is.na(home_world_cup_host_flag), FALSE, home_world_cup_host_flag),
    away_world_cup_host_flag = ifelse(is.na(away_world_cup_host_flag), FALSE, away_world_cup_host_flag),

    neutral_flag = as.integer(neutral_flag),
    same_confederation_flag = as.integer(same_confederation_flag),
    friendly_flag = as.integer(friendly_flag),
    tournament_match_flag = as.integer(tournament_match_flag),
    home_world_cup_host_flag = as.integer(home_world_cup_host_flag),
    away_world_cup_host_flag = as.integer(away_world_cup_host_flag)
  )

feature_cols <- c(
  "last5_points_diff",
  "last10_points_diff",
  "last5_goal_diff_diff",
  "last10_goal_diff_diff",
  "rest_days_diff_home_away",

  "home_last5_points_avg",
  "away_last5_points_avg",
  "home_last10_points_avg",
  "away_last10_points_avg",

  "home_last5_goal_diff_avg",
  "away_last5_goal_diff_avg",
  "home_last10_goal_diff_avg",
  "away_last10_goal_diff_avg",

  "home_clean_sheet_rate_10",
  "away_clean_sheet_rate_10",
  "home_scoring_rate_10",
  "away_scoring_rate_10",

  "home_days_since_last_match",
  "away_days_since_last_match",

  "abs_last5_points_diff",
  "abs_last10_points_diff",
  "abs_last5_goal_diff_diff",
  "abs_last10_goal_diff_diff",
  "abs_rest_days_diff_home_away",

  "neutral_flag",
  "same_confederation_flag",
  "friendly_flag",
  "tournament_match_flag",
  "home_world_cup_host_flag",
  "away_world_cup_host_flag"
)

model_df <- df %>%
  select(match_id, match_date, all_of(feature_cols), target_result_90)

# 缺失值填充：数值列用中位数
for (col in feature_cols) {
  med <- suppressWarnings(median(model_df[[col]], na.rm = TRUE))
  if (is.na(med)) med <- 0
  model_df[[col]][is.na(model_df[[col]])] <- med
}

# 标签编码：H=0, D=1, A=2
label_map <- c("H" = 0, "D" = 1, "A" = 2)
inv_label_map <- c("0" = "H", "1" = "D", "2" = "A")

model_df <- model_df %>%
  mutate(
    target_label = unname(label_map[target_result_90])
  )

# =========================
# 4) train / test split
# =========================
train_df <- model_df %>% filter(match_date < split_date)
test_df  <- model_df %>% filter(match_date >= split_date)

message("Train rows: ", nrow(train_df))
message("Test rows: ", nrow(test_df))

if (nrow(train_df) == 0) stop("Train set is empty.")
if (nrow(test_df) == 0) stop("Test set is empty.")

message("Train target distribution:")
print(table(train_df$target_result_90))

message("Test target distribution:")
print(table(test_df$target_result_90))

# =========================
# 5) matrix for xgboost
# =========================
train_matrix <- as.matrix(train_df[, feature_cols])
test_matrix  <- as.matrix(test_df[, feature_cols])

dtrain <- xgb.DMatrix(data = train_matrix, label = train_df$target_label)
dtest  <- xgb.DMatrix(data = test_matrix, label = test_df$target_label)

# =========================
# 6) model training
# =========================
params <- list(
  objective = "multi:softprob",
  num_class = 3,
  eval_metric = "mlogloss",
  max_depth = 4,
  eta = 0.05,
  min_child_weight = 3,
  subsample = 0.85,
  colsample_bytree = 0.85,
  gamma = 0.1
)

watchlist <- list(train = dtrain, eval = dtest)

model <- xgb.train(
  params = params,
  data = dtrain,
  nrounds = 300,
  watchlist = watchlist,
  early_stopping_rounds = 20,
  verbose = 1
)

message("Model training finished.")
message("Best iteration: ", model$best_iteration)

# =========================
# 7) predict
# =========================
pred_prob_vec <- predict(model, dtest)
pred_prob <- matrix(pred_prob_vec, ncol = 3, byrow = TRUE)
colnames(pred_prob) <- c("H", "D", "A")
pred_prob <- as.data.frame(pred_prob)

pred_label <- max.col(pred_prob, ties.method = "first") - 1
pred_class <- factor(inv_label_map[as.character(pred_label)], levels = c("H", "D", "A"))

# =========================
# 8) evaluation
# =========================
conf_mat <- confusionMatrix(
  data = pred_class,
  reference = factor(test_df$target_result_90, levels = c("H", "D", "A"))
)

accuracy <- mean(pred_class == test_df$target_result_90)

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

message("========== XGBoost Multiclass Result ==========")
message("Accuracy: ", round(accuracy, 4))
message("Log Loss: ", round(logloss, 6))

print(conf_mat)

# =========================
# 9) feature importance
# =========================
importance_matrix <- xgb.importance(
  feature_names = feature_cols,
  model = model
)

print(importance_matrix)

# =========================
# 10) preview predictions
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
