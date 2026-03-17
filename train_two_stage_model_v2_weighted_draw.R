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
# 3) cleaning + feature engineering
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

# 缺失值先填，避免组合特征出现连锁 NA
raw_feature_cols <- c(
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

for (col in raw_feature_cols) {
  med <- suppressWarnings(median(df[[col]], na.rm = TRUE))
  if (is.na(med)) med <- 0
  df[[col]][is.na(df[[col]])] <- med
}

# 新增：平局专用组合特征
df <- df %>%
  mutate(
    sum_clean_sheet_rate_10 = home_clean_sheet_rate_10 + away_clean_sheet_rate_10,
    sum_scoring_rate_10 = home_scoring_rate_10 + away_scoring_rate_10,
    avg_clean_sheet_rate_10 = (home_clean_sheet_rate_10 + away_clean_sheet_rate_10) / 2,
    avg_scoring_rate_10 = (home_scoring_rate_10 + away_scoring_rate_10) / 2,

    sum_last10_points_avg = home_last10_points_avg + away_last10_points_avg,
    sum_last10_goal_diff_avg = home_last10_goal_diff_avg + away_last10_goal_diff_avg,
    sum_last5_goal_diff_avg = home_last5_goal_diff_avg + away_last5_goal_diff_avg,

    # 两边近期净胜球波动越接近、越低，越可能胶着
    goal_diff_balance_proxy = abs(home_last10_goal_diff_avg) + abs(away_last10_goal_diff_avg)
  )

# Stage 1 特征：专注平局
stage1_feature_cols <- c(
  "abs_last5_points_diff",
  "abs_last10_points_diff",
  "abs_last5_goal_diff_diff",
  "abs_last10_goal_diff_diff",
  "abs_rest_days_diff_home_away",

  "home_clean_sheet_rate_10",
  "away_clean_sheet_rate_10",
  "home_scoring_rate_10",
  "away_scoring_rate_10",

  "sum_clean_sheet_rate_10",
  "sum_scoring_rate_10",
  "avg_clean_sheet_rate_10",
  "avg_scoring_rate_10",

  "sum_last10_points_avg",
  "sum_last10_goal_diff_avg",
  "sum_last5_goal_diff_avg",
  "goal_diff_balance_proxy",

  "home_days_since_last_match",
  "away_days_since_last_match",

  "neutral_flag",
  "same_confederation_flag",
  "friendly_flag",
  "tournament_match_flag",
  "home_world_cup_host_flag",
  "away_world_cup_host_flag"
)

# Stage 2 特征：专注主胜/客胜
stage2_feature_cols <- c(
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
  select(
    match_id,
    match_date,
    all_of(unique(c(stage1_feature_cols, stage2_feature_cols))),
    target_result_90
  )

message("Rows after cleaning/feature engineering: ", nrow(model_df))

# =========================
# 4) split train/test by time
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
# 5) Stage 1: Draw vs Not Draw (weighted)
# =========================
train_stage1 <- train_df %>%
  mutate(draw_flag = ifelse(target_result_90 == "D", 1, 0))

test_stage1 <- test_df %>%
  mutate(draw_flag = ifelse(target_result_90 == "D", 1, 0))

n_draw <- sum(train_stage1$draw_flag == 1)
n_not_draw <- sum(train_stage1$draw_flag == 0)

draw_weight <- ifelse(n_draw > 0, n_not_draw / n_draw, 1)

message("Stage 1 draw samples: ", n_draw)
message("Stage 1 non-draw samples: ", n_not_draw)
message("Stage 1 draw weight: ", round(draw_weight, 4))

stage1_weights <- ifelse(train_stage1$draw_flag == 1, draw_weight, 1)

dtrain_stage1 <- xgb.DMatrix(
  data = as.matrix(train_stage1[, stage1_feature_cols]),
  label = train_stage1$draw_flag,
  weight = stage1_weights
)

dtest_stage1 <- xgb.DMatrix(
  data = as.matrix(test_stage1[, stage1_feature_cols]),
  label = test_stage1$draw_flag
)

params_stage1 <- list(
  objective = "binary:logistic",
  eval_metric = "logloss",
  max_depth = 3,
  eta = 0.03,
  min_child_weight = 2,
  subsample = 0.9,
  colsample_bytree = 0.9,
  gamma = 0.05
)

watchlist_stage1 <- list(train = dtrain_stage1, eval = dtest_stage1)

model_stage1 <- xgb.train(
  params = params_stage1,
  data = dtrain_stage1,
  nrounds = 500,
  watchlist = watchlist_stage1,
  early_stopping_rounds = 30,
  verbose = 1
)

message("Stage 1 finished.")
message("Stage 1 best iteration: ", model_stage1$best_iteration)

p_draw <- predict(model_stage1, dtest_stage1)

# 诊断用：扫描多个阈值
threshold_grid <- seq(0.15, 0.40, by = 0.01)
threshold_eval <- lapply(threshold_grid, function(th) {
  pred_label <- ifelse(p_draw >= th, 1, 0)
  actual <- test_stage1$draw_flag

  tp <- sum(pred_label == 1 & actual == 1)
  fp <- sum(pred_label == 1 & actual == 0)
  fn <- sum(pred_label == 0 & actual == 1)
  tn <- sum(pred_label == 0 & actual == 0)

  precision <- ifelse(tp + fp == 0, NA, tp / (tp + fp))
  recall <- ifelse(tp + fn == 0, NA, tp / (tp + fn))
  f1 <- ifelse(is.na(precision) | is.na(recall) | (precision + recall == 0),
               NA, 2 * precision * recall / (precision + recall))
  accuracy <- mean(pred_label == actual)

  data.frame(
    threshold = th,
    precision = precision,
    recall = recall,
    f1 = f1,
    accuracy = accuracy
  )
}) %>% bind_rows()

best_threshold_row <- threshold_eval %>%
  arrange(desc(f1), desc(recall), desc(accuracy)) %>%
  slice(1)

best_draw_threshold <- best_threshold_row$threshold[[1]]
message("Best Stage 1 threshold by F1: ", best_draw_threshold)
print(threshold_eval)

# =========================
# 6) Stage 2: Home vs Away on non-draw only
# =========================
train_stage2 <- train_df %>%
  filter(target_result_90 != "D") %>%
  mutate(
    ha_label = ifelse(target_result_90 == "H", 1, 0)  # H=1, A=0
  )

test_stage2 <- test_df %>%
  filter(target_result_90 != "D") %>%
  mutate(
    ha_label = ifelse(target_result_90 == "H", 1, 0)
  )

if (nrow(train_stage2) == 0) stop("No non-draw rows in train set for stage 2.")
if (nrow(test_stage2) == 0) stop("No non-draw rows in test set for stage 2.")

dtrain_stage2 <- xgb.DMatrix(
  data = as.matrix(train_stage2[, stage2_feature_cols]),
  label = train_stage2$ha_label
)

dtest_stage2 <- xgb.DMatrix(
  data = as.matrix(test_stage2[, stage2_feature_cols]),
  label = test_stage2$ha_label
)

params_stage2 <- list(
  objective = "binary:logistic",
  eval_metric = "logloss",
  max_depth = 4,
  eta = 0.05,
  min_child_weight = 3,
  subsample = 0.85,
  colsample_bytree = 0.85,
  gamma = 0.1
)

watchlist_stage2 <- list(train = dtrain_stage2, eval = dtest_stage2)

model_stage2 <- xgb.train(
  params = params_stage2,
  data = dtrain_stage2,
  nrounds = 300,
  watchlist = watchlist_stage2,
  early_stopping_rounds = 20,
  verbose = 1
)

message("Stage 2 finished.")
message("Stage 2 best iteration: ", model_stage2$best_iteration)

dtest_all_stage2 <- xgb.DMatrix(
  data = as.matrix(test_df[, stage2_feature_cols])
)

p_home_given_not_draw <- predict(model_stage2, dtest_all_stage2)
p_away_given_not_draw <- 1 - p_home_given_not_draw

# =========================
# 7) combine final H/D/A probabilities
# =========================
prob_D <- p_draw
prob_H <- (1 - prob_D) * p_home_given_not_draw
prob_A <- (1 - prob_D) * p_away_given_not_draw

pred_prob <- data.frame(
  H = prob_H,
  D = prob_D,
  A = prob_A
)

row_sums <- rowSums(pred_prob)
pred_prob$H <- pred_prob$H / row_sums
pred_prob$D <- pred_prob$D / row_sums
pred_prob$A <- pred_prob$A / row_sums

pred_class <- apply(pred_prob, 1, function(x) names(x)[which.max(x)])
pred_class <- factor(pred_class, levels = c("H", "D", "A"))

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

message("========== Two-Stage V2 Result ==========")
message("Accuracy: ", round(accuracy, 4))
message("Log Loss: ", round(logloss, 6))

print(conf_mat)

# =========================
# 9) stage diagnostics
# =========================
stage1_pred_label_050 <- ifelse(p_draw >= 0.5, 1, 0)
stage1_pred_label_best <- ifelse(p_draw >= best_draw_threshold, 1, 0)

message("========== Stage 1 @ threshold 0.50 ==========")
print(confusionMatrix(
  data = factor(stage1_pred_label_050, levels = c(0, 1)),
  reference = factor(test_stage1$draw_flag, levels = c(0, 1))
))

message("========== Stage 1 @ best threshold ==========")
print(confusionMatrix(
  data = factor(stage1_pred_label_best, levels = c(0, 1)),
  reference = factor(test_stage1$draw_flag, levels = c(0, 1))
))

stage2_prob_test <- predict(model_stage2, dtest_stage2)
stage2_pred_label <- ifelse(stage2_prob_test >= 0.5, 1, 0)

message("========== Stage 2 H vs A ==========")
print(confusionMatrix(
  data = factor(stage2_pred_label, levels = c(0, 1)),
  reference = factor(test_stage2$ha_label, levels = c(0, 1))
))

# =========================
# 10) feature importance
# =========================
message("========== Stage 1 Importance ==========")
imp_stage1 <- xgb.importance(feature_names = stage1_feature_cols, model = model_stage1)
print(imp_stage1)

message("========== Stage 2 Importance ==========")
imp_stage2 <- xgb.importance(feature_names = stage2_feature_cols, model = model_stage2)
print(imp_stage2)

# =========================
# 11) preview predictions
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

print(head(result_preview, 30))
