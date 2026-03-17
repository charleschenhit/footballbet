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

# 缺失值填充
for (col in feature_cols) {
  med <- suppressWarnings(median(model_df[[col]], na.rm = TRUE))
  if (is.na(med)) med <- 0
  model_df[[col]][is.na(model_df[[col]])] <- med
}

message("Rows after cleaning: ", nrow(model_df))

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
# 5) Stage 1: Draw vs Not Draw
# =========================
train_stage1 <- train_df %>%
  mutate(draw_flag = ifelse(target_result_90 == "D", 1, 0))

test_stage1 <- test_df %>%
  mutate(draw_flag = ifelse(target_result_90 == "D", 1, 0))

dtrain_stage1 <- xgb.DMatrix(
  data = as.matrix(train_stage1[, feature_cols]),
  label = train_stage1$draw_flag
)

dtest_stage1 <- xgb.DMatrix(
  data = as.matrix(test_stage1[, feature_cols]),
  label = test_stage1$draw_flag
)

params_stage1 <- list(
  objective = "binary:logistic",
  eval_metric = "logloss",
  max_depth = 4,
  eta = 0.05,
  min_child_weight = 3,
  subsample = 0.85,
  colsample_bytree = 0.85,
  gamma = 0.1
)

watchlist_stage1 <- list(train = dtrain_stage1, eval = dtest_stage1)

model_stage1 <- xgb.train(
  params = params_stage1,
  data = dtrain_stage1,
  nrounds = 300,
  watchlist = watchlist_stage1,
  early_stopping_rounds = 20,
  verbose = 1
)

message("Stage 1 finished.")
message("Stage 1 best iteration: ", model_stage1$best_iteration)

# 概率：P(D)
p_draw <- predict(model_stage1, dtest_stage1)

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
  data = as.matrix(train_stage2[, feature_cols]),
  label = train_stage2$ha_label
)

dtest_stage2 <- xgb.DMatrix(
  data = as.matrix(test_stage2[, feature_cols]),
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

# 对整个 test_df 预测 P(H | not_draw)
dtest_all_stage2 <- xgb.DMatrix(
  data = as.matrix(test_df[, feature_cols])
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

# 再归一化一次，避免浮点误差
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

message("========== Two-Stage Model Result ==========")
message("Accuracy: ", round(accuracy, 4))
message("Log Loss: ", round(logloss, 6))

print(conf_mat)

# =========================
# 9) Stage-specific diagnostics
# =========================
stage1_pred_label <- ifelse(p_draw >= 0.5, 1, 0)
stage1_conf_mat <- confusionMatrix(
  data = factor(stage1_pred_label, levels = c(0, 1)),
  reference = factor(test_stage1$draw_flag, levels = c(0, 1))
)

message("========== Stage 1: Draw vs Not Draw ==========")
print(stage1_conf_mat)

stage2_prob_test <- predict(model_stage2, dtest_stage2)
stage2_pred_label <- ifelse(stage2_prob_test >= 0.5, 1, 0)

stage2_conf_mat <- confusionMatrix(
  data = factor(stage2_pred_label, levels = c(0, 1)),
  reference = factor(test_stage2$ha_label, levels = c(0, 1))
)

message("========== Stage 2: Home vs Away (Non-draw only) ==========")
print(stage2_conf_mat)

# =========================
# 10) feature importance
# =========================
message("========== Stage 1 Importance ==========")
imp_stage1 <- xgb.importance(feature_names = feature_cols, model = model_stage1)
print(imp_stage1)

message("========== Stage 2 Importance ==========")
imp_stage2 <- xgb.importance(feature_names = feature_cols, model = model_stage2)
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

print(head(result_preview, 20))
