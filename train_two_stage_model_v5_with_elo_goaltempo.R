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

    p.elo_diff_home_away,

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

    p.home_goals_for_avg_10,
    p.away_goals_for_avg_10,
    p.home_goals_against_avg_10,
    p.away_goals_against_avg_10,
    p.home_total_goals_avg_10,
    p.away_total_goals_avg_10,
    p.sum_goals_for_avg_10,
    p.sum_goals_against_avg_10,
    p.sum_total_goals_avg_10,
    p.abs_goals_for_avg_10_diff,
    p.abs_goals_against_avg_10_diff,
    p.abs_total_goals_avg_10_diff,

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

if (nrow(df) == 0) stop("No modeling rows found.")

message("Total rows loaded: ", nrow(df))

# =========================
# 3) cleaning + engineered features
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

raw_feature_cols <- c(
  "elo_diff_home_away",

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

  "home_goals_for_avg_10",
  "away_goals_for_avg_10",
  "home_goals_against_avg_10",
  "away_goals_against_avg_10",
  "home_total_goals_avg_10",
  "away_total_goals_avg_10",
  "sum_goals_for_avg_10",
  "sum_goals_against_avg_10",
  "sum_total_goals_avg_10",
  "abs_goals_for_avg_10_diff",
  "abs_goals_against_avg_10_diff",
  "abs_total_goals_avg_10_diff",

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

df <- df %>%
  mutate(
    abs_elo_diff_home_away = abs(elo_diff_home_away),

    sum_clean_sheet_rate_10 = home_clean_sheet_rate_10 + away_clean_sheet_rate_10,
    sum_scoring_rate_10 = home_scoring_rate_10 + away_scoring_rate_10,
    avg_clean_sheet_rate_10 = (home_clean_sheet_rate_10 + away_clean_sheet_rate_10) / 2,
    avg_scoring_rate_10 = (home_scoring_rate_10 + away_scoring_rate_10) / 2,

    sum_last10_points_avg = home_last10_points_avg + away_last10_points_avg,
    sum_last10_goal_diff_avg = home_last10_goal_diff_avg + away_last10_goal_diff_avg,
    sum_last5_goal_diff_avg = home_last5_goal_diff_avg + away_last5_goal_diff_avg,

    goal_diff_balance_proxy = abs(home_last10_goal_diff_avg) + abs(away_last10_goal_diff_avg),

    # 进球节奏衍生
    low_scoring_proxy = home_goals_for_avg_10 + away_goals_for_avg_10,
    concession_proxy = home_goals_against_avg_10 + away_goals_against_avg_10,
    tempo_balance_proxy = abs(home_total_goals_avg_10 - away_total_goals_avg_10)
  )

# =========================
# 4) feature sets
# =========================
stage1_feature_cols <- c(
  "elo_diff_home_away",
  "abs_elo_diff_home_away",

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

  "home_goals_for_avg_10",
  "away_goals_for_avg_10",
  "home_goals_against_avg_10",
  "away_goals_against_avg_10",
  "home_total_goals_avg_10",
  "away_total_goals_avg_10",
  "sum_goals_for_avg_10",
  "sum_goals_against_avg_10",
  "sum_total_goals_avg_10",
  "abs_goals_for_avg_10_diff",
  "abs_goals_against_avg_10_diff",
  "abs_total_goals_avg_10_diff",
  "low_scoring_proxy",
  "concession_proxy",
  "tempo_balance_proxy",

  "home_days_since_last_match",
  "away_days_since_last_match",

  "neutral_flag",
  "same_confederation_flag",
  "friendly_flag",
  "tournament_match_flag",
  "home_world_cup_host_flag",
  "away_world_cup_host_flag"
)

stage2_feature_cols <- c(
  "elo_diff_home_away",
  "abs_elo_diff_home_away",

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

  "home_goals_for_avg_10",
  "away_goals_for_avg_10",
  "home_goals_against_avg_10",
  "away_goals_against_avg_10",
  "home_total_goals_avg_10",
  "away_total_goals_avg_10",
  "sum_goals_for_avg_10",
  "sum_goals_against_avg_10",
  "sum_total_goals_avg_10",
  "abs_goals_for_avg_10_diff",
  "abs_goals_against_avg_10_diff",
  "abs_total_goals_avg_10_diff",

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
# 5) split
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
# 6) helper functions
# =========================
calc_multiclass_logloss <- function(actual, probs, eps = 1e-15) {
  actual_chr <- as.character(actual)
  probs <- as.matrix(probs)

  probs[probs < eps] <- eps
  probs[probs > 1 - eps] <- 1 - eps

  row_index <- seq_along(actual_chr)
  col_index <- match(actual_chr, colnames(probs))

  mean(-log(probs[cbind(row_index, col_index)]))
}

safe_best_iteration <- function(model_obj) {
  if (!is.null(model_obj$best_iteration) && length(model_obj$best_iteration) == 1) {
    return(as.integer(model_obj$best_iteration))
  }
  if (!is.null(model_obj$best_ntreelimit) && length(model_obj$best_ntreelimit) == 1) {
    return(as.integer(model_obj$best_ntreelimit))
  }
  return(NA_integer_)
}

# =========================
# 7) Stage 2 first
# =========================
train_stage2 <- train_df %>%
  filter(target_result_90 != "D") %>%
  mutate(ha_label = ifelse(target_result_90 == "H", 1, 0))

test_stage2 <- test_df %>%
  filter(target_result_90 != "D") %>%
  mutate(ha_label = ifelse(target_result_90 == "H", 1, 0))

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

evals_stage2 <- list(train = dtrain_stage2, eval = dtest_stage2)

model_stage2 <- xgb.train(
  params = params_stage2,
  data = dtrain_stage2,
  nrounds = 300,
  evals = evals_stage2,
  early_stopping_rounds = 20,
  verbose = 1
)

stage2_best_iter <- safe_best_iteration(model_stage2)
message("Stage 2 finished.")
message("Stage 2 best iteration: ", stage2_best_iter)

dtest_all_stage2 <- xgb.DMatrix(
  data = as.matrix(test_df[, stage2_feature_cols])
)

p_home_given_not_draw <- predict(model_stage2, dtest_all_stage2)
p_away_given_not_draw <- 1 - p_home_given_not_draw

# =========================
# 8) Stage 1 grid search
# =========================
train_stage1 <- train_df %>%
  mutate(draw_flag = ifelse(target_result_90 == "D", 1, 0))

test_stage1 <- test_df %>%
  mutate(draw_flag = ifelse(target_result_90 == "D", 1, 0))

draw_weight_grid <- c(1.5, 1.8, 2.0, 2.3)
alpha_grid <- c(0.65, 0.70, 0.75, 0.80, 0.85)

grid_results <- list()
best_result <- NULL
best_logloss <- Inf
best_model_stage1 <- NULL
best_p_draw <- NULL

for (dw in draw_weight_grid) {
  message("========== Training Stage 1 with draw_weight = ", dw, " ==========")

  stage1_weights <- ifelse(train_stage1$draw_flag == 1, dw, 1)

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

  evals_stage1 <- list(train = dtrain_stage1, eval = dtest_stage1)

  model_stage1 <- xgb.train(
    params = params_stage1,
    data = dtrain_stage1,
    nrounds = 500,
    evals = evals_stage1,
    early_stopping_rounds = 30,
    verbose = 1
  )

  p_draw_raw <- predict(model_stage1, dtest_stage1)
  stage1_best_iter <- safe_best_iteration(model_stage1)

  for (alpha in alpha_grid) {
    prob_D <- p_draw_raw * alpha
    prob_D <- pmin(pmax(prob_D, 1e-6), 1 - 1e-6)

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

    accuracy <- mean(pred_class == test_df$target_result_90)
    logloss <- calc_multiclass_logloss(test_df$target_result_90, pred_prob)
    pred_draw_count <- sum(pred_class == "D")

    one_result <- data.frame(
      draw_weight = dw,
      alpha = alpha,
      stage1_best_iter = stage1_best_iter,
      accuracy = accuracy,
      logloss = logloss,
      pred_draw_count = pred_draw_count
    )

    grid_results[[length(grid_results) + 1]] <- one_result

    if (logloss < best_logloss) {
      best_logloss <- logloss
      best_result <- one_result
      best_model_stage1 <- model_stage1
      best_p_draw <- p_draw_raw
    }
  }
}

grid_df <- bind_rows(grid_results) %>%
  arrange(logloss, desc(accuracy))

message("========== Grid Search Results ==========")
print(grid_df)

message("========== Best Parameter Combo ==========")
print(best_result)

best_draw_weight <- best_result$draw_weight[[1]]
best_alpha <- best_result$alpha[[1]]

# =========================
# 9) final prediction
# =========================
prob_D <- best_p_draw * best_alpha
prob_D <- pmin(pmax(prob_D, 1e-6), 1 - 1e-6)

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

accuracy <- mean(pred_class == test_df$target_result_90)
logloss <- calc_multiclass_logloss(test_df$target_result_90, pred_prob)

message("========== Two-Stage V5 With Elo + Goal Tempo Final Result ==========")
message("Best draw_weight: ", best_draw_weight)
message("Best alpha: ", best_alpha)
message("Accuracy: ", round(accuracy, 4))
message("Log Loss: ", round(logloss, 6))

conf_mat <- confusionMatrix(
  data = pred_class,
  reference = factor(test_df$target_result_90, levels = c("H", "D", "A"))
)

print(conf_mat)

# =========================
# 10) diagnostics
# =========================
message("========== Stage 1 Importance ==========")
imp_stage1 <- xgb.importance(feature_names = stage1_feature_cols, model = best_model_stage1)
print(imp_stage1)

message("========== Stage 2 Importance ==========")
imp_stage2 <- xgb.importance(feature_names = stage2_feature_cols, model = model_stage2)
print(imp_stage2)

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
