library(DBI)
library(RPostgres)
library(dplyr)
library(tidyr)
library(purrr)
library(tibble)
library(lubridate)

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
team_feature_version <- "v1_basic"
pair_feature_version <- "v2_enhanced"

# =========================
# 2) load source data
# =========================
matches_sql <- sprintf("
  SELECT
    match_id,
    match_date
  FROM %s.matches
", schema_name)

matches_df <- dbGetQuery(con, matches_sql) %>%
  mutate(match_date = as.Date(match_date))

team_stats_sql <- sprintf("
  SELECT
    id,
    match_id,
    team_id,
    opponent_team_id,
    is_home,
    goals_for,
    goals_against
  FROM %s.team_match_stats_raw
", schema_name)

team_stats_df <- dbGetQuery(con, team_stats_sql)

team_pre_sql <- sprintf("
  SELECT
    id,
    match_id,
    team_id,
    opponent_team_id,
    feature_date,
    is_home,
    feature_version
  FROM %s.team_pre_match_features
  WHERE feature_version = '%s'
", schema_name, team_feature_version)

team_pre_df <- dbGetQuery(con, team_pre_sql) %>%
  mutate(feature_date = as.Date(feature_date))

pair_sql <- sprintf("
  SELECT
    id,
    match_id,
    feature_version
  FROM %s.team_pair_pre_match_features
  WHERE feature_version = '%s'
", schema_name, pair_feature_version)

pair_df <- dbGetQuery(con, pair_sql)

if (nrow(matches_df) == 0) stop("No rows in matches")
if (nrow(team_stats_df) == 0) stop("No rows in team_match_stats_raw")
if (nrow(team_pre_df) == 0) stop("No rows in team_pre_match_features for target version")
if (nrow(pair_df) == 0) stop("No rows in team_pair_pre_match_features for target version")

message("matches rows: ", nrow(matches_df))
message("team_match_stats_raw rows: ", nrow(team_stats_df))
message("team_pre rows: ", nrow(team_pre_df))
message("pair rows: ", nrow(pair_df))

# =========================
# 3) historical base
# =========================
hist_df <- team_stats_df %>%
  left_join(matches_df, by = "match_id") %>%
  mutate(
    match_date = as.Date(match_date),
    total_goals = goals_for + goals_against
  ) %>%
  arrange(team_id, match_date, match_id)

# =========================
# 4) helper to compute rolling goal tempo features
# =========================
calc_goal_tempo_features <- function(team_id_value, feature_date_value) {
  prev_matches <- hist_df %>%
    filter(
      team_id == team_id_value,
      match_date < feature_date_value
    ) %>%
    arrange(desc(match_date), desc(match_id))

  last5 <- prev_matches %>% slice_head(n = 5)
  last10 <- prev_matches %>% slice_head(n = 10)

  tibble(
    last5_goals_for_avg = if (nrow(last5) > 0) mean(last5$goals_for, na.rm = TRUE) else NA_real_,
    last5_goals_against_avg = if (nrow(last5) > 0) mean(last5$goals_against, na.rm = TRUE) else NA_real_,
    last10_goals_for_avg = if (nrow(last10) > 0) mean(last10$goals_for, na.rm = TRUE) else NA_real_,
    last10_goals_against_avg = if (nrow(last10) > 0) mean(last10$goals_against, na.rm = TRUE) else NA_real_,
    last5_total_goals_avg = if (nrow(last5) > 0) mean(last5$total_goals, na.rm = TRUE) else NA_real_,
    last10_total_goals_avg = if (nrow(last10) > 0) mean(last10$total_goals, na.rm = TRUE) else NA_real_
  )
}

# =========================
# 5) build team_pre updates
# =========================
message("Computing rolling goal tempo features for team_pre_match_features...")

team_pre_goal_updates <- pmap_dfr(
  list(
    team_pre_df$id,
    team_pre_df$match_id,
    team_pre_df$team_id,
    team_pre_df$feature_date
  ),
  function(id, match_id, team_id, feature_date) {
    feat <- calc_goal_tempo_features(team_id, feature_date)

    tibble(
      id = as.integer(id),
      match_id = as.integer(match_id),
      team_id = as.integer(team_id),
      last5_goals_for_avg = feat$last5_goals_for_avg,
      last5_goals_against_avg = feat$last5_goals_against_avg,
      last10_goals_for_avg = feat$last10_goals_for_avg,
      last10_goals_against_avg = feat$last10_goals_against_avg,
      last5_total_goals_avg = feat$last5_total_goals_avg,
      last10_total_goals_avg = feat$last10_total_goals_avg
    )
  }
)

message("team_pre updates rows: ", nrow(team_pre_goal_updates))

# =========================
# 6) update team_pre_match_features
# =========================
for (i in seq_len(nrow(team_pre_goal_updates))) {
  row <- team_pre_goal_updates[i, ]

  dbExecute(
    con,
    sprintf("
      UPDATE %s.team_pre_match_features
      SET
        last5_goals_for_avg = $1,
        last5_goals_against_avg = $2,
        last10_goals_for_avg = $3,
        last10_goals_against_avg = $4,
        last5_total_goals_avg = $5,
        last10_total_goals_avg = $6
      WHERE id = $7
    ", schema_name),
    params = list(
      row$last5_goals_for_avg,
      row$last5_goals_against_avg,
      row$last10_goals_for_avg,
      row$last10_goals_against_avg,
      row$last5_total_goals_avg,
      row$last10_total_goals_avg,
      row$id
    )
  )
}

message("Updated team_pre_match_features goal tempo fields.")

# =========================
# 7) rebuild pair-level goal tempo features
# =========================
team_pre_goal_sql <- sprintf("
  SELECT
    match_id,
    team_id,
    opponent_team_id,
    is_home,
    last10_goals_for_avg,
    last10_goals_against_avg,
    last10_total_goals_avg
  FROM %s.team_pre_match_features
  WHERE feature_version = '%s'
", schema_name, team_feature_version)

team_pre_goal_df <- dbGetQuery(con, team_pre_goal_sql)

home_goal_df <- team_pre_goal_df %>%
  filter(is_home == TRUE) %>%
  transmute(
    match_id,
    home_team_id = team_id,
    away_team_id = opponent_team_id,
    home_goals_for_avg_10 = last10_goals_for_avg,
    home_goals_against_avg_10 = last10_goals_against_avg,
    home_total_goals_avg_10 = last10_total_goals_avg
  ) %>%
  distinct(match_id, .keep_all = TRUE)

away_goal_df <- team_pre_goal_df %>%
  filter(is_home == FALSE) %>%
  transmute(
    match_id,
    home_team_id = opponent_team_id,
    away_team_id = team_id,
    away_goals_for_avg_10 = last10_goals_for_avg,
    away_goals_against_avg_10 = last10_goals_against_avg,
    away_total_goals_avg_10 = last10_total_goals_avg
  ) %>%
  distinct(match_id, .keep_all = TRUE)

matches_pair_sql <- sprintf("
  SELECT
    match_id,
    home_team_id,
    away_team_id
  FROM %s.matches
", schema_name)

matches_pair_df <- dbGetQuery(con, matches_pair_sql)

pair_goal_updates <- pair_df %>%
  left_join(matches_pair_df, by = "match_id") %>%
  left_join(home_goal_df, by = c("match_id", "home_team_id", "away_team_id")) %>%
  left_join(away_goal_df, by = c("match_id", "home_team_id", "away_team_id")) %>%
  mutate(
    sum_goals_for_avg_10 = home_goals_for_avg_10 + away_goals_for_avg_10,
    sum_goals_against_avg_10 = home_goals_against_avg_10 + away_goals_against_avg_10,
    sum_total_goals_avg_10 = home_total_goals_avg_10 + away_total_goals_avg_10,

    abs_goals_for_avg_10_diff = abs(home_goals_for_avg_10 - away_goals_for_avg_10),
    abs_goals_against_avg_10_diff = abs(home_goals_against_avg_10 - away_goals_against_avg_10),
    abs_total_goals_avg_10_diff = abs(home_total_goals_avg_10 - away_total_goals_avg_10)
  ) %>%
  select(
    id,
    home_goals_for_avg_10,
    away_goals_for_avg_10,
    home_goals_against_avg_10,
    away_goals_against_avg_10,
    home_total_goals_avg_10,
    away_total_goals_avg_10,
    sum_goals_for_avg_10,
    sum_goals_against_avg_10,
    sum_total_goals_avg_10,
    abs_goals_for_avg_10_diff,
    abs_goals_against_avg_10_diff,
    abs_total_goals_avg_10_diff
  )

message("pair updates rows: ", nrow(pair_goal_updates))

# =========================
# 8) update team_pair_pre_match_features
# =========================
for (i in seq_len(nrow(pair_goal_updates))) {
  row <- pair_goal_updates[i, ]

  dbExecute(
    con,
    sprintf("
      UPDATE %s.team_pair_pre_match_features
      SET
        home_goals_for_avg_10 = $1,
        away_goals_for_avg_10 = $2,
        home_goals_against_avg_10 = $3,
        away_goals_against_avg_10 = $4,
        home_total_goals_avg_10 = $5,
        away_total_goals_avg_10 = $6,
        sum_goals_for_avg_10 = $7,
        sum_goals_against_avg_10 = $8,
        sum_total_goals_avg_10 = $9,
        abs_goals_for_avg_10_diff = $10,
        abs_goals_against_avg_10_diff = $11,
        abs_total_goals_avg_10_diff = $12
      WHERE id = $13
    ", schema_name),
    params = list(
      row$home_goals_for_avg_10,
      row$away_goals_for_avg_10,
      row$home_goals_against_avg_10,
      row$away_goals_against_avg_10,
      row$home_total_goals_avg_10,
      row$away_total_goals_avg_10,
      row$sum_goals_for_avg_10,
      row$sum_goals_against_avg_10,
      row$sum_total_goals_avg_10,
      row$abs_goals_for_avg_10_diff,
      row$abs_goals_against_avg_10_diff,
      row$abs_total_goals_avg_10_diff,
      row$id
    )
  )
}

message("Updated team_pair_pre_match_features goal tempo fields.")

# =========================
# 9) validation
# =========================
check_team_pre <- dbGetQuery(con, sprintf("
  SELECT
    COUNT(*) AS total_rows,
    COUNT(last10_goals_for_avg) AS c1,
    COUNT(last10_goals_against_avg) AS c2,
    COUNT(last10_total_goals_avg) AS c3
  FROM %s.team_pre_match_features
  WHERE feature_version = '%s'
", schema_name, team_feature_version))
print(check_team_pre)

check_pair <- dbGetQuery(con, sprintf("
  SELECT
    COUNT(*) AS total_rows,
    COUNT(home_goals_for_avg_10) AS c1,
    COUNT(away_goals_for_avg_10) AS c2,
    COUNT(sum_total_goals_avg_10) AS c3,
    COUNT(abs_total_goals_avg_10_diff) AS c4
  FROM %s.team_pair_pre_match_features
  WHERE feature_version = '%s'
", schema_name, pair_feature_version))
print(check_pair)

sample_pair <- dbGetQuery(con, sprintf("
  SELECT
    match_id,
    home_goals_for_avg_10,
    away_goals_for_avg_10,
    home_goals_against_avg_10,
    away_goals_against_avg_10,
    sum_total_goals_avg_10,
    abs_total_goals_avg_10_diff
  FROM %s.team_pair_pre_match_features
  WHERE feature_version = '%s'
  LIMIT 20
", schema_name, pair_feature_version))
print(sample_pair)

dbDisconnect(con)
