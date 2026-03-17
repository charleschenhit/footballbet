library(DBI)
library(RPostgres)
library(dplyr)
library(tibble)

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

# =========================
# 2) read source tables
# =========================
matches_sql <- sprintf("
  SELECT
    match_id,
    match_date,
    home_team_id,
    away_team_id,
    result_90,
    is_neutral
  FROM %s.matches
", schema_name)

matches_df <- dbGetQuery(con, matches_sql)

team_pre_sql <- sprintf("
  SELECT
    match_id,
    team_id,
    opponent_team_id,
    feature_date,
    is_home,
    is_neutral,

    elo,
    opponent_elo,
    elo_diff,

    fifa_rank,
    opponent_fifa_rank,
    fifa_rank_diff,

    last5_points_avg,
    last10_points_avg,
    last5_goal_diff_avg,
    last10_goal_diff_avg,

    last5_xg_for_avg,
    last5_xg_against_avg,
    last10_xg_for_avg,
    last10_xg_against_avg,

    last5_shots_for_avg,
    last5_shots_against_avg,

    clean_sheet_rate_10,
    scoring_rate_10,

    days_since_last_match,
    rest_days_diff,

    same_confederation_flag,
    host_flag,
    world_cup_host_flag,
    tournament_match_flag,
    friendly_flag,

    label_result_90,
    label_win,
    label_draw,
    label_loss,

    feature_version
  FROM %s.team_pre_match_features
  WHERE feature_version = '%s'
", schema_name, feature_version)

team_pre_df <- dbGetQuery(con, team_pre_sql)

if (nrow(matches_df) == 0) stop("No rows in matches")
if (nrow(team_pre_df) == 0) stop("No rows in team_pre_match_features for this feature_version")

message("matches rows: ", nrow(matches_df))
message("team_pre_match_features rows: ", nrow(team_pre_df))

# =========================
# 3) split home / away feature rows
# =========================
home_df <- team_pre_df %>%
  filter(is_home == TRUE) %>%
  transmute(
    match_id,
    feature_date,

    home_team_id = team_id,
    away_team_id = opponent_team_id,

    home_elo = elo,
    home_fifa_rank = fifa_rank,

    home_last5_points_avg = last5_points_avg,
    home_last10_points_avg = last10_points_avg,
    home_last5_goal_diff_avg = last5_goal_diff_avg,
    home_last10_goal_diff_avg = last10_goal_diff_avg,

    home_last5_xg_for_avg = last5_xg_for_avg,
    home_last5_xg_against_avg = last5_xg_against_avg,

    home_last5_shots_for_avg = last5_shots_for_avg,
    home_last5_shots_against_avg = last5_shots_against_avg,

    home_clean_sheet_rate_10 = clean_sheet_rate_10,
    home_scoring_rate_10 = scoring_rate_10,

    home_days_since_last_match = days_since_last_match,
    home_rest_days_diff = rest_days_diff,

    home_same_confederation_flag = same_confederation_flag,
    home_host_flag = host_flag,
    home_world_cup_host_flag = world_cup_host_flag,
    home_tournament_match_flag = tournament_match_flag,
    home_friendly_flag = friendly_flag,

    feature_version
  ) %>%
  distinct(match_id, .keep_all = TRUE)

away_df <- team_pre_df %>%
  filter(is_home == FALSE) %>%
  transmute(
    match_id,
    feature_date,

    home_team_id = opponent_team_id,
    away_team_id = team_id,

    away_elo = elo,
    away_fifa_rank = fifa_rank,

    away_last5_points_avg = last5_points_avg,
    away_last10_points_avg = last10_points_avg,
    away_last5_goal_diff_avg = last5_goal_diff_avg,
    away_last10_goal_diff_avg = last10_goal_diff_avg,

    away_last5_xg_for_avg = last5_xg_for_avg,
    away_last5_xg_against_avg = last5_xg_against_avg,

    away_last5_shots_for_avg = last5_shots_for_avg,
    away_last5_shots_against_avg = last5_shots_against_avg,

    away_clean_sheet_rate_10 = clean_sheet_rate_10,
    away_scoring_rate_10 = scoring_rate_10,

    away_days_since_last_match = days_since_last_match,
    away_rest_days_diff = rest_days_diff,

    away_same_confederation_flag = same_confederation_flag,
    away_host_flag = host_flag,
    away_world_cup_host_flag = world_cup_host_flag,
    away_tournament_match_flag = tournament_match_flag,
    away_friendly_flag = friendly_flag,

    feature_version
  ) %>%
  distinct(match_id, .keep_all = TRUE)

# =========================
# 4) join home + away into one row per match
# =========================
pair_df <- matches_df %>%
  select(match_id, match_date, home_team_id, away_team_id, result_90, is_neutral) %>%
  left_join(home_df, by = c("match_id", "home_team_id", "away_team_id")) %>%
  left_join(away_df, by = c("match_id", "home_team_id", "away_team_id"), suffix = c("", "_awayjoin"))

# =========================
# 5) build pair-level features
# =========================
pair_features <- pair_df %>%
  transmute(
    match_id = as.integer(match_id),
    feature_date = as.Date(coalesce(feature_date, match_date)),

    home_team_id = as.integer(home_team_id),
    away_team_id = as.integer(away_team_id),

    elo_diff_home_away = ifelse(
      is.na(home_elo) | is.na(away_elo),
      NA_real_,
      as.numeric(home_elo - away_elo)
    ),

    fifa_rank_diff_home_away = ifelse(
      is.na(home_fifa_rank) | is.na(away_fifa_rank),
      NA_integer_,
      as.integer(home_fifa_rank - away_fifa_rank)
    ),

    last5_points_diff = ifelse(
      is.na(home_last5_points_avg) | is.na(away_last5_points_avg),
      NA_real_,
      as.numeric(home_last5_points_avg - away_last5_points_avg)
    ),

    last10_points_diff = ifelse(
      is.na(home_last10_points_avg) | is.na(away_last10_points_avg),
      NA_real_,
      as.numeric(home_last10_points_avg - away_last10_points_avg)
    ),

    last5_goal_diff_diff = ifelse(
      is.na(home_last5_goal_diff_avg) | is.na(away_last5_goal_diff_avg),
      NA_real_,
      as.numeric(home_last5_goal_diff_avg - away_last5_goal_diff_avg)
    ),

    last10_goal_diff_diff = ifelse(
      is.na(home_last10_goal_diff_avg) | is.na(away_last10_goal_diff_avg),
      NA_real_,
      as.numeric(home_last10_goal_diff_avg - away_last10_goal_diff_avg)
    ),

    last5_xg_for_diff = ifelse(
      is.na(home_last5_xg_for_avg) | is.na(away_last5_xg_for_avg),
      NA_real_,
      as.numeric(home_last5_xg_for_avg - away_last5_xg_for_avg)
    ),

    last5_xg_against_diff = ifelse(
      is.na(home_last5_xg_against_avg) | is.na(away_last5_xg_against_avg),
      NA_real_,
      as.numeric(home_last5_xg_against_avg - away_last5_xg_against_avg)
    ),

    rest_days_diff_home_away = ifelse(
      is.na(home_days_since_last_match) | is.na(away_days_since_last_match),
      NA_integer_,
      as.integer(home_days_since_last_match - away_days_since_last_match)
    ),

    host_advantage_flag = isTRUE(home_host_flag) | isTRUE(away_host_flag),
    neutral_flag = isTRUE(is_neutral),

    same_confederation_flag = case_when(
      !is.na(home_same_confederation_flag) ~ home_same_confederation_flag,
      !is.na(away_same_confederation_flag) ~ away_same_confederation_flag,
      TRUE ~ NA
    ),

    target_result_90 = result_90,
    target_home_win = result_90 == "H",
    target_draw = result_90 == "D",
    target_away_win = result_90 == "A",

    feature_version = feature_version
  ) %>%
  distinct(match_id, feature_version, .keep_all = TRUE)

message("Pair feature rows built: ", nrow(pair_features))

# =========================
# 6) remove existing rows of same feature_version
# =========================
existing_sql <- sprintf("
  SELECT match_id, feature_version
  FROM %s.team_pair_pre_match_features
  WHERE feature_version = '%s'
", schema_name, feature_version)

existing_df <- dbGetQuery(con, existing_sql)

pair_features_to_insert <- pair_features %>%
  anti_join(existing_df, by = c("match_id", "feature_version"))

message("Rows ready to insert: ", nrow(pair_features_to_insert))

# =========================
# 7) write to PostgreSQL
# =========================
if (nrow(pair_features_to_insert) > 0) {
  dbWriteTable(
    con,
    Id(schema = schema_name, table = "team_pair_pre_match_features"),
    value = pair_features_to_insert,
    append = TRUE,
    row.names = FALSE
  )
  message("Inserted rows: ", nrow(pair_features_to_insert))
} else {
  message("No new rows to insert.")
}

# =========================
# 8) validation
# =========================
check_sql <- sprintf("
  SELECT COUNT(*) AS cnt
  FROM %s.team_pair_pre_match_features
", schema_name)

check_df <- dbGetQuery(con, check_sql)
print(check_df)

sample_sql <- sprintf("
  SELECT
    id,
    match_id,
    feature_date,
    home_team_id,
    away_team_id,
    elo_diff_home_away,
    fifa_rank_diff_home_away,
    last5_points_diff,
    last10_points_diff,
    last5_goal_diff_diff,
    last10_goal_diff_diff,
    rest_days_diff_home_away,
    host_advantage_flag,
    neutral_flag,
    same_confederation_flag,
    target_result_90,
    feature_version
  FROM %s.team_pair_pre_match_features
  ORDER BY id DESC
  LIMIT 20
", schema_name)

sample_df <- dbGetQuery(con, sample_sql)
print(sample_df)

dbDisconnect(con)
