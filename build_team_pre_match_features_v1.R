library(DBI)
library(RPostgres)
library(dplyr)
library(tidyr)
library(purrr)
library(lubridate)
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
    competition_name,
    competition_type,
    season_year,
    stage_name,
    home_team_id,
    away_team_id,
    is_neutral,
    host_team_id,
    result_90
  FROM %s.matches
  ORDER BY match_date, match_id
", schema_name)

matches_df <- dbGetQuery(con, matches_sql)

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

teams_sql <- sprintf("
  SELECT
    team_id,
    team_name,
    confederation
  FROM %s.teams
", schema_name)

teams_df <- dbGetQuery(con, teams_sql)

if (nrow(matches_df) == 0) stop("No data in matches")
if (nrow(team_stats_df) == 0) stop("No data in team_match_stats_raw")
if (nrow(teams_df) == 0) stop("No data in teams")

message("matches rows: ", nrow(matches_df))
message("team_match_stats_raw rows: ", nrow(team_stats_df))
message("teams rows: ", nrow(teams_df))

# =========================
# 3) build historical match base
# =========================
hist_df <- team_stats_df %>%
  left_join(
    matches_df %>%
      select(
        match_id, match_date, competition_type,
        is_neutral, host_team_id, result_90
      ),
    by = "match_id"
  ) %>%
  mutate(
    match_date = as.Date(match_date),
    points = case_when(
      goals_for > goals_against ~ 3,
      goals_for == goals_against ~ 1,
      goals_for < goals_against ~ 0,
      TRUE ~ NA_real_
    ),
    goal_diff = goals_for - goals_against,
    clean_sheet = case_when(
      is.na(goals_against) ~ NA_real_,
      goals_against == 0 ~ 1,
      TRUE ~ 0
    ),
    scored_flag = case_when(
      is.na(goals_for) ~ NA_real_,
      goals_for > 0 ~ 1,
      TRUE ~ 0
    )
  ) %>%
  arrange(team_id, match_date, match_id)

# =========================
# 4) build target rows: one row per team per match
# =========================
match_team_rows <- matches_df %>%
  transmute(
    match_id,
    match_date = as.Date(match_date),
    competition_type,
    is_neutral,
    host_team_id,
    home_team_id,
    away_team_id,
    result_90
  ) %>%
  bind_rows(
    matches_df %>%
      transmute(
        match_id,
        match_date = as.Date(match_date),
        competition_type,
        is_neutral,
        host_team_id,
        team_id = home_team_id,
        opponent_team_id = away_team_id,
        is_home = TRUE,
        result_90
      ),
    matches_df %>%
      transmute(
        match_id,
        match_date = as.Date(match_date),
        competition_type,
        is_neutral,
        host_team_id,
        team_id = away_team_id,
        opponent_team_id = home_team_id,
        is_home = FALSE,
        result_90
      )
  ) %>%
  filter(!is.na(team_id), !is.na(opponent_team_id)) %>%
  distinct(match_id, team_id, .keep_all = TRUE)

# =========================
# 5) helper: compute rolling features for one team before a given match date
# =========================
calc_team_features <- function(team_id_value, match_date_value) {
  prev_matches <- hist_df %>%
    filter(
      team_id == team_id_value,
      match_date < match_date_value
    ) %>%
    arrange(desc(match_date), desc(match_id))

  last5 <- prev_matches %>% slice_head(n = 5)
  last10 <- prev_matches %>% slice_head(n = 10)

  days_since_last_match <- if (nrow(prev_matches) == 0) {
    NA_integer_
  } else {
    as.integer(match_date_value - max(prev_matches$match_date, na.rm = TRUE))
  }

  tibble(
    last5_points_avg = if (nrow(last5) > 0) mean(last5$points, na.rm = TRUE) else NA_real_,
    last10_points_avg = if (nrow(last10) > 0) mean(last10$points, na.rm = TRUE) else NA_real_,
    last5_goal_diff_avg = if (nrow(last5) > 0) mean(last5$goal_diff, na.rm = TRUE) else NA_real_,
    last10_goal_diff_avg = if (nrow(last10) > 0) mean(last10$goal_diff, na.rm = TRUE) else NA_real_,
    clean_sheet_rate_10 = if (nrow(last10) > 0) mean(last10$clean_sheet, na.rm = TRUE) else NA_real_,
    scoring_rate_10 = if (nrow(last10) > 0) mean(last10$scored_flag, na.rm = TRUE) else NA_real_,
    days_since_last_match = days_since_last_match
  )
}

# =========================
# 6) compute feature rows
# =========================
message("Computing team_pre_match_features... this may take some time.")

feature_rows <- pmap_dfr(
  list(
    match_team_rows$match_id,
    match_team_rows$match_date,
    match_team_rows$competition_type,
    match_team_rows$is_neutral,
    match_team_rows$host_team_id,
    match_team_rows$team_id,
    match_team_rows$opponent_team_id,
    match_team_rows$is_home,
    match_team_rows$result_90
  ),
  function(match_id, match_date, competition_type, is_neutral, host_team_id,
           team_id, opponent_team_id, is_home, result_90) {

    team_feat <- calc_team_features(team_id, match_date)
    opp_feat  <- calc_team_features(opponent_team_id, match_date)

    team_conf <- teams_df %>%
      filter(team_id == !!team_id) %>%
      slice(1)

    opp_conf <- teams_df %>%
      filter(team_id == !!opponent_team_id) %>%
      slice(1)

    same_confederation_flag <- if (nrow(team_conf) == 0 || nrow(opp_conf) == 0) {
      NA
    } else {
      !is.na(team_conf$confederation[1]) &&
        !is.na(opp_conf$confederation[1]) &&
        team_conf$confederation[1] == opp_conf$confederation[1]
    }

    host_flag <- if (!is.na(host_team_id)) team_id == host_team_id else FALSE
    world_cup_host_flag <- host_flag && !is.na(competition_type) && competition_type == "world_cup"
    tournament_match_flag <- !is.na(competition_type) && competition_type != "friendly"
    friendly_flag <- !is.na(competition_type) && competition_type == "friendly"

    label_result_90 <- case_when(
      result_90 == "D" ~ "D",
      result_90 == "H" & isTRUE(is_home) ~ "W",
      result_90 == "A" & isFALSE(is_home) ~ "W",
      result_90 == "H" & isFALSE(is_home) ~ "L",
      result_90 == "A" & isTRUE(is_home) ~ "L",
      TRUE ~ NA_character_
    )

    tibble(
      match_id = as.integer(match_id),
      team_id = as.integer(team_id),
      opponent_team_id = as.integer(opponent_team_id),
      feature_date = as.Date(match_date),
      is_home = isTRUE(is_home),
      is_neutral = isTRUE(is_neutral),

      elo = NA_real_,
      opponent_elo = NA_real_,
      elo_diff = NA_real_,

      fifa_rank = NA_integer_,
      opponent_fifa_rank = NA_integer_,
      fifa_rank_diff = NA_integer_,

      last5_points_avg = team_feat$last5_points_avg,
      last10_points_avg = team_feat$last10_points_avg,
      last5_goal_diff_avg = team_feat$last5_goal_diff_avg,
      last10_goal_diff_avg = team_feat$last10_goal_diff_avg,

      last5_xg_for_avg = NA_real_,
      last5_xg_against_avg = NA_real_,
      last10_xg_for_avg = NA_real_,
      last10_xg_against_avg = NA_real_,

      last5_shots_for_avg = NA_real_,
      last5_shots_against_avg = NA_real_,

      clean_sheet_rate_10 = team_feat$clean_sheet_rate_10,
      scoring_rate_10 = team_feat$scoring_rate_10,

      days_since_last_match = team_feat$days_since_last_match,
      rest_days_diff = ifelse(
        is.na(team_feat$days_since_last_match) | is.na(opp_feat$days_since_last_match),
        NA_integer_,
        as.integer(team_feat$days_since_last_match - opp_feat$days_since_last_match)
      ),

      same_confederation_flag = same_confederation_flag,
      host_flag = host_flag,
      world_cup_host_flag = world_cup_host_flag,
      tournament_match_flag = tournament_match_flag,
      friendly_flag = friendly_flag,

      label_result_90 = label_result_90,
      label_win = label_result_90 == "W",
      label_draw = label_result_90 == "D",
      label_loss = label_result_90 == "L",

      feature_version = feature_version
    )
  }
)

message("Feature rows built: ", nrow(feature_rows))

# =========================
# 7) remove existing rows of same feature_version
# =========================
existing_sql <- sprintf("
  SELECT match_id, team_id, feature_version
  FROM %s.team_pre_match_features
  WHERE feature_version = '%s'
", schema_name, feature_version)

existing_df <- dbGetQuery(con, existing_sql)

feature_rows_to_insert <- feature_rows %>%
  anti_join(existing_df, by = c("match_id", "team_id", "feature_version"))

message("Rows ready to insert: ", nrow(feature_rows_to_insert))

# =========================
# 8) write to PostgreSQL
# =========================
if (nrow(feature_rows_to_insert) > 0) {
  dbWriteTable(
    con,
    Id(schema = schema_name, table = "team_pre_match_features"),
    value = feature_rows_to_insert,
    append = TRUE,
    row.names = FALSE
  )
  message("Inserted rows: ", nrow(feature_rows_to_insert))
} else {
  message("No new rows to insert.")
}

# =========================
# 9) validation
# =========================
check_sql <- sprintf("
  SELECT COUNT(*) AS cnt
  FROM %s.team_pre_match_features
", schema_name)

check_df <- dbGetQuery(con, check_sql)
print(check_df)

sample_sql <- sprintf("
  SELECT
    id,
    match_id,
    team_id,
    opponent_team_id,
    feature_date,
    is_home,
    last5_points_avg,
    last10_points_avg,
    last5_goal_diff_avg,
    last10_goal_diff_avg,
    clean_sheet_rate_10,
    scoring_rate_10,
    days_since_last_match,
    rest_days_diff,
    label_result_90,
    feature_version
  FROM %s.team_pre_match_features
  ORDER BY id DESC
  LIMIT 20
", schema_name)

sample_df <- dbGetQuery(con, sample_sql)
print(sample_df)

dbDisconnect(con)
