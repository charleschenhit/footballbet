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
team_feature_version <- "v1_basic"
pair_feature_version <- "v2_enhanced"

# =========================
# 2) load matches + teams
# =========================
matches_sql <- sprintf("
  SELECT
    match_id,
    match_date,
    competition_type,
    home_team_id,
    away_team_id,
    home_score,
    away_score,
    is_neutral
  FROM %s.matches
  WHERE home_team_id IS NOT NULL
    AND away_team_id IS NOT NULL
    AND home_score IS NOT NULL
    AND away_score IS NOT NULL
  ORDER BY match_date, match_id
", schema_name)

matches_df <- dbGetQuery(con, matches_sql)

teams_sql <- sprintf("
  SELECT team_id
  FROM %s.teams
", schema_name)

teams_df <- dbGetQuery(con, teams_sql)

if (nrow(matches_df) == 0) stop("No matches found.")
if (nrow(teams_df) == 0) stop("No teams found.")

message("matches rows: ", nrow(matches_df))
message("teams rows: ", nrow(teams_df))

# =========================
# 3) helper functions
# =========================
get_k_value <- function(comp_type) {
  if (is.na(comp_type)) return(30)

  switch(
    comp_type,
    "world_cup" = 60,
    "continental_cup" = 50,
    "qualifier" = 40,
    "friendly" = 20,
    30
  )
}

get_goal_diff_multiplier <- function(home_score, away_score) {
  gd <- abs(home_score - away_score)

  if (gd <= 1) return(1.0)
  if (gd == 2) return(1.5)
  return(1.75)
}

expected_score <- function(rating_a, rating_b) {
  1 / (1 + 10 ^ (-(rating_a - rating_b) / 400))
}

actual_score_home <- function(home_score, away_score) {
  if (home_score > away_score) return(1.0)
  if (home_score == away_score) return(0.5)
  return(0.0)
}

# =========================
# 4) initialize Elo ratings
# =========================
elo_map <- setNames(
  rep(1500, nrow(teams_df)),
  teams_df$team_id
)

elo_history <- vector("list", nrow(matches_df))

# =========================
# 5) rolling Elo computation
# =========================
for (i in seq_len(nrow(matches_df))) {
  row <- matches_df[i, ]

  home_id <- as.character(row$home_team_id)
  away_id <- as.character(row$away_team_id)

  home_pre <- elo_map[[home_id]]
  away_pre <- elo_map[[away_id]]

  if (is.null(home_pre)) home_pre <- 1500
  if (is.null(away_pre)) away_pre <- 1500

  home_advantage <- ifelse(isTRUE(row$is_neutral), 0, 100)

  home_adj <- home_pre + home_advantage
  away_adj <- away_pre

  exp_home <- expected_score(home_adj, away_adj)
  exp_away <- 1 - exp_home

  act_home <- actual_score_home(row$home_score, row$away_score)
  act_away <- 1 - act_home

  k <- get_k_value(row$competition_type)
  gmult <- get_goal_diff_multiplier(row$home_score, row$away_score)

  home_post <- home_pre + k * gmult * (act_home - exp_home)
  away_post <- away_pre + k * gmult * (act_away - exp_away)

  elo_history[[i]] <- tibble(
    match_id = as.integer(row$match_id),
    home_team_id = as.integer(row$home_team_id),
    away_team_id = as.integer(row$away_team_id),
    home_pre_elo = round(home_pre, 2),
    away_pre_elo = round(away_pre, 2),
    home_post_elo = round(home_post, 2),
    away_post_elo = round(away_post, 2)
  )

  elo_map[[home_id]] <- home_post
  elo_map[[away_id]] <- away_post
}

elo_match_df <- bind_rows(elo_history)

message("elo_match_df rows: ", nrow(elo_match_df))

# =========================
# 6) update team_pre_match_features
# =========================
team_pre_sql <- sprintf("
  SELECT
    id,
    match_id,
    team_id,
    opponent_team_id,
    is_home
  FROM %s.team_pre_match_features
  WHERE feature_version = '%s'
", schema_name, team_feature_version)

team_pre_df <- dbGetQuery(con, team_pre_sql)

if (nrow(team_pre_df) == 0) {
  stop(sprintf("No rows in %s.team_pre_match_features for feature_version = '%s'",
               schema_name, team_feature_version))
}

team_pre_updates <- team_pre_df %>%
  left_join(
    elo_match_df,
    by = "match_id"
  ) %>%
  mutate(
    elo = ifelse(is_home, home_pre_elo, away_pre_elo),
    opponent_elo = ifelse(is_home, away_pre_elo, home_pre_elo),
    elo_diff = elo - opponent_elo
  ) %>%
  select(id, elo, opponent_elo, elo_diff)

# 逐行更新
for (i in seq_len(nrow(team_pre_updates))) {
  row <- team_pre_updates[i, ]

  dbExecute(
    con,
    sprintf("
      UPDATE %s.team_pre_match_features
      SET
        elo = $1,
        opponent_elo = $2,
        elo_diff = $3
      WHERE id = $4
    ", schema_name),
    params = list(row$elo, row$opponent_elo, row$elo_diff, row$id)
  )
}

message("Updated team_pre_match_features Elo fields.")

# =========================
# 7) update team_pair_pre_match_features
# =========================
pair_sql <- sprintf("
  SELECT
    id,
    match_id
  FROM %s.team_pair_pre_match_features
  WHERE feature_version = '%s'
", schema_name, pair_feature_version)

pair_df <- dbGetQuery(con, pair_sql)

if (nrow(pair_df) == 0) {
  stop(sprintf("No rows in %s.team_pair_pre_match_features for feature_version = '%s'",
               schema_name, pair_feature_version))
}

pair_updates <- pair_df %>%
  left_join(
    elo_match_df %>%
      transmute(
        match_id,
        elo_diff_home_away = home_pre_elo - away_pre_elo
      ),
    by = "match_id"
  )

for (i in seq_len(nrow(pair_updates))) {
  row <- pair_updates[i, ]

  dbExecute(
    con,
    sprintf("
      UPDATE %s.team_pair_pre_match_features
      SET elo_diff_home_away = $1
      WHERE id = $2
    ", schema_name),
    params = list(row$elo_diff_home_away, row$id)
  )
}

message("Updated team_pair_pre_match_features Elo fields.")

# =========================
# 8) validation
# =========================
check_team_pre <- dbGetQuery(con, sprintf("
  SELECT
    COUNT(*) AS total_rows,
    COUNT(elo) AS non_null_elo,
    COUNT(opponent_elo) AS non_null_opponent_elo,
    COUNT(elo_diff) AS non_null_elo_diff
  FROM %s.team_pre_match_features
  WHERE feature_version = '%s'
", schema_name, team_feature_version))

print(check_team_pre)

check_pair <- dbGetQuery(con, sprintf("
  SELECT
    COUNT(*) AS total_rows,
    COUNT(elo_diff_home_away) AS non_null_elo_diff_home_away
  FROM %s.team_pair_pre_match_features
  WHERE feature_version = '%s'
", schema_name, pair_feature_version))

print(check_pair)

sample_pair <- dbGetQuery(con, sprintf("
  SELECT
    match_id,
    elo_diff_home_away,
    feature_version
  FROM %s.team_pair_pre_match_features
  WHERE feature_version = '%s'
  LIMIT 20
", schema_name, pair_feature_version))

print(sample_pair)

dbDisconnect(con)
