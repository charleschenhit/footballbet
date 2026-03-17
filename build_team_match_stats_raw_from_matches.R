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

# =========================
# 2) read matches
# =========================
matches_sql <- sprintf("
  SELECT
    match_id,
    match_date,
    home_team_id,
    away_team_id,
    home_score,
    away_score,
    data_source
  FROM %s.matches
  WHERE home_team_id IS NOT NULL
    AND away_team_id IS NOT NULL
", schema_name)

matches_df <- dbGetQuery(con, matches_sql)

if (nrow(matches_df) == 0) {
  stop(sprintf("No rows found in %s.matches", schema_name))
}

message("Matches loaded: ", nrow(matches_df))

# =========================
# 3) build two-row team perspective
# =========================
home_rows <- matches_df %>%
  transmute(
    match_id = as.integer(match_id),
    team_id = as.integer(home_team_id),
    opponent_team_id = as.integer(away_team_id),
    is_home = TRUE,
    goals_for = as.integer(home_score),
    goals_against = as.integer(away_score),
    source_name = coalesce(data_source, "matches_derived")
  )

away_rows <- matches_df %>%
  transmute(
    match_id = as.integer(match_id),
    team_id = as.integer(away_team_id),
    opponent_team_id = as.integer(home_team_id),
    is_home = FALSE,
    goals_for = as.integer(away_score),
    goals_against = as.integer(home_score),
    source_name = coalesce(data_source, "matches_derived")
  )

team_stats_new <- bind_rows(home_rows, away_rows) %>%
  mutate(
    source_name = paste0(source_name, "_derived")
  ) %>%
  distinct(match_id, team_id, .keep_all = TRUE)

message("Derived team rows: ", nrow(team_stats_new))

# =========================
# 4) load existing rows to avoid duplicates
# =========================
existing_sql <- sprintf("
  SELECT match_id, team_id
  FROM %s.team_match_stats_raw
", schema_name)

existing_df <- dbGetQuery(con, existing_sql)

message("Existing team_match_stats_raw rows: ", nrow(existing_df))

team_stats_to_insert <- team_stats_new %>%
  anti_join(existing_df, by = c("match_id", "team_id"))

message("Rows ready to insert: ", nrow(team_stats_to_insert))

# =========================
# 5) align with table schema
#    simplified version only
# =========================
team_stats_to_insert <- team_stats_to_insert %>%
  transmute(
    match_id = match_id,
    team_id = team_id,
    opponent_team_id = opponent_team_id,
    is_home = is_home,

    goals_for = goals_for,
    goals_against = goals_against,

    xg_for = NA_real_,
    xg_against = NA_real_,

    shots_for = NA_integer_,
    shots_against = NA_integer_,
    shots_on_target_for = NA_integer_,
    shots_on_target_against = NA_integer_,

    possession_pct = NA_real_,
    passes_completed = NA_integer_,
    passes_attempted = NA_integer_,

    corners_for = NA_integer_,
    corners_against = NA_integer_,
    fouls_for = NA_integer_,
    fouls_against = NA_integer_,
    yellow_for = NA_integer_,
    yellow_against = NA_integer_,
    red_for = NA_integer_,
    red_against = NA_integer_,

    set_piece_xg_for = NA_real_,
    set_piece_xg_against = NA_real_,
    ppda_for = NA_real_,
    ppda_against = NA_real_,

    source_name = source_name
  )

# =========================
# 6) write to PostgreSQL
# =========================
if (nrow(team_stats_to_insert) > 0) {
  dbWriteTable(
    con,
    Id(schema = schema_name, table = "team_match_stats_raw"),
    value = team_stats_to_insert,
    append = TRUE,
    row.names = FALSE
  )
  message("Inserted rows: ", nrow(team_stats_to_insert))
} else {
  message("No new rows to insert.")
}

# =========================
# 7) validation
# =========================
check_sql <- sprintf("
  SELECT COUNT(*) AS cnt
  FROM %s.team_match_stats_raw
", schema_name)

check_df <- dbGetQuery(con, check_sql)
print(check_df)

sample_sql <- sprintf("
  SELECT
    id,
    match_id,
    team_id,
    opponent_team_id,
    is_home,
    goals_for,
    goals_against,
    source_name
  FROM %s.team_match_stats_raw
  ORDER BY id DESC
  LIMIT 10
", schema_name)

sample_df <- dbGetQuery(con, sample_sql)
print(sample_df)

dbDisconnect(con)
