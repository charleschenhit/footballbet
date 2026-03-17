library(DBI)
library(RPostgres)
library(dplyr)
library(readr)
library(stringr)
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
x <- dbGetQuery(con, "SELECT current_database();")
print(x)
y <- dbGetQuery(con, "
  SELECT table_schema, table_name
  FROM information_schema.tables
  WHERE table_name IN ('teams', 'team_name_mapping', 'matches')
  ORDER BY table_schema, table_name;
")
print(y)

# =========================
# 2) helper functions
# =========================
normalize_team_name <- function(x) {
  x %>%
    str_replace_all("\\s+", " ") %>%
    str_trim()
}

calc_result_90 <- function(home_score, away_score) {
  case_when(
    is.na(home_score) | is.na(away_score) ~ NA_character_,
    home_score > away_score ~ "H",
    home_score < away_score ~ "A",
    TRUE ~ "D"
  )
}

#map_tournament_type <- function(tournament_name) {
#  x <- tolower(tournament_name %||% "")
#
#  case_when(
#    str_detect(x, "world cup qualification|wcq|qualif") ~ "qualifier",
#    str_detect(x, "world cup") ~ "world_cup",
#    str_detect(x, "friend") ~ "friendly",
#    str_detect(x, "euro|european championship|copa america|gold cup|african cup|asian cup|nations league|confederations cup") ~ "continental_cup",
#    TRUE ~ "other"
#  )
#}
#
#infer_stage_name <- function(tournament_name) {
#  x <- tolower(tournament_name %||% "")
#
#  case_when(
#    str_detect(x, "group") ~ "group",
#    str_detect(x, "round of 16|last 16") ~ "round_of_16",
#    str_detect(x, "quarter") ~ "quarterfinal",
#    str_detect(x, "semi") ~ "semifinal",
#    str_detect(x, "final") ~ "final",
#    TRUE ~ NA_character_
#  )
#}
#
#`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0 && !is.na(a)) a else b

map_tournament_type <- function(tournament_name) {
  x <- tolower(as.character(tournament_name))
  x[is.na(x)] <- ""

  case_when(
    str_detect(x, "world cup qualification|wcq|qualif") ~ "qualifier",
    str_detect(x, "world cup") ~ "world_cup",
    str_detect(x, "friend") ~ "friendly",
    str_detect(x, "euro|european championship|copa america|gold cup|african cup|asian cup|nations league|confederations cup") ~ "continental_cup",
    TRUE ~ "other"
  )
}

infer_stage_name <- function(tournament_name) {
  x <- tolower(as.character(tournament_name))
  x[is.na(x)] <- ""

  case_when(
    str_detect(x, "group") ~ "group",
    str_detect(x, "round of 16|last 16") ~ "round_of_16",
    str_detect(x, "quarter") ~ "quarterfinal",
    str_detect(x, "semi") ~ "semifinal",
    str_detect(x, "final") ~ "final",
    TRUE ~ NA_character_
  )
}

# =========================
# 3) data source
#    二选一
# =========================

# 方案 A：直接从 URL 读取
#csv_url <- "https://raw.githubusercontent.com/martj42/international_results/master/results.csv"

# 方案 B：如果 URL 不通，就改成本地路径
csv_local <- "/Users/chenchao/Documents/footballbet/results.csv"

use_local_file <- TRUE

if (use_local_file) {
  raw_df <- read_csv(csv_local, show_col_types = FALSE)
} else {
  raw_df <- read_csv(csv_url, show_col_types = FALSE)
}

message("Rows loaded: ", nrow(raw_df))
print(names(raw_df))

# 这个数据集常见字段：
# date, home_team, away_team, home_score, away_score, tournament, city, country, neutral

required_cols <- c("date", "home_team", "away_team", "home_score", "away_score", "tournament", "country", "neutral")
missing_cols <- setdiff(required_cols, names(raw_df))
if (length(missing_cols) > 0) {
  stop(paste("Missing required columns:", paste(missing_cols, collapse = ", ")))
}

# =========================
# 4) standardize rows
# =========================
matches_std <- raw_df %>%
  transmute(
    match_date       = as.Date(date),
    competition_name = as.character(tournament),
    competition_type = map_tournament_type(tournament),
    season_year      = year(as.Date(date)),
    stage_name       = infer_stage_name(tournament),

    home_team_name   = normalize_team_name(as.character(home_team)),
    away_team_name   = normalize_team_name(as.character(away_team)),

    venue_name       = NA_character_,
    venue_city       = if ("city" %in% names(raw_df)) as.character(city) else NA_character_,
    venue_country    = if ("country" %in% names(raw_df)) as.character(country) else NA_character_,
    is_neutral       = as.logical(neutral),

    home_score       = as.integer(home_score),
    away_score       = as.integer(away_score),

    result_90        = calc_result_90(as.integer(home_score), as.integer(away_score)),
    result_ft        = calc_result_90(as.integer(home_score), as.integer(away_score)),

    extra_time_flag  = FALSE,
    penalty_flag     = FALSE,
    home_penalty_score = NA_integer_,
    away_penalty_score = NA_integer_,
    host_team_id     = NA_integer_,
    data_source      = "international_results"
  ) %>%
  filter(!is.na(match_date), !is.na(home_team_name), !is.na(away_team_name))

# =========================
# 5) load teams + mapping
# =========================
teams_df <- dbGetQuery(con, "
  SELECT team_id, team_name
  FROM footballbet.teams
")

mapping_df <- dbGetQuery(con, "
  SELECT source_name, source_team_name, team_id
  FROM footballbet.team_name_mapping
  WHERE source_name = 'international_results'
")

direct_match_df <- teams_df %>%
  transmute(
    source_team_name = team_name,
    team_id = team_id
  ) %>%
  mutate(source_name = "international_results")

all_mapping_df <- bind_rows(mapping_df, direct_match_df) %>%
  distinct(source_name, source_team_name, .keep_all = TRUE)

mapped <- matches_std %>%
  left_join(
    all_mapping_df %>% select(source_team_name, home_team_id = team_id),
    by = c("home_team_name" = "source_team_name")
  ) %>%
  left_join(
    all_mapping_df %>% select(source_team_name, away_team_id = team_id),
    by = c("away_team_name" = "source_team_name")
  )

unmapped_home <- mapped %>%
  filter(is.na(home_team_id)) %>%
  distinct(home_team_name) %>%
  arrange(home_team_name)

unmapped_away <- mapped %>%
  filter(is.na(away_team_id)) %>%
  distinct(away_team_name) %>%
  arrange(away_team_name)

#if (nrow(unmapped_home) > 0 || nrow(unmapped_away) > 0) {
#  message("Unmapped teams found.")
#
#  if (nrow(unmapped_home) > 0) {
#    print(unmapped_home)
#    write_csv(unmapped_home, "unmapped_home_teams_international_results.csv")
#  }
#
#  if (nrow(unmapped_away) > 0) {
#    print(unmapped_away)
#    write_csv(unmapped_away, "unmapped_away_teams_international_results.csv")
#  }
#
#  stop("Please add missing names into team_name_mapping and rerun.")
#}
if (nrow(unmapped_home) > 0 || nrow(unmapped_away) > 0) {
  message("Unmapped teams found. They will be excluded from this MVP import.")

  if (nrow(unmapped_home) > 0) {
    print(unmapped_home, n = 50)
    write_csv(unmapped_home, "unmapped_home_teams_international_results.csv")
  }

  if (nrow(unmapped_away) > 0) {
    print(unmapped_away, n = 50)
    write_csv(unmapped_away, "unmapped_away_teams_international_results.csv")
  }
}

mapped_filtered <- mapped %>%
  filter(!is.na(home_team_id), !is.na(away_team_id))

# =========================
# 6) final payload
# =========================
matches_final <- mapped_filtered %>%
  transmute(
    match_date = match_date,
    match_datetime = as.POSIXct(NA),
    competition_name = competition_name,
    competition_type = competition_type,
    season_year = season_year,
    stage_name = stage_name,
    home_team_id = as.integer(home_team_id),
    away_team_id = as.integer(away_team_id),
    venue_name = venue_name,
    venue_city = venue_city,
    venue_country = venue_country,
    is_neutral = is_neutral,
    host_team_id = host_team_id,
    home_score = home_score,
    away_score = away_score,
    extra_time_flag = extra_time_flag,
    penalty_flag = penalty_flag,
    home_penalty_score = home_penalty_score,
    away_penalty_score = away_penalty_score,
    result_90 = result_90,
    result_ft = result_ft,
    data_source = data_source
  ) %>%
  distinct()

message("Total scraped rows: ", nrow(mapped))
message("Mapped rows kept: ", nrow(mapped_filtered))
message("Dropped rows: ", nrow(mapped) - nrow(mapped_filtered))

# =========================
# 7) de-dup against existing matches
# =========================
existing_matches <- dbGetQuery(con, "
  SELECT
    match_date,
    competition_name,
    home_team_id,
    away_team_id,
    COALESCE(home_score, -1) AS home_score,
    COALESCE(away_score, -1) AS away_score
  FROM footballbet.matches
")

to_insert <- matches_final %>%
  mutate(
    home_score_key = coalesce(home_score, -1L),
    away_score_key = coalesce(away_score, -1L)
  ) %>%
  anti_join(
    existing_matches %>%
      mutate(
        home_score_key = home_score,
        away_score_key = away_score
      ) %>%
      select(match_date, competition_name, home_team_id, away_team_id, home_score_key, away_score_key),
    by = c("match_date", "competition_name", "home_team_id", "away_team_id", "home_score_key", "away_score_key")
  ) %>%
  select(-home_score_key, -away_score_key)

message("Rows ready to insert: ", nrow(to_insert))

# =========================
# 8) write to PostgreSQL
# =========================
if (nrow(to_insert) > 0) {
  dbWriteTable(
    con,
    Id(schema = "footballbet", table = "matches"),
    value = to_insert,
    append = TRUE,
    row.names = FALSE
  )
  message("Inserted rows: ", nrow(to_insert))
} else {
  message("No new rows to insert.")
}

# =========================
# 9) validation
# =========================
check_df <- dbGetQuery(con, "
  SELECT competition_type, COUNT(*) AS cnt
  FROM footballbet.matches
  GROUP BY competition_type
  ORDER BY cnt DESC
")
print(check_df)

dbDisconnect(con)
