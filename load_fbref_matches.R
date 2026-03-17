library(DBI)
library(RPostgres)
library(dplyr)
library(purrr)
library(stringr)
library(readr)
library(tibble)
library(worldfootballR)

# =========================
# 1) PostgreSQL connection
# =========================
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "postgres",
  host     = "localhost",
  port     = 5432,
  user     = "postgres",
  password = "1q2w3e"  # 如果需要密码再打开
)

# =========================
# 2) competition config
#    FBref international pages
# =========================
season_years <- 2018:2025

comp_urls <- tibble::tribble(
  ~competition_name,                     ~competition_type, ~non_dom_league_url,
  "FIFA World Cup",                      "world_cup",       "https://fbref.com/en/comps/1/history/World-Cup-Seasons",
  "International Friendlies (M)",        "friendly",        "https://fbref.com/en/comps/218/history/Friendlies-M-Seasons",
  "UEFA European Championship",          "continental_cup", "https://fbref.com/en/comps/676/history/European-Championship-Seasons",
  "Copa America",                        "continental_cup", "https://fbref.com/en/comps/685/history/Copa-America-Seasons",
  "FIFA World Cup Qualification - CAF",  "qualifier",       "https://fbref.com/en/comps/2/history/WCQ----CAF-M-Seasons",
  "FIFA World Cup Qualification - UEFA", "qualifier",       "https://fbref.com/en/comps/6/history/WCQ----UEFA-M-Seasons",
  "FIFA World Cup Qualification - AFC",  "qualifier",       "https://fbref.com/en/comps/7/history/WCQ----AFC-M-Seasons",
  "FIFA World Cup Qualification - CONMEBOL", "qualifier",   "https://fbref.com/en/comps/4/history/WCQ----CONMEBOL-M-Seasons",
  "FIFA World Cup Qualification - CONCACAF", "qualifier",   "https://fbref.com/en/comps/8/history/WCQ----CONCACAF-M-Seasons",
  "FIFA World Cup Qualification - OFC",  "qualifier",       "https://fbref.com/en/comps/5/history/WCQ----OFC-M-Seasons"
)

# =========================
# 3) helper functions
# =========================

safe_fb_match_results <- function(comp_name, comp_type, comp_url, years) {
  map_dfr(years, function(y) {
    message(sprintf("Fetching %s - %s", comp_name, y))
    Sys.sleep(3)  # FBref 有 rate limiting，放慢点

    tryCatch({
      df <- fb_match_results(
        country = "",
        gender = "M",
        season_end_year = y,
        tier = "",
        non_dom_league_url = comp_url
      )

      if (is.null(df) || nrow(df) == 0) return(NULL)

      df %>%
        mutate(
          competition_name = comp_name,
          competition_type = comp_type,
          season_year = y,
          data_source = "fbref"
        )
    }, error = function(e) {
      message(sprintf("FAILED %s - %s : %s", comp_name, y, e$message))
      NULL
    })
  })
}

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

infer_stage_name <- function(raw_comp_round) {
  if (is.null(raw_comp_round)) return(NA_character_)
  x <- tolower(raw_comp_round)

  case_when(
    str_detect(x, "group") ~ "group",
    str_detect(x, "round of 16|last 16") ~ "round_of_16",
    str_detect(x, "quarter") ~ "quarterfinal",
    str_detect(x, "semi") ~ "semifinal",
    str_detect(x, "final") ~ "final",
    str_detect(x, "third place") ~ "third_place",
    TRUE ~ NA_character_
  )
}

# 尝试自动识别常见列名差异
pick_col <- function(df, candidates) {
  hit <- intersect(candidates, names(df))
  if (length(hit) == 0) return(NULL)
  hit[[1]]
}

# =========================
# 4) fetch all match results
# =========================
all_results_raw <- pmap_dfr(
  comp_urls,
  function(competition_name, competition_type, non_dom_league_url) {
    safe_fb_match_results(
      comp_name = competition_name,
      comp_type = competition_type,
      comp_url  = non_dom_league_url,
      years     = season_years
    )
  }
)

if (nrow(all_results_raw) == 0) {
  stop("No match results fetched from FBref.")
}

message("Fetched rows: ", nrow(all_results_raw))
message("Columns available:")
print(names(all_results_raw))

# =========================
# 5) standardize schema from worldfootballR output
#    Different versions may expose slightly different names
# =========================
date_col        <- pick_col(all_results_raw, c("Date", "date"))
home_team_col   <- pick_col(all_results_raw, c("Home", "home_team", "Squad_Home", "home"))
away_team_col   <- pick_col(all_results_raw, c("Away", "away_team", "Squad_Away", "away"))
home_score_col  <- pick_col(all_results_raw, c("HomeGoals", "home_goals", "home_score", "GF_Home"))
away_score_col  <- pick_col(all_results_raw, c("AwayGoals", "away_goals", "away_score", "GF_Away"))
venue_col       <- pick_col(all_results_raw, c("Venue", "venue"))
round_col       <- pick_col(all_results_raw, c("Round", "round"))
match_report_col <- pick_col(all_results_raw, c("Match Report", "match_report", "match_url", "url"))

required_cols <- list(date_col, home_team_col, away_team_col, home_score_col, away_score_col)
if (any(sapply(required_cols, is.null))) {
  stop("Could not detect one or more required columns from fb_match_results() output. Please inspect names(all_results_raw).")
}

matches_std <- all_results_raw %>%
  transmute(
    match_date_raw   = .data[[date_col]],
    home_team_name   = normalize_team_name(as.character(.data[[home_team_col]])),
    away_team_name   = normalize_team_name(as.character(.data[[away_team_col]])),
    home_score       = suppressWarnings(as.integer(.data[[home_score_col]])),
    away_score       = suppressWarnings(as.integer(.data[[away_score_col]])),
    venue_name       = if (!is.null(venue_col)) as.character(.data[[venue_col]]) else NA_character_,
    stage_name_raw   = if (!is.null(round_col)) as.character(.data[[round_col]]) else NA_character_,
    match_url        = if (!is.null(match_report_col)) as.character(.data[[match_report_col]]) else NA_character_,
    competition_name = competition_name,
    competition_type = competition_type,
    season_year      = season_year,
    data_source      = data_source
  ) %>%
  mutate(
    match_date = as.Date(match_date_raw),
    result_90  = calc_result_90(home_score, away_score),
    stage_name = infer_stage_name(stage_name_raw),
    is_neutral = TRUE  # 国际赛事先默认中立；后面可按赛事修正
  ) %>%
  filter(!is.na(match_date), !is.na(home_team_name), !is.na(away_team_name))

# =========================
# 6) read teams and mapping
# =========================
teams_df <- dbGetQuery(con, "
  SELECT team_id, team_name
  FROM teams
")

mapping_df <- dbGetQuery(con, "
  SELECT source_name, source_team_name, team_id
  FROM team_name_mapping
  WHERE source_name = 'fbref'
")

# 直接名字匹配
direct_match_df <- teams_df %>%
  transmute(
    source_team_name = team_name,
    team_id = team_id
  ) %>%
  mutate(source_name = "fbref")

all_mapping_df <- bind_rows(mapping_df, direct_match_df) %>%
  distinct(source_name, source_team_name, .keep_all = TRUE)

matches_mapped <- matches_std %>%
  left_join(
    all_mapping_df %>% select(source_team_name, home_team_id = team_id),
    by = c("home_team_name" = "source_team_name")
  ) %>%
  left_join(
    all_mapping_df %>% select(source_team_name, away_team_id = team_id),
    by = c("away_team_name" = "source_team_name")
  )

unmapped_home <- matches_mapped %>%
  filter(is.na(home_team_id)) %>%
  distinct(home_team_name) %>%
  arrange(home_team_name)

unmapped_away <- matches_mapped %>%
  filter(is.na(away_team_id)) %>%
  distinct(away_team_name) %>%
  arrange(away_team_name)

if (nrow(unmapped_home) > 0 || nrow(unmapped_away) > 0) {
  message("Unmapped team names detected.")
  if (nrow(unmapped_home) > 0) {
    message("Unmapped home teams:")
    print(unmapped_home)
  }
  if (nrow(unmapped_away) > 0) {
    message("Unmapped away teams:")
    print(unmapped_away)
  }

  write_csv(unmapped_home, "unmapped_home_teams.csv")
  write_csv(unmapped_away, "unmapped_away_teams.csv")

  stop("Please resolve unmapped teams in team_name_mapping, then rerun.")
}

# =========================
# 7) prepare final matches payload
# =========================
matches_final <- matches_mapped %>%
  transmute(
    match_date        = match_date,
    match_datetime    = as.POSIXct(NA),
    competition_name  = competition_name,
    competition_type  = competition_type,
    season_year       = season_year,
    stage_name        = stage_name,
    home_team_id      = as.integer(home_team_id),
    away_team_id      = as.integer(away_team_id),
    venue_name        = venue_name,
    venue_city        = NA_character_,
    venue_country     = NA_character_,
    is_neutral        = is_neutral,
    host_team_id      = NA_integer_,
    home_score        = home_score,
    away_score        = away_score,
    extra_time_flag   = FALSE,
    penalty_flag      = FALSE,
    home_penalty_score = NA_integer_,
    away_penalty_score = NA_integer_,
    result_90         = result_90,
    result_ft         = result_90,
    data_source       = data_source
  ) %>%
  distinct()

# =========================
# 8) de-duplicate against existing matches
#    simple natural key for MVP
# =========================
existing_matches <- dbGetQuery(con, "
  SELECT
    match_date,
    competition_name,
    home_team_id,
    away_team_id,
    COALESCE(home_score, -1) AS home_score,
    COALESCE(away_score, -1) AS away_score
  FROM matches
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
# 9) write to PostgreSQL
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
# 10) quick validation
# =========================
check_df <- dbGetQuery(con, "
  SELECT competition_type, COUNT(*) AS cnt
  FROM matches
  GROUP BY competition_type
  ORDER BY cnt DESC
")
print(check_df)

dbDisconnect(con)
