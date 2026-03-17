library(DBI)
library(RPostgres)
library(dplyr)
library(rvest)
library(xml2)
library(stringr)
library(readr)
library(tibble)
library(janitor)
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

infer_stage_name <- function(round_text) {
  x <- tolower(round_text %||% "")
  case_when(
    str_detect(x, "group") ~ "group",
    str_detect(x, "round of 16|last 16") ~ "round_of_16",
    str_detect(x, "quarter") ~ "quarterfinal",
    str_detect(x, "semi") ~ "semifinal",
    str_detect(x, "final") ~ "final",
    str_detect(x, "third") ~ "third_place",
    TRUE ~ NA_character_
  )
}

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0 && !is.na(a)) a else b

pick_results_table <- function(page) {
  tables <- page %>%
    html_table(fill = TRUE)

  if (length(tables) == 0) return(NULL)

  # 尝试找包含日期/主客队/比分的表
  for (tb in tables) {
    nm <- names(tb) %>% tolower()
    has_date <- any(str_detect(nm, "date"))
    has_home <- any(str_detect(nm, "home|squad"))
    has_away <- any(str_detect(nm, "away|opponent"))
    has_score <- any(str_detect(nm, "score|homegoals|awaygoals|gf"))
    if (has_date && (has_home || has_away)) {
      return(tb)
    }
  }

  # 找不到就返回第一张表，后面人工检查
  tables[[1]]
}

safe_parse_int <- function(x) {
  suppressWarnings(as.integer(str_extract(as.character(x), "\\d+")))
}

# =========================
# 3) config: single competition test
#    先从世界杯测试
# =========================
competition_name <- "FIFA World Cup"
competition_type <- "world_cup"

season_urls <- c(
  "https://fbref.com/en/comps/1/2018/schedule/2018-World-Cup-Scores-and-Fixtures",
  "https://fbref.com/en/comps/1/2022/schedule/2022-World-Cup-Scores-and-Fixtures"
)

# 你后面可以把更多赛事URL继续加进来

# =========================
# 4) scrape one page at a time
# =========================
all_results <- purrr::map_dfr(season_urls, function(url) {
  message("Fetching: ", url)
  Sys.sleep(3)

  page <- tryCatch(read_html(url), error = function(e) NULL)
  if (is.null(page)) {
    message("Failed to fetch: ", url)
    return(NULL)
  }

  tb <- pick_results_table(page)
  if (is.null(tb) || nrow(tb) == 0) {
    message("No table found: ", url)
    return(NULL)
  }

  tb <- janitor::clean_names(tb)
  print(names(tb))

  # 尝试兼容常见列名
  date_col <- intersect(names(tb), c("date", "match_date"))[1]
  home_col <- intersect(names(tb), c("home", "home_team", "squad_home"))[1]
  away_col <- intersect(names(tb), c("away", "away_team", "squad_away"))[1]
  score_col <- intersect(names(tb), c("score", "match_report"))[1]
  round_col <- intersect(names(tb), c("round", "stage"))[1]
  venue_col <- intersect(names(tb), c("venue"))[1]

  if (is.na(date_col) || is.na(home_col) || is.na(away_col)) {
    message("Required columns not found for: ", url)
    return(NULL)
  }

  out <- tb %>%
    transmute(
      match_date = as.Date(.data[[date_col]]),
      home_team_name = normalize_team_name(.data[[home_col]]),
      away_team_name = normalize_team_name(.data[[away_col]]),
      score_raw = if (!is.na(score_col)) as.character(.data[[score_col]]) else NA_character_,
      stage_name_raw = if (!is.na(round_col)) as.character(.data[[round_col]]) else NA_character_,
      venue_name = if (!is.na(venue_col)) as.character(.data[[venue_col]]) else NA_character_
    ) %>%
    mutate(
      home_score = safe_parse_int(str_split_fixed(score_raw, "–|-", 2)[,1]),
      away_score = safe_parse_int(str_split_fixed(score_raw, "–|-", 2)[,2]),
      competition_name = competition_name,
      competition_type = competition_type,
      season_year = year(match_date),
      result_90 = calc_result_90(home_score, away_score),
      stage_name = infer_stage_name(stage_name_raw),
      is_neutral = TRUE,
      data_source = "fbref_direct"
    ) %>%
    filter(!is.na(match_date), !is.na(home_team_name), !is.na(away_team_name))

  out
})

if (nrow(all_results) == 0) {
  stop("No rows scraped. Check page structure.")
}

# =========================
# 5) load teams + mapping
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

direct_match_df <- teams_df %>%
  transmute(
    source_team_name = team_name,
    team_id = team_id
  ) %>%
  mutate(source_name = "fbref")

all_mapping_df <- bind_rows(mapping_df, direct_match_df) %>%
  distinct(source_name, source_team_name, .keep_all = TRUE)

mapped <- all_results %>%
  left_join(
    all_mapping_df %>% select(source_team_name, home_team_id = team_id),
    by = c("home_team_name" = "source_team_name")
  ) %>%
  left_join(
    all_mapping_df %>% select(source_team_name, away_team_id = team_id),
    by = c("away_team_name" = "source_team_name")
  )

unmapped <- mapped %>%
  filter(is.na(home_team_id) | is.na(away_team_id)) %>%
  select(home_team_name, away_team_name) %>%
  distinct()

if (nrow(unmapped) > 0) {
  print(unmapped)
  write_csv(unmapped, "unmapped_fbref_direct.csv")
  stop("Please resolve team_name_mapping first.")
}

# =========================
# 6) prepare final insert
# =========================
to_insert <- mapped %>%
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
    venue_city = NA_character_,
    venue_country = NA_character_,
    is_neutral = is_neutral,
    host_team_id = NA_integer_,
    home_score = home_score,
    away_score = away_score,
    extra_time_flag = FALSE,
    penalty_flag = FALSE,
    home_penalty_score = NA_integer_,
    away_penalty_score = NA_integer_,
    result_90 = result_90,
    result_ft = result_90,
    data_source = data_source
  ) %>%
  distinct()

dbWriteTable(
  con,
  Id(schema = "footballbet", table = "matches"),
  value = to_insert,
  append = TRUE,
  row.names = FALSE
)

dbDisconnect(con)
