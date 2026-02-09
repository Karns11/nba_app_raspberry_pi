library(hoopR)
library(dplyr)
library(purrr)
library(tidyr)
library(DBI)
library(RPostgres)

# ============================================================================
# CONFIGURATION
# ============================================================================

pg_host <- Sys.getenv("PG_HOST")
pg_port <- Sys.getenv("PG_PORT")
pg_user <- Sys.getenv("PG_USER")
pg_password <- Sys.getenv("PG_PASSWORD")
pg_db <- Sys.getenv("PG_DB")


# ============================================================================
# DATABASE CONNECTION
# ============================================================================
print("Connecting to PostgreSQL database...")

con <- dbConnect(
  RPostgres::Postgres(),
  host = pg_host,
  port = pg_port,
  user = pg_user,
  password = pg_password,
  dbname = pg_db
)

print("Connected to PostgreSQL database successfully.") 

on.exit(dbDisconnect(con))


# ============================================================================
# CREATE TABLE IF NOT EXISTS
# ============================================================================

cat("Ensuring table schema exists...\n")

# Drop the old table if it exists with wrong schema
dbExecute(con, "DROP TABLE IF EXISTS raw.nba_player_game_logs;")

create_table_sql <- "
CREATE TABLE IF NOT EXISTS raw.nba_player_game_logs (
    game_id VARCHAR(50),
    game_date DATE,
    away_team_id BIGINT,
    home_team_id BIGINT,
    team_id BIGINT,
    team_name VARCHAR(255),
    team_city VARCHAR(100),
    team_tricode VARCHAR(10),
    team_slug VARCHAR(50),
    person_id BIGINT,
    first_name VARCHAR(255),
    family_name VARCHAR(255),
    name_i VARCHAR(255),
    player_slug VARCHAR(255),
    position VARCHAR(10),
    comment TEXT,
    jersey_num VARCHAR(25),
    minutes VARCHAR(20),
    field_goals_made INTEGER,
    field_goals_attempted INTEGER,
    field_goals_percentage NUMERIC,
    three_pointers_made INTEGER,
    three_pointers_attempted INTEGER,
    three_pointers_percentage NUMERIC,
    free_throws_made INTEGER,
    free_throws_attempted INTEGER,
    free_throws_percentage NUMERIC,
    rebounds_offensive INTEGER,
    rebounds_defensive INTEGER,
    rebounds_total INTEGER,
    assists INTEGER,
    steals INTEGER,
    blocks INTEGER,
    turnovers INTEGER,
    fouls_personal INTEGER,
    points INTEGER,
    plus_minus_points INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_id, person_id)
);
"

dbExecute(con, create_table_sql)

# Create indexes if they don't exist
index_sql_1 <- "CREATE INDEX IF NOT EXISTS idx_game_id ON raw.nba_player_game_logs(game_id);"
index_sql_2 <- "CREATE INDEX IF NOT EXISTS idx_person_id ON raw.nba_player_game_logs(person_id);"
index_sql_3 <- "CREATE INDEX IF NOT EXISTS idx_team_id ON raw.nba_player_game_logs(team_id);"

dbExecute(con, index_sql_1)
dbExecute(con, index_sql_2)
dbExecute(con, index_sql_3)

cat("Table schema verified/created.\n")


# ============================================================================
# DETERMINE DATE RANGE FOR INCREMENTAL LOAD
# ============================================================================

table_exists <- dbExistsTable(con, "raw.nba_player_game_logs")

if (table_exists) {
  max_date_query <- "SELECT MAX(game_date) as max_date FROM raw.nba_player_game_logs"
  result <- dbGetQuery(con, max_date_query)
  max_date <- result$max_date[1]

  if (!is.na(max_date)) {
    start_date <- as.Date(max_date) + 1
    cat("Most recent game in DB:", as.character(max_date), "\n")
    cat("Fetching games from:", as.character(start_date), "\n")
  } else {
    start_date <- NULL
    cat("Table is empty. Fetching entire current season.\n")
  }
} else {
  start_date <- NULL
  cat("Table doesn't exist yet. Fetching entire current season.\n")
}



# ============================================================================
# FETCH NBA DATA
# ============================================================================

current_season <- year_to_season(most_recent_nba_season() - 1)
cat("Current season:", current_season, "\n")

cat("Fetching game list...\n")
games_list <- nba_leaguegamefinder(
  league_id = '00', 
  season = current_season
)

games_list_df <- games_list$LeagueGameFinderResults


if (!is.null(start_date)) {
  games_list_df <- games_list_df %>%
    mutate(GAME_DATE = as.Date(GAME_DATE, format = "%Y-%m-%d")) %>%
    filter(GAME_DATE >= start_date & GAME_DATE < Sys.Date())
  
  cat("Found", nrow(games_list_df), "new games since", as.character(start_date), "\n")
  
  if (nrow(games_list_df) == 0) {
    cat("No new games to process. Exiting.\n")
    quit(save = "no", status = 0)
  }
} else {
  games_list_df <- games_list_df %>%
    mutate(GAME_DATE = as.Date(GAME_DATE, format = "%Y-%m-%d")) %>%
    filter(GAME_DATE < Sys.Date())
}

games_list_df_unique <- games_list_df %>%
  distinct(GAME_ID)

game_ids <- games_list_df_unique$GAME_ID

cat("Processing", length(game_ids), "unique games...\n")

# ============================================================================
# FETCH BOX SCORES
# ============================================================================

all_player_logs <- list()

for (i in seq_along(game_ids)) {
  cat("Processing game", i, "of", length(game_ids), "- Game ID:", game_ids[i], "\n")
  
  tryCatch({
    v3boxscore_results <- nba_boxscoretraditionalv3(
      game_id = game_ids[i],
      start_period = 0,
      end_period = 14,
      start_range = 0,
      end_range = 0,
      range_type = 0
    )
    
    # Combine home and away player stats
    home_stats <- v3boxscore_results$home_team_player_traditional
    away_stats <- v3boxscore_results$away_team_player_traditional
    
    if (!is.null(home_stats) && nrow(home_stats) > 0) {
      all_player_logs[[length(all_player_logs) + 1]] <- home_stats
    }
    
    if (!is.null(away_stats) && nrow(away_stats) > 0) {
      all_player_logs[[length(all_player_logs) + 1]] <- away_stats
    }
    
    # Rate limiting
    Sys.sleep(0.1)
    
  }, error = function(e) {
    cat("ERROR processing game", game_ids[i], ":", conditionMessage(e), "\n")
  })
}


# ============================================================================
# COMBINE AND CLEAN DATA
# ============================================================================

if (length(all_player_logs) == 0) {
  cat("No player logs fetched. Exiting.\n")
  quit(save = "no", status = 0)
}

player_logs_df <- bind_rows(all_player_logs)

# Convert hoopR_data object to standard data.frame
player_logs_df <- as.data.frame(player_logs_df)

cat("Fetched", nrow(player_logs_df), "player game logs (including team totals)\n")

# Clean column names to lowercase
player_logs_df <- player_logs_df %>%
  rename_all(tolower)

# **CRITICAL: Filter out rows with NULL person_id (team totals, headers, etc.)**
player_logs_df <- player_logs_df %>%
  filter(!is.na(person_id), person_id != "", person_id != 0)

cat("After filtering: ", nrow(player_logs_df), "valid player game logs\n")

# Additional data quality checks
player_logs_df <- player_logs_df %>%
  filter(!is.na(game_id), game_id != "")

cat("Final dataset: ", nrow(player_logs_df), "records to load\n")

# Exit if no valid data after filtering
if (nrow(player_logs_df) == 0) {
  cat("No valid player logs after filtering. Exiting.\n")
  quit(save = "no", status = 0)
}

games_list_df_unique_game_and_dates <- games_list_df %>%
  distinct(GAME_ID, GAME_DATE)

player_logs_df <- player_logs_df %>%
  left_join(games_list_df_unique_game_and_dates, by = c("game_id" = "GAME_ID")) %>%
  rename(game_date = GAME_DATE)
  
# ============================================================================
# LOAD DATA TO DATABASE
# ============================================================================

cat("Loading data to PostgreSQL...\n")


raw_nba_player_game_logs_table <- DBI::Id(schema = "raw", table = "nba_player_game_logs")

dbWriteTable(
con, 
name = raw_nba_player_game_logs_table,
value = player_logs_df,
append = TRUE,
row.names = FALSE
)

cat("Successfully loaded", nrow(player_logs_df), "records to database.\n")

# ============================================================================
# SUMMARY
# ============================================================================

summary_query <- "
  SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT game_id) as unique_games,
    COUNT(DISTINCT person_id) as unique_players
  FROM raw.nba_player_game_logs
"

summary <- dbGetQuery(con, summary_query)
cat("\n=== DATABASE SUMMARY ===\n")
print(summary)
cat("========================\n")

cat("Script completed successfully.\n")


#--------------------------------#
# #Dim tables

# #player dim function
# player_info <- nba_commonplayerinfo(league_id = '00', player_id = '2544')
# player_info_df <- as.data.frame(player_info)


# #team dim function
# team_info <- nba_teams()

# #total/avg stats df
# avg_stats <- nba_leaguedashplayerstats(league_id = '00', season = year_to_season(most_recent_nba_season() - 1))
# avg_stats_df <- as.data.frame(avg_stats)


