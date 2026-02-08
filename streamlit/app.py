from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
import streamlit as st
import os
import numpy as np


# Get credentials from environment variables
db_user = os.getenv("PG_USER", "nba_app_v2_role")
db_password = os.getenv("PG_PASSWORD", "Dingernation1#@!")
db_host = os.getenv("PG_HOST", "10.0.0.143")
db_port = os.getenv("PG_PORT", "5433")
db_name = os.getenv("PG_DB", "nba_data")

# URL-encode the password
password_encoded = quote_plus(db_password)

engine = create_engine(
    f"postgresql+psycopg2://{db_user}:{password_encoded}@{db_host}:{db_port}/{db_name}"
)

STAT_COLS = [
    "minutes_pg",
    "points",
    "field_goals_made",
    "field_goals_attempted",
    "field_goals_percentage",
    "three_pointers_made",
    "three_pointers_attempted",
    "three_pointers_percentage",
    "free_throws_made",
    "free_throws_attempted",
    "free_throws_percentage",
    "rebounds_offensive",
    "rebounds_defensive",
    "rebounds_total",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "fouls_personal",
    "plus_minus_points"
]

# Stat name mapping for cleaner display
STAT_NAME_MAP = {
    'minutes_pg': 'Minutes',
    'points': 'Points',
    'field_goals_made': 'FGM',
    'field_goals_attempted': 'FGA',
    'field_goals_percentage': 'FG%',
    'three_pointers_made': '3PM',
    'three_pointers_attempted': '3PA',
    'three_pointers_percentage': '3P%',
    'free_throws_made': 'FTM',
    'free_throws_attempted': 'FTA',
    'free_throws_percentage': 'FT%',
    'rebounds_offensive': 'OREB',
    'rebounds_defensive': 'DREB',
    'rebounds_total': 'REB',
    'assists': 'AST',
    'steals': 'STL',
    'blocks': 'BLK',
    'turnovers': 'TO',
    'fouls_personal': 'PF',
    'plus_minus_points': '+/-'
}

def format_stat_names(df):
    """Format stat column names for display"""
    if isinstance(df, pd.Series):
        df.index = df.index.map(lambda x: STAT_NAME_MAP.get(x, x))
    else:
        df.index = df.index.map(lambda x: STAT_NAME_MAP.get(x, x))
    return df

st.title("NBA Player Stats")

# Initialize df in session state to persist across reruns
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()

player_search = st.text_input("Player")

if player_search:
    players = pd.read_sql(
        """
        WITH ranked_players AS (
            SELECT 
                person_id,
                first_name,
                family_name,
                team_tricode,
                position,
                game_date,
                ROW_NUMBER() OVER (
                    PARTITION BY person_id 
                    ORDER BY game_date DESC
                ) as rn
            FROM raw.nba_player_game_logs
            WHERE LOWER(first_name || ' ' || family_name)
                  LIKE LOWER(%(search)s)
        )
        SELECT 
            person_id,
            first_name,
            family_name,
            team_tricode,
            COALESCE(position, '') as position
        FROM ranked_players
        WHERE rn = 1
        ORDER BY family_name, first_name
        """,
        engine,
        params={"search": f"%{player_search}%"}
    )

    if not players.empty:
        player_label = players.apply(
            lambda x: f"{x.first_name} {x.family_name} ({x.team_tricode}{', ' + x.position if x.position else ''})",
            axis=1
        )

        selected = st.selectbox(
            "Select Player",
            options=players.index,
            format_func=lambda i: player_label.iloc[i]
        )

        # Convert numpy.int64 to Python int
        player_id = int(players.loc[selected, "person_id"])

        since_date = st.date_input("Since", value=pd.to_datetime("2025-10-21"))

        if st.button("Search for stats!"):
            st.session_state.df = pd.read_sql(
                """
                SELECT
                    game_date,
                    team_tricode as team,
                    CASE
                        WHEN team_id = home_team_id THEN away_team_tricode
                        ELSE home_team_tricode
                    END AS opponent_tricode,
                    team_id,
                    home_team_id, 
                    minutes,
                    points,
                    field_goals_made,
                    field_goals_attempted,
                    field_goals_percentage,
                    three_pointers_made,
                    three_pointers_attempted,
                    three_pointers_percentage,
                    free_throws_made,
                    free_throws_attempted,
                    free_throws_percentage,
                    rebounds_offensive,
                    rebounds_defensive,
                    rebounds_total,
                    assists,
                    steals,
                    blocks,
                    turnovers,
                    fouls_personal,
                    plus_minus_points

                FROM (
                    SELECT
                        *,
                        MAX(CASE WHEN team_id = home_team_id THEN team_tricode END)
                            OVER (PARTITION BY game_date, home_team_id) AS home_team_tricode,
                        MAX(CASE WHEN team_id = away_team_id THEN team_tricode END)
                            OVER (PARTITION BY game_date, away_team_id) AS away_team_tricode
                    FROM raw.nba_player_game_logs
                ) t

                WHERE person_id = %(player_id)s
                AND game_date >= %(since_date)s
                ORDER BY game_date DESC

                """,
                engine,
                params={"player_id": player_id, "since_date": since_date}
            )


df = st.session_state.df

if not df.empty:
    df = df.copy()

    def minutes_to_decimal(x):
        if pd.isna(x) or x == "":
            return np.nan
        mins, secs = x.split(":")
        return int(mins) + int(secs) / 60
    
    df["minutes_pg"] = df["minutes"].apply(minutes_to_decimal)


if not df.empty:
    display_cols = [col for col in df.columns if col not in ['team_id', 'home_team_id', 'minutes']]

    df_display = df[display_cols].copy()
    df_display.columns = df_display.columns.str.replace('_', ' ').str.title()
    
    # Additional custom renames
    column_renames = {
        'Game Date': 'Date',
        'Opponent Tricode': 'Opponent',
        'Minutes Pg': 'Minutes',
        'Field Goals Made': 'FGM',
        'Field Goals Attempted': 'FGA',
        'Field Goals Percentage': 'FG%',
        'Three Pointers Made': '3PM',
        'Three Pointers Attempted': '3PA',
        'Three Pointers Percentage': '3P%',
        'Free Throws Made': 'FTM',
        'Free Throws Attempted': 'FTA',
        'Free Throws Percentage': 'FT%',
        'Rebounds Offensive': 'OREB',
        'Rebounds Defensive': 'DREB',
        'Rebounds Total': 'REB',
        'Assists': 'AST',
        'Steals': 'STL',
        'Blocks': 'BLK',
        'Turnovers': 'TO',
        'Fouls Personal': 'PF',
        'Plus Minus Points': '+/-'
    }
    
    df_display.rename(columns=column_renames, inplace=True)

    st.dataframe(df_display)

    if st.checkbox("Show Current Season Averages"):
        season_avg = df[STAT_COLS].mean().round(2)
        season_avg = format_stat_names(season_avg)
        st.subheader("Season Averages")
        st.dataframe(season_avg.to_frame("Avg"))

    if st.checkbox("Show Home/Away Averages"):
        df_copy = df.copy()
        df_copy["home_away"] = np.where(
            df_copy["team_id"] == df_copy["home_team_id"],
            "Home",
            "Away"
        )

        ha_avg = (
            df_copy
            .groupby("home_away")[STAT_COLS]
            .mean()
            .round(2)
        )
        ha_avg.columns = ha_avg.columns.map(lambda x: STAT_NAME_MAP.get(x, x))
        st.subheader("Home vs Away")
        st.dataframe(ha_avg)

    if st.checkbox("Show Last 5 GP Averages"):
        last_5 = df.head(5)[STAT_COLS].mean().round(2)
        last_5 = format_stat_names(last_5)
        st.subheader("Last 5 Games Avg")
        st.dataframe(last_5.to_frame("Avg"))

    teams = df["opponent_tricode"].dropna().unique()

    if len(teams) > 0:
        selected_team = st.selectbox("Averages vs Team", sorted(teams))

        vs_df = df[df["opponent_tricode"] == selected_team]

        games_played = len(vs_df)

        vs_team_avg = vs_df[STAT_COLS].mean().round(2)
        vs_team_avg = format_stat_names(vs_team_avg)

        st.subheader(f"Averages vs {selected_team}")
        st.caption(f"Games Played: {games_played}")

        st.dataframe(vs_team_avg.to_frame("Avg"))