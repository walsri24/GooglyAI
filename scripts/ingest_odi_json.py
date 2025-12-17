# scripts/ingest_odi_json.py

from pathlib import Path
import pandas as pd

from googlyai.data_pipeline.db import get_connection
from googlyai.data_pipeline.odi.loader import load_match_json
from googlyai.data_pipeline.odi.parser import (
    parse_match_info,
    parse_registry,
    parse_ball_by_ball,
)

ODI_DIR = Path("data/raw/odis_json")
BATCH_SIZE = 50


def flush_to_db(con, matches, players, balls):
    if matches:
        con.execute(
            "INSERT INTO matches SELECT * FROM matches_df",
            {"matches_df": pd.DataFrame(matches)}
        )

    if players:
        con.execute(
            "INSERT INTO players SELECT DISTINCT * FROM players_df",
            {"players_df": pd.DataFrame(players)}
        )

    if balls:
        con.execute(
            "INSERT INTO ball_by_ball SELECT * FROM balls_df",
            {"balls_df": pd.DataFrame(balls)}
        )


def main():
    con = get_connection()

    # One-time setup
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA memory_limit='4GB';")

    # Create tables if not exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT,
            match_date DATE,
            season INTEGER,
            match_type TEXT,
            team1 TEXT,
            team2 TEXT,
            venue TEXT,
            winner TEXT,
            win_by_runs INTEGER,
            win_by_wickets INTEGER,
            toss_winner TEXT,
            toss_decision TEXT
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id TEXT,
            player_name TEXT
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ball_by_ball (
            match_id TEXT,
            innings INTEGER,
            batting_team TEXT,
            over INTEGER,
            ball INTEGER,
            batter TEXT,
            batter_id TEXT,
            bowler TEXT,
            bowler_id TEXT,
            non_striker TEXT,
            runs_batter INTEGER,
            runs_extras INTEGER,
            runs_total INTEGER,
            is_wicket BOOLEAN,
            wicket_kind TEXT,
            player_out TEXT,
            player_out_id TEXT
        )
    """)

    matches_batch = []
    players_batch = []
    balls_batch = []

    for idx, file in enumerate(ODI_DIR.glob("*.json"), start=1):
        data = load_match_json(file)
        if not data:
            continue

        matches_batch.append(parse_match_info(data))
        players_batch.extend(parse_registry(data))
        balls_batch.extend(parse_ball_by_ball(data))

        if idx % BATCH_SIZE == 0:
            flush_to_db(con, matches_batch, players_batch, balls_batch)
            matches_batch.clear()
            players_batch.clear()
            balls_batch.clear()
            print(f"✔ Ingested {idx} matches")

    # Final flush
    flush_to_db(con, matches_batch, players_batch, balls_batch)
    print("✅ ODI ingestion complete")


if __name__ == "__main__":
    main()
