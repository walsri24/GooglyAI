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
        df_matches = pd.DataFrame(matches)
        con.register("df_matches", df_matches)
        con.execute("INSERT INTO matches SELECT * FROM df_matches")
        con.unregister("df_matches")

    if players:
        df_players = pd.DataFrame(players)
        con.register("df_players", df_players)
        con.execute("INSERT INTO players SELECT DISTINCT * FROM df_players")
        con.unregister("df_players")

    if balls:
        df_balls = pd.DataFrame(balls)
        con.register("df_balls", df_balls)
        con.execute("INSERT INTO ball_by_ball SELECT * FROM df_balls")
        con.unregister("df_balls")


def main():
    con = get_connection()

    # Performance pragmas
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA memory_limit='4GB';")

    # IMPORTANT: drop & recreate tables (fresh rebuild)
    con.execute("DROP TABLE IF EXISTS matches")
    con.execute("DROP TABLE IF EXISTS players")
    con.execute("DROP TABLE IF EXISTS ball_by_ball")

    con.execute("""
                CREATE TABLE matches
                (
                    match_id       TEXT,
                    match_date     DATE,
                    season         TEXT,
                    match_type     TEXT,
                    team1          TEXT,
                    team2          TEXT,
                    venue          TEXT,
                    winner         TEXT,
                    win_by_runs    INTEGER,
                    win_by_wickets INTEGER,
                    toss_winner    TEXT,
                    toss_decision  TEXT
                )
                """)

    con.execute("""
        CREATE TABLE players (
            player_id TEXT,
            player_name TEXT
        )
    """)

    con.execute("""
        CREATE TABLE ball_by_ball (
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

    match_count = 0

    for file in ODI_DIR.glob("*.json"):
        data = load_match_json(file)
        if not data:
            continue

        matches_batch.append(parse_match_info(data))
        players_batch.extend(parse_registry(data))
        balls_batch.extend(parse_ball_by_ball(data))

        match_count += 1

        if match_count % BATCH_SIZE == 0:
            flush_to_db(con, matches_batch, players_batch, balls_batch)
            matches_batch.clear()
            players_batch.clear()
            balls_batch.clear()
            print(f"✔ Ingested {match_count} matches")

    # FINAL FLUSH (CRITICAL)
    flush_to_db(con, matches_batch, players_batch, balls_batch)

    print(f"✅ ODI ingestion complete: {match_count} matches")


if __name__ == "__main__":
    main()
