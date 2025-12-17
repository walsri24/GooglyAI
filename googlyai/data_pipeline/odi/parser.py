# googlyai/data_pipeline/odi/parser.py
from googlyai.data_pipeline.odi.schema import safe_get

def parse_match_info(data: dict):
    info = data.get("info", {})

    outcome = info.get("outcome", {})
    by = outcome.get("by", {})

    teams = info.get("teams", [None, None])

    return {
        "match_id": str(safe_get(info, ["match_type_number"])),
        "match_date": safe_get(info, ["dates", 0]),
        "season": info.get("season"),
        "match_type": info.get("match_type"),
        "team1": teams[0],
        "team2": teams[1],
        "venue": info.get("venue"),
        "winner": outcome.get("winner"),
        "win_by_runs": by.get("runs"),
        "win_by_wickets": by.get("wickets"),
        "toss_winner": safe_get(info, ["toss", "winner"]),
        "toss_decision": safe_get(info, ["toss", "decision"]),
    }


def parse_registry(data: dict):
    registry = safe_get(data, ["info", "registry", "people"], {})
    return [
        {"player_name": name, "player_id": pid}
        for name, pid in registry.items()
    ]


def parse_ball_by_ball(data: dict):
    info = data["info"]
    match_id = str(info["match_type_number"])
    registry = safe_get(info, ["registry", "people"], {})

    rows = []

    for i, inning in enumerate(data.get("innings", []), start=1):
        team = inning.get("team")

        for over_data in inning.get("overs", []):
            over_no = over_data.get("over")

            for ball_no, delivery in enumerate(over_data.get("deliveries", []), start=1):
                runs = delivery.get("runs", {})

                wicket = delivery.get("wickets", [{}])[0] if "wickets" in delivery else {}

                rows.append({
                    "match_id": match_id,
                    "innings": i,
                    "batting_team": team,
                    "over": over_no,
                    "ball": ball_no,
                    "batter": delivery.get("batter"),
                    "batter_id": registry.get(delivery.get("batter")),
                    "bowler": delivery.get("bowler"),
                    "bowler_id": registry.get(delivery.get("bowler")),
                    "non_striker": delivery.get("non_striker"),
                    "runs_batter": runs.get("batter", 0),
                    "runs_extras": runs.get("extras", 0),
                    "runs_total": runs.get("total", 0),
                    "is_wicket": "wickets" in delivery,
                    "wicket_kind": wicket.get("kind"),
                    "player_out": wicket.get("player_out"),
                    "player_out_id": registry.get(wicket.get("player_out")),
                })

    return rows
