QUERY_LIBRARY = {

    # =========================================================
    # 1. PLAYER PERFORMANCE / MATCH STATS
    # =========================================================

    "player_performance_gw": {
        "intent": "player_performance",
        "entities": ["players", "season", "gameweek"],
        "cypher": """
            MATCH (p:Player {player_name: $players})
                  -[stats:PLAYED_IN]->(f:Fixture {season: $season, fixture_number: $gameweek})
            RETURN p.player_name, stats
        """
    },

    "player_performance_season": {
        "intent": "player_performance",
        "entities": ["players", "season"],
        "cypher": """
            MATCH (p:Player {player_name: $players})-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            RETURN p.player_name, SUM(stats.total_points) AS total_points,
                   SUM(stats.goals_scored) AS goals, SUM(stats.assists) AS assists
        """
    },

    "player_stat_single_value": {
        "intent": "player_performance",
        "entities": ["players", "season", "stat"],
        "cypher": """
            MATCH (p:Player {player_name: $players})-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            RETURN p.player_name AS player, $stat AS stat,
                   SUM(stats[$stat]) AS total_stat
        """
    },

    # =========================================================
    # 2. PLAYER HISTORY
    # =========================================================

    "player_history_all": {
        "intent": "player_history",
        "entities": ["players"],
        "cypher": """
            MATCH (p:Player {player_name: $players})-[stats:PLAYED_IN]->(f:Fixture)
            RETURN f.season, f.fixture_number AS gw, stats
            ORDER BY f.season, gw
        """
    },

    "player_history_season": {
        "intent": "player_history",
        "entities": ["players", "season"],
        "cypher": """
            MATCH (p:Player {player_name: $players})-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            RETURN f.fixture_number AS gw, stats
            ORDER BY gw
        """
    },

    "player_history_specific_stat": {
        "intent": "player_history",
        "entities": ["players", "stat"],
        "cypher": """
            MATCH (p:Player {player_name: $players})-[stats:PLAYED_IN]->(f:Fixture)
            RETURN f.season, f.fixture_number AS gw, stats[$stat] AS value
            ORDER BY f.season, gw
        """
    },

    # =========================================================
    # 3. COMPARE PLAYERS
    # =========================================================

    "compare_players_points": {
        "intent": "compare_players",
        "entities": ["players", "season"],
        "cypher": """
            MATCH (p1:Player {player_name: $players[0]})-[s1:PLAYED_IN]->(f1:Fixture {season: $season})
            MATCH (p2:Player {player_name: $players[1]})-[s2:PLAYED_IN]->(f2:Fixture {season: $season})
            RETURN p1.player_name AS player1, SUM(s1.total_points) AS p1_points,
                   p2.player_name AS player2, SUM(s2.total_points) AS p2_points
        """
    },

    "compare_players_stat": {
        "intent": "compare_players",
        "entities": ["players", "season", "stat"],
        "cypher": """
            MATCH (p1:Player {player_name: $players[0]})-[s1:PLAYED_IN]->(f1:Fixture {season: $season})
            MATCH (p2:Player {player_name: $players[1]})-[s2:PLAYED_IN]->(f2:Fixture {season: $season})
            RETURN p1.player_name AS p1, SUM(s1[$stat]) AS p1_val,
                   p2.player_name AS p2, SUM(s2[$stat]) AS p2_val
        """
    },

    # =========================================================
    # 4. TOP PLAYERS / RANKINGS
    # =========================================================

    "top_players_by_position": {
        "intent": "top_players_position",
        "entities": ["position", "season"],
        "cypher": """
            MATCH (p:Player)-[:PLAYS_AS]->(pos:Position {name: $position})
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            RETURN p.player_name, SUM(stats.total_points) AS total_points
            ORDER BY total_points DESC
            LIMIT 10
        """
    },

    "top_scorer_season": {
        "intent": "top_players_position",
        "entities": ["season"],
        "cypher": """
            MATCH (p:Player)-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            RETURN p.player_name, SUM(stats.goals_scored) AS goals
            ORDER BY goals DESC
            LIMIT 10
        """
    },

    "top_assisters_season": {
        "intent": "top_players_position",
        "entities": ["season"],
        "cypher": """
            MATCH (p:Player)-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            RETURN p.player_name, SUM(stats.assists) AS assists
            ORDER BY assists DESC
            LIMIT 10
        """
    },

    "top_goalkeepers_saves": {
        "intent": "top_players_position",
        "entities": ["season"],
        "cypher": """
            MATCH (p:Player)-[:PLAYS_AS]->(:Position {name: "GK"})
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            RETURN p.player_name, SUM(stats.saves) AS saves
            ORDER BY saves DESC
            LIMIT 10
        """
    },

    # =========================================================
    # 5. TEAM ANALYSIS
    # =========================================================

    "team_stats_by_fixture": {
        "intent": "team_analysis",
        "entities": ["team", "season"],
        "cypher": """
            MATCH (f:Fixture {season: $season})
            OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home:Team {name: $team})
            OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team {name: $team})
            WITH f, home, away
            WHERE home IS NOT NULL OR away IS NOT NULL
            MATCH (p:Player)-[stats:PLAYED_IN]->(f)
            RETURN f.fixture_number AS gw,
                   SUM(stats.total_points) AS total_points,
                   SUM(stats.goals_scored) AS goals,
                   SUM(stats.assists) AS assists
        """
    },

    "team_defensive_record": {
        "intent": "team_analysis",
        "entities": ["team", "season"],
        "cypher": """
            MATCH (f:Fixture {season: $season})
            OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(h:Team {name: $team})
            OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(a:Team {name: $team})
            WITH f, h, a
            WHERE h IS NOT NULL OR a IS NOT NULL
            MATCH (p:Player)-[stats:PLAYED_IN]->(f)
            RETURN f.fixture_number AS gw,
                   SUM(stats.clean_sheets) AS clean_sheets,
                   SUM(stats.goals_conceded) AS conceded
        """
    },

    # =========================================================
    # 6. FIXTURE LISTS
    # =========================================================

    "team_fixtures_list": {
        "intent": "team_fixtures",
        "entities": ["team", "season"],
        "cypher": """
            MATCH (f:Fixture {season: $season})
            OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home:Team {name: $team})
            OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team {name: $team})
            WITH f, home, away
            WHERE home IS NOT NULL OR away IS NOT NULL
            RETURN f.fixture_number, f.kickoff_time,
                   CASE WHEN home IS NOT NULL THEN "Home" ELSE "Away" END AS venue
            ORDER BY f.fixture_number
        """
    },

    "fixture_by_gameweek": {
        "intent": "team_fixtures",
        "entities": ["season", "gameweek"],
        "cypher": """
            MATCH (f:Fixture {season: $season, fixture_number: $gameweek})
            MATCH (f)-[:HAS_HOME_TEAM]->(home:Team)
            MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team)
            RETURN home.name AS home, away.name AS away, f.kickoff_time
        """
    },

    # =========================================================
    # 7. FIXTURE DIFFICULTY (FDR)
    # =========================================================

    "team_fixture_difficulty": {
        "intent": "fixture_difficulty",
        "entities": ["team", "season"],
        "cypher": """
            MATCH (f:Fixture {season: $season})
            OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home:Team {name: $team})
            OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team {name: $team})
            WITH f, home, away
            WHERE home IS NOT NULL OR away IS NOT NULL
            RETURN f.fixture_number, f.kickoff_time, f.difficulty
        """
    },

    # =========================================================
    # 8. SEARCH
    # =========================================================

    "search_player": {
        "intent": "search_player",
        "entities": ["players"],
        "cypher": """
            MATCH (p:Player {player_name: $players})
            OPTIONAL MATCH (p)-[:PLAYS_AS]->(pos:Position)
            RETURN p.player_name, p.player_element, pos.name AS position
        """
    },

    "search_team": {
        "intent": "search_team",
        "entities": ["team"],
        "cypher": """
            MATCH (t:Team {name: $team})
            RETURN t.name
        """
    },

    "search_player_team": {
        "intent": "search_player",
        "entities": ["players"],
        "cypher": """
            MATCH (p:Player {player_name: $players})-[:PLAYED_IN]->(f:Fixture)
            MATCH (f)-[:HAS_HOME_TEAM|HAS_AWAY_TEAM]->(t:Team)
            RETURN DISTINCT t.name AS team
        """
    },

    # =========================================================
    # 9. RECOMMENDATIONS
    # =========================================================

    "recommend_player_top_points": {
        "intent": "recommend_player",
        "entities": ["season"],
        "cypher": """
            MATCH (p:Player)-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            RETURN p.player_name, SUM(stats.total_points) AS points
            ORDER BY points DESC
            LIMIT 5
        """
    },

    "recommend_budget_midfielders": {
        "intent": "recommend_player",
        "entities": ["season"],
        "cypher": """
            MATCH (p:Player)-[:PLAYS_AS]->(:Position {name: "MID"})
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            WHERE p.cost <= 6.0
            RETURN p.player_name, p.cost, SUM(stats.total_points) AS points
            ORDER BY points DESC
            LIMIT 10
        """
    },

    "recommend_clean_sheet_defenders": {
        "intent": "recommend_player",
        "entities": ["season"],
        "cypher": """
            MATCH (p:Player)-[:PLAYS_AS]->(:Position {name: "DEF"})
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            RETURN p.player_name, SUM(stats.clean_sheets) AS clean_sheets
            ORDER BY clean_sheets DESC
            LIMIT 10
        """
    },

    # =========================================================
    # 10. ADVANCED QUERIES
    # =========================================================

    "player_form_last_5": {
        "intent": "player_performance",
        "entities": ["players", "season"],
        "cypher": """
            MATCH (p:Player {player_name: $players})-[stats:PLAYED_IN]->(f:Fixture {season: $season})
            RETURN p.player_name, COLLECT(stats.total_points)[-5..] AS last_5_points
        """
    },

    "team_top_scorer": {
        "intent": "team_analysis",
        "entities": ["team", "season"],
        "cypher": """
            MATCH (t:Team {name: $team})<-[:HAS_HOME_TEAM|HAS_AWAY_TEAM]-(f:Fixture {season: $season})
            MATCH (p:Player)-[stats:PLAYED_IN]->(f)
            RETURN p.player_name, SUM(stats.goals_scored) AS goals
            ORDER BY goals DESC
            LIMIT 1
        """
    },

    "team_conceded_total": {
        "intent": "team_analysis",
        "entities": ["team", "season"],
        "cypher": """
            MATCH (f:Fixture {season: $season})
            OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home:Team {name: $team})
            OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team {name: $team})
            WITH f, home, away
            WHERE home IS NOT NULL OR away IS NOT NULL
            MATCH (p:Player)-[stats:PLAYED_IN]->(f)
            RETURN SUM(stats.goals_conceded) AS total_conceded
        """
    },

}