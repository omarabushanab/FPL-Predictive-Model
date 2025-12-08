QUERY_LIBRARY = {

    # =========================================================
    # 1. PLAYER PERFORMANCE / MATCH STATS
    # =========================================================

    "player_performance_gw": {
        "intent": "player_performance",
        "entities": ["players", "season", "gameweek"],
        "cypher": """
            MATCH (p:Player)
            WHERE p.player_name IN $players
            MATCH (s:Season)
            WHERE s.season_name IN $season
            MATCH (s)-[:HAS_GW]->(gw:Gameweek)
            WHERE gw.GW_number IN $gameweek
            MATCH (gw)-[:HAS_FIXTURE]->(f:Fixture)
            MATCH (p)-[stats:PLAYED_IN]->(f)
            RETURN p.player_name, stats
        """
    },

    "player_performance_season": {
        "intent": "player_performance",
        "entities": ["players", "season"],
        "cypher": """
            MATCH (p:Player)
            WHERE p.player_name IN $players
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
            RETURN p.player_name,
                   SUM(stats.total_points) AS total_points,
                   SUM(stats.goals_scored) AS goals,
                   SUM(stats.assists) AS assists
        """
    },

    "player_stat_single_value": {
        "intent": "player_performance",
        "entities": ["players", "season", "stat"],
        "cypher": """
            MATCH (p:Player)
            WHERE p.player_name IN $players
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
            RETURN p.player_name AS player,
                   $stat[0] AS stat,
                   SUM(stats[$stat[0]]) AS total_stat
        """
    },

    # =========================================================
    # 2. PLAYER HISTORY
    # =========================================================

    "player_history_all": {
        "intent": "player_history",
        "entities": ["players"],
        "cypher": """
            MATCH (p:Player)
            WHERE p.player_name IN $players
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            RETURN f.season, f.fixture_number AS gw, stats
            ORDER BY f.season, gw
        """
    },

    "player_history_season": {
        "intent": "player_history",
        "entities": ["players", "season"],
        "cypher": """
            MATCH (p:Player)
            WHERE p.player_name IN $players
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
            RETURN f.fixture_number AS gw, stats
            ORDER BY gw
        """
    },

    "player_history_specific_stat": {
        "intent": "player_history",
        "entities": ["players", "stat"],
        "cypher": """
            MATCH (p:Player)
            WHERE p.player_name IN $players
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            RETURN f.season,
                   f.fixture_number AS gw,
                   stats[$stat[0]] AS value
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
            MATCH (p:Player)
            WHERE p.player_name IN $players
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
            RETURN p.player_name AS player,
                   SUM(stats.total_points) AS points
            ORDER BY points DESC
        """
    },

    "compare_players_stat": {
        "intent": "compare_players",
        "entities": ["players", "season", "stat"],
        "cypher": """
            MATCH (p:Player)
            WHERE p.player_name IN $players
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
            RETURN p.player_name AS player,
                   SUM(stats[$stat[0]]) AS stat_value
            ORDER BY stat_value DESC
        """
    },

    # =========================================================
    # 4. TOP PLAYERS / RANKINGS
    # =========================================================

    "top_players_by_position": {
        "intent": "top_players_position",
        "entities": ["positions", "season"],
        "cypher": """
            MATCH (p:Player)-[:PLAYS_AS]->(pos:Position)
            WHERE pos.name IN $positions
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
            RETURN p.player_name,
                   SUM(stats.total_points) AS total_points
            ORDER BY total_points DESC
            LIMIT 10
        """
    },

    "top_scorer_season": {
        "intent": "top_players_position",
        "entities": ["season"],
        "cypher": """
            MATCH (p:Player)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
            RETURN p.player_name,
                   SUM(stats.goals_scored) AS goals
            ORDER BY goals DESC
            LIMIT 10
        """
    },

    "top_assisters_season": {
        "intent": "top_players_position",
        "entities": ["season"],
        "cypher": """
            MATCH (p:Player)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
            RETURN p.player_name,
                   SUM(stats.assists) AS assists
            ORDER BY assists DESC
            LIMIT 10
        """
    },

    "top_goalkeepers_saves": {
        "intent": "top_players_position",
        "entities": ["season"],
        "cypher": """
            MATCH (p:Player)-[:PLAYS_AS]->(:Position {name: "GK"})
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
            RETURN p.player_name,
                   SUM(stats.saves) AS saves
            ORDER BY saves DESC
            LIMIT 10
        """
    },

    # =========================================================
    # 5. TEAM ANALYSIS
    # =========================================================

    "team_stats_by_fixture": {
        "intent": "team_analysis",
        "entities": ["teams", "season"],
        "cypher": """
            MATCH (f:Fixture)
            WHERE f.season IN $season
            OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home:Team)
            OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team)
            WHERE home.name IN $teams OR away.name IN $teams
            MATCH (p:Player)-[stats:PLAYED_IN]->(f)
            RETURN f.fixture_number AS gw,
                   SUM(stats.total_points) AS total_points,
                   SUM(stats.goals_scored) AS goals,
                   SUM(stats.assists) AS assists
        """
    },

    "team_defensive_record": {
        "intent": "team_analysis",
        "entities": ["teams", "season"],
        "cypher": """
            MATCH (f:Fixture)
            WHERE f.season IN $season
            OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(h:Team)
            OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(a:Team)
            WHERE h.name IN $teams OR a.name IN $teams
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
        "entities": ["teams", "season"],
        "cypher": """
            MATCH (f:Fixture)
            WHERE f.season IN $season
            OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home:Team)
            OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team)
            WHERE home.name IN $teams OR away.name IN $teams
            RETURN f.fixture_number,
                   f.kickoff_time,
                   CASE WHEN home IS NOT NULL THEN "Home" ELSE "Away" END AS venue
            ORDER BY f.fixture_number
        """
    },

    "fixture_by_gameweek": {
        "intent": "team_fixtures",
        "entities": ["season", "gameweek"],
        "cypher": """
            MATCH (f:Fixture)
            WHERE f.season IN $season AND f.fixture_number IN $gameweek
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
        "entities": ["teams", "season"],
        "cypher": """
            MATCH (f:Fixture)
            WHERE f.season IN $season
            OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home:Team)
            OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team)
            WHERE home.name IN $teams OR away.name IN $teams
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
            MATCH (p:Player)
            WHERE p.player_name IN $players
            OPTIONAL MATCH (p)-[:PLAYS_AS]->(pos:Position)
            RETURN p.player_name, p.player_element, pos.name AS position
        """
    },

    "search_team": {
        "intent": "search_team",
        "entities": ["teams"],
        "cypher": """
            MATCH (t:Team)
            WHERE t.name IN $teams
            RETURN t.name
        """
    },

    "search_player_team": {
        "intent": "search_player",
        "entities": ["players"],
        "cypher": """
            MATCH (p:Player)
            WHERE p.player_name IN $players
            MATCH (p)-[:PLAYED_IN]->(f:Fixture)
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
            MATCH (p:Player)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
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
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season AND p.cost <= 6.0
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
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
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
            MATCH (p:Player)
            WHERE p.player_name IN $players
            MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season IN $season
            RETURN p.player_name,
                   COLLECT(stats.total_points)[-5..] AS last_5_points
        """
    },

    "team_top_scorer": {
        "intent": "team_analysis",
        "entities": ["teams", "season"],
        "cypher": """
            MATCH (t:Team)
            WHERE t.name IN $teams
            MATCH (t)<-[:HAS_HOME_TEAM|HAS_AWAY_TEAM]-(f:Fixture)
            WHERE f.season IN $season
            MATCH (p:Player)-[stats:PLAYED_IN]->(f)
            RETURN p.player_name,
                   SUM(stats.goals_scored) AS goals
            ORDER BY goals DESC
            LIMIT 1
        """
    },

    "team_conceded_total": {
        "intent": "team_analysis",
        "entities": ["teams", "season"],
        "cypher": """
            MATCH (f:Fixture)
            WHERE f.season IN $season
            OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home:Team)
            OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team)
            WHERE home.name IN $teams OR away.name IN $teams
            MATCH (p:Player)-[stats:PLAYED_IN]->(f)
            RETURN SUM(stats.goals_conceded) AS total_conceded
        """
    }

}
