QUERY_LIBRARY = {

# =========================================================
# 1. PLAYER PERFORMANCE
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
          AND gw.season IN $season

        MATCH (gw)-[:HAS_FIXTURE]->(f:Fixture)
        MATCH (p)-[stats:PLAYED_IN]->(f)

        RETURN 
            p.player_name AS player,
            s.season_name AS season,
            gw.GW_number AS gameweek,
            stats
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

        RETURN 
            p.player_name AS player,
            f.season AS season,
            SUM(stats.total_points) AS total_points,
            SUM(stats.goals_scored) AS goals,
            SUM(stats.assists) AS assists,
            SUM(stats.minutes) AS minutes
    """
},

# =========================================================
# 2. PLAYER RANKINGS
# =========================================================

"top_players_by_position": {
    "intent": "player_ranking",
    "entities": ["position", "season"],
    "cypher": """
        MATCH (p:Player)-[:PLAYS_AS]->(pos:Position)
        WHERE pos.name IN $position

        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season

        RETURN 
            pos.name AS position,
            f.season AS season,
            p.player_name AS player,
            SUM(stats.total_points) AS points
        ORDER BY points DESC
        LIMIT 10
    """
},

"top_players_by_goals": {
    "intent": "player_ranking",
    "entities": ["season"],
    "cypher": """
        MATCH (p:Player)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season

        RETURN 
            f.season AS season,
            p.player_name AS player,
            SUM(stats.goals_scored) AS goals
        ORDER BY goals DESC
        LIMIT 10
    """
},

"top_players_by_assists": {
    "intent": "player_ranking",
    "entities": ["season"],
    "cypher": """
        MATCH (p:Player)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season

        RETURN 
            f.season AS season,
            p.player_name AS player,
            SUM(stats.assists) AS assists
        ORDER BY assists DESC
        LIMIT 10
    """
},

"player_avg_points": {
    "intent": "player_statistics",
    "entities": ["players", "season"],
    "cypher": """
        MATCH (p:Player)
        WHERE p.player_name IN $players

        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season

        RETURN 
            p.player_name AS player,
            f.season AS season,
            AVG(stats.total_points) AS avg_points
    """
},

# =========================================================
# 3. TEAM ANALYSIS
# =========================================================

"team_total_points": {
    "intent": "team_analysis",
    "entities": ["team", "season"],
    "cypher": """
        MATCH (t:Team)
        WHERE t.name IN $team

        MATCH (t)<-[:PLAYS_FOR]-(p:Player)
        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season

        RETURN 
            t.name AS team,
            f.season AS season,
            SUM(stats.total_points) AS team_points
    """
},

"team_goals_scored": {
    "intent": "team_analysis",
    "entities": ["team", "season"],
    "cypher": """
        MATCH (t:Team)
        WHERE t.name IN $team

        MATCH (t)<-[:PLAYS_FOR]-(p:Player)
        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season

        RETURN 
            t.name AS team,
            f.season AS season,
            SUM(stats.goals_scored) AS goals
    """
},

"team_top_scorers": {
    "intent": "team_analysis",
    "entities": ["team", "season"],
    "cypher": """
        MATCH (t:Team)
        WHERE t.name IN $team

        MATCH (t)<-[:PLAYS_FOR]-(p:Player)
        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season

        RETURN 
            t.name AS team,
            f.season AS season,
            p.player_name AS player,
            SUM(stats.goals_scored) AS goals
        ORDER BY goals DESC
        LIMIT 5
    """
},

# =========================================================
# 4. FIXTURES
# =========================================================

"fixtures_by_gameweek": {
    "intent": "fixture_query",
    "entities": ["season", "gameweek"],
    "cypher": """
        MATCH (s:Season)
        WHERE s.season_name IN $season

        MATCH (s)-[:HAS_GW]->(gw:Gameweek)
        WHERE gw.GW_number IN $gameweek
          AND gw.season IN $season

        MATCH (gw)-[:HAS_FIXTURE]->(f:Fixture)

        RETURN 
            s.season_name AS season,
            gw.GW_number AS gameweek,
            f.fixture_number AS fixture,
            f.kickoff_time AS kickoff_time
    """
},

"fixtures_by_team": {
    "intent": "fixture_query",
    "entities": ["team", "season"],
    "cypher": """
        MATCH (t:Team)
        WHERE t.name IN $team

        MATCH (t)-[:PARTICIPATES_IN]->(f:Fixture)
        WHERE f.season IN $season

        RETURN 
            t.name AS team,
            f.season AS season,
            f.fixture_number AS fixture,
            f.kickoff_time AS kickoff_time
    """
},

# =========================================================
# 5. STATISTICS
# =========================================================

"league_total_goals": {
    "intent": "statistics",
    "entities": ["season"],
    "cypher": """
        MATCH ()-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season

        RETURN 
            f.season AS season,
            SUM(stats.goals_scored) AS total_goals
    """
},

"league_total_assists": {
    "intent": "statistics",
    "entities": ["season"],
    "cypher": """
        MATCH ()-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season

        RETURN 
            f.season AS season,
            SUM(stats.assists) AS total_assists
    """
},

"most_popular_position": {
    "intent": "statistics",
    "entities": [],
    "cypher": """
        MATCH (p:Player)-[:PLAYS_AS]->(pos:Position)
        RETURN 
            pos.name AS position,
            COUNT(p) AS count
        ORDER BY count DESC
        LIMIT 1
    """
}

}
