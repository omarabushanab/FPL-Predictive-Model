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
        MATCH (gw)-[:HAS_FIXTURE]->(f:Fixture)
        MATCH (p)-[stats:PLAYED_IN]->(f)
        RETURN p.player_name AS player,
               s.season_name AS season,
               gw.GW_number AS gameweek,
               f.fixture_number AS fixture,
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
            p.player_name AS player_name,
            f.fixture_number AS fixture,
            f.season AS season,
            SUM(stats.total_points) AS total_points,
            SUM(stats.goals_scored) AS goals,
            SUM(stats.assists) AS assists,
            SUM(stats.minutes) AS minutes
    """
},

"player_history": {
    "intent": "player_history",
    "entities": ["players"],
    "cypher": """
        MATCH (p:Player)
        WHERE p.player_name IN $players
        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        RETURN p.player_name AS player,
               SUM(stats.total_points) AS total_points,
               SUM(stats.goals_scored) AS goals,
               SUM(stats.assists) AS assists,
               SUM(stats.minutes) AS minutes
        ORDER BY total_points DESC
    """
},


"compare_players": {
    "intent": "compare_players",
    "entities": ["players", "season"],
    "cypher": """
        MATCH (p:Player)
        WHERE p.player_name IN $players
        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season
        WITH p.player_name AS player_name, f.season AS season,
             SUM(stats.total_points) AS total_points,
             SUM(stats.goals_scored) AS goals,
             SUM(stats.assists) AS assists,
             SUM(stats.minutes) AS minutes
        RETURN player_name, season, total_points, goals, assists, minutes
        ORDER BY player_name, season
    """
},

"top_players_position": {
    "intent": "top_players_position",
    "entities": ["positions", "season", "gameweek"],
    "cypher": """
        MATCH (s:Season)-[:HAS_GW]->(gw:Gameweek)-[:HAS_FIXTURE]->(f:Fixture)
        WHERE s.season_name IN $season
          AND gw.GW_number IN $gameweek

        MATCH (p:Player)-[:PLAYS_AS]->(pos:Position)
        WHERE pos.name IN $positions

        MATCH (p)-[stats:PLAYED_IN]->(f)

        WITH p, pos, SUM(stats.total_points) AS total_points,
                  SUM(stats.goals_scored) AS goals,
                  SUM(stats.assists) AS assists
        RETURN 
               p.player_name AS player,
               pos.name AS position,
               total_points,
               goals,
               assists
        ORDER BY total_points DESC
        LIMIT 10
    """
},

# =========================================================
# 2. TEAM ANALYSIS
# =========================================================

"team_analysis": {
    "intent": "team_analysis",
    "entities": ["teams", "season"],
    "cypher": """
        MATCH (s:Season)-[:HAS_GW]->(gw:Gameweek)-[:HAS_FIXTURE]->(f:Fixture)
        WHERE s.season_name IN $season

        MATCH (t:Team)
        WHERE t.name IN $teams

        // Get players who played in the team's fixture (home or away)
        MATCH (p:Player)-[stats:PLAYED_IN]->(f)
        WHERE (f)-[:HAS_HOME_TEAM]->(t) OR (f)-[:HAS_AWAY_TEAM]->(t)

        WITH t, SUM(stats.total_points) AS team_points,
            SUM(stats.goals_scored) AS goals,
            SUM(stats.assists) AS assists

        RETURN  
            t.name AS team_name,
            team_points,
            goals,
            assists

    """
},

"team_fixtures": {
    "intent": "team_fixtures",
    "entities": ["teams", "season", "gameweek"],
    "cypher": """
        MATCH (s:Season)-[:HAS_GW]->(gw:Gameweek)-[:HAS_FIXTURE]->(f:Fixture)
        WHERE s.season_name IN $season
        AND gw.GW_number IN $gameweek

        MATCH (t:Team)
        WHERE t.name IN $teams

        MATCH (f)-[:HAS_HOME_TEAM]->(home:Team)
        MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team)

        WHERE t = home OR t = away

        WITH DISTINCT f, t, home, away, gw

        RETURN t.name AS team_name,
            f.fixture_number AS fixture,
            CASE WHEN t = home THEN away.name ELSE home.name END AS opponent,
            f.kickoff_time AS kickoff_time,
            gw.GW_number AS gameweek
        ORDER BY gw.GW_number, f.fixture_number



    """
},

"fixture_difficulty": {
    "intent": "fixture_difficulty",
    "entities": ["teams", "season", "gameweek"],
    "cypher": """
        MATCH (t:Team)
        WHERE t.team_name IN $teams
        MATCH (t)-[:HAS_FIXTURE]->(f:Fixture)
        WHERE f.season IN $season AND f.gameweek IN $gameweek
        RETURN t.team_name AS team,
               f.fixture_number AS fixture,
               f.opponent AS opponent,
               f.difficulty_rating AS difficulty
        ORDER BY difficulty DESC
    """
},

# =========================================================
# 3. SEARCH / LOOKUP
# =========================================================

"search_player": {
    "intent": "search_player",
    "entities": ["players"],
    "cypher": """
        MATCH (p:Player)
        WHERE p.player_name IN $players
        RETURN p.player_name AS player,
               p.position AS position,
               p.team AS team
    """
},

"search_team": {
    "intent": "search_team",
    "entities": ["teams"],
    "cypher": """
        MATCH (t:Team)
        WHERE t.team_name IN $teams
        RETURN t.team_name AS team,
               t.stadium AS stadium,
               t.manager AS manager
    """
},

# =========================================================
# 4. RECOMMENDATIONS
# =========================================================

"recommend_player": {
    "intent": "recommend_player",
    "entities": ["positions", "season", "gameweek"],
    "cypher": """
        MATCH (p:Player)
        WHERE p.position IN $positions
        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season AND f.gameweek IN $gameweek
        RETURN p.player_name AS player,
               SUM(stats.total_points) AS total_points,
               SUM(stats.goals_scored) AS goals,
               SUM(stats.assists) AS assists
        ORDER BY total_points DESC
        LIMIT 5
    """
},

# =========================================================
# 5. STATISTICS
# =========================================================

"players_stat_summary": {
    "intent": "player_performance",
    "entities": ["players", "season"],
    "cypher": """
        MATCH (p:Player)
        WHERE p.player_name IN $players
        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season
        RETURN p.player_name AS player,
               AVG(stats.total_points) AS avg_points,
               AVG(stats.goals_scored) AS avg_goals,
               AVG(stats.assists) AS avg_assists,
               AVG(stats.minutes) AS avg_minutes
    """
},

"team_stat_summary": {
    "intent": "team_analysis",
    "entities": ["teams", "season"],
    "cypher": """
        MATCH (t:Team)<-[:PART_OF]-(p:Player)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE t.team_name IN $teams AND f.season IN $season
        RETURN t.team_name AS team,
               AVG(stats.total_points) AS avg_points,
               AVG(stats.goals_scored) AS avg_goals,
               AVG(stats.assists) AS avg_assists
    """
},

"top_players_overall": {
    "intent": "top_players_position",
    "entities": ["season", "gameweek"],
    "cypher": """
        MATCH (p:Player)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season AND f.gameweek IN $gameweek
        RETURN p.player_name AS player,
               SUM(stats.total_points) AS total_points
        ORDER BY total_points DESC
        LIMIT 10
    """
},

"players_minutes_leaders": {
    "intent": "player_performance",
    "entities": ["players", "season"],
    "cypher": """
        MATCH (p:Player)
        WHERE p.player_name IN $players
        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season
        RETURN p.player_name AS player,
               SUM(stats.minutes) AS total_minutes
        ORDER BY total_minutes DESC
    """
},

"players_goals_assists": {
    "intent": "player_performance",
    "entities": ["players", "season"],
    "cypher": """
        MATCH (p:Player)
        WHERE p.player_name IN $players
        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season
        RETURN p.player_name AS player,
               SUM(stats.goals_scored) AS goals,
               SUM(stats.assists) AS assists
        ORDER BY goals DESC
    """
},

"team_clean_sheets": {
    "intent": "team_analysis",
    "entities": ["teams", "season"],
    "cypher": """
        MATCH (t:Team)<-[:PART_OF]-(p:Player)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE t.team_name IN $teams AND f.season IN $season
        RETURN t.team_name AS team,
               SUM(stats.clean_sheets) AS clean_sheets
        ORDER BY clean_sheets DESC
    """
},

"top_scoring_teams": {
    "intent": "team_analysis",
    "entities": ["season"],
    "cypher": """
        MATCH (t:Team)<-[:PART_OF]-(p:Player)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season
        RETURN t.team_name AS team,
               SUM(stats.goals_scored) AS total_goals
        ORDER BY total_goals DESC
    """
},

"players_form_last_5gw": {
    "intent": "player_performance",
    "entities": ["players", "season", "gameweek"],
    "cypher": """
        MATCH (p:Player)
        WHERE p.player_name IN $players
        MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season AND f.gameweek IN $gameweek
        RETURN p.player_name AS player,
               SUM(stats.total_points) AS points_last_5
        ORDER BY points_last_5 DESC
    """
},

"recommended_captain": {
    "intent": "recommend_player",
    "entities": ["season", "gameweek"],
    "cypher": """
        MATCH (p:Player)-[stats:PLAYED_IN]->(f:Fixture)
        WHERE f.season IN $season AND f.gameweek IN $gameweek
        RETURN p.player_name AS player,
               SUM(stats.total_points) AS points
        ORDER BY points DESC
        LIMIT 1
    """
}

}