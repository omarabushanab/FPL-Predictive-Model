QUERY_LIBRARY = {

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

"player_performance_season_stat": {
  "intent": "player_performance",
  "entities": ["players", "season", "stat"],
  "cypher": """
    MATCH (p:Player)-[stats:PLAYED_IN]->(:Fixture)
          <-[:HAS_FIXTURE]-(:Gameweek)
          <-[:HAS_GW]-(s:Season)
    WHERE p.player_name IN $players
      AND s.season_name IN $season

    UNWIND $stat AS stat_name

    RETURN p.player_name AS player,
           stat_name AS stat,
           SUM(stats[stat_name]) AS total_value
    ORDER BY player, stat
  """
} ,

"player_history": {
  "intent": "player_history",
  "entities": ["players"],
  "cypher": """
    MATCH (p:Player)
    WHERE p.player_name IN $players

    MATCH (p)-[stats:PLAYED_IN]->(:Fixture)
          <-[:HAS_FIXTURE]-(:Gameweek)
          <-[:HAS_GW]-(s:Season)

    WITH p, s, collect(properties(stats)) AS stats_list

    RETURN p.player_name AS player,
           s.season_name AS season,
           reduce(result = {},
                  st IN stats_list |
                  {
                    minutes:           coalesce(result.minutes, 0) + coalesce(st.minutes, 0),
                    goals_scored:      coalesce(result.goals_scored, 0) + coalesce(st.goals_scored, 0),
                    assists:           coalesce(result.assists, 0) + coalesce(st.assists, 0),
                    total_points:      coalesce(result.total_points, 0) + coalesce(st.total_points, 0),
                    bonus:             coalesce(result.bonus, 0) + coalesce(st.bonus, 0),
                    clean_sheets:      coalesce(result.clean_sheets, 0) + coalesce(st.clean_sheets, 0),
                    goals_conceded:    coalesce(result.goals_conceded, 0) + coalesce(st.goals_conceded, 0),
                    own_goals:         coalesce(result.own_goals, 0) + coalesce(st.own_goals, 0),
                    penalties_saved:   coalesce(result.penalties_saved, 0) + coalesce(st.penalties_saved, 0),
                    penalties_missed:  coalesce(result.penalties_missed, 0) + coalesce(st.penalties_missed, 0),
                    yellow_cards:      coalesce(result.yellow_cards, 0) + coalesce(st.yellow_cards, 0),
                    red_cards:         coalesce(result.red_cards, 0) + coalesce(st.red_cards, 0),
                    saves:             coalesce(result.saves, 0) + coalesce(st.saves, 0),
                    bps:               coalesce(result.bps, 0) + coalesce(st.bps, 0),
                    influence:         coalesce(result.influence, 0) + coalesce(st.influence, 0),
                    creativity:        coalesce(result.creativity, 0) + coalesce(st.creativity, 0),
                    threat:            coalesce(result.threat, 0) + coalesce(st.threat, 0),
                    ict_index:         coalesce(result.ict_index, 0) + coalesce(st.ict_index, 0),
                    form:              coalesce(result.form, 0) + coalesce(st.form, 0)
                  }
           ) AS aggregated_stats

    ORDER BY player, season
  """
},

"compare_players": {
  "intent": "compare_players",
  "entities": ["players", "season"],
  "cypher": """
    MATCH (p:Player)
    WHERE p.player_name IN $players

    MATCH (p)-[stats:PLAYED_IN]->(:Fixture)
          <-[:HAS_FIXTURE]-(:Gameweek)
          <-[:HAS_GW]-(s:Season)
    WHERE s.season_name IN $season

    WITH p, s, collect(properties(stats)) AS stats_list

    RETURN p.player_name AS player,
           s.season_name AS season,
           reduce(result = {},
                  st IN stats_list |
                  {
                    minutes:           coalesce(result.minutes, 0) + coalesce(st.minutes, 0),
                    goals_scored:      coalesce(result.goals_scored, 0) + coalesce(st.goals_scored, 0),
                    assists:           coalesce(result.assists, 0) + coalesce(st.assists, 0),
                    total_points:      coalesce(result.total_points, 0) + coalesce(st.total_points, 0),
                    bonus:             coalesce(result.bonus, 0) + coalesce(st.bonus, 0),
                    clean_sheets:      coalesce(result.clean_sheets, 0) + coalesce(st.clean_sheets, 0),
                    goals_conceded:    coalesce(result.goals_conceded, 0) + coalesce(st.goals_conceded, 0),
                    own_goals:         coalesce(result.own_goals, 0) + coalesce(st.own_goals, 0),
                    penalties_saved:   coalesce(result.penalties_saved, 0) + coalesce(st.penalties_saved, 0),
                    penalties_missed:  coalesce(result.penalties_missed, 0) + coalesce(st.penalties_missed, 0),
                    yellow_cards:      coalesce(result.yellow_cards, 0) + coalesce(st.yellow_cards, 0),
                    red_cards:         coalesce(result.red_cards, 0) + coalesce(st.red_cards, 0),
                    saves:             coalesce(result.saves, 0) + coalesce(st.saves, 0),
                    bps:               coalesce(result.bps, 0) + coalesce(st.bps, 0),
                    influence:         coalesce(result.influence, 0) + coalesce(st.influence, 0),
                    creativity:        coalesce(result.creativity, 0) + coalesce(st.creativity, 0),
                    threat:            coalesce(result.threat, 0) + coalesce(st.threat, 0),
                    ict_index:         coalesce(result.ict_index, 0) + coalesce(st.ict_index, 0),
                    form:              coalesce(result.form, 0) + coalesce(st.form, 0)
                  }
           ) AS aggregated_stats

    ORDER BY season, aggregated_stats.total_points DESC
  """
},

"team_analysis_season": {
  "intent": "team_analysis",
  "entities": ["teams", "season"],
  "cypher": """
    MATCH (t:Team)
    WHERE t.name IN $teams

    MATCH (t)<-[:HAS_HOME_TEAM|HAS_AWAY_TEAM]-(f:Fixture)
          <-[:HAS_FIXTURE]-(:Gameweek)
          <-[:HAS_GW]-(s:Season)
    WHERE s.season_name IN $season

    MATCH (p:Player)-[stats:PLAYED_IN]->(f)

    WITH t, s, collect(properties(stats)) AS stats_list

    RETURN t.name AS team,
           s.season_name AS season,
           reduce(result = {},
                  st IN stats_list |
                  {
                    minutes:           coalesce(result.minutes, 0) + coalesce(st.minutes, 0),
                    goals_scored:      coalesce(result.goals_scored, 0) + coalesce(st.goals_scored, 0),
                    assists:           coalesce(result.assists, 0) + coalesce(st.assists, 0),
                    total_points:      coalesce(result.total_points, 0) + coalesce(st.total_points, 0),
                    bonus:             coalesce(result.bonus, 0) + coalesce(st.bonus, 0),
                    clean_sheets:      coalesce(result.clean_sheets, 0) + coalesce(st.clean_sheets, 0),
                    goals_conceded:    coalesce(result.goals_conceded, 0) + coalesce(st.goals_conceded, 0),
                    own_goals:         coalesce(result.own_goals, 0) + coalesce(st.own_goals, 0),
                    penalties_saved:   coalesce(result.penalties_saved, 0) + coalesce(st.penalties_saved, 0),
                    penalties_missed:  coalesce(result.penalties_missed, 0) + coalesce(st.penalties_missed, 0),
                    yellow_cards:      coalesce(result.yellow_cards, 0) + coalesce(st.yellow_cards, 0),
                    red_cards:         coalesce(result.red_cards, 0) + coalesce(st.red_cards, 0),
                    saves:             coalesce(result.saves, 0) + coalesce(st.saves, 0),
                    bps:               coalesce(result.bps, 0) + coalesce(st.bps, 0),
                    influence:         coalesce(result.influence, 0) + coalesce(st.influence, 0),
                    creativity:        coalesce(result.creativity, 0) + coalesce(st.creativity, 0),
                    threat:            coalesce(result.threat, 0) + coalesce(st.threat, 0),
                    ict_index:         coalesce(result.ict_index, 0) + coalesce(st.ict_index, 0),
                    form:              coalesce(result.form, 0) + coalesce(st.form, 0)
                  }
           ) AS aggregated_stats

    ORDER BY team, season
  """
},

  "team_analysis_stat": {
  "intent": "team_analysis",
  "entities": ["teams", "stat"],
  "cypher": """
    MATCH (t:Team)
    WHERE t.name IN $teams

    MATCH (t)<-[:HAS_HOME_TEAM|HAS_AWAY_TEAM]-(f:Fixture)
    MATCH (p:Player)-[stats:PLAYED_IN]->(f)

    UNWIND $stat AS stat_name

    RETURN t.name AS team,
           stat_name AS stat,
           SUM(stats[stat_name]) AS total_value
    ORDER BY team, stat
  """
},


"team_fixtures_season": {
  "intent": "team_fixtures",
  "entities": ["teams", "season"],
  "cypher": """
    MATCH (t:Team)
    WHERE t.name IN $teams

    MATCH (t)<-[:HAS_HOME_TEAM|HAS_AWAY_TEAM]-(f:Fixture)
          <-[:HAS_FIXTURE]-(:Gameweek)
          <-[:HAS_GW]-(s:Season)
    WHERE s.season_name IN $season

    OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home:Team)
    OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team)

    WITH t, f,
         CASE
           WHEN home = t THEN away
           ELSE home
         END AS opponent

    RETURN t.name AS team,
           opponent.name AS opponent,
           f.fixture_number AS fixture_number,
           f.kickoff_time AS kickoff_time
    ORDER BY kickoff_time
  """
},



"recommend_player_position_season": {
  "intent": "recommend_player",
  "entities": ["positions", "season", "stat"],
  "cypher": """
    MATCH (p:Player)-[:PLAYS_AS]->(pos:Position)
    WHERE pos.name IN $positions

    MATCH (s:Season)-[:HAS_GW]->(gw:Gameweek)-[:HAS_FIXTURE]->(f:Fixture)
    WHERE s.season_name IN $season

    MATCH (p)-[stats:PLAYED_IN]->(f)

    WITH p, stats, $stat AS stat_list
    // sum over all requested stats
    WITH p, REDUCE(total = 0, key IN stat_list | total + coalesce(stats[key], 0)) AS score
    RETURN p.player_name AS player, SUM(score) AS total_score
    ORDER BY total_score DESC
    LIMIT 5
  """
},

"recommend_player_position_gw": {
  "intent": "recommend_player",
  "entities": ["positions", "gameweek", "stat"],
  "cypher": """
    MATCH (p:Player)-[:PLAYS_AS]->(pos:Position)
    WHERE pos.name IN $positions

    MATCH (gw:Gameweek)-[:HAS_FIXTURE]->(f:Fixture)
    WHERE gw.GW_number IN $gameweek

    MATCH (p)-[stats:PLAYED_IN]->(f)

    WITH p, stats, $stat AS stat_list
    // sum over all requested stats
    WITH p, REDUCE(total = 0, key IN stat_list | total + coalesce(stats[key], 0)) AS score

    RETURN p.player_name AS player, SUM(score) AS total_score
    ORDER BY total_score DESC
    LIMIT 5
  """
},

"top_scorers_by_position": {
  "intent": "recommend_player",
  "entities": ["positions", "season"],
  "cypher": """
    MATCH (p:Player)-[:PLAYS_AS]->(pos:Position)
    WHERE pos.name IN $positions

    MATCH (s:Season)-[:HAS_GW]->(:Gameweek)-[:HAS_FIXTURE]->(f:Fixture)
    WHERE s.season_name IN $season

    MATCH (p)-[stats:PLAYED_IN]->(f)

    WITH p, pos, SUM(stats.goals_scored) AS contributions
    RETURN pos.name AS position,
           p.player_name AS player,
           contributions
    ORDER BY position, contributions DESC
    LIMIT 5
  """
},

# ===================== Fixture difficulty queries =====================

  "fixture_difficulty": {
    "intent": "fixture_difficulty", 
    "entities": ["teams", "season", "gameweek"],
    "cypher": """
      // Get all fixtures for specified teams in season/gameweeks
      MATCH (t:Team)
      WHERE t.name IN $teams
      
      MATCH (s:Season)
      WHERE s.season_name IN $season
      
      MATCH (s)-[:HAS_GW]->(gw:Gameweek)
      WHERE gw.GW_number IN $gameweek
      
      MATCH (gw)-[:HAS_FIXTURE]->(f:Fixture)
      WHERE (f)-[:HAS_HOME_TEAM]->(t) OR (f)-[:HAS_AWAY_TEAM]->(t)
      
      // Get opponent team
      OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home:Team)
      OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away:Team)
      
      WITH t, f, gw, s, home, away,
           CASE WHEN home = t THEN away ELSE home END AS opponent
      
      // Calculate team's historical points
      OPTIONAL MATCH (s)-[:HAS_GW]->(past_gw:Gameweek)
      WHERE past_gw.GW_number < gw.GW_number
      
      OPTIONAL MATCH (past_gw)-[:HAS_FIXTURE]->(past_f:Fixture)
      WHERE (past_f)-[:HAS_HOME_TEAM]->(t) OR (past_f)-[:HAS_AWAY_TEAM]->(t)
      
      OPTIONAL MATCH (player:Player)-[stats:PLAYED_IN]->(past_f)
      WITH t, f, opponent, gw, s, home, away,
           COALESCE(SUM(stats.total_points), 0) AS team_points
      
      // Calculate opponent's historical points  
      OPTIONAL MATCH (s)-[:HAS_GW]->(opp_past_gw:Gameweek)
      WHERE opp_past_gw.GW_number < gw.GW_number
      
      OPTIONAL MATCH (opp_past_gw)-[:HAS_FIXTURE]->(opp_past_f:Fixture)
      WHERE (opp_past_f)-[:HAS_HOME_TEAM]->(opponent) OR (opp_past_f)-[:HAS_AWAY_TEAM]->(opponent)
      
      OPTIONAL MATCH (opp_player:Player)-[opp_stats:PLAYED_IN]->(opp_past_f)
      WITH t, f, opponent, gw, team_points, home, away,
           COALESCE(SUM(opp_stats.total_points), 0) AS opponent_points
      
      // Calculate difficulty
      WITH t, f, opponent, gw, team_points, opponent_points,
           CASE
             WHEN team_points = 0 AND opponent_points = 0 THEN 'Unknown'
             WHEN opponent_points > team_points * 1.2 THEN 'Hard'
             WHEN opponent_points < team_points * 0.8 THEN 'Easy'
             ELSE 'Medium'
           END AS difficulty
      
      RETURN t.name AS team,
             opponent.name AS opponent,
             team_points,
             opponent_points,
             difficulty,
             gw.GW_number AS gameweek,
             f.fixture_number AS fixture,
             f.kickoff_time AS kickoff_time
      ORDER BY f.kickoff_time
    """
  },


# ===================== search_player =====================

"search_player": {
  "intent": "search_player",
  "entities": ["players"],
  "cypher": """
    MATCH (p:Player)
    WHERE p.player_name IN $players

    OPTIONAL MATCH (p)-[:PLAYS_AS]->(pos:Position)
    OPTIONAL MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
    OPTIONAL MATCH (f)-[:HAS_HOME_TEAM]->(home_team:Team)
    OPTIONAL MATCH (f)-[:HAS_AWAY_TEAM]->(away_team:Team)
    OPTIONAL MATCH (home_team)<-[:HAS_HOME_TEAM|:HAS_AWAY_TEAM]-(f)

    WITH p, pos, collect(DISTINCT {
      fixture_number: f.fixture_number,
      season: f.season,
      kickoff_time: f.kickoff_time,
      team: CASE WHEN home_team IS NOT NULL AND (f)-[:HAS_HOME_TEAM]->(home_team) THEN home_team.name
                 ELSE away_team.name END,
      total_points: stats.total_points,
      goals_scored: stats.goals_scored,
      assists: stats.assists
    }) AS recent_stats

    RETURN p.player_name AS player,
           pos.name AS position,
           recent_stats
    ORDER BY player
  """
} ,
"search_team_pos": {
  "intent": "search_team",
  "entities": ["teams", "season", "position"],
  "cypher": """
    // Match season
    MATCH (s:Season)
    WHERE s.season_name IN $season

    // Match team
    MATCH (t:Team)
    WHERE t.name IN $teams

    // Match fixtures in season
    MATCH (s)-[:HAS_GW]->(:Gameweek)-[:HAS_FIXTURE]->(f:Fixture)

    // Match players AND bind position immediately
    MATCH (p:Player)-[:PLAYED_IN]->(f)
    MATCH (p)-[:PLAYS_AS]->(pos:Position)
    WHERE pos.name IN $position

    // Check if fixture involves the team
    WITH s, t, pos, p,
         CASE
           WHEN (f)-[:HAS_HOME_TEAM]->(t)
             OR (f)-[:HAS_AWAY_TEAM]->(t)
           THEN 1 ELSE 0
         END AS is_team_fixture

    // Aggregate per player (position-safe)
    WITH s, t, pos, p,
         sum(is_team_fixture) AS fixtures_with_team,
         count(*) AS total_fixtures_played

    // Player must mainly belong to the team
    WHERE fixtures_with_team > 0
      AND fixtures_with_team * 2 >= total_fixtures_played

    RETURN
      t.name AS team,
      s.season_name AS season,
      pos.name AS position_name,
      collect(DISTINCT {
        player_name: p.player_name
      }) AS players_info

    ORDER BY team, season, position_name
  """
}




}