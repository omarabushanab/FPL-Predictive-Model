import pandas as pd 
from neo4j import GraphDatabase



def read_config(path):
    config = {}
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip().strip('"')
    return config

config = read_config("config.txt")

uri = config["uri"]
user = config["user"]
password = config["password"]



class Neo4jConnection:
    def __init__(self,uri,user,password):
        self.driver = GraphDatabase.driver(uri,auth=(user,password))

    def clear_database(self):
        """
        DELETE all nodes and relationships from the database
        """
        query = "MATCH (n) DETACH DELETE n"
        result = self.execute_query(query)
        print("cleared all nodes and relationships from the databases")

    def close(self):
        self.driver.close()

    def execute_query(self,query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]
        


conn = Neo4jConnection(uri,user,password)
conn.clear_database()

df = pd.read_csv("fpl_two_seasons.csv")


def createSeasons(conn, df):
    
    seasons = df['season'].unique().tolist()

    query = """
    UNWIND $seasons AS season_name
    MERGE (s: Season {season_name: season_name})
    RETURN count(s)
    """
    return conn.execute_query(query, {"seasons": seasons})

def createGW(conn, df):
    gameweek_data = df[['season', 'GW']].drop_duplicates()
    gameweeks = gameweek_data.to_dict('records')
    query = """
    UNWIND $gameweeks AS gw
    MERGE (g:Gameweek {season: gw.season, GW_number: gw.GW})
    RETURN count(g)
    """
    results = conn.execute_query(query, parameters={"gameweeks": gameweeks})
    print(f"Successfully processed {results} Gameweek nodes")

def createFixtures(conn,df):
    fixture_data = df[['season','fixture','kickoff_time']].drop_duplicates()
    fixtures = fixture_data.to_dict('records')

    query = """
    UNWIND $fixtures AS fixture
    MERGE (f:Fixture {season:fixture.season, fixture_number:fixture.fixture})
    SET f.kickoff_time = fixture.kickoff_time
    RETURN count(f)
    """

    result = conn.execute_query(query,parameters= {"fixtures": fixtures})
    print(f"Successfully created {result} fixture nodes")

def createTeams(conn,df):
    teams_data = df[['home_team']].drop_duplicates()
    teams = teams_data.to_dict('records')

    query = """
    UNWIND $teams AS teams
    MERGE (t: Team {name: teams.home_team})
    RETURN count(t)
    """
    result = conn.execute_query(query, parameters={"teams": teams})
    print(f"Successfully executed {result} Team nodes")

def createPlayers(conn,df):
    players_data = df[['name', 'element']].drop_duplicates()
    players = players_data.to_dict('records')

    query = """
    UNWIND $players AS player
    MERGE (p: Player {player_name: player.name, player_element: player.element})
    RETURN count(p)
    """
    result = conn.execute_query(query, parameters={"players": players})
    print(f"Successfully created {result} Player nodes")

def createPositions(conn,df):
    positions_data = df[['position']].drop_duplicates()
    positions = positions_data.to_dict('records')

    query = """
    UNWIND $positions AS position
    MERGE (p: Position {name: position.position})
    RETURN count(p)
    """
    result = conn.execute_query(query, parameters={"positions": positions})
    print(f"Successfully created {result} Positions nodes")


createSeasons(conn, df)
createGW(conn,df)
createFixtures(conn,df)
createTeams(conn,df)
createPlayers(conn,df)
createPositions(conn,df)

def create_has_gw_relation(conn,df):
    data = df[['season','GW']].drop_duplicates()
    rel = data.to_dict('records')

    query = """
    UNWIND $rel AS rel
    MATCH (s: Season {season_name: rel.season})
    MATCH (g: Gameweek {season: rel.season, GW_number: rel.GW})
    MERGE (s) - [r:HAS_GW] -> (g)
    RETURN count(r) as relations_created
    """

    result = conn.execute_query(query, parameters={"rel":rel})
    print(f"Successfully created {result[0]['relations_created']} :HAS_GW relation")


def create_has_fixture_relation(conn, df):
    data = df[['season','fixture','GW']].drop_duplicates()
    rel = data.to_dict('records')
    
    query = """
    UNWIND $rel AS rel
    MATCH (g:Gameweek {season: rel.season, GW_number: rel.GW})
    MATCH (f:Fixture {season: rel.season, fixture_number: rel.fixture})
    MERGE (g)-[r:HAS_FIXTURE]->(f)
    RETURN count(r) as relations_created
    """
    
    result = conn.execute_query(query, parameters={"rel": rel})
    count = result[0]['relations_created']
    print(f"Created {count} HAS_FIXTURE relationships")
    return count

def create_home_team_relation(conn, df):
    data = df[['season','fixture','home_team']].drop_duplicates()
    rel = data.to_dict('records')
    
    query = """
    UNWIND $rel AS rel
    MATCH (f:Fixture {season: rel.season, fixture_number: rel.fixture})
    MATCH (t:Team {name: rel.home_team})
    MERGE (f)-[r:HAS_HOME_TEAM]->(t)
    RETURN count(r) as relations_created
    """
    
    result = conn.execute_query(query, parameters={"rel": rel})
    count = result[0]['relations_created']
    print(f"Created {count} HAS_HOME_TEAM relationships")
    return count

def create_away_team_relation(conn, df):
    data = df[['season','fixture','away_team']].drop_duplicates()
    rel = data.to_dict('records')
    
    query = """
    UNWIND $rel AS rel
    MATCH (f:Fixture {season: rel.season, fixture_number: rel.fixture})
    MATCH (t:Team {name: rel.away_team})
    MERGE (f)-[r:HAS_AWAY_TEAM]->(t)
    RETURN count(r) as relations_created
    """
    
    result = conn.execute_query(query, parameters={"rel": rel})
    count = result[0]['relations_created']
    print(f"Created {count} HAS_AWAY_TEAM relationships")
    return count


def create_plays_as_relation(conn, df):
    data = df[['name', 'element','position']].drop_duplicates()
    rel = data.to_dict('records')
    
    query = """
    UNWIND $rel AS rel
    MATCH (p:Player {player_name: rel.name, player_element: rel.element})
    MATCH (pos:Position {name: rel.position})
    MERGE (p)-[r:PLAYS_AS]->(pos)
    RETURN count(r) as relations_created
    """
    
    result = conn.execute_query(query, parameters={"rel": rel})
    count = result[0]['relations_created']
    print(f"Created {count} PLAYS_AS relationships")
    return count

def create_played_in_relation(conn, df, batch_size=2000):
    data = df[['name', 'element', 'season', 'fixture', 'minutes',
               'goals_scored', 'assists', 'total_points', 'bonus',
               'clean_sheets', 'goals_conceded', 'own_goals',
               'penalties_saved', 'penalties_missed', 'yellow_cards',
               'red_cards', 'saves', 'bps', 'influence', 'creativity',
               'threat', 'ict_index', 'form']]
    
    rel = data.to_dict('records')

    for r in rel:
        r["props"] = {k: v for k, v in r.items()
                      if k not in ["name", "element", "season", "fixture"]}

    query = """
    UNWIND $batch AS rel
    MATCH (p:Player {player_name: rel.name, player_element: rel.element})
    MATCH (f:Fixture {season: rel.season, fixture_number: rel.fixture})
    MERGE (p)-[r:PLAYED_IN]->(f)
    SET r += rel.props
    """

    total = 0
    for i in range(0, len(rel), batch_size):
        batch = rel[i:i+batch_size]
        conn.execute_query(query, parameters={"batch": batch})
        total += len(batch)
        print(f"Inserted {total}/{len(rel)}")

    print("DONE")
    return total

create_has_gw_relation(conn,df)
create_has_fixture_relation(conn,df)
create_home_team_relation(conn,df)
create_away_team_relation(conn,df)
create_plays_as_relation(conn,df)
create_played_in_relation(conn,df)


