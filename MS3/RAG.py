import sys
import os

# Add the graphretrievallayer folder to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "graphRetrievalLayer"))

from baseline import QUERY_LIBRARY


# print(QUERY_LIBRARY.keys())


# preprocessing.input_preprocessing("Get top players by position in season 2023")

def choose_query(intent, entities):
    for name, template in QUERY_LIBRARY.items():
        if template["intent"] == intent and all(
                e in entities and entities[e] for e in template["entities"]):
            return template["cypher"]
    return None