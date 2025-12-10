from neo4j.graph import Node, Relationship
from sentence_transformers import SentenceTransformer
from transformers import AutoModel, AutoTokenizer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


def entity_to_string(entity):
    """
    Converts a Neo4j Node or Relationship into a descriptive string,
    without IDs or internal Neo4j metadata.
    For relationships, it outputs: start_node relationship_type end_node relationship_properties
    """
    
    # ----------- NODE -----------
    if isinstance(entity, Node):
        label = list(entity.labels)[0] if entity.labels else "Node"
        props_text = " ".join(f"{key} {entity[key]}" for key in entity.keys()) if entity.keys() else ""
        return f"{label} {props_text}".strip()
    
    # -------- RELATIONSHIP -----------
    elif isinstance(entity, Relationship):
        start_str = entity_to_string(entity.start_node)
        end_str = entity_to_string(entity.end_node)
        rel_type = entity.type
        rel_props = " ".join(f"{key} {entity[key]}" for key in entity.keys()) if entity.keys() else ""
        
        if rel_props:
            return f"{start_str} {rel_type} {end_str} {rel_props}"
        else:
            return f"{start_str} {rel_type} {end_str}"

    # -------- FALLBACK -----------
    else:
        return str(entity)

def record_to_string(record):
    strings = []
    for v in record.values():
        # Skip nodes that are part of a relationship to avoid duplication
        if isinstance(v, Node):
            # Only include if not already in a relationship string
            continue
        strings.append(entity_to_string(v))
    return " ".join(strings)

def vectorize_sentence_transformer(texts, model_name='all-MiniLM-L6-v2'):
    """
    Vectorize a list of strings using Sentence Transformers.
    
    Args:
        texts (list of str): Texts to vectorize
        model_name (str): Pretrained sentence-transformer model
        
    Returns:
        embeddings (list of list of floats): Dense embeddings
    """
    model = SentenceTransformer(model_name)
    embeddings = model.encode(texts, convert_to_tensor=False)
    return embeddings


def Features_vector_embeddings(record, model_name = 'sentence-transformers/all-MiniLM-L6-v2'): 
    vectorize_sentence_transformer(record_to_string(record), model_name=model_name)



#example usage

# for record in result:
#     for value in record.values():
#         print(entity_to_string(value))


def build_feature_index(records, model_name='all-MiniLM-L6-v2'):
    
    from sentence_transformers import SentenceTransformer
    
    # Load model
    model = SentenceTransformer(model_name)
    
    # Convert all records to strings
    feature_texts = [record_to_string(record) for record in records]
    
    # Generate embeddings in batch
    feature_embeddings = model.encode(feature_texts, convert_to_tensor=False)
    
    # Return index structure
    return {
        'feature_texts': feature_texts,
        'feature_embeddings': feature_embeddings,
        'feature_records': records,
        'model': model
    }

def search_similar_features(index, user_input, k=5):
    
    if not index['feature_texts']:
        return []
    
    # Embed user input
    user_embedding = index['model'].encode([user_input], convert_to_tensor=False)
    
    # Calculate cosine similarities
    similarities = cosine_similarity(user_embedding, index['feature_embeddings'])[0]
    
    # Get indices of top k similarities
    top_indices = np.argsort(similarities)[::-1][:k]
    
    # Filter by threshold and collect results
    results = []
    for idx in top_indices:
        # if similarities[idx] >= similarity_threshold:
            result = {
                'text': index['feature_texts'][idx],
                'similarity': float(similarities[idx]),
                'record': index['feature_records'][idx]
            }
            results.append(result)
    
    return results

def search_similar_features_with_embedding(index, user_embedding, k=5, similarity_threshold=0.5):
    
    if not index['feature_texts']:
        return []
    
    # Calculate cosine similarities
    similarities = cosine_similarity([user_embedding], index['feature_embeddings'])[0]
    
    # Get indices of top k similarities
    top_indices = np.argsort(similarities)[::-1][:k]
    
    # Filter by threshold and collect results
    results = []
    for idx in top_indices:
        if similarities[idx] >= similarity_threshold:
            result = {
                'text': index['feature_texts'][idx],
                'similarity': float(similarities[idx]),
                'record': index['feature_records'][idx]
            }
            results.append(result)
    
    return results

def get_top_k_features_for_llm(index, user_input, k=5):
    
    # Search for similar features
    similar_features = search_similar_features(index, user_input, k=k)
    
    # Extract just the feature texts and records
    top_features = [feature['text'] for feature in similar_features]
    feature_records = [feature['record'] for feature in similar_features]
    
    return {
        'user_input': user_input,
        'top_features': top_features,
        'feature_records': feature_records
    }


