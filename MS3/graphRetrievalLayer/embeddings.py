from neo4j.graph import Node, Relationship


from neo4j.graph import Node, Relationship
from sentence_transformers import SentenceTransformer
from transformers import AutoModel, AutoTokenizer

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

def vectorize_huggingface(texts, model_name='sentence-transformers/all-MiniLM-L6-v2'):
    """
    Vectorize a list of strings using a Hugging Face Transformer with mean pooling.
    
    Args:
        texts (list of str): Texts to vectorize
        model_name (str): Pretrained HF model
        
    Returns:
        embeddings (list of list of floats): Dense embeddings
    """
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
    
    embeddings = []
    for text in texts:
        inputs = tokenizer(text, return_tensors='pt', truncation=True, padding=True)
        outputs = model(**inputs)
        # Mean pooling over the token embeddings
        token_embeddings = outputs.last_hidden_state  # shape: [1, seq_len, hidden_dim]
        attention_mask = inputs['attention_mask']
        mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        pooled = (token_embeddings * mask_expanded).sum(1) / mask_expanded.sum(1)
        embeddings.append(pooled.detach().numpy()[0])
    return embeddings

def Features_vector_embeddings(record, model_name = 'sentence-transformers/all-MiniLM-L6-v2'): 
    vectorize_sentence_transformer(record_to_string(record), model_name=model_name)

#example usage

# for record in result:
#     for value in record.values():
#         print(entity_to_string(value))
