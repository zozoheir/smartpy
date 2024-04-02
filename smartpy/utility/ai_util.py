import numpy as np
import openai

def get_cosine_similarity(a, b):
    a = np.array(a)
    b = np.array(b)
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

def get_top_n_similar_vectors(input_vector, vector_list, top_n=5):
    similarities = [get_cosine_similarity(input_vector, vector) for vector in vector_list]
    top_similarities_indices = np.argsort(similarities)[-top_n:][::-1]
    return top_similarities_indices

def get_embedding(text, model="text-embedding-ada-002"):
    text = text.replace("\n", " ")
    try:
        embedding = openai.Embedding.create(input=[text], model=model)['data'][0]['embedding']
    except Exception as e:
        raise e
    return embedding


def get_top_n_diverse_texts(texts, embeddings, top_n=5):
    """
    Selects top_n texts that maximize diversity based on their embeddings.

    Parameters:
    - texts (list of str): List of textual documents.
    - embeddings (list of list of float): List of embeddings corresponding to the texts.
    - top_n (int): Number of diverse texts to return.

    Returns:
    - list of str: The selected top_n diverse texts.
    """
    num_texts = len(texts)
    similarity_matrix = np.zeros((num_texts, num_texts))
    for i in range(num_texts):
        for j in range(i + 1, num_texts):
            similarity = get_cosine_similarity(embeddings[i], embeddings[j])
            similarity_matrix[i][j] = similarity_matrix[j][i] = similarity

    selected_texts = []
    selected_indices = set()

    for _ in range(min(top_n, num_texts)):
        max_diversity_score = -1
        selected_index = -1
        for i in range(num_texts):
            if i in selected_indices:
                continue
            diversity_score = min(
                [similarity_matrix[i][j] for j in selected_indices] + [1])  # +[1] to handle first selection
            if diversity_score > max_diversity_score:
                max_diversity_score = diversity_score
                selected_index = i

        if selected_index != -1:
            selected_texts.append(texts[selected_index])
            selected_indices.add(selected_index)

    return selected_texts