"""Sentence-transformer embeddings for duplicate detection and naming (§05.4).

Uses sentence-transformers (when available) to compute semantic embeddings
for cost center descriptions. Falls back to TF-IDF when not installed.
"""

from __future__ import annotations

import hashlib

import numpy as np
import structlog

logger = structlog.get_logger()

_model_cache: dict[str, object] = {}


def _get_transformer(model_name: str = "all-MiniLM-L6-v2"):
    """Lazy-load sentence-transformer model."""
    if model_name in _model_cache:
        return _model_cache[model_name]
    try:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(model_name)
        _model_cache[model_name] = model
        logger.info("embeddings.loaded", model=model_name)
        return model
    except ImportError:
        logger.info("embeddings.sentence_transformers_not_installed")
        return None


def embed_texts(
    texts: list[str],
    model_name: str = "all-MiniLM-L6-v2",
) -> np.ndarray:
    """Compute embeddings for a list of texts.

    Returns an (N, D) array of float32 embeddings.
    Falls back to simple TF-IDF character-ngram hashing if
    sentence-transformers is not installed.
    """
    model = _get_transformer(model_name)
    if model is not None:
        return model.encode(texts, show_progress_bar=False, convert_to_numpy=True)
    return _tfidf_fallback(texts)


def _tfidf_fallback(texts: list[str], dim: int = 128) -> np.ndarray:
    """Deterministic hash-based vectorization fallback."""
    result = np.zeros((len(texts), dim), dtype=np.float32)
    for i, text in enumerate(texts):
        tokens = text.lower().split()
        for tok in tokens:
            h = int(hashlib.md5(tok.encode()).hexdigest(), 16)  # noqa: S324
            idx = h % dim
            result[i, idx] += 1.0
        norm = np.linalg.norm(result[i])
        if norm > 0:
            result[i] /= norm
    return result


def find_duplicates(
    names: list[str],
    ids: list[int | str],
    threshold: float = 0.85,
    model_name: str = "all-MiniLM-L6-v2",
) -> list[dict]:
    """Find near-duplicate cost center names using cosine similarity.

    Returns list of {id_a, id_b, name_a, name_b, similarity} pairs
    exceeding the threshold.
    """
    if len(names) < 2:
        return []
    embeddings = embed_texts(names, model_name)
    # Normalize for cosine similarity via dot product
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms = np.maximum(norms, 1e-10)
    normed = embeddings / norms

    results: list[dict] = []
    n = len(names)
    # Pairwise cosine (chunked for memory efficiency)
    chunk_size = 500
    for start in range(0, n, chunk_size):
        end = min(start + chunk_size, n)
        sims = normed[start:end] @ normed.T
        for i_local in range(end - start):
            i_global = start + i_local
            for j in range(i_global + 1, n):
                sim = float(sims[i_local, j])
                if sim >= threshold:
                    results.append(
                        {
                            "id_a": ids[i_global],
                            "id_b": ids[j],
                            "name_a": names[i_global],
                            "name_b": names[j],
                            "similarity": round(sim, 4),
                        }
                    )
    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results


def suggest_names(
    current_name: str,
    reference_names: list[str],
    pattern: str = "{entity}_{function}_{region}",
    top_k: int = 5,
) -> list[dict]:
    """Suggest standardized naming alternatives.

    Uses embedding similarity to find the closest reference names,
    then reformats them according to the naming pattern.
    """
    if not reference_names:
        return []
    all_texts = [current_name, *reference_names]
    embeddings = embed_texts(all_texts)

    target_emb = embeddings[0:1]
    ref_embs = embeddings[1:]

    norms_t = np.linalg.norm(target_emb, axis=1, keepdims=True)
    norms_r = np.linalg.norm(ref_embs, axis=1, keepdims=True)
    norms_t = np.maximum(norms_t, 1e-10)
    norms_r = np.maximum(norms_r, 1e-10)

    sims = (target_emb / norms_t) @ (ref_embs / norms_r).T
    sim_scores = sims[0]

    top_indices = np.argsort(sim_scores)[::-1][:top_k]
    results = []
    for idx in top_indices:
        results.append(
            {
                "suggested_name": reference_names[idx],
                "similarity": round(float(sim_scores[idx]), 4),
                "pattern": pattern,
            }
        )
    return results
