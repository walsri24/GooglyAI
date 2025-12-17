# googlyai/data_pipeline/odi/schema.py
def safe_get(d, path, default=None):
    """
    path: list of keys
    """
    for key in path:
        if not isinstance(d, dict):
            return default
        d = d.get(key)
        if d is None:
            return default
    return d
