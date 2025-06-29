from cachebox import Cache
import pickle

# Global cache instance
#with open('cache.pkl', 'rb') as f:
#    _cache = pickle.load(f)
_cache = Cache(maxsize=0)

def get_cache() -> Cache:
    """Get the global cache instance."""
    return _cache

def save_cache():
    with open('cache.pkl', 'wb') as f:
        pickle.dump(_cache, f)
