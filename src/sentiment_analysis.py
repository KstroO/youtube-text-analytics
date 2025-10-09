from concurrent.futures import ProcessPoolExecutor
from tqdm.notebook import tqdm
from typing import List
import time

def init_worker():
    global analyzer
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

def get_compound(text):
    return analyzer.polarity_scores(text)["compound"]

def get_compound_parallel(comments: List[str], workers: int = 4, chunk_size: int = None) -> List[str]:
    
    if chunk_size is None:
        chunk_size = max(1, len(comments) // (workers * 4))

    compound_list = []
    with ProcessPoolExecutor(max_workers=workers, initializer=init_worker) as executor:
        futures_iterator = executor.map(get_compound, comments, chunksize=chunk_size)

        for f in tqdm(futures_iterator, total=len(comments)):
            compound_list.append(f)
    return compound_list

if __name__ == "__main__":
    pass