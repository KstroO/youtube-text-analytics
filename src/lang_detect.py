from concurrent.futures import ProcessPoolExecutor
from langdetect import detect_langs, DetectorFactory
from tqdm.notebook import tqdm
import time

def init_workers():
    DetectorFactory.seed = 0

def detect_single(text: str, min_confidence: int = 0.9) -> str:
    try:
        langs = detect_langs(text)
        if not langs:
            return "und"
        top = langs[0]
        if top.prob >= min_confidence:
            return top.lang
        return "und"
    except Exception:
        return "und"
    
def detect_parallel(texts, max_workers = 4, chunk_size = None):
    if chunk_size == None:
        chunk_size = max(1, len(texts) // (max_workers * 4))

    with ProcessPoolExecutor(max_workers=max_workers, initializer=init_workers) as executor:
        futures_iterator = executor.map(detect_single, texts, chunksize=chunk_size)

        # start tracking
        start = time.time()
        result = []
        for r in tqdm(futures_iterator, total=len(texts)):
            result.append(r)
        end = time.time()

    print(f"Finished translation in {end - start:.2f}s, {len(texts)/(end - start):.2f} c/s")
    return result


# (doesn't work on windows without this)
if __name__ == "__main__":
    pass