from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from typing import List
import threading
import psutil
import time

def monitor_cpu(interval=0.5, stop_event=None, usage_list: List[float]=None):
    while not stop_event.is_set():
        usage = psutil.cpu_percent(interval=interval)
        usage_list.append(usage)
        
def init_worker():
    global analyzer
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

def get_compound(text):
    return analyzer.polarity_scores(text)["compound"]

def get_compound_parallel_benchmark(comments: List[str], workers: int = 4, chunk_size: int = None) -> tuple:
    
    if chunk_size is None:
        chunk_size = max(1, len(comments) // (workers * 4))

    compound_list = []

    # Start CPU monitor thread
    cpu_usage = []
    stop_event = threading.Event()
    monitor_thread = threading.Thread(target=monitor_cpu, args=(0.5, stop_event, cpu_usage))
    monitor_thread.start()

    with ProcessPoolExecutor(max_workers=workers, initializer=init_worker) as executor:
        futures_iterator = executor.map(get_compound, comments, chunksize=chunk_size)
        start = time.time()
        for f in tqdm(futures_iterator, total=len(comments)):
            compound_list.append(f)
        end = time.time()

    # Stop CPU monitor
    stop_event.set()
    monitor_thread.join()

    avg_cpu = sum(cpu_usage) / len(cpu_usage) if cpu_usage else 0.0

    print(f"Sentiment scores collection finished in {end - start:.2f}s, {len(comments)/(end - start):.2f} c/s")
    print(f"Average CPU Usage: {avg_cpu:.2f}%")

    return workers, end - start, len(comments)/(end - start), avg_cpu

if __name__ == "__main__":
    pass