from pipeline.set_sentiment_score_async import set_sentiment_async
import asyncio
import time

tic = time.perf_counter()
asyncio.run(set_sentiment_async())
toc = time.perf_counter()
print(f"Timing is {toc - tic:0.4f} seconds")
