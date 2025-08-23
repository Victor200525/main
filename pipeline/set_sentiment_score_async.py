import json
import asyncio, aiofiles
import polars as pl
from datetime import datetime
import os
import config
from core.sentiment import SentimentHuggingFaceAsync
from loguru import logger

# Константы
BATCH_SIZE = 300
QUEUE_TIMEOUT = 10.0

logger.remove() # чтобы не писал в консоль
# Добавляем лог-файл (enqueue=True включает неблокирующую очередь)
logger.add("async_log.log", format="{time} | {level} | {message}", rotation="1 MB", enqueue=True)

async def read_all_files_to_queue(queue, input_dir):
    """АСИНХРОННО читает все файлы используя aiofiles"""
    for filename in os.listdir(input_dir):
        file_path = os.path.join(input_dir, filename)
        if not os.path.isfile(file_path):
            continue
            
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as file:
            async for line in file:
                try:
                    data = json.loads(line)
                    selftext = data.get('selftext', '')
                    
                    if not selftext.strip() or selftext.lower() in {"[deleted]", "[removed]"}:
                        continue
                        
                    item = {
                        'selftext': selftext,
                        'upvotes': data.get('score', 0),
                        'num_of_comments': data.get('num_comments', 0),
                        'date': datetime.utcfromtimestamp(data.get('created_utc', 0)).date(),
                        'filename': filename
                    }
                    
                    await queue.put(item)
                    #await asyncio.sleep(0)  # Даем шанс другим задачам
                    
                except (json.JSONDecodeError, ValueError):
                    continue
                    
    await queue.put(None)

async def process_queue(sentiment_model, queue, output_dir):
    """АСИНХРОННО обрабатывает очередь"""
    batch = []
    while True:
        try:
            item = await asyncio.wait_for(queue.get(), timeout=QUEUE_TIMEOUT)
            
            if item is None:
                if batch:
                    await process_batch(sentiment_model, batch, output_dir)
                break
                
            batch.append(item)
            
            if len(batch) >= BATCH_SIZE:
                logger.info('batch is full')
                await process_batch(sentiment_model, batch, output_dir)
                batch = []
                
        except asyncio.TimeoutError:
            if batch:
                await process_batch(sentiment_model, batch, output_dir)
                batch = []

async def process_batch(sentiment_model, batch, output_dir):
    """Обрабатывает один батч и сохраняет"""
    logger.info('batch processing is starting')
    tasks = [sentiment_model.get_sentiment(item['selftext']) for item in batch]
    sentiments = await asyncio.gather(*tasks, return_exceptions=True)

    processed = []
    for i, item in enumerate(batch):
        if isinstance(sentiments[i], Exception):
            continue
        processed.append({
            'text': item['selftext'],
            'upvotes': item['upvotes'],
            'numofcomms': item['num_of_comments'],
            'sentiment': sentiments[i],
            'Date': str(item['date'])
        })
            
    if processed:
        logger.info('batch processing has finished')
        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, save_to_delta, processed, output_dir)

def save_to_delta(data, output_dir):
    """СИНХРОННО сохраняет в Delta Lake"""
    try:
        df = pl.DataFrame(data)
        df.write_delta(output_dir, mode="append")
        logger.info('delta table has been saved')
    except Exception:
        pass

async def set_sentiment_async():
    INPUT_DIR = config.INPUT_DIR
    OUTPUT_DIR = config.STAGE_DIR
    
    sentiment_model = SentimentHuggingFaceAsync(
        base_url="https://gxdy-work.hf.space",
        api_key="value1"
    )
    
    queue = asyncio.Queue()
    
    # Запускаем чтение файлов и обработку очереди параллельно
    reader_task = asyncio.create_task(read_all_files_to_queue(queue, INPUT_DIR))
    processor_task = asyncio.create_task(process_queue(sentiment_model, queue, OUTPUT_DIR))
    
    await asyncio.gather(reader_task, processor_task)

