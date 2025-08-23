import asyncio
import httpx
import time
import random, string

class SentimentHuggingFaceAsync:
    def __init__(self, base_url, api_key):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {"x-api-key": self.api_key}
        self.client = httpx.AsyncClient()  # создаём клиент один раз

    # добавляем client как параметр, чтобы переиспользовать
    async def get_sentiment(self, text: str) -> float:
        try:
            payload = {"text": text}
            response = await self.client.post(
                f"{self.base_url}/analyze",
                json=payload,
                headers=self.headers,
                timeout=50.0  # минимальный таймаут для ускорения
            )
            result = response.json()
            label = result["result"][0]["label"].lower()
            score = result["result"][0]["score"]

            if label == "positive":
                return score
            elif label == "negative":
                return -score
            else:
                return 0
        except Exception as e:
            # Ошибки собираем, но не печатаем сразу, чтобы не тормозить
            return 0

async def send_same_text_multiple_times(text: str, times: int):
    clf = SentimentHuggingFaceAsync("https://gxdy-work.hf.space", "value1")
    tasks = []
    for _ in range(times):
        # Генерация случайной строки из букв и цифр
        random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=300))
        text1 = text + ' ' + random_str
        await clf.get_sentiment(text1)
        #task = asyncio.create_task(clf.get_sentiment(text1))
        #tasks.append(task)
    #results = await asyncio.gather(*tasks, return_exceptions=True)

    #for i, score in enumerate(results, 1):
    #    print(f"Запрос {i}: | score={score}")

text_to_send = "I hate async programming!"  # один и тот же текст
tic = time.perf_counter()
asyncio.run(send_same_text_multiple_times(text_to_send, 100)) 
toc = time.perf_counter()
print(f"Timing is {toc - tic:0.4f} seconds")
 