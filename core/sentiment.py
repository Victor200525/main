import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import requests  # для Hugging Face API

class SentimentHuggingFace:
    def __init__(self, base_url, api_key):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {"x-api-key": self.api_key}

    def get_sentiment(self, text: str) -> float:
        """Отправляет текст в Hugging Face API и возвращает +score / -score / 0"""
        try:
            payload = {"text": text}
            response = requests.post(f"{self.base_url}/analyze", json=payload, headers=self.headers)
            result = response.json()
            label = result["result"][0]["label"].lower()
            score = result["result"][0]["score"]

            if label == "positive":
                return 1 * score
            elif label == "negative":
                return -1 * score
            else:
                return 0
        except Exception as e:
            print(f"Ошибка API: {e}")
            return 0
        
class SentimentNTLK:
    def __init__(self):
        nltk.download('vader_lexicon', quiet=True)
        self.sia = SentimentIntensityAnalyzer()

    def get_sentiment(self, selftext: str) -> float:
        """Вычисляет тональность текста через NLTK"""
        try:
            sentiment = self.sia.polarity_scores(selftext)
        except Exception as e:
            print(f"Ошибка анализа тональности: {e}")
            sentiment = {}

        compound = sentiment.get('compound', 0)
        #norm_score_log = math.log1p(max(upvotes, 0))
        #norm_score_0_1 = norm_score_log / math.log1p(max(1, upvotes))
        #weight_balanced = 0.5 * norm_score_0_1 + 0.5 * ((compound + 1) / 2)
        return compound
import httpx
import asyncio


class SentimentHuggingFaceAsync:
    def __init__(self, base_url, api_key):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {"x-api-key": self.api_key}

    async def get_sentiment(self, text: str) -> float:
        """Отправляет текст в Hugging Face API (асинхронно) и возвращает +score / -score / 0"""
        try:
            payload = {"text": text}
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{self.base_url}/analyze",
                                             json=payload,
                                             headers=self.headers,
                                             timeout=30.0)
            result = response.json()
            label = result["result"][0]["label"].lower()
            score = result["result"][0]["score"]

            if label == "positive":
                return 1 * score
            elif label == "negative":
                return -1 * score
            else:
                return 0
        except Exception as e:
            print(f"Ошибка API: {e}")
            return 0


# Пример использования
async def main():
    clf = SentimentHuggingFaceAsync("https://api.huggingface.co", "YOUR_API_KEY")
    score = await clf.get_sentiment("I love async programming!")
    print(score)


if __name__ == "__main__":
    asyncio.run(main())
      