import yfinance as yf
import json
import datetime

class YFClient:
    def __init__(self, ticker_symbol='BTC-USD', period='60d', interval='1m', news_count=1000):
        self.ticker_symbol = ticker_symbol
        self.period = period
        self.interval = interval
        self.news_count = news_count

    def download_ticker(self):
        ticker = yf.Ticker(self.ticker_symbol)
        data = ticker.history(period=self.period, interval=self.interval)
        close = data['Close'].to_dict()
        with open(f'{self.ticker_symbol}_historical_data.json', 'w') as f:
            json.dump(close, f, indent=4)

    def download_news(self):
        news = yf.Ticker(self.ticker_symbol).get_news(count=self.news_count)
        with open(f'{self.ticker_symbol}_news.json', 'w') as f:
            json.dump(news, f, indent=4)

    def prepare_data(self):
        with open(f'{self.ticker_symbol}_historical_data.json', 'r') as f:
            ticker = json.load(f)
        with open(f'{self.ticker_symbol}_news.json', 'r') as f:
            news = json.load(f)

        output = []
        for item in news:
            title = item['content']['title']
            summary = item['content']['summary']
            pubDate = item['content']['pubDate']

            pubDate_ts = int(datetime.datetime.strptime(pubDate, '%Y-%m-%dT%H:%M:%SZ').timestamp())

            index = pubDate_ts - (pubDate_ts % 300)
            price = ticker.get(f"{index}000")
            future_price = ticker.get(f"{index + 300}000")

            if price is None or future_price is None:
                print(f"Skipping entry with missing price data: title={title}, pubDate={pubDate}, index={index}")
                continue

            difference = price - future_price
            output.append({
                'title': title,
                'index': index,
                'price': price,
                'future_price': future_price,
                'difference': difference,
                'percentage': (difference / price) * 100,
                'summary': summary,
                'pubDate': pubDate,
                'pubDate_ts': pubDate_ts,
            })

        with open(f'{self.ticker_symbol}_news_with_price.json', 'w') as f:
            json.dump(output, f, indent=4)

if __name__ == "__main__":
    client = YFClient(ticker_symbol='BTC-USD', period='60d', interval='1m', news_count=1000)
    client.download_ticker()
    client.download_news()
    client.prepare_data()
