import json
import datetime
from pathlib import Path
from typing import Dict, List
import yfinance as yf


class YahooFinanceClient:
    """Client for downloading and processing Yahoo Finance data for cryptocurrencies."""

    def __init__(self, ticker: str = 'BTC-USD', data_dir: str = '.'):
        self.ticker = ticker
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.processed_data_path = self.data_dir / f'{ticker}_news_with_price.json'

    def download_ticker(self, period: str = '1mo', interval: str = '5m') -> Dict:
        """Download historical ticker data and return it as a dictionary."""
        data = yf.download(tickers=self.ticker, period=period, interval=interval)
        encoded = data.to_json()
        decoded = json.loads(encoded)
        return decoded[f"('Open', '{self.ticker}')"]

    def download_news(self, count: int = 1000) -> List[Dict]:
        """Download news articles from Yahoo Finance."""
        ticker_obj = yf.Ticker(self.ticker)
        news = ticker_obj.get_news(count=count)
        return news if news else []

    def prepare_data(self, ticker_data: Dict, news_data: List[Dict], time_window: int = 300) -> List[Dict]:
        """Prepare training data by combining news with price info."""
        output = []
        for item in news_data:
            try:
                title = item['content']['title']
                summary = item['content']['summary']
                pub_date = item['content']['pubDate']

                pub_date_ts = int(datetime.datetime.strptime(pub_date, '%Y-%m-%dT%H:%M:%SZ').timestamp())
                index = pub_date_ts - (pub_date_ts % time_window)

                price = ticker_data.get(f"{index}000")
                future_price = ticker_data.get(f"{index + time_window}000")

                if price is None or future_price is None:
                    continue

                difference = price - future_price
                percentage = (difference / price) * 100

                output.append({
                    'title': title,
                    'summary': summary,
                    'pubDate': pub_date,
                    'pubDate_ts': pub_date_ts,
                    'index': index,
                    'price': price,
                    'future_price': future_price,
                    'difference': difference,
                    'percentage': percentage
                })
            except (KeyError, ValueError):
                continue
        return output

    def get_processed_data(self, period: str = '1mo', interval: str = '5m',
                          news_count: int = 1000, time_window: int = 300) -> List[Dict]:
        """Run the full data pipeline and save only the final combined output."""
        print(f"Downloading data for {self.ticker}...")
        ticker_data = self.download_ticker(period, interval)
        news_data = self.download_news(news_count)
        processed_data = self.prepare_data(ticker_data, news_data, time_window)

        with open(self.processed_data_path, 'w') as f:
            json.dump(processed_data, f, indent=4)

        print(f"✅ Pipeline complete. Saved {len(processed_data)} entries to {self.processed_data_path}")
        return processed_data


def main():
    client = YahooFinanceClient(ticker='BTC-USD', data_dir='.')
    client.get_processed_data()


if __name__ == "__main__":
    main()
