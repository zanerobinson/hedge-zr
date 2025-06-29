import datetime
import os
import pandas as pd
import requests
from tenacity import retry, wait_random_exponential
from cachebox import Cache
import time

from src.data.cache import get_cache
from src.data.models import (
    CompanyNews,
    CompanyNewsResponse,
    FinancialMetrics,
    FinancialMetricsResponse,
    Price,
    PriceResponse,
    LineItems,
    LineItemsResponse,
    InsiderTrade,
    InsiderTradeResponse,
    CompanyFactsResponse,
)

# Global cache instance
_cache = get_cache()

@retry(wait=wait_random_exponential(multiplier=1, max=60))
def get_prices(ticker: str, start_date: str, end_date: str) -> list[Price]:
    """Fetch price data from cache or API."""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_prices_{start_date}_{end_date}"
    
    if cache_key in _cache:
        return [Price(**price) for price in _cache[f"{cache_key}"]]

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    url = f"https://api.financialdatasets.ai/prices/?ticker={ticker}&interval=day&interval_multiplier=1&start_date={start_date}&end_date={end_date}"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error fetching prices: {ticker} - {response.status_code} - {response.text}")
    if response.status_code == 404:
        return []

    # Parse response with Pydantic model
    price_response = PriceResponse(**response.json())
    prices = price_response.prices

    if not prices:
        return []

    # Cache the results using the comprehensive cache key
    _cache.insert(cache_key, prices)

    return prices


@retry(wait=wait_random_exponential(multiplier=1, max=60))
def get_financial_metrics(
    ticker: str,
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> list[FinancialMetrics]:
    """Fetch financial metrics from cache or API."""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_metrics_{period}_{end_date}_{limit}"
    
    # Check cache first - simple exact match
    if cache_key in _cache:
        return [FinancialMetrics(**metric) for metric in _cache[f"{cache_key}"]]

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    url = f"https://api.financialdatasets.ai/financial-metrics/?ticker={ticker}&report_period_lte={end_date}&limit={limit}&period={period}"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error fetching metrics: {ticker} - {response.status_code} - {response.text}")

    # Parse response with Pydantic model
    metrics_response = FinancialMetricsResponse(**response.json())
    financial_metrics = metrics_response.financial_metrics
    
    '''del after
    '''
    
    with open('cache.txt', 'a') as f:
        for line in financial_metrics:
            f.write(f"FinancialMetrics - {line}\n")
    
    '''del before
    '''
    
    if not financial_metrics:
        return []

    # Cache the results using the comprehensive cache key
    _cache.insert(cache_key, financial_metrics)

    return financial_metrics


retry(wait=wait_random_exponential(multiplier=1, max=60))
def search_line_items(
    ticker: str,
    line_items_list: list[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> list[LineItems]:    
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_line-items_{period}_{end_date}"
    
    # Check cache first - simple exact match
    if cache_key in _cache:
        return [LineItems(**item) for item in _cache[f"{cache_key}"]]

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    url = "https://api.financialdatasets.ai/financials/search/line-items"
    
    line_items = [
        'book_value_per_share',
        'capital_expenditure',
        'cash_and_equivalents',
        'current_assets',
        'current_liabilities',
        'debt_to_equity',
        'depreciation_and_amortization',
        'dividends_and_other_cash_distributions',
        'earnings_per_share',
        'ebit',
        'ebitda',
        'free_cash_flow',
        'goodwill_and_intangible_assets',
        'gross_margin',
        'gross_profit',
        'interest_expense',
        'issuance_or_purchase_of_equity_shares',
        'net_income',
        'operating_expense',
        'operating_income',
        'operating_margin',
        'outstanding_shares',
        'research_and_development',
        'return_on_invested_capital',
        'revenue',
        'shareholders_equity',
        'total_assets',
        'total_debt',
        'total_liabilities',
        'working_capital',
    ]
    
    body = {
        "tickers": [ticker],
        "line_items": line_items,
        "end_date": end_date,
        "period": period,
        "limit": limit,
    }

    print("Hello")
    
    response = requests.post(url, headers=headers, json=body)
    if response.status_code != 200:
        print(f"Error fetching line items: {ticker} - {response.status_code} - {response.text}")
    items_response = LineItemsResponse(**response.json())
    search_results = items_response.search_results

    # Cache the results as dicts using the comprehensive cache key
    _cache.insert(cache_key, search_results)

    return search_results


@retry(wait=wait_random_exponential(multiplier=1, max=60))
def get_insider_trades(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[InsiderTrade]:
    """Fetch insider trades from cache or API."""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_insider-trades_{start_date or 'none'}_{end_date}_{limit}"
    
    # Check cache first - simple exact match
    if cache_key in _cache:
        return [InsiderTrade(**trade) for trade in  _cache[f"{cache_key}"]]

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    all_trades = []
    current_end_date = end_date

    while True:
        url = f"https://api.financialdatasets.ai/insider-trades/?ticker={ticker}&filing_date_lte={current_end_date}"
        if start_date:
            url += f"&filing_date_gte={start_date}"
        url += f"&limit={limit}"

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching data: {ticker} - {response.status_code} - {response.text}")

        data = response.json()
        response_model = InsiderTradeResponse(**data)
        insider_trades = response_model.insider_trades

        if not insider_trades:
            break

        all_trades.extend(insider_trades)

        # Only continue pagination if we have a start_date and got a full page
        if not start_date or len(insider_trades) < limit:
            break

        # Update end_date to the oldest filing date from current batch for next iteration
        current_end_date = min(trade.filing_date for trade in insider_trades).split("T")[0]

        # If we've reached or passed the start_date, we can stop
        if current_end_date <= start_date:
            break
    
    if not all_trades:
        return []

    # Cache the results using the comprehensive cache key
    _cache.insert(cache_key, all_trades)

    return all_trades

@retry(wait=wait_random_exponential(multiplier=1, max=60))
def get_company_news(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[CompanyNews]:
    """Fetch company news from cache or API."""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_company-news_{start_date or 'none'}_{end_date}_{limit}"
    
    # Check cache first - simple exact match
    if cache_key in _cache:
        return [CompanyNews(**news) for news in _cache[f"{cache_key}"]]

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    all_news = []
    current_end_date = end_date

    while True:
        url = f"https://api.financialdatasets.ai/news/?ticker={ticker}&end_date={current_end_date}"
        if start_date:
            url += f"&start_date={start_date}"
        url += f"&limit={limit}"

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching data: {ticker} - {response.status_code} - {response.text}")

        data = response.json()
        response_model = CompanyNewsResponse(**data)
        company_news = response_model.news

        if not company_news:
            break

        all_news.extend(company_news)

        # Only continue pagination if we have a start_date and got a full page
        if not start_date or len(company_news) < limit:
            break

        # Update end_date to the oldest date from current batch for next iteration
        current_end_date = min(news.date for news in company_news).split("T")[0]

        # If we've reached or passed the start_date, we can stop
        if current_end_date <= start_date:
            break

    if not all_news:
        return []

    # Cache the results using the comprehensive cache key
    _cache.insert(cache_key, all_news)

    return all_news


@retry(wait=wait_random_exponential(multiplier=1, max=60))
def get_market_cap(
    ticker: str,
    end_date: str,
) -> float | None:
    """Fetch market cap; rework to stop multiple API calls"""
    cache_key = f"{ticker}_metrics_ttm_{end_date}_10"

    if cache_key in _cache:
        return float([FinancialMetrics(**metric)[0].market_cap for metric in _cache[f"{cache_key}"]][0])

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    url = f"https://api.financialdatasets.ai/financial-metrics/?ticker={ticker}&report_period_lte={end_date}&limit=1&period=ttm"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error fetching market cap: {ticker} - {response.status_code} - {response.text}")

    # Parse response with Pydantic model
    metrics_response = FinancialMetricsResponse(**response.json())
    financial_metrics = metrics_response.financial_metrics
    market_cap = financial_metrics[0].market_cap
    
    if not market_cap:
        return []

    # Cache the results using the comprehensive cache key
    _cache.insert(cache_key, financial_metrics)

    return market_cap


def prices_to_df(prices: list[Price]) -> pd.DataFrame:
    """Convert prices to a DataFrame."""
    df = pd.DataFrame([p.model_dump() for p in prices])
    df["Date"] = pd.to_datetime(df["time"])
    df.set_index("Date", inplace=True)
    numeric_cols = ["open", "close", "high", "low", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_index(inplace=True)
    return df


# Update the get_price_data function to use the new functions
def get_price_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    prices = get_prices(ticker, start_date, end_date)
    return prices_to_df(prices)
