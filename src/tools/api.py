import os
from typing import Dict, Any, List
import pandas as pd
import requests

import requests

def get_financial_metrics(
    ticker: str,
    report_period: str,
    period: str = 'ttm',
    limit: int = 1
) -> str | int:
    """Fetch financial metrics from the API."""
    # headers = {"X-API-KEY": os.environ.get("FINANCIAL_DATASETS_API_KEY")}
    # url = (
    #     f"https://api.financialdatasets.ai/financial-metrics/"
    #     f"?ticker={ticker}"
    #     f"&report_period_lte={report_period}"
    #     f"&limit={limit}"
    #     f"&period={period}"
    # )
    # response = requests.get(url, headers=headers)
    # if response.status_code != 200:
    #     raise Exception(
    #         f"Error fetching data: {response.status_code} - {response.text}"
    #     )
    # data = response.json()
    data = {
          "ticker": "<string>",
          "market_cap": 123,
          "enterprise_value": 123,
          "price_to_earnings_ratio": 123,
          "price_to_book_ratio": 123,
          "price_to_sales_ratio": 123,
          "enterprise_value_to_ebitda_ratio": 123,
          "enterprise_value_to_revenue_ratio": 123,
          "free_cash_flow_yield": 123,
          "peg_ratio": 123,
          "gross_margin": 123,
          "operating_margin": 123,
          "net_margin": 123,
          "return_on_equity": 123,
          "return_on_assets": 123,
          "return_on_invested_capital": 123,
          "asset_turnover": 123,
          "inventory_turnover": 123,
          "receivables_turnover": 123,
          "days_sales_outstanding": 123,
          "operating_cycle": 123,
          "working_capital_turnover": 123,
          "current_ratio": 123,
          "quick_ratio": 123,
          "cash_ratio": 123,
          "operating_cash_flow_ratio": 123,
          "debt_to_equity": 123,
          "debt_to_assets": 123,
          "interest_coverage": 123,
          "revenue_growth": 123,
          "earnings_growth": 123,
          "book_value_growth": 123,
          "earnings_per_share_growth": 123,
          "free_cash_flow_growth": 123,
          "operating_income_growth": 123,
          "ebitda_growth": 123,
          "payout_ratio": 123,
          "earnings_per_share": 123,
          "book_value_per_share": 123,
          "free_cash_flow_per_share": 123
        }
    financial_metrics = data.get("financial_metrics")
    if not financial_metrics:
        raise ValueError("No financial metrics returned")
    return financial_metrics

def search_line_items(
    ticker: str,
    line_items: List[str],
    period: str = 'ttm',
    limit: int = 1
) -> List[Dict[str, Any]]:
    """Fetch cash flow statements from the API."""
    headers = {"X-API-KEY": os.environ.get("FINANCIAL_DATASETS_API_KEY")}
    url = "https://api.financialdatasets.ai/financials/search/line-items"

    body = {
        "tickers": [ticker],
        "line_items": line_items,
        "period": period,
        "limit": limit
    }
    response = requests.post(url, headers=headers, json=body)
    if response.status_code != 200:
        raise Exception(
            f"Error fetching data: {response.status_code} - {response.text}"
        )
    data = response.json()
    search_results = data.get("search_results")
    if not search_results:
        raise ValueError("No search results returned")
    return search_results

def get_insider_trades(
    ticker: str,
    end_date: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fetch insider trades for a given ticker and date range.
    """
    headers = {"X-API-KEY": os.environ.get("FINANCIAL_DATASETS_API_KEY")}
    url = (
        f"https://api.financialdatasets.ai/insider-trades/"
        f"?ticker={ticker}"
        f"&filing_date_lte={end_date}"
        f"&limit={limit}"
    )
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(
            f"Error fetching data: {response.status_code} - {response.text}"
        )
    data = response.json()
    insider_trades = data.get("insider_trades")
    if not insider_trades:
        raise ValueError("No insider trades returned")
    return insider_trades

def get_market_cap(
    ticker: str,
) -> List[Dict[str, Any]]:
    """Fetch market cap from the API."""
    headers = {"X-API-KEY": os.environ.get("FINANCIAL_DATASETS_API_KEY")}
    url = (
        f'https://api.financialdatasets.ai/company/facts'
        f'?ticker={ticker}'
    )

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(
            f"Error fetching data: {response.status_code} - {response.text}"
        )
    data = response.json()
    company_facts = data.get('company_facts')
    if not company_facts:
        raise ValueError("No company facts returned")
    return company_facts.get('market_cap')

def get_prices(
    ticker: str,
    start_date: str,
    end_date: str
) -> List[Dict[str, Any]]:
    """Fetch price data from the API."""
    headers = {"X-API-KEY": os.environ.get("FINANCIAL_DATASETS_API_KEY")}
    url = (
        f"https://api.financialdatasets.ai/prices/"
        f"?ticker={ticker}"
        f"&interval=day"
        f"&interval_multiplier=1"
        f"&start_date={start_date}"
        f"&end_date={end_date}"
    )
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(
            f"Error fetching data: {response.status_code} - {response.text}"
        )
    data = response.json()
    prices = data.get("prices")
    if not prices:
        raise ValueError("No price data returned")
    return prices

def prices_to_df(prices: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert prices to a DataFrame."""
    df = pd.DataFrame(prices)
    df["Date"] = pd.to_datetime(df["time"])
    df.set_index("Date", inplace=True)
    numeric_cols = ["open", "close", "high", "low", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_index(inplace=True)
    return df

# Update the get_price_data function to use the new functions
def get_price_data(
    ticker: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    prices = get_prices(ticker, start_date, end_date)
    return prices_to_df(prices)
