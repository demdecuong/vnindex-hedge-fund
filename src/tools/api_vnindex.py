import os
from typing import Dict, Any, List
import pandas as pd
import requests
from datetime import datetime

import requests

from vnstock3 import Vnstock


def get_financial_metrics(
        ticker: str,
        report_period: str,
        period: str = 'year',
        limit: int = 1
) -> List[Dict[str, Any]]:
    """Fetch financial metrics from the API.
    ['ticker', 'yearReport', 'lengthReport', '(ST+LT borrowings)/Equity',
       'Debt/Equity', 'Fixed Asset-To-Equity',
       'Owners' Equity/Charter Capital', 'Asset Turnover',
       'Fixed Asset Turnover', 'Days Sales Outstanding',
       'Days Inventory Outstanding', 'Days Payable Outstanding', 'Cash Cycle',
       'Inventory Turnover', 'EBIT Margin (%)', 'Gross Profit Margin (%)',
       'Net Profit Margin (%)', 'ROE (%)', 'ROIC (%)', 'ROA (%)',
       'EBITDA (Bn. VND)', 'EBIT (Bn. VND)', 'Dividend yield (%)',
       'Current Ratio', 'Cash Ratio', 'Quick Ratio', 'Interest Coverage',
       'Financial Leverage', 'P/B', 'Market Capital (Bn. VND)',
       'Outstanding Share (Mil. Shares)', 'P/E', 'P/S', 'P/Cash Flow',
       'EPS (VND)', 'BVPS (VND)', 'EV/EBITDA', 'Revenue (Bn. VND)',
       'Revenue YoY (%)', 'Attribute to parent company (Bn. VND)',
       'Attribute to parent company YoY (%)', 'Net Sales', 'Cost of Sales',
       'Gross Profit', 'Financial Income', 'Financial Expenses',
       'Interest Expenses', 'General & Admin Expenses',
       'Operating Profit/Loss', 'Net other income/expenses',
       'Profit before tax', 'Business income tax - current',
       'Business income tax - deferred', 'Net Profit For the Year',
       'Minority Interest', 'Attributable to parent company', 'Sales',
       'Other income', 'Other Income/Expenses', 'Net Profit/Loss before tax',
       'Depreciation and Amortisation', 'Provision for credit losses',
       'Unrealized foreign exchange gain/loss',
       'Profit/Loss from investing activities', 'Interest Expense',
       'Interest income and dividends',
       'Operating profit before changes in working capital',
       'Increase/Decrease in receivables', 'Increase/Decrease in inventories',
       'Increase/Decrease in payables',
       'Increase/Decrease in prepaid expenses', 'Interest paid',
       'Business Income Tax paid', 'Other receipts from operating activities',
       'Other payments on operating activities',
       'Net cash inflows/outflows from operating activities',
       'Purchase of fixed assets', 'Proceeds from disposal of fixed assets',
       'Loans granted, purchases of debt instruments (Bn. VND)',
       'Collection of loans, proceeds from sales of debts instruments (Bn. VND)',
       'Investment in other entities',
       'Proceeds from divestment in other entities', 'Gain on Dividend',
       'Net Cash Flows from Investing Activities',
       'Increase in charter captial', 'Payments for share repurchases',
       'Proceeds from borrowings', 'Repayment of borrowings', 'Dividends paid',
       'Cash flows from financial activities',
       'Net increase/decrease in cash and cash equivalents',
       'Cash and cash equivalents',
       'Cash and Cash Equivalents at the end of period',
       'Increase/Decrease in receivables', 'Increase/Decrease in payables']
    """

    stock = Vnstock().stock(symbol=ticker, source='VCI')
    # df = stock.quote.history(symbol='FPT', start='2024-01-01', end='2024-12-25', interval='1D')
    df = stock.finance.ratio(period=period, lang='en', dropna=True)
    df.columns = ['_'.join(filter(None, col)).strip().split('_')[1] for col in df.columns.values]
    data = stock.finance.income_statement(period=period, lang='en', dropna=True)
    df = df.merge(data, on=['ticker', 'yearReport'], how='left')
    data = stock.finance.cash_flow(period=period, dropna=True)
    df = df.merge(data, on=['ticker', 'yearReport'], how='left')

    df = df[df["yearReport"] == datetime.strptime(report_period, '%Y-%m-%d').year - 1]

    # Calculate Free Cash Flow
    operating_cash_flow = df['Net cash inflows/outflows from operating activities']
    capital_expenditures = df['Purchase of fixed assets']
    outstanding_shares = df['Outstanding Share (Mil. Shares)']

    df['free_cash_flow'] = operating_cash_flow - capital_expenditures
    df['free_cash_flow_per_share'] = df['free_cash_flow'] / outstanding_shares
    df['earnings_per_share'] = df['Net Profit For the Year'] / outstanding_shares
    # Calculate Price Per Share
    df['price_per_share'] = df['BVPS (VND)'] * df['P/B']
    # Calculate Market Cap
    df['market_cap'] = df['Outstanding Share (Mil. Shares)'] * df['price_per_share']

    # rename
    df = df.rename(columns={
        "ROE (%)": "return_on_equity",
        "Net Profit Margin (%)": "net_margin",
        "EBIT Margin (%)": "operating_margin",
        "Revenue YoY (%)": "revenue_growth",
        "Attribute to parent company YoY (%)": "earnings_growth",
        "BVPS (VND)": "book_value_growth",
        "Current Ratio": "current_ratio",
        "Debt/Equity": "debt_to_equity",
        "P/E": "price_to_earnings_ratio",
        "P/B": "price_to_book_ratio",
        "P/S": "price_to_sales_ratio"
    })

    financial_metrics = df.to_json(orient='records', date_format='iso')

    if not financial_metrics:
        raise ValueError("No financial metrics returned")
    return financial_metrics


def search_line_items(
        ticker: str,
        line_items: List[str],
        period: str = 'year',
        limit: int = 1
) -> List[Dict[str, Any]]:
    """Fetch cash flow statements from the API.
    {
      "search_results": [
        {
          "ticker": "<string>",
          "report_period": "2023-12-25",
          "period": "annual",
          "currency": "<string>"
        }
      ]
    }
    """

    stock = Vnstock().stock(symbol=ticker, source='VCI')
    search_results = stock.finance.cash_flow(period=period, lang='en', dropna=True).head()
    # search_results = data[data['yearReport']]
    search_results = search_results.to_json(orient='records', date_format='iso')
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
    {
        "insider_trades": [
            {
                "ticker": "<string>",
                "issuer": "<string>",
                "name": "<string>",
                "title": "<string>",
                "is_board_director": true,
                "transaction_date": "2023-12-25",
                "transaction_shares": 123,
                "transaction_price_per_share": 123,
                "transaction_value": 123,
                "shares_owned_before_transaction": 123,
                "shares_owned_after_transaction": 123,
                "security_title": "<string>",
                "filing_date": "2023-12-25"
            }
        ]
    }
    """
    company = Vnstock().stock(symbol=ticker, source='TCBS').company
    insider_trades = company.insider_deals()
    insider_trades = insider_trades[insider_trades['deal_announce_date'] <= end_date]

    insider_trades = insider_trades.rename(columns={
        "deal_quantity": "transaction_shares"
    })
    insider_trades = insider_trades.to_json(orient='records',date_format='iso')

    return insider_trades


def get_market_cap(
        ticker: str,
) -> List[Dict[str, Any]]:
    """Fetch market cap from the API.
    {
      "company_facts": {
        "ticker": "<string>",
        "name": "<string>",
        "cik": "<string>",
        "market_cap": 123,
        "weighted_average_shares": 123,
        "number_of_employees": 123,
        "sic_code": "<string>",
        "sic_description": "<string>",
        "website_url": "<string>",
        "listing_date": "2023-12-25",
        "is_active": true
      }
    }
    """
    company = Vnstock().stock(symbol=ticker, source='TCBS').company

    company_facts = company.overview().to_json(orient='records')
    if not company_facts:
        raise ValueError("No company facts returned")
    return company_facts


def get_prices(
        ticker: str,
        start_date: str,
        end_date: str
) -> List[Dict[str, Any]]:
    stock = Vnstock().stock(symbol=ticker, source='VCI')

    df = stock.quote.history(
        symbol=ticker,
        start=start_date,
        end=end_date,
        interval='1D'
    )

    prices = df.to_json(orient='records', date_format='iso')

    if not prices:
        raise ValueError("No price data returned")
    return prices


def prices_to_df(prices: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert prices to a DataFrame."""

    df = pd.DataFrame(eval(prices))
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
