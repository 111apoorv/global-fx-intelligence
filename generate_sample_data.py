"""
Generate Sample Data for Local Streamlit Testing
-------------------------------------------------
Creates sample_normalized.parquet for offline development.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sample():
    """Generate sample normalized transaction data."""
    np.random.seed(42)
    
    # Configuration
    n_transactions = 5000
    start_date = datetime(2025, 9, 1)
    n_days = 90
    
    # Data distributions
    currencies = ['USD', 'EUR', 'GBP', 'INR', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'SGD']
    currency_weights = [0.30, 0.20, 0.15, 0.10, 0.08, 0.05, 0.04, 0.03, 0.03, 0.02]
    
    product_types = ['ECOM', 'RETAIL', 'SUBSCRIPTION', 'TRAVEL', 'FOREX', 'REMITTANCE', 'INVESTMENT']
    channels = ['ONLINE', 'POS', 'MOBILE', 'ATM', 'WIRE']
    countries = ['US', 'UK', 'DE', 'FR', 'IN', 'JP', 'CA', 'AU', 'SG', 'CH', 'CN', 'NL']
    
    # Base FX rates
    fx_rates_base = {
        'USD': 1.00, 'EUR': 1.08, 'GBP': 1.27, 'INR': 0.012, 'JPY': 0.0067,
        'CAD': 0.74, 'AUD': 0.65, 'CHF': 1.13, 'CNY': 0.14, 'SGD': 0.74
    }
    
    # Generate dates
    dates = [start_date + timedelta(days=i) for i in range(n_days)]
    
    # Generate transactions
    transactions = []
    
    for i in range(n_transactions):
        txn_date = np.random.choice(dates)
        currency = np.random.choice(currencies, p=currency_weights)
        product_type = np.random.choice(product_types)
        
        # Amount based on product type
        if product_type == 'ECOM':
            amount = np.random.uniform(10, 500)
        elif product_type == 'RETAIL':
            amount = np.random.uniform(5, 200)
        elif product_type == 'SUBSCRIPTION':
            amount = np.random.choice([9.99, 14.99, 19.99, 29.99, 49.99, 99.99])
        elif product_type == 'TRAVEL':
            amount = np.random.uniform(100, 5000)
        elif product_type == 'FOREX':
            amount = np.random.uniform(500, 50000)
        elif product_type == 'REMITTANCE':
            amount = np.random.uniform(100, 10000)
        else:  # INVESTMENT
            amount = np.random.uniform(1000, 100000)
        
        # Add some daily FX rate variation
        day_offset = (txn_date - start_date).days
        rate_variation = 1 + np.random.uniform(-0.02, 0.02) * (day_offset / n_days)
        fx_rate = fx_rates_base[currency] * rate_variation
        
        amount_usd = amount * fx_rate
        
        transactions.append({
            'txn_id': f'TXN{str(i+1).zfill(7)}',
            'customer_id': f'C{str(np.random.randint(1, 501)).zfill(5)}',
            'txn_ts': txn_date.strftime('%Y-%m-%d %H:%M:%S'),
            'txn_date': txn_date,
            'amount': round(amount, 2),
            'currency': currency,
            'fx_rate': round(fx_rate, 6),
            'amount_usd': round(amount_usd, 2),
            'merchant_country': np.random.choice(countries),
            'channel': np.random.choice(channels),
            'product_type': product_type,
            'customer_segment': np.random.choice(['RETAIL', 'PREMIUM', 'CORPORATE', 'SMB']),
            'base_currency': 'USD',
            'fx_rate_missing': False
        })
    
    df = pd.DataFrame(transactions)
    
    # Save to parquet
    df.to_parquet('sample_normalized.parquet', index=False)
    print(f"Generated {len(df)} sample transactions")
    print(f"Saved to: sample_normalized.parquet")
    
    # Print summary
    print("\n=== Sample Data Summary ===")
    print(f"Date range: {df['txn_date'].min()} to {df['txn_date'].max()}")
    print(f"Total volume: ${df['amount_usd'].sum():,.2f}")
    print(f"Unique customers: {df['customer_id'].nunique()}")
    print("\nBy Currency:")
    print(df.groupby('currency')['amount_usd'].sum().sort_values(ascending=False))
    
    return df

if __name__ == "__main__":
    generate_sample()


