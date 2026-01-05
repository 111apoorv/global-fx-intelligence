# dashboard.py - FX Analytics Dashboard
# Apoorv - Cloud Computing Project
# Last updated: Dec 2025
# AI-Enhanced with Groq LLM

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json

# tried using boto3 for s3 but had issues, keeping it for later
import boto3
from io import BytesIO
import pyarrow.parquet as pq

# Groq AI Integration
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

# Groq API Key - Set via environment variable or Streamlit secrets
import os
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

# setting up the page
st.set_page_config(
    page_title="FX Intelligence Dashboard",
    page_icon="🌐",
    layout="wide"
)

# css styles - DARK PREMIUM THEME
custom_css = """
<style>
    /* Import premium fonts */
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
    
    /* Root variables */
    :root {
        --bg-primary: #0a0e17;
        --bg-secondary: #111827;
        --bg-card: #1a1f2e;
        --bg-card-hover: #242937;
        --accent-gold: #d4af37;
        --accent-gold-light: #f4d03f;
        --accent-cyan: #00d4ff;
        --accent-emerald: #10b981;
        --accent-rose: #f43f5e;
        --text-primary: #f8fafc;
        --text-secondary: #94a3b8;
        --text-muted: #64748b;
        --border-color: #2d3748;
        --glow-gold: rgba(212, 175, 55, 0.4);
    }
    
    .stApp {
        background: linear-gradient(135deg, var(--bg-primary) 0%, #0f1629 50%, var(--bg-primary) 100%);
        font-family: 'Outfit', sans-serif;
    }
    
    /* Animated gradient keyframes */
    @keyframes gradientShift {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.6; transform: scale(1.1); }
    }
    
    @keyframes glow {
        0%, 100% { box-shadow: 0 0 5px var(--glow-gold), 0 0 15px var(--glow-gold); }
        50% { box-shadow: 0 0 20px var(--glow-gold), 0 0 40px var(--glow-gold); }
    }
    
    @keyframes slideIn {
        from { opacity: 0; transform: translateY(-30px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    @keyframes fadeInUp {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    @keyframes float {
        0%, 100% { transform: translateY(0px); }
        50% { transform: translateY(-10px); }
    }
    
    @keyframes borderGlow {
        0%, 100% { border-color: var(--accent-gold); box-shadow: 0 0 10px var(--glow-gold); }
        50% { border-color: var(--accent-gold-light); box-shadow: 0 0 25px var(--glow-gold); }
    }
    
    @keyframes shimmer {
        0% { left: -100%; }
        100% { left: 100%; }
    }
    
    @keyframes rotateGradient {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    .header-section {
        background: linear-gradient(135deg, #0f1629 0%, #1a1f35 25%, #0f1629 50%, #1a1f35 75%, #0f1629 100%);
        background-size: 400% 400%;
        animation: gradientShift 12s ease infinite;
        padding: 2.5rem 2rem 2rem 2rem;
        border-radius: 24px;
        margin-bottom: 2rem;
        text-align: center;
        box-shadow: 0 20px 60px rgba(0,0,0,0.5), inset 0 1px 0 rgba(255,255,255,0.05);
        border: 1px solid var(--border-color);
        border-bottom: 3px solid var(--accent-gold);
        position: relative;
        overflow: hidden;
    }
    
    .header-section::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 200%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(212,175,55,0.08), transparent);
        animation: shimmer 4s infinite;
    }
    
    .header-section::after {
        content: '';
        position: absolute;
        top: -50%;
        right: -50%;
        width: 100%;
        height: 100%;
        background: radial-gradient(circle, rgba(212,175,55,0.03) 0%, transparent 70%);
        animation: rotateGradient 20s linear infinite;
    }
    
    .header-section h1 {
        color: var(--text-primary);
        font-size: 2.8rem;
        font-weight: 800;
        margin: 0;
        text-transform: uppercase;
        letter-spacing: 6px;
        text-shadow: 0 0 30px rgba(212,175,55,0.3);
        animation: slideIn 0.8s ease-out;
        position: relative;
        z-index: 1;
    }
    .header-section h1 span {
        background: linear-gradient(135deg, var(--accent-gold) 0%, var(--accent-gold-light) 50%, var(--accent-gold) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        filter: drop-shadow(0 0 20px var(--glow-gold));
    }
    .header-section p {
        color: var(--text-secondary);
        font-size: 1.1rem;
        margin-top: 0.8rem;
        letter-spacing: 2px;
        font-weight: 300;
        position: relative;
        z-index: 1;
    }
    
    /* Live indicator */
    .live-badge {
        display: inline-flex;
        align-items: center;
        background: linear-gradient(135deg, var(--accent-emerald) 0%, #059669 100%);
        color: white;
        padding: 0.4rem 1rem;
        border-radius: 30px;
        font-size: 0.7rem;
        font-weight: 700;
        margin-left: 1rem;
        text-transform: uppercase;
        letter-spacing: 2px;
        box-shadow: 0 4px 15px rgba(16,185,129,0.4), inset 0 1px 0 rgba(255,255,255,0.2);
        animation: glow 2s ease-in-out infinite;
        position: relative;
        z-index: 1;
    }
    .live-dot {
        width: 8px;
        height: 8px;
        background: white;
        border-radius: 50%;
        margin-right: 8px;
        animation: pulse 1.2s infinite;
        box-shadow: 0 0 10px white;
    }
    
    /* Stats ticker */
    .stats-ticker {
        display: flex;
        justify-content: center;
        gap: 3rem;
        margin-top: 1.5rem;
        padding-top: 1.5rem;
        border-top: 1px solid rgba(212,175,55,0.2);
        position: relative;
        z-index: 1;
    }
    .ticker-item {
        text-align: center;
        animation: fadeInUp 1s ease-out;
        animation-fill-mode: both;
    }
    .ticker-item:nth-child(1) { animation-delay: 0.1s; }
    .ticker-item:nth-child(2) { animation-delay: 0.2s; }
    .ticker-item:nth-child(3) { animation-delay: 0.3s; }
    .ticker-item:nth-child(4) { animation-delay: 0.4s; }
    
    .ticker-value {
        background: linear-gradient(135deg, var(--accent-gold) 0%, var(--accent-gold-light) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-size: 1.6rem;
        font-weight: 700;
        font-family: 'JetBrains Mono', monospace;
    }
    .ticker-label {
        color: var(--text-muted);
        font-size: 0.65rem;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-top: 0.3rem;
        font-weight: 500;
    }
    
    /* Date time display */
    .datetime-display {
        position: absolute;
        top: 15px;
        right: 20px;
        color: var(--text-muted);
        font-size: 0.75rem;
        text-align: right;
        font-family: 'JetBrains Mono', monospace;
        z-index: 1;
    }
    .datetime-display span {
        color: var(--accent-gold);
    }
    
    /* Metrics Cards */
    .stMetric {
        background: linear-gradient(145deg, var(--bg-card) 0%, var(--bg-secondary) 100%);
        padding: 1.2rem;
        border-radius: 16px;
        border: 1px solid var(--border-color);
        border-left: 4px solid var(--accent-gold);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.03);
        transition: all 0.3s ease;
    }
    .stMetric:hover {
        transform: translateY(-2px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.4), 0 0 20px var(--glow-gold);
        border-color: var(--accent-gold);
    }
    .stMetric label { 
        color: var(--text-secondary) !important; 
        font-weight: 500 !important;
        letter-spacing: 0.5px;
    }
    .stMetric [data-testid="stMetricValue"] { 
        color: var(--text-primary) !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-weight: 600 !important;
    }
    
    /* Summary Panel */
    .summary-panel {
        background: linear-gradient(145deg, var(--bg-card) 0%, rgba(26,31,46,0.95) 100%);
        padding: 1.8rem;
        border-radius: 20px;
        border: 1px solid var(--border-color);
        border-left: 4px solid var(--accent-gold);
        margin: 1rem 0;
        box-shadow: 0 10px 40px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.03);
        backdrop-filter: blur(10px);
        position: relative;
        overflow: hidden;
    }
    .summary-panel::before {
        content: '';
        position: absolute;
        top: 0;
        right: 0;
        width: 150px;
        height: 150px;
        background: radial-gradient(circle, rgba(212,175,55,0.05) 0%, transparent 70%);
    }
    .summary-panel h3 { 
        background: linear-gradient(135deg, var(--accent-gold) 0%, var(--accent-gold-light) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-top: 0; 
        font-weight: 700;
        font-size: 1.2rem;
        letter-spacing: 1px;
    }
    .summary-panel p { 
        color: var(--text-secondary); 
        line-height: 2;
        font-weight: 400;
    }
    .summary-panel strong {
        color: var(--text-primary);
    }
    
    /* Alert Panel */
    .alert-panel {
        background: linear-gradient(145deg, rgba(244,63,94,0.08) 0%, rgba(244,63,94,0.03) 100%);
        padding: 1.8rem;
        border-radius: 20px;
        border: 1px solid rgba(244,63,94,0.3);
        border-left: 4px solid var(--accent-rose);
        margin: 1rem 0;
        box-shadow: 0 10px 40px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.03);
        backdrop-filter: blur(10px);
    }
    .alert-panel h3 { 
        color: var(--accent-rose); 
        margin-top: 0;
        font-weight: 700;
        font-size: 1.1rem;
    }
    .alert-panel p {
        color: var(--text-secondary);
        line-height: 1.8;
    }
    .alert-panel strong {
        color: var(--text-primary);
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0a0e17 0%, #111827 50%, #0a0e17 100%);
        border-right: 1px solid var(--border-color);
    }
    section[data-testid="stSidebar"] .stMarkdown { 
        color: var(--text-secondary); 
    }
    section[data-testid="stSidebar"] h2 { 
        background: linear-gradient(135deg, var(--accent-gold) 0%, var(--accent-gold-light) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-weight: 700 !important;
    }
    section[data-testid="stSidebar"] .stRadio label {
        color: var(--text-secondary) !important;
    }
    
    /* Download Button */
    .stDownloadButton button {
        background: linear-gradient(135deg, var(--accent-gold) 0%, #b8960c 100%);
        border: none;
        color: #0a0e17;
        font-weight: 700;
        padding: 0.8rem 2rem;
        border-radius: 12px;
        letter-spacing: 1px;
        text-transform: uppercase;
        box-shadow: 0 4px 20px var(--glow-gold);
        transition: all 0.3s ease;
    }
    .stDownloadButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 30px var(--glow-gold);
    }
    
    /* Subheaders */
    h2, h3, .stSubheader {
        color: var(--text-primary) !important;
        font-weight: 600 !important;
    }
    
    /* General text */
    p, span, label, .stMarkdown {
        color: var(--text-secondary);
    }
    
    /* Select boxes */
    .stSelectbox > div > div {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 12px !important;
        color: var(--text-primary) !important;
    }
    .stSelectbox label {
        color: var(--text-secondary) !important;
    }
    
    /* Date input */
    .stDateInput > div > div {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 12px !important;
    }
    .stDateInput label {
        color: var(--text-secondary) !important;
    }
    
    /* Data tables */
    .stDataFrame {
        border: 1px solid var(--border-color) !important;
        border-radius: 16px !important;
        overflow: hidden;
    }
    .stDataFrame > div {
        background: var(--bg-card) !important;
    }
    
    /* Multiselect */
    .stMultiSelect > div > div {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 12px !important;
    }
    .stMultiSelect label {
        color: var(--text-secondary) !important;
    }
    
    /* Horizontal rule */
    hr {
        border-color: var(--border-color) !important;
        opacity: 0.5;
    }
    
    /* Spinner */
    .stSpinner > div {
        border-top-color: var(--accent-gold) !important;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 12px !important;
        color: var(--text-secondary) !important;
    }
    
    /* Filter section */
    .filter-section {
        background: linear-gradient(145deg, var(--bg-card) 0%, var(--bg-secondary) 100%);
        padding: 1.2rem 1.5rem;
        border-radius: 16px;
        border: 1px solid var(--border-color);
        border-left: 4px solid var(--accent-cyan);
        margin-bottom: 1.5rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.2);
    }
    .filter-section h4 {
        color: var(--accent-cyan) !important;
        margin: 0;
        font-weight: 600;
        letter-spacing: 1px;
    }
    
    /* Chart containers */
    .stPlotlyChart {
        background: var(--bg-card);
        border-radius: 16px;
        border: 1px solid var(--border-color);
        padding: 1rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.2);
    }
    
    /* Footer */
    .footer-text {
        text-align: center;
        color: var(--text-muted);
        font-size: 0.85rem;
        padding: 2rem 0;
        border-top: 1px solid var(--border-color);
        margin-top: 2rem;
    }
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

# currency symbols dict - had to look these up
curr_symbols = {
    "USD": "$", "EUR": "€", "GBP": "£", "INR": "₹", "JPY": "¥",
    "CAD": "C$", "AUD": "A$", "CHF": "Fr", "CNY": "¥", "SGD": "S$"
}

# function to load data from s3
# using caching so it doesnt reload every time
@st.cache_data(ttl=300)
def get_s3_data():
    try:
        s3_client = boto3.client('s3', region_name='us-east-2')
        bucket_name = 'apoorv-financial-pipeline-2025'
        data_prefix = 'output/normalized/'
        
        paginator = s3_client.get_paginator('list_objects_v2')
        
        all_dfs = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=data_prefix):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.parquet'):
                    # extract currency from partition path
                    curr = None
                    if 'currency=' in key:
                        curr = key.split('currency=')[1].split('/')[0]
                    
                    response = s3_client.get_object(Bucket=bucket_name, Key=key)
                    temp_df = pd.read_parquet(BytesIO(response['Body'].read()))
                    
                    if curr and 'currency' not in temp_df.columns:
                        temp_df['currency'] = curr
                    
                    all_dfs.append(temp_df)
        
        if all_dfs:
            result = pd.concat(all_dfs, ignore_index=True)
            if 'currency' not in result.columns:
                st.warning("currency column not found in s3 data")
                return None
            return result
        return None
    except Exception as err:
        st.error(f"s3 error: {err}")
        return None

# fallback - generate sample data if s3 fails
@st.cache_data
def generate_sample():
    try:
        return pd.read_parquet("sample_normalized.parquet")
    except:
        # generate fake data for testing
        import numpy as np
        np.random.seed(42)
        
        date_range = pd.date_range('2025-09-01', periods=90, freq='D')
        currencies = ['USD', 'EUR', 'GBP', 'INR', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'SGD']
        products = ['ECOM', 'RETAIL', 'SUBSCRIPTION', 'TRAVEL', 'FOREX', 'REMITTANCE', 'INVESTMENT']
        channels = ['ONLINE', 'POS', 'MOBILE', 'ATM', 'WIRE']
        
        num_rows = 5000
        
        # creating the dataframe
        df = pd.DataFrame({
            'txn_id': [f'TXN{str(i).zfill(7)}' for i in range(num_rows)],
            'customer_id': [f'C{str(np.random.randint(1, 500)).zfill(5)}' for _ in range(num_rows)],
            'txn_date': np.random.choice(date_range, num_rows),
            'amount': np.random.exponential(1000, num_rows),
            'currency': np.random.choice(currencies, num_rows, p=[0.3, 0.2, 0.15, 0.1, 0.08, 0.05, 0.04, 0.03, 0.03, 0.02]),
            'product_type': np.random.choice(products, num_rows),
            'channel': np.random.choice(channels, num_rows),
            'merchant_country': np.random.choice(['US', 'UK', 'DE', 'FR', 'IN', 'JP', 'CA', 'AU'], num_rows),
        })
        
        # fx rates - approximate values
        rates = {'USD': 1.0, 'EUR': 1.08, 'GBP': 1.27, 'INR': 0.012, 'JPY': 0.0067,
                 'CAD': 0.74, 'AUD': 0.65, 'CHF': 1.13, 'CNY': 0.14, 'SGD': 0.74}
        df['fx_rate'] = df['currency'].map(rates)
        df['amount_usd'] = df['amount'] * df['fx_rate']
        df['base_currency'] = 'USD'
        
        return df

# create summary text
def create_summary(data):
    vol = data['amount_usd'].sum()
    txn_count = len(data)
    avg = data['amount_usd'].mean()
    customers = data['customer_id'].nunique()
    
    # find top currency
    top_curr = data.groupby('currency')['amount_usd'].sum().idxmax()
    top_curr_pct = data.groupby('currency')['amount_usd'].sum().max() / vol * 100
    
    # find top product
    if 'product_type' in data.columns:
        top_prod = data.groupby('product_type')['amount_usd'].sum().idxmax()
        top_prod_pct = data.groupby('product_type')['amount_usd'].sum().max() / vol * 100
    else:
        top_prod = "N/A"
        top_prod_pct = 0
    
    # check concentration
    conc = data.groupby('currency')['amount_usd'].sum() / vol * 100
    high_conc = conc[conc > 25].to_dict()
    
    html = f"""
    <div class="summary-panel">
    <h3>📊 Executive Summary</h3>
    <p>
    <strong>Overview:</strong> Analyzed <strong>{txn_count:,}</strong> transactions totaling 
    <strong>${vol:,.0f}</strong> across <strong>{data['currency'].nunique()}</strong> currencies 
    from <strong>{customers:,}</strong> customers.<br><br>
    
    <strong>Top Currency:</strong> <strong>{top_curr}</strong> accounts for <strong>{top_curr_pct:.1f}%</strong> 
    of volume. Leading product: <strong>{top_prod}</strong> (<strong>{top_prod_pct:.1f}%</strong>).<br><br>
    
    <strong>Risk Assessment:</strong> {"⚠️ High concentration in " + ", ".join(high_conc.keys()) + ". Consider diversification." if high_conc else "✅ Currency mix is balanced."}
    </p>
    </div>
    """
    return html

# detect unusual transactions
def find_anomalies(data):
    threshold1 = 50000  # 50k usd
    threshold2 = 100000  # 100k usd
    
    results = {
        'high': len(data[data['amount_usd'] > threshold1]),
        'very_high': len(data[data['amount_usd'] > threshold2]),
        'total': len(data)
    }
    
    # get biggest transactions
    if 'product_type' in data.columns:
        top5 = data.nlargest(5, 'amount_usd')[['txn_id', 'amount_usd', 'currency', 'product_type']].to_dict('records')
    else:
        top5 = data.nlargest(5, 'amount_usd')[['txn_id', 'amount_usd', 'currency']].to_dict('records')
    
    return results, top5

# ============================================
# 🤖 AI ASSISTANT FUNCTIONS (Groq Integration)
# ============================================

def get_data_context(df):
    """Generate a summary of the data for the AI to understand."""
    context = {
        "total_transactions": len(df),
        "total_volume_usd": round(df['amount_usd'].sum(), 2),
        "avg_transaction_usd": round(df['amount_usd'].mean(), 2),
        "unique_customers": df['customer_id'].nunique(),
        "currencies": df['currency'].unique().tolist(),
        "currency_volumes": df.groupby('currency')['amount_usd'].sum().to_dict(),
        "currency_counts": df.groupby('currency').size().to_dict(),
    }
    
    if 'product_type' in df.columns:
        context["product_types"] = df['product_type'].unique().tolist()
        context["product_volumes"] = df.groupby('product_type')['amount_usd'].sum().to_dict()
    
    if 'channel' in df.columns:
        context["channels"] = df['channel'].unique().tolist()
        context["channel_volumes"] = df.groupby('channel')['amount_usd'].sum().to_dict()
    
    if 'merchant_country' in df.columns:
        context["countries"] = df['merchant_country'].unique().tolist()
        context["country_volumes"] = df.groupby('merchant_country')['amount_usd'].sum().to_dict()
    
    if 'txn_date' in df.columns:
        context["date_range"] = {
            "start": str(df['txn_date'].min()),
            "end": str(df['txn_date'].max())
        }
        # Daily trends
        daily = df.groupby('txn_date')['amount_usd'].sum()
        context["highest_volume_day"] = str(daily.idxmax())
        context["lowest_volume_day"] = str(daily.idxmin())
    
    # Anomalies
    high_value = df[df['amount_usd'] > 50000]
    context["high_value_transactions"] = len(high_value)
    context["anomaly_rate"] = round(len(high_value) / len(df) * 100, 2)
    
    # Top transactions
    top5 = df.nlargest(5, 'amount_usd')[['txn_id', 'amount_usd', 'currency']].to_dict('records')
    context["top_5_transactions"] = top5
    
    return context

def ask_ai_assistant(question, data_context, api_key):
    """Send a question to Groq AI and get a response."""
    if not GROQ_AVAILABLE:
        return "❌ Groq package not installed. Run: pip install groq"
    
    if not api_key:
        return "⚠️ Please enter your Groq API key in the sidebar."
    
    try:
        client = Groq(api_key=api_key)
        
        system_prompt = f"""You are an expert FX (Foreign Exchange) financial analyst AI assistant for a Global FX Intelligence Dashboard. 
You have access to the following real-time transaction data:

{json.dumps(data_context, indent=2, default=str)}

Your role:
- Answer questions about the FX transaction data clearly and concisely
- Provide insights, trends, and recommendations
- Use specific numbers from the data when possible
- Format currency values with $ and commas (e.g., $1,234,567)
- Be professional but friendly
- If asked about something not in the data, say so politely
- Keep responses concise (2-4 sentences unless more detail is requested)
- Use emojis sparingly to highlight key points (📈 📉 💰 ⚠️ ✅)"""

        chat_completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question}
            ],
            model="llama-3.3-70b-versatile",
            temperature=0.7,
            max_tokens=500,
        )
        
        return chat_completion.choices[0].message.content
    
    except Exception as e:
        return f"❌ Error: {str(e)}"

# main app function
def run_app():
    # get current time for header
    from datetime import datetime
    current_time = datetime.now().strftime("%B %d, %Y | %I:%M %p")
    
    # header with live indicator and stats ticker
    st.markdown(f"""
    <div class="header-section">
        <div class="datetime-display">
            📅 {current_time}<br>
            <span style="color: #ff9900;">AWS us-east-2</span>
        </div>
        <h1>Global <span>FX Intelligence</span> Platform
            <span class="live-badge"><span class="live-dot"></span>LIVE</span>
        </h1>
        <p>Real-Time Multi-Currency Transaction Analytics & Normalization Engine</p>
        <div class="stats-ticker">
            <div class="ticker-item">
                <div class="ticker-value">10</div>
                <div class="ticker-label">Currencies</div>
            </div>
            <div class="ticker-item">
                <div class="ticker-value">8</div>
                <div class="ticker-label">Countries</div>
            </div>
            <div class="ticker-item">
                <div class="ticker-value">5K+</div>
                <div class="ticker-label">Transactions</div>
            </div>
            <div class="ticker-item">
                <div class="ticker-value">90</div>
                <div class="ticker-label">Days Data</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # sidebar settings
    with st.sidebar:
        st.header("⚙️ Options")
        source = st.radio("Data Source", ["Local Sample", "AWS S3"], index=0)
        
        st.markdown("---")
        
        # 🤖 AI Assistant Section
        st.markdown("""
        <div style="background: linear-gradient(135deg, #d4af37 0%, #f4d03f 100%); 
                    padding: 0.5rem 1rem; border-radius: 10px; margin-bottom: 1rem;">
            <h3 style="color: #0a0e17; margin: 0; font-size: 1rem;">🤖 AI Assistant</h3>
        </div>
        """, unsafe_allow_html=True)
        
        # API Key input
        groq_api_key = st.text_input(
            "🔑 Groq API Key",
            type="password",
            value=GROQ_API_KEY,
            placeholder="Enter your Groq API key...",
            help="Get FREE key at: https://console.groq.com/keys"
        )
        
        if groq_api_key:
            st.markdown("""<p style="font-size: 0.8rem; color: #10b981;">✅ API Key configured</p>""", unsafe_allow_html=True)
        else:
            st.markdown("""
            <p style="font-size: 0.75rem; color: #f59e0b;">
            ⚠️ Enter API key to enable AI<br>
            👉 <a href="https://console.groq.com/keys" target="_blank" style="color: #d4af37;">Get FREE Key</a>
            </p>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        # debug info
        with st.expander("Debug Info"):
            st.write("Loading...")
    
    # load the data
    with st.spinner("Fetching data..."):
        if source == "AWS S3":
            df = get_s3_data()
            if df is None:
                st.warning("S3 failed, using sample data")
                df = generate_sample()
        else:
            df = generate_sample()
    
    # check if data loaded
    if df is None or len(df) == 0:
        st.error("No data loaded!")
        return
    
    # update debug info
    with st.sidebar:
        with st.expander("Debug Info", expanded=False):
            st.write(f"Records: {len(df):,}")
            st.write(f"Cols: {list(df.columns)}")
    
    # make sure we have required columns
    if 'currency' not in df.columns or 'amount_usd' not in df.columns:
        st.error(f"Missing columns! Have: {list(df.columns)}")
        return
    
    # fix date column
    if 'txn_date' in df.columns:
        df['txn_date'] = pd.to_datetime(df['txn_date'])
    
    # FILTERS SECTION
    st.markdown("""
    <div class="filter-section">
        <h4>🔍 FILTERS</h4>
    </div>
    """, unsafe_allow_html=True)
    
    c1, c2, c3, c4 = st.columns(4)
    
    # date filter
    with c1:
        if 'txn_date' in df.columns:
            min_d = df['txn_date'].min()
            max_d = df['txn_date'].max()
            dates = st.date_input("📅 Date Range", value=(min_d, max_d), min_value=min_d, max_value=max_d)
            if len(dates) == 2:
                df = df[(df['txn_date'] >= pd.Timestamp(dates[0])) & (df['txn_date'] <= pd.Timestamp(dates[1]))]
    
    # currency filter
    with c2:
        curr_opts = ["All"] + sorted(df['currency'].unique().tolist())
        sel_curr = st.selectbox("💱 Currency", curr_opts)
        if sel_curr != "All":
            df = df[df['currency'] == sel_curr]
    
    # product filter
    with c3:
        if 'product_type' in df.columns:
            prod_opts = ["All"] + sorted(df['product_type'].unique().tolist())
            sel_prod = st.selectbox("📦 Product", prod_opts)
            if sel_prod != "All":
                df = df[df['product_type'] == sel_prod]
    
    # channel filter
    with c4:
        if 'channel' in df.columns:
            chan_opts = ["All"] + sorted(df['channel'].unique().tolist())
            sel_chan = st.selectbox("📱 Channel", chan_opts)
            if sel_chan != "All":
                df = df[df['channel'] == sel_chan]
    
    st.markdown(f"<p style='text-align: right; color: #64748b; font-family: JetBrains Mono, monospace;'>Showing <strong style='color: #d4af37;'>{len(df):,}</strong> records</p>", unsafe_allow_html=True)
    
    # Summary and Anomaly panels
    left_col, right_col = st.columns([2, 1])
    
    with left_col:
        st.markdown(create_summary(df), unsafe_allow_html=True)
    
    with right_col:
        anomaly_data, _ = find_anomalies(df)
        rate = anomaly_data['high'] / anomaly_data['total'] * 100 if anomaly_data['total'] > 0 else 0
        st.markdown(f"""
        <div class="alert-panel">
        <h3>🚨 Anomaly Detection</h3>
        <p>
        <strong>High-Value (>$50K):</strong> {anomaly_data['high']:,}<br>
        <strong>Very High (>$100K):</strong> {anomaly_data['very_high']:,}<br>
        <strong>Anomaly Rate:</strong> {rate:.1f}%
        </p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # KPI metrics
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Transactions", f"{len(df):,}")
    with m2:
        st.metric("Total Volume (USD)", f"${df['amount_usd'].sum():,.2f}")
    with m3:
        st.metric("Avg Transaction", f"${df['amount_usd'].mean():,.2f}")
    with m4:
        st.metric("Unique Customers", f"{df['customer_id'].nunique():,}")
    
    st.markdown("---")
    
    # 🤖 AI CHAT ASSISTANT SECTION
    st.markdown("""
    <div style="background: linear-gradient(145deg, #1a1f2e 0%, #111827 100%);
                padding: 1.5rem; border-radius: 20px; border: 1px solid #2d3748;
                border-left: 4px solid #d4af37; margin-bottom: 1.5rem;
                box-shadow: 0 10px 40px rgba(0,0,0,0.3);">
        <h3 style="background: linear-gradient(135deg, #d4af37 0%, #f4d03f 100%);
                   -webkit-background-clip: text; -webkit-text-fill-color: transparent;
                   background-clip: text; margin: 0 0 0.5rem 0; font-size: 1.3rem;">
            🤖 AI Financial Analyst
        </h3>
        <p style="color: #94a3b8; margin: 0; font-size: 0.9rem;">
            Ask me anything about your FX transaction data
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize chat history
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    
    # Chat input
    ai_col1, ai_col2 = st.columns([4, 1])
    
    with ai_col1:
        user_question = st.text_input(
            "Ask a question",
            placeholder="e.g., What's my top performing currency? Show me unusual transactions...",
            label_visibility="collapsed"
        )
    
    with ai_col2:
        ask_button = st.button("🚀 Ask AI", type="primary", use_container_width=True)
    
    # Sample questions
    st.markdown("""
    <p style="color: #64748b; font-size: 0.8rem; margin-top: -0.5rem;">
        💡 Try: "What's my highest volume currency?" • "Are there any suspicious transactions?" • "Give me a weekly summary"
    </p>
    """, unsafe_allow_html=True)
    
    # Process question
    if ask_button and user_question:
        with st.spinner("🤖 Analyzing your data..."):
            # Get data context
            data_context = get_data_context(df)
            # Get AI response
            ai_response = ask_ai_assistant(user_question, data_context, groq_api_key)
            
            # Add to chat history
            st.session_state.chat_messages.append({"role": "user", "content": user_question})
            st.session_state.chat_messages.append({"role": "assistant", "content": ai_response})
    
    # Display chat history (last 3 exchanges)
    if st.session_state.chat_messages:
        st.markdown("""
        <div style="background: rgba(26,31,46,0.5); border-radius: 12px; padding: 1rem; margin-top: 1rem;">
        """, unsafe_allow_html=True)
        
        for msg in st.session_state.chat_messages[-6:]:  # Show last 3 Q&A pairs
            if msg["role"] == "user":
                st.markdown(f"""
                <div style="background: rgba(212,175,55,0.1); border-left: 3px solid #d4af37; 
                            padding: 0.8rem; border-radius: 8px; margin-bottom: 0.5rem;">
                    <strong style="color: #d4af37;">You:</strong>
                    <span style="color: #f8fafc;"> {msg["content"]}</span>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="background: rgba(0,212,255,0.05); border-left: 3px solid #00d4ff; 
                            padding: 0.8rem; border-radius: 8px; margin-bottom: 0.5rem;">
                    <strong style="color: #00d4ff;">🤖 AI:</strong>
                    <span style="color: #e2e8f0;"> {msg["content"]}</span>
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown("</div>", unsafe_allow_html=True)
        
        # Clear chat button
        if st.button("🗑️ Clear Chat", type="secondary"):
            st.session_state.chat_messages = []
            st.rerun()
    
    st.markdown("---")
    
    # Charts - Row 1
    chart1, chart2 = st.columns(2)
    
    with chart1:
        st.subheader("📊 Volume by Currency")
        vol_by_curr = df.groupby('currency')['amount_usd'].sum().sort_values(ascending=True)
        fig1 = px.bar(x=vol_by_curr.values, y=vol_by_curr.index, orientation='h',
                      color=vol_by_curr.values, color_continuous_scale=[[0, '#1a1f2e'], [0.5, '#d4af37'], [1, '#f4d03f']],
                      labels={'x': 'Volume (USD)', 'y': 'Currency'})
        fig1.update_layout(
            showlegend=False, height=400,
            template='plotly_dark',
            paper_bgcolor='rgba(26,31,46,0.8)',
            plot_bgcolor='rgba(17,24,39,0.8)',
            font=dict(family='Outfit', color='#94a3b8'),
            xaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
            yaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
            coloraxis_colorbar=dict(tickfont=dict(color='#94a3b8'))
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with chart2:
        st.subheader("🥧 Transaction Count Distribution")
        count_by_curr = df.groupby('currency').size()
        dark_gold_palette = ['#d4af37', '#00d4ff', '#10b981', '#f43f5e', '#a855f7', '#f59e0b', '#06b6d4', '#ec4899', '#84cc16', '#6366f1']
        fig2 = px.pie(values=count_by_curr.values, names=count_by_curr.index, hole=0.45,
                      color_discrete_sequence=dark_gold_palette)
        fig2.update_layout(
            height=400,
            template='plotly_dark',
            paper_bgcolor='rgba(26,31,46,0.8)',
            plot_bgcolor='rgba(17,24,39,0.8)',
            font=dict(family='Outfit', color='#94a3b8'),
            legend=dict(font=dict(color='#94a3b8'))
        )
        fig2.update_traces(textfont=dict(color='#f8fafc'))
        st.plotly_chart(fig2, use_container_width=True)
    
    # Charts - Row 2
    chart3, chart4 = st.columns(2)
    
    with chart3:
        st.subheader("📈 Daily Volume Trend")
        if 'txn_date' in df.columns:
            daily = df.groupby('txn_date')['amount_usd'].sum().reset_index()
            fig3 = px.line(daily, x='txn_date', y='amount_usd',
                           labels={'txn_date': 'Date', 'amount_usd': 'Volume (USD)'})
            fig3.update_traces(line_color='#d4af37', line_width=3, line_shape='spline')
            fig3.add_scatter(x=daily['txn_date'], y=daily['amount_usd'], mode='markers',
                           marker=dict(color='#d4af37', size=6, line=dict(color='#f4d03f', width=2)),
                           showlegend=False)
            fig3.update_layout(
                height=400,
                template='plotly_dark',
                paper_bgcolor='rgba(26,31,46,0.8)',
                plot_bgcolor='rgba(17,24,39,0.8)',
                font=dict(family='Outfit', color='#94a3b8'),
                xaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
                yaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748')
            )
            st.plotly_chart(fig3, use_container_width=True)
    
    with chart4:
        st.subheader("📊 Product Type Breakdown")
        if 'product_type' in df.columns:
            prod_vol = df.groupby('product_type')['amount_usd'].sum().sort_values(ascending=False)
            fig4 = px.bar(x=prod_vol.index, y=prod_vol.values,
                          color=prod_vol.values, color_continuous_scale=[[0, '#1a1f2e'], [0.5, '#00d4ff'], [1, '#06b6d4']],
                          labels={'x': 'Product', 'y': 'Volume (USD)'})
            fig4.update_layout(
                showlegend=False, height=400,
                template='plotly_dark',
                paper_bgcolor='rgba(26,31,46,0.8)',
                plot_bgcolor='rgba(17,24,39,0.8)',
                font=dict(family='Outfit', color='#94a3b8'),
                xaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
                yaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748')
            )
            st.plotly_chart(fig4, use_container_width=True)
    
    # Currency trends chart
    st.markdown("---")
    st.subheader("💹 Currency Trends Over Time")
    
    if 'txn_date' in df.columns:
        trends = df.groupby(['txn_date', 'currency'])['amount_usd'].sum().reset_index()
        dark_gold_palette = ['#d4af37', '#00d4ff', '#10b981', '#f43f5e', '#a855f7', '#f59e0b', '#06b6d4', '#ec4899', '#84cc16', '#6366f1']
        fig5 = px.line(trends, x='txn_date', y='amount_usd', color='currency',
                       labels={'txn_date': 'Date', 'amount_usd': 'Volume', 'currency': 'Currency'},
                       color_discrete_sequence=dark_gold_palette)
        fig5.update_layout(
            height=500,
            template='plotly_dark',
            paper_bgcolor='rgba(26,31,46,0.8)',
            plot_bgcolor='rgba(17,24,39,0.8)',
            font=dict(family='Outfit', color='#94a3b8'),
            xaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
            yaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
            legend=dict(orientation="h", y=1.02, font=dict(color='#94a3b8'))
        )
        fig5.update_traces(line_width=2)
        st.plotly_chart(fig5, use_container_width=True)
    
    # GEO MAP - World map showing transactions
    st.markdown("---")
    st.subheader("🗺️ Global Transaction Heatmap")
    
    if 'merchant_country' in df.columns:
        # country code mapping for plotly
        country_codes = {
            'US': 'USA', 'UK': 'GBR', 'DE': 'DEU', 'FR': 'FRA', 
            'IN': 'IND', 'JP': 'JPN', 'CA': 'CAN', 'AU': 'AUS',
            'CN': 'CHN', 'SG': 'SGP', 'BR': 'BRA', 'MX': 'MEX',
            'IT': 'ITA', 'ES': 'ESP', 'NL': 'NLD', 'CH': 'CHE'
        }
        
        # aggregate by country
        geo_data = df.groupby('merchant_country').agg({
            'amount_usd': 'sum',
            'txn_id': 'count'
        }).reset_index()
        geo_data.columns = ['country', 'volume', 'transactions']
        geo_data['country_code'] = geo_data['country'].map(country_codes)
        geo_data = geo_data.dropna(subset=['country_code'])
        
        # create the map
        fig_map = px.scatter_geo(
            geo_data,
            locations='country_code',
            size='volume',
            color='volume',
            hover_name='country',
            hover_data={'transactions': True, 'volume': ':$,.0f'},
            color_continuous_scale=[[0, '#1a1f2e'], [0.3, '#d4af37'], [0.7, '#f4d03f'], [1, '#fef3c7']],
            projection='natural earth',
            title=''
        )
        fig_map.update_layout(
            height=500,
            template='plotly_dark',
            paper_bgcolor='rgba(26,31,46,0.8)',
            font=dict(family='Outfit', color='#94a3b8'),
            geo=dict(
                showland=True,
                landcolor='#1a1f2e',
                showocean=True,
                oceancolor='#0a0e17',
                showcoastlines=True,
                coastlinecolor='#2d3748',
                showframe=False,
                bgcolor='rgba(10,14,23,0.8)',
                showcountries=True,
                countrycolor='#2d3748'
            ),
            margin=dict(l=0, r=0, t=30, b=0),
            coloraxis_colorbar=dict(tickfont=dict(color='#94a3b8'))
        )
        fig_map.update_traces(marker=dict(line=dict(width=2, color='#d4af37')))
        st.plotly_chart(fig_map, use_container_width=True)
        
        # show top 3 countries below map
        top3 = geo_data.nlargest(3, 'volume')
        map_c1, map_c2, map_c3 = st.columns(3)
        if len(top3) >= 1:
            with map_c1:
                st.metric(f"🥇 {top3.iloc[0]['country']}", f"${top3.iloc[0]['volume']:,.0f}")
        if len(top3) >= 2:
            with map_c2:
                st.metric(f"🥈 {top3.iloc[1]['country']}", f"${top3.iloc[1]['volume']:,.0f}")
        if len(top3) >= 3:
            with map_c3:
                st.metric(f"🥉 {top3.iloc[2]['country']}", f"${top3.iloc[2]['volume']:,.0f}")
    
    # More charts
    st.markdown("---")
    ch5, ch6 = st.columns(2)
    
    with ch5:
        st.subheader("📱 Channel Analysis")
        if 'channel' in df.columns:
            chan_stats = df.groupby('channel').agg({'txn_id': 'count', 'amount_usd': 'sum'})
            chan_stats.columns = ['count', 'volume']
            
            # dual axis chart
            fig6 = make_subplots(specs=[[{"secondary_y": True}]])
            fig6.add_trace(go.Bar(x=chan_stats.index, y=chan_stats['count'], name='Count', 
                                  marker_color='#d4af37', marker_line_color='#f4d03f', marker_line_width=1), secondary_y=False)
            fig6.add_trace(go.Scatter(x=chan_stats.index, y=chan_stats['volume'], name='Volume', 
                                      mode='lines+markers', marker_color='#00d4ff', line_color='#00d4ff',
                                      marker=dict(size=10, line=dict(color='#06b6d4', width=2))), secondary_y=True)
            fig6.update_yaxes(title_text="Count", secondary_y=False, gridcolor='#2d3748', color='#94a3b8')
            fig6.update_yaxes(title_text="Volume (USD)", secondary_y=True, gridcolor='#2d3748', color='#94a3b8')
            fig6.update_xaxes(gridcolor='#2d3748', color='#94a3b8')
            fig6.update_layout(
                height=400,
                template='plotly_dark',
                paper_bgcolor='rgba(26,31,46,0.8)',
                plot_bgcolor='rgba(17,24,39,0.8)',
                font=dict(family='Outfit', color='#94a3b8'),
                legend=dict(font=dict(color='#94a3b8'))
            )
            st.plotly_chart(fig6, use_container_width=True)
    
    with ch6:
        st.subheader("🌍 Top Countries")
        if 'merchant_country' in df.columns:
            country_vol = df.groupby('merchant_country')['amount_usd'].sum().sort_values(ascending=False).head(10)
            fig7 = px.bar(x=country_vol.index, y=country_vol.values,
                          color=country_vol.values, color_continuous_scale=[[0, '#1a1f2e'], [0.5, '#10b981'], [1, '#34d399']],
                          labels={'x': 'Country', 'y': 'Volume (USD)'})
            fig7.update_layout(
                showlegend=False, height=400,
                template='plotly_dark',
                paper_bgcolor='rgba(26,31,46,0.8)',
                plot_bgcolor='rgba(17,24,39,0.8)',
                font=dict(family='Outfit', color='#94a3b8'),
                xaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
                yaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748')
            )
            st.plotly_chart(fig7, use_container_width=True)
    
    # Data table
    st.markdown("---")
    st.subheader("📋 Raw Data")
    
    cols_to_show = st.multiselect("Select columns", df.columns.tolist(),
                                   default=['txn_id', 'txn_date', 'currency', 'amount', 'amount_usd', 'product_type', 'channel'])
    
    if cols_to_show:
        st.dataframe(df[cols_to_show].head(100), use_container_width=True, height=400)
    
    # download button
    st.download_button("📥 Download CSV", df.to_csv(index=False).encode('utf-8'),
                       "fx_data_export.csv", "text/csv")
    
    # footer
    st.markdown("---")
    st.markdown("""
    <div class="footer-text">
        <span style="color: #d4af37;">◆</span> Global FX Intelligence Platform <span style="color: #d4af37;">◆</span><br>
        <span style="font-size: 0.75rem; color: #64748b;">Powered by AWS EMR + Apache Spark + Streamlit</span>
    </div>
    """, unsafe_allow_html=True)

# run the app
if __name__ == "__main__":
    run_app()

