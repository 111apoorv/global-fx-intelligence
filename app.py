"""
FX Normalization Dashboard
--------------------------
Interactive Streamlit UI for exploring normalized transaction data.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import boto3
from io import BytesIO
import pyarrow.parquet as pq

# Page configuration
st.set_page_config(
    page_title="Global FX Intelligence Platform",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for DARK PREMIUM THEME
st.markdown("""
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
    
    /* Main Background */
    .stApp {
        background: linear-gradient(135deg, var(--bg-primary) 0%, #0f1629 50%, var(--bg-primary) 100%);
        font-family: 'Outfit', sans-serif;
    }
    
    /* Animations */
    @keyframes shimmer {
        0% { left: -100%; }
        100% { left: 100%; }
    }
    
    @keyframes glow {
        0%, 100% { box-shadow: 0 0 5px var(--glow-gold), 0 0 15px var(--glow-gold); }
        50% { box-shadow: 0 0 20px var(--glow-gold), 0 0 40px var(--glow-gold); }
    }
    
    @keyframes slideIn {
        from { opacity: 0; transform: translateY(-30px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    /* Main Header - Dark Premium Theme */
    .main-header {
        background: linear-gradient(135deg, #0f1629 0%, #1a1f35 50%, #0f1629 100%);
        padding: 2.5rem;
        border-radius: 24px;
        margin-bottom: 2rem;
        text-align: center;
        box-shadow: 0 20px 60px rgba(0,0,0,0.5), inset 0 1px 0 rgba(255,255,255,0.05);
        border: 1px solid var(--border-color);
        border-bottom: 3px solid var(--accent-gold);
        position: relative;
        overflow: hidden;
    }
    .main-header::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 200%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(212,175,55,0.08), transparent);
        animation: shimmer 4s infinite;
    }
    .main-header h1 {
        color: var(--text-primary);
        font-size: 2.8rem;
        font-weight: 800;
        margin: 0;
        text-transform: uppercase;
        letter-spacing: 5px;
        text-shadow: 0 0 30px rgba(212,175,55,0.3);
        animation: slideIn 0.8s ease-out;
        position: relative;
        z-index: 1;
    }
    .main-header h1 span {
        background: linear-gradient(135deg, var(--accent-gold) 0%, var(--accent-gold-light) 50%, var(--accent-gold) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        filter: drop-shadow(0 0 20px var(--glow-gold));
    }
    .main-header p {
        color: var(--text-secondary);
        font-size: 1.1rem;
        margin-top: 0.8rem;
        font-weight: 300;
        letter-spacing: 2px;
        position: relative;
        z-index: 1;
    }
    
    /* Metrics styling - Dark Premium */
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
    
    /* Executive Summary Box - Dark Premium */
    .exec-summary {
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
    .exec-summary::before {
        content: '';
        position: absolute;
        top: 0;
        right: 0;
        width: 150px;
        height: 150px;
        background: radial-gradient(circle, rgba(212,175,55,0.05) 0%, transparent 70%);
    }
    .exec-summary h3 {
        background: linear-gradient(135deg, var(--accent-gold) 0%, var(--accent-gold-light) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-top: 0;
        font-weight: 700;
        font-size: 1.2rem;
        letter-spacing: 1px;
    }
    .exec-summary p {
        color: var(--text-secondary);
        line-height: 2;
    }
    .exec-summary strong {
        color: var(--text-primary);
    }
    
    /* Anomaly Alert Box - Dark Premium with Rose Accent */
    .anomaly-box {
        background: linear-gradient(145deg, rgba(244,63,94,0.08) 0%, rgba(244,63,94,0.03) 100%);
        padding: 1.8rem;
        border-radius: 20px;
        border: 1px solid rgba(244,63,94,0.3);
        border-left: 4px solid var(--accent-rose);
        margin: 1rem 0;
        box-shadow: 0 10px 40px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.03);
        backdrop-filter: blur(10px);
    }
    .anomaly-box h3 {
        color: var(--accent-rose);
        margin-top: 0;
        font-weight: 700;
        font-size: 1.1rem;
    }
    .anomaly-box p {
        color: var(--text-secondary);
        line-height: 1.8;
    }
    .anomaly-box strong {
        color: var(--text-primary);
    }
    
    /* Filter Box - Dark Premium with Cyan */
    .filter-box {
        background: linear-gradient(145deg, var(--bg-card) 0%, var(--bg-secondary) 100%);
        padding: 1.2rem 1.5rem;
        border-radius: 16px;
        border: 1px solid var(--border-color);
        border-left: 4px solid var(--accent-cyan);
        margin-bottom: 1.5rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.2);
    }
    .filter-box h4 {
        color: var(--accent-cyan) !important;
        margin: 0;
        font-weight: 600;
        letter-spacing: 1px;
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
    
    /* Data table */
    .stDataFrame {
        border: 1px solid var(--border-color) !important;
        border-radius: 16px !important;
        overflow: hidden;
    }
    .stDataFrame > div {
        background: var(--bg-card) !important;
    }
    
    /* Sidebar - Dark Premium */
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
    
    /* Divider lines */
    hr {
        border-color: var(--border-color) !important;
        opacity: 0.5;
    }
    
    /* Download button - Gold Premium */
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
    
    /* Multiselect */
    .stMultiSelect > div > div {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 12px !important;
    }
    .stMultiSelect label {
        color: var(--text-secondary) !important;
    }
    
    /* Info box */
    .stAlert {
        background: rgba(212,175,55,0.1);
        border: 1px solid var(--accent-gold);
        color: var(--text-primary);
        border-radius: 12px;
    }
    
    /* Chart containers */
    .stPlotlyChart {
        background: var(--bg-card);
        border-radius: 16px;
        border: 1px solid var(--border-color);
        padding: 1rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.2);
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 12px !important;
        color: var(--text-secondary) !important;
    }
</style>
""", unsafe_allow_html=True)

# Currency symbols
CURRENCY_SYMBOLS = {
    "USD": "$", "EUR": "€", "GBP": "£", "INR": "₹", "JPY": "¥",
    "CAD": "C$", "AUD": "A$", "CHF": "Fr", "CNY": "¥", "SGD": "S$"
}

@st.cache_data(ttl=300)
def load_data_from_s3():
    """Load normalized data from S3 parquet files."""
    try:
        s3 = boto3.client('s3', region_name='us-east-2')
        bucket = 'apoorv-financial-pipeline-2025'
        prefix = 'output/normalized/'
        
        paginator = s3.get_paginator('list_objects_v2')
        
        dfs = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.parquet'):
                    currency = None
                    if 'currency=' in key:
                        currency = key.split('currency=')[1].split('/')[0]
                    
                    obj_response = s3.get_object(Bucket=bucket, Key=key)
                    df = pd.read_parquet(BytesIO(obj_response['Body'].read()))
                    
                    if currency and 'currency' not in df.columns:
                        df['currency'] = currency
                    
                    dfs.append(df)
        
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            if 'currency' not in combined.columns:
                st.warning("Currency column missing from S3 data")
                return None
            return combined
        return None
    except Exception as e:
        st.error(f"Error loading from S3: {e}")
        return None

@st.cache_data
def load_sample_data():
    """Load sample data for local testing."""
    try:
        return pd.read_parquet("sample_normalized.parquet")
    except:
        import numpy as np
        np.random.seed(42)
        
        dates = pd.date_range('2025-09-01', periods=90, freq='D')
        currencies = ['USD', 'EUR', 'GBP', 'INR', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'SGD']
        product_types = ['ECOM', 'RETAIL', 'SUBSCRIPTION', 'TRAVEL', 'FOREX', 'REMITTANCE', 'INVESTMENT']
        channels = ['ONLINE', 'POS', 'MOBILE', 'ATM', 'WIRE']
        
        n = 5000
        data = {
            'txn_id': [f'TXN{str(i).zfill(7)}' for i in range(n)],
            'customer_id': [f'C{str(np.random.randint(1, 500)).zfill(5)}' for _ in range(n)],
            'txn_date': np.random.choice(dates, n),
            'amount': np.random.exponential(1000, n),
            'currency': np.random.choice(currencies, n, p=[0.3, 0.2, 0.15, 0.1, 0.08, 0.05, 0.04, 0.03, 0.03, 0.02]),
            'product_type': np.random.choice(product_types, n),
            'channel': np.random.choice(channels, n),
            'merchant_country': np.random.choice(['US', 'UK', 'DE', 'FR', 'IN', 'JP', 'CA', 'AU'], n),
        }
        
        df = pd.DataFrame(data)
        
        fx_rates = {'USD': 1.0, 'EUR': 1.08, 'GBP': 1.27, 'INR': 0.012, 'JPY': 0.0067,
                    'CAD': 0.74, 'AUD': 0.65, 'CHF': 1.13, 'CNY': 0.14, 'SGD': 0.74}
        df['fx_rate'] = df['currency'].map(fx_rates)
        df['amount_usd'] = df['amount'] * df['fx_rate']
        df['base_currency'] = 'USD'
        
        return df

def generate_executive_summary(df):
    """Generate AI-style executive summary."""
    total_volume = df['amount_usd'].sum()
    total_txn = len(df)
    avg_txn = df['amount_usd'].mean()
    unique_customers = df['customer_id'].nunique()
    
    # Top currency
    top_currency = df.groupby('currency')['amount_usd'].sum().idxmax()
    top_currency_pct = (df.groupby('currency')['amount_usd'].sum().max() / total_volume * 100)
    
    # Top product
    if 'product_type' in df.columns:
        top_product = df.groupby('product_type')['amount_usd'].sum().idxmax()
        top_product_pct = (df.groupby('product_type')['amount_usd'].sum().max() / total_volume * 100)
    else:
        top_product = "N/A"
        top_product_pct = 0
    
    # Currency concentration risk
    currency_concentration = df.groupby('currency')['amount_usd'].sum() / total_volume * 100
    high_concentration = currency_concentration[currency_concentration > 25].to_dict()
    
    summary = f"""
    <div class="exec-summary">
    <h3>📊 Executive Intelligence Summary</h3>
    <p>
    <strong>Overview:</strong> Processed <strong>{total_txn:,}</strong> transactions worth 
    <strong>${total_volume:,.0f}</strong> across <strong>{df['currency'].nunique()}</strong> currencies 
    from <strong>{unique_customers:,}</strong> unique customers.<br><br>
    
    <strong>Key Finding:</strong> <strong>{top_currency}</strong> dominates with <strong>{top_currency_pct:.1f}%</strong> 
    of total volume. <strong>{top_product}</strong> is the highest-value product category 
    at <strong>{top_product_pct:.1f}%</strong> of revenue.<br><br>
    
    <strong>💡 Recommendation:</strong> {"⚠️ High concentration risk in " + ", ".join(high_concentration.keys()) + ". Consider diversification strategy." if high_concentration else "✅ Currency distribution is well-balanced."}
    </p>
    </div>
    """
    return summary

def detect_anomalies(df):
    """Detect anomalous transactions."""
    # Define thresholds
    high_value_threshold = 50000  # $50K
    very_high_threshold = 100000  # $100K
    
    anomalies = {
        'high_value': len(df[df['amount_usd'] > high_value_threshold]),
        'very_high': len(df[df['amount_usd'] > very_high_threshold]),
        'total': len(df)
    }
    
    # Get top 5 highest transactions
    top_5 = df.nlargest(5, 'amount_usd')[['txn_id', 'amount_usd', 'currency', 'product_type']].to_dict('records') if 'product_type' in df.columns else df.nlargest(5, 'amount_usd')[['txn_id', 'amount_usd', 'currency']].to_dict('records')
    
    return anomalies, top_5

def main():
    # Professional Header
    st.markdown("""
    <div class="main-header">
        <h1>Global <span>FX Intelligence</span> Platform</h1>
        <p>Real-Time Multi-Currency Transaction Analytics & Normalization Engine</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Data source selection in sidebar (minimal)
    with st.sidebar:
        st.header("⚙️ Settings")
        data_source = st.radio(
            "Data Source",
            ["Sample Data (Local)", "S3 (Production)"],
            index=0
        )
        st.markdown("---")
        with st.expander("📋 Data Info"):
            st.write("Loading...")
    
    # Load data
    with st.spinner("Loading data..."):
        if data_source == "S3 (Production)":
            df = load_data_from_s3()
            if df is None:
                st.warning("Could not load data from S3. Using sample data instead.")
                df = load_sample_data()
        else:
            df = load_sample_data()
    
    if df is None or df.empty:
        st.error("No data available. Please check your data source.")
        return
    
    # Update sidebar info
    with st.sidebar:
        with st.expander("📋 Data Info", expanded=False):
            st.write(f"Rows: {len(df):,}")
            st.write(f"Columns: {list(df.columns)}")
    
    # Ensure required columns exist
    required_cols = ['currency', 'amount_usd']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"Missing required columns: {missing}. Available: {list(df.columns)}")
        return
    
    # Convert date columns
    if 'txn_date' in df.columns:
        df['txn_date'] = pd.to_datetime(df['txn_date'])
    
    # FILTERS ROW - Above KPIs (Compact Dropdowns)
    st.markdown("""
    <div class="filter-box">
        <h4>🔍 QUICK FILTERS</h4>
    </div>
    """, unsafe_allow_html=True)
    
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
    
    with filter_col1:
        if 'txn_date' in df.columns:
            min_date = df['txn_date'].min()
            max_date = df['txn_date'].max()
            date_range = st.date_input(
                "📅 Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )
            if len(date_range) == 2:
                df = df[(df['txn_date'] >= pd.Timestamp(date_range[0])) & 
                       (df['txn_date'] <= pd.Timestamp(date_range[1]))]
    
    with filter_col2:
        all_currencies = ["All Currencies"] + sorted(df['currency'].unique().tolist())
        selected_currency = st.selectbox("💱 Currency", all_currencies, index=0)
        if selected_currency != "All Currencies":
            df = df[df['currency'] == selected_currency]
    
    with filter_col3:
        if 'product_type' in df.columns:
            all_products = ["All Products"] + sorted(df['product_type'].unique().tolist())
            selected_product = st.selectbox("📦 Product Type", all_products, index=0)
            if selected_product != "All Products":
                df = df[df['product_type'] == selected_product]
    
    with filter_col4:
        if 'channel' in df.columns:
            all_channels = ["All Channels"] + sorted(df['channel'].unique().tolist())
            selected_channel = st.selectbox("📱 Channel", all_channels, index=0)
            if selected_channel != "All Channels":
                df = df[df['channel'] == selected_channel]
    
    st.markdown(f"<p style='text-align: right; color: #64748b; margin-top: -10px; font-family: JetBrains Mono, monospace;'>📊 Showing <strong style='color: #d4af37;'>{len(df):,}</strong> transactions</p>", unsafe_allow_html=True)
    
    # Executive Summary & Anomaly Detection Row
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown(generate_executive_summary(df), unsafe_allow_html=True)
    
    with col2:
        anomalies, top_5 = detect_anomalies(df)
        st.markdown(f"""
        <div class="anomaly-box">
        <h3>🚨 Anomaly Detection</h3>
        <p>
        <strong>High-Value Transactions (>${"50K"}):</strong> {anomalies['high_value']:,}<br>
        <strong>Very High (>${"100K"}):</strong> {anomalies['very_high']:,}<br>
        <strong>Anomaly Rate:</strong> {anomalies['high_value']/anomalies['total']*100:.1f}%
        </p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Key Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_txn = len(df)
        st.metric("Total Transactions", f"{total_txn:,}")
    
    with col2:
        total_usd = df['amount_usd'].sum()
        st.metric("Total Volume (USD)", f"${total_usd:,.2f}")
    
    with col3:
        avg_usd = df['amount_usd'].mean()
        st.metric("Avg Transaction (USD)", f"${avg_usd:,.2f}")
    
    with col4:
        unique_customers = df['customer_id'].nunique()
        st.metric("Unique Customers", f"{unique_customers:,}")
    
    st.markdown("---")
    
    # Charts Row 1
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Transaction Volume by Currency")
        currency_volume = df.groupby('currency')['amount_usd'].sum().sort_values(ascending=True)
        
        fig = px.bar(
            x=currency_volume.values,
            y=currency_volume.index,
            orientation='h',
            color=currency_volume.values,
            color_continuous_scale=[[0, '#1a1f2e'], [0.5, '#d4af37'], [1, '#f4d03f']],
            labels={'x': 'Total Volume (USD)', 'y': 'Currency'}
        )
        fig.update_layout(
            showlegend=False, height=400,
            template='plotly_dark',
            paper_bgcolor='rgba(26,31,46,0.8)',
            plot_bgcolor='rgba(17,24,39,0.8)',
            font=dict(family='Outfit', color='#94a3b8'),
            xaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
            yaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
            coloraxis_colorbar=dict(tickfont=dict(color='#94a3b8'))
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🥧 Transaction Count by Currency")
        currency_count = df.groupby('currency').size()
        dark_gold_palette = ['#d4af37', '#00d4ff', '#10b981', '#f43f5e', '#a855f7', '#f59e0b', '#06b6d4', '#ec4899', '#84cc16', '#6366f1']
        
        fig = px.pie(
            values=currency_count.values,
            names=currency_count.index,
            hole=0.45,
            color_discrete_sequence=dark_gold_palette
        )
        fig.update_layout(
            height=400,
            template='plotly_dark',
            paper_bgcolor='rgba(26,31,46,0.8)',
            plot_bgcolor='rgba(17,24,39,0.8)',
            font=dict(family='Outfit', color='#94a3b8'),
            legend=dict(font=dict(color='#94a3b8'))
        )
        fig.update_traces(textfont=dict(color='#f8fafc'))
        st.plotly_chart(fig, use_container_width=True)
    
    # Charts Row 2
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Daily Transaction Trend")
        if 'txn_date' in df.columns:
            daily_volume = df.groupby('txn_date')['amount_usd'].sum().reset_index()
            
            fig = px.line(
                daily_volume,
                x='txn_date',
                y='amount_usd',
                labels={'txn_date': 'Date', 'amount_usd': 'Volume (USD)'}
            )
            fig.update_traces(line_color='#d4af37', line_width=3, line_shape='spline')
            fig.add_scatter(x=daily_volume['txn_date'], y=daily_volume['amount_usd'], mode='markers',
                           marker=dict(color='#d4af37', size=6, line=dict(color='#f4d03f', width=2)),
                           showlegend=False)
            fig.update_layout(
                height=400,
                template='plotly_dark',
                paper_bgcolor='rgba(26,31,46,0.8)',
                plot_bgcolor='rgba(17,24,39,0.8)',
                font=dict(family='Outfit', color='#94a3b8'),
                xaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
                yaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748')
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Volume by Product Type")
        if 'product_type' in df.columns:
            product_volume = df.groupby('product_type')['amount_usd'].sum().sort_values(ascending=False)
            
            fig = px.bar(
                x=product_volume.index,
                y=product_volume.values,
                color=product_volume.values,
                color_continuous_scale=[[0, '#1a1f2e'], [0.5, '#00d4ff'], [1, '#06b6d4']],
                labels={'x': 'Product Type', 'y': 'Volume (USD)'}
            )
            fig.update_layout(
                showlegend=False, height=400,
                template='plotly_dark',
                paper_bgcolor='rgba(26,31,46,0.8)',
                plot_bgcolor='rgba(17,24,39,0.8)',
                font=dict(family='Outfit', color='#94a3b8'),
                xaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
                yaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748')
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Currency Trends Over Time
    st.markdown("---")
    st.subheader("💹 Currency Volume Trends Over Time")
    
    if 'txn_date' in df.columns:
        daily_by_currency = df.groupby(['txn_date', 'currency'])['amount_usd'].sum().reset_index()
        dark_gold_palette = ['#d4af37', '#00d4ff', '#10b981', '#f43f5e', '#a855f7', '#f59e0b', '#06b6d4', '#ec4899', '#84cc16', '#6366f1']
        
        fig = px.line(
            daily_by_currency,
            x='txn_date',
            y='amount_usd',
            color='currency',
            labels={'txn_date': 'Date', 'amount_usd': 'Volume (USD)', 'currency': 'Currency'},
            color_discrete_sequence=dark_gold_palette
        )
        fig.update_layout(
            height=500,
            template='plotly_dark',
            paper_bgcolor='rgba(26,31,46,0.8)',
            plot_bgcolor='rgba(17,24,39,0.8)',
            font=dict(family='Outfit', color='#94a3b8'),
            xaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
            yaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(color='#94a3b8'))
        )
        fig.update_traces(line_width=2)
        st.plotly_chart(fig, use_container_width=True)
    
    # Channel Analysis
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📱 Transaction by Channel")
        if 'channel' in df.columns:
            channel_data = df.groupby('channel').agg({
                'txn_id': 'count',
                'amount_usd': 'sum'
            }).rename(columns={'txn_id': 'count', 'amount_usd': 'volume'})
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig.add_trace(
                go.Bar(x=channel_data.index, y=channel_data['count'], name='Transaction Count', 
                       marker_color='#d4af37', marker_line_color='#f4d03f', marker_line_width=1),
                secondary_y=False
            )
            fig.add_trace(
                go.Scatter(x=channel_data.index, y=channel_data['volume'], name='Volume (USD)', 
                          mode='lines+markers', marker_color='#00d4ff', line_color='#00d4ff',
                          marker=dict(size=10, line=dict(color='#06b6d4', width=2))),
                secondary_y=True
            )
            
            fig.update_yaxes(title_text="Transaction Count", secondary_y=False, gridcolor='#2d3748', color='#94a3b8')
            fig.update_yaxes(title_text="Volume (USD)", secondary_y=True, gridcolor='#2d3748', color='#94a3b8')
            fig.update_xaxes(gridcolor='#2d3748', color='#94a3b8')
            fig.update_layout(
                height=400,
                template='plotly_dark',
                paper_bgcolor='rgba(26,31,46,0.8)',
                plot_bgcolor='rgba(17,24,39,0.8)',
                font=dict(family='Outfit', color='#94a3b8'),
                legend=dict(font=dict(color='#94a3b8'))
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🌍 Top Merchant Countries")
        if 'merchant_country' in df.columns:
            country_volume = df.groupby('merchant_country')['amount_usd'].sum().sort_values(ascending=False).head(10)
            
            fig = px.bar(
                x=country_volume.index,
                y=country_volume.values,
                color=country_volume.values,
                color_continuous_scale=[[0, '#1a1f2e'], [0.5, '#10b981'], [1, '#34d399']],
                labels={'x': 'Country', 'y': 'Volume (USD)'}
            )
            fig.update_layout(
                showlegend=False, height=400,
                template='plotly_dark',
                paper_bgcolor='rgba(26,31,46,0.8)',
                plot_bgcolor='rgba(17,24,39,0.8)',
                font=dict(family='Outfit', color='#94a3b8'),
                xaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748'),
                yaxis=dict(gridcolor='#2d3748', zerolinecolor='#2d3748')
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Detailed Data Table
    st.markdown("---")
    st.subheader("📋 Transaction Details")
    
    display_cols = st.multiselect(
        "Select columns to display",
        df.columns.tolist(),
        default=['txn_id', 'txn_date', 'currency', 'amount', 'amount_usd', 'product_type', 'channel']
    )
    
    if display_cols:
        st.dataframe(
            df[display_cols].head(100),
            use_container_width=True,
            height=400
        )
    
    st.download_button(
        label="📥 Download Data as CSV",
        data=df.to_csv(index=False).encode('utf-8'),
        file_name="fx_normalized_transactions.csv",
        mime="text/csv"
    )
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #64748b; padding: 2rem 0;'>"
        "<span style='color: #d4af37;'>◆</span> Global FX Intelligence Platform <span style='color: #d4af37;'>◆</span><br>"
        "<span style='font-size: 0.75rem; color: #475569;'>Powered by AWS EMR + Apache Spark + Streamlit</span>"
        "</div>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
