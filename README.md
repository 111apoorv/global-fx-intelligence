<p align="center">
  <img src="https://img.shields.io/badge/🌐-Global%20FX%20Intelligence-gold?style=for-the-badge&labelColor=0a0e17" alt="Global FX Intelligence"/>
</p>

<h1 align="center">
  💱 Global FX Intelligence Platform
</h1>

<p align="center">
  <strong>AI-Powered Multi-Currency Transaction Analytics Dashboard</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Streamlit-1.28+-FF4B4B?style=flat-square&logo=streamlit&logoColor=white" alt="Streamlit"/>
  <img src="https://img.shields.io/badge/Plotly-5.18+-3F4F75?style=flat-square&logo=plotly&logoColor=white" alt="Plotly"/>
  <img src="https://img.shields.io/badge/AI-Groq%20LLM-d4af37?style=flat-square&logo=openai&logoColor=white" alt="Groq AI"/>
  <img src="https://img.shields.io/badge/AWS-EMR%20|%20S3-FF9900?style=flat-square&logo=amazonaws&logoColor=white" alt="AWS"/>
  <img src="https://img.shields.io/badge/License-MIT-green?style=flat-square" alt="License"/>
</p>

<p align="center">
  <a href="#-features">Features</a> •
  <a href="#-demo">Demo</a> •
  <a href="#-quick-start">Quick Start</a> •
  <a href="#-ai-assistant">AI Assistant</a> •
  <a href="#-tech-stack">Tech Stack</a> •
  <a href="#-deployment">Deployment</a>
</p>

---

## 🎯 Overview

A **real-time financial analytics dashboard** for monitoring and analyzing multi-currency FX (Foreign Exchange) transactions. Built with a stunning **dark premium theme** and powered by **AI** for intelligent insights.

> 🤖 **NEW:** Integrated AI Financial Analyst powered by Groq's Llama 3.3 70B - Ask questions about your data in plain English!

---

## ✨ Features

### 📊 **Analytics & Visualization**
- 📈 Real-time transaction volume tracking
- 💱 Multi-currency support (USD, EUR, GBP, INR, JPY, CAD, AUD, CHF, CNY, SGD)
- 🗺️ Global transaction heatmap
- 📉 Daily/Weekly trend analysis
- 🥧 Currency distribution charts
- 📱 Channel & Product breakdowns

### 🤖 **AI-Powered Intelligence**
- 💬 **Natural Language Queries** - Ask questions in plain English
- 📋 **Auto-Generated Insights** - Executive summaries at a glance
- 🚨 **Smart Anomaly Detection** - Flag suspicious transactions
- 💡 **AI Recommendations** - Data-driven suggestions

### 🎨 **Premium UI/UX**
- 🌙 **Dark Premium Theme** - Elegant gold & cyan accents
- ✨ **Animated Effects** - Shimmer, glow, and smooth transitions
- 📱 **Responsive Design** - Works on all screen sizes
- 🔤 **Premium Typography** - Outfit & JetBrains Mono fonts

### 🔧 **Technical Features**
- ⚡ Real-time data refresh
- 🔍 Advanced filtering (Date, Currency, Product, Channel)
- 📥 CSV export functionality
- ☁️ AWS S3 integration
- 🔐 Secure API key management

---

## 🎬 Demo

### 🖥️ Dashboard Preview

```
┌────────────────────────────────────────────────────────────────┐
│  🌐 Global FX Intelligence Platform              [LIVE] 🟢    │
│  Real-Time Multi-Currency Transaction Analytics               │
│  ┌──────────┬──────────┬──────────┬──────────┐               │
│  │ 10       │ 8        │ 5K+      │ 90       │               │
│  │Currencies│Countries │ Txns     │ Days     │               │
│  └──────────┴──────────┴──────────┴──────────┘               │
├────────────────────────────────────────────────────────────────┤
│  🤖 AI Financial Analyst                                      │
│  ┌────────────────────────────────────────────────────┐       │
│  │ What's my top performing currency?              🚀 │       │
│  └────────────────────────────────────────────────────┘       │
│  💬 USD is your top currency with 30.3% of volume ($15M)...  │
├────────────────────────────────────────────────────────────────┤
│  📊 Volume by Currency    │    🥧 Transaction Distribution    │
│  ████████████ USD         │         ┌─────────┐              │
│  ████████ EUR             │        /    USD    \             │
│  ██████ GBP               │       │  30%  EUR  │             │
│  ███ CAD                  │        \    15%   /              │
└────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- pip package manager

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/fx-intelligence-dashboard.git
cd fx-intelligence-dashboard

# Install dependencies
pip install -r requirements.txt

# Run the dashboard
streamlit run dashboard.py
```

### 🌐 Access the Dashboard

Open your browser and navigate to:
```
http://localhost:8501
```

---

## 🤖 AI Assistant

### Powered by Groq (FREE & Fast!)

The dashboard includes an **AI Financial Analyst** that can answer questions about your data:

| Example Questions | What You Get |
|------------------|--------------|
| *"What's my top currency?"* | Volume analysis with percentages |
| *"Show suspicious transactions"* | Anomaly detection results |
| *"Give me a weekly summary"* | Executive summary report |
| *"Compare EUR vs GBP"* | Comparative analysis |
| *"Any risk concerns?"* | Risk assessment & recommendations |

### 🔑 Get Your FREE API Key

1. Visit [console.groq.com/keys](https://console.groq.com/keys)
2. Sign up (FREE)
3. Create an API key
4. Paste in the sidebar (or it's pre-configured!)

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|------------|
| **Frontend** | Streamlit, Plotly, Custom CSS |
| **Backend** | Python, Pandas, NumPy |
| **AI/ML** | Groq API, Llama 3.3 70B |
| **Cloud** | AWS EMR, S3, Spark |
| **Data** | Parquet, PyArrow |

---

## 📁 Project Structure

```
streamlit_app/
├── 📄 dashboard.py          # Main dashboard with AI
├── 📄 app.py                # Alternative dashboard
├── 📄 requirements.txt      # Python dependencies
├── 📄 generate_sample_data.py
├── 📁 .streamlit/
│   └── config.toml          # Theme configuration
└── 📄 sample_normalized.parquet
```

---

## ☁️ Deployment

### Deploy to Streamlit Cloud (FREE)

1. Push to GitHub
2. Visit [share.streamlit.io](https://share.streamlit.io)
3. Connect your repo
4. Deploy! 🚀

### Environment Variables (Optional)

```bash
GROQ_API_KEY=your_api_key_here
```

---

## 📊 Data Schema

| Column | Type | Description |
|--------|------|-------------|
| `txn_id` | string | Unique transaction ID |
| `customer_id` | string | Customer identifier |
| `txn_date` | datetime | Transaction date |
| `amount` | float | Original amount |
| `currency` | string | Source currency (USD, EUR, etc.) |
| `amount_usd` | float | Normalized USD amount |
| `product_type` | string | ECOM, RETAIL, FOREX, etc. |
| `channel` | string | ONLINE, POS, MOBILE, ATM, WIRE |
| `merchant_country` | string | Country code |

---

## 🎨 Theme Customization

The dashboard uses CSS variables for easy theming:

```css
:root {
    --bg-primary: #0a0e17;
    --accent-gold: #d4af37;
    --accent-cyan: #00d4ff;
    --accent-emerald: #10b981;
    --accent-rose: #f43f5e;
}
```

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👨‍💻 Author

**Apoorv** - Cloud Computing Project (Dec 2025)

---

## ⭐ Star History

If you find this project useful, please consider giving it a ⭐!

---

<p align="center">
  <img src="https://img.shields.io/badge/Made%20with-❤️-red?style=for-the-badge" alt="Made with Love"/>
  <img src="https://img.shields.io/badge/Powered%20by-AI-gold?style=for-the-badge" alt="Powered by AI"/>
</p>

<p align="center">
  <strong>🌐 Global FX Intelligence Platform</strong><br>
  <em>Real-Time • AI-Powered • Beautiful</em>
</p>

