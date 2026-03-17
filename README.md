# 🚀 Smaartbrand Intelligence Pipeline

AI-powered review intelligence and LinkedIn content generation system for the hospitality industry.

## Overview

Transform 500K+ hotel reviews into publishable LinkedIn thought-leadership articles through a 4-stage AI pipeline:

```
Query Agent → Insight Extractor → Chart Generator → Article Writer
```

## Features

- **Natural Language Querying**: Ask questions in English, Hindi, Tamil, Telugu, or Kannada
- **Smart Data Processing**: Uses mention PERCENTAGES (not counts), excludes Unknown values
- **8 Sentiment Aspects**: Dining, Cleanliness, Amenities, Staff, Room, Location, Value, General
- **Persona Analysis**: Traveler type, Stay purpose, Gender segmentation
- **Auto Chart Generation**: Heatmaps, Radar charts, Treemaps, Bar charts
- **LinkedIn-Optimized Articles**: Pre-written hooks, hashtag suggestions, engagement CTAs

## Data Verticals

| Vertical | Status | Reviews |
|----------|--------|---------|
| Hotels | ✅ Live | 500K+ |
| Automotive | Q2 2026 | 250K+ |
| Mobile Phones | Q3 2026 | TBD |

## Deploy to Railway

### 1. One-Click Deploy

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/smaartbrand)

### 2. Manual Deploy

```bash
# Clone the repository
git clone https://github.com/your-org/smaartbrand-pipeline.git
cd smaartbrand-pipeline

# Install Railway CLI
npm install -g @railway/cli

# Login and deploy
railway login
railway init
railway up
```

### 3. Set Environment Variables

In Railway dashboard, add:

```
GCP_CREDENTIALS_JSON = <your-service-account-json>
```

**Option A**: Paste the entire JSON service account key  
**Option B**: Base64 encode the JSON first

## Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set credentials
export GCP_CREDENTIALS_JSON='{"type":"service_account",...}'

# Run app
streamlit run app.py
```

## BigQuery Schema

The pipeline queries these tables:

| Table | Purpose |
|-------|---------|
| `product_list` | Hotel master data (name, city, stars) |
| `product_description` | Hotel metadata (brand, amenities, location) |
| `product_user_review_enriched` | Reviews with persona fields |
| `product_user_review_sentiment` | Sentiment snippets by aspect |

### Important Data Rules

1. **Mention Percentage**: All metrics show `%` not raw counts
2. **Exclude Unknown**: `inferred_gender`, `traveler_type`, `stay_purpose` = "Unknown" are filtered out
3. **8 Aspects**: Mapped from `aspect_id` (1-8)

## Pipeline Stages

### Stage 1: Query Agent
- Natural language → BigQuery SQL
- Multi-language support (auto-corrects spelling)
- Date range and hotel filtering

### Stage 2: Insight Extractor
- Satisfaction scoring by aspect
- Persona mix analysis
- Competitive gap detection
- LLM-powered summaries

### Stage 3: Chart Generator
- Auto-selects best visualization
- Teal color palette (#0d9488)
- Export as PNG for LinkedIn

### Stage 4: Article Writer
- Pre-written hook library
- LinkedIn-optimized format (1200-1500 chars)
- Hashtag suggestions
- Editable output

## 5-Week Content Calendar

| Week | Theme | Hook |
|------|-------|------|
| 1 | Breakfast Wars | "Your hotel's biryani is trending—but your dosa isn't..." |
| 2 | The Staff Effect | "Guests forgive bad WiFi. They never forgive rude staff." |
| 3 | Business vs Leisure | "Business travelers complain about X. Leisure guests?..." |
| 4 | Location Paradox | "'Near the airport' vs 'in the city center'—which wins?" |
| 5 | Value Perception | "5-star price, 3-star reviews. Why premium doesn't..." |

## Tech Stack

- **Frontend**: Streamlit 1.31+
- **Data**: Google BigQuery
- **LLM**: Gemini Pro (via BigQuery ML)
- **Charts**: Plotly 5.18+
- **Deployment**: Railway

## License

Proprietary - Acquink Technologies Pvt. Ltd. © 2026

## Support

For issues or feature requests, contact: support@acquink.com
