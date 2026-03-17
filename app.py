import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import json
import os
import numpy as np
import base64
import re
from datetime import datetime, timedelta

# ═══════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Smaartbrand Intelligence Pipeline",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ═══════════════════════════════════════════════════════════════
# STYLES
# ═══════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.main-header {
    text-align: center; padding: 20px;
    background: linear-gradient(135deg, #1e3a5f 0%, #0d9488 100%);
    border-radius: 12px; margin-bottom: 20px;
}
.main-header h1 { color: white; margin: 0; font-size: 1.8rem; }
.main-header p { color: rgba(255,255,255,0.85); margin: 6px 0 0 0; font-size: 0.85rem; }

.compare-card {
    background: linear-gradient(135deg, #0d9488 0%, #115e59 100%);
    padding: 20px; border-radius: 12px; color: white; text-align: center;
    margin: 8px 0;
}
.compare-card h3 { margin: 0 0 8px 0; font-size: 1rem; opacity: 0.9; }
.compare-card .metric { font-size: 2.2rem; font-weight: 700; }
.compare-card .label { font-size: 0.8rem; opacity: 0.8; }

.insight-card {
    background: #f8fafc; border-left: 4px solid #0d9488;
    padding: 16px; margin: 12px 0; border-radius: 0 8px 8px 0;
}
.insight-title { font-weight: 600; color: #1e3a5f; margin-bottom: 8px; }

.winner-badge {
    background: #10b981; color: white; padding: 2px 10px;
    border-radius: 12px; font-size: 11px; font-weight: 600;
}
.loser-badge {
    background: #ef4444; color: white; padding: 2px 10px;
    border-radius: 12px; font-size: 11px; font-weight: 600;
}

.article-preview {
    background: white; border: 1px solid #e5e7eb;
    border-radius: 12px; padding: 24px; margin: 16px 0;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    white-space: pre-wrap; line-height: 1.6;
}

.linkedin-badge {
    background: #0077b5; color: white;
    padding: 4px 12px; border-radius: 20px;
    font-size: 12px; font-weight: 500;
}

.vs-text {
    font-size: 1.5rem; font-weight: 700; color: #f59e0b;
    text-align: center; padding: 10px;
}
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════
PROJECT = "gen-lang-client-0143536012"
DATASET = "analyst"

ASPECT_MAP = {
    1: "Dining", 2: "Cleanliness", 3: "Amenities", 4: "Staff",
    5: "Room", 6: "Location", 7: "Value for Money", 8: "General"
}
ASPECT_ICONS = {
    "Dining": "🍽️", "Cleanliness": "🧹", "Amenities": "🏊", "Staff": "👨‍💼",
    "Room": "🛏️", "Location": "📍", "Value for Money": "💰", "General": "⭐"
}

TEAL_PALETTE = ["#f0fdfa", "#99f6e4", "#5eead4", "#2dd4bf", "#14b8a6", "#0d9488", "#0f766e", "#115e59"]
POSITIVE_COLOR = "#10b981"
NEGATIVE_COLOR = "#ef4444"

# Pre-written hook lines for different comparison types
HOOK_LIBRARY = {
    "brand_dining": "🍽️ {brand1} vs {brand2}: Who serves breakfast better? We analyzed {total:,} reviews to find out.",
    "brand_staff": "👨‍💼 Guests forgive bad WiFi. They never forgive rude staff. Here's how {brand1} and {brand2} compare.",
    "brand_value": "💰 {brand1} charges premium prices. But do guests feel they get premium service? The data surprises.",
    "brand_overall": "🏆 {brand1} vs {brand2}: The ultimate showdown. {total:,} guest reviews reveal a clear winner.",
    "city_business": "💼 {city1} vs {city2}: Where do Business travelers complain more? The answer might surprise you.",
    "city_leisure": "🏖️ Leisure travelers love {city1}. But {city2} is catching up fast. Here's the data.",
    "city_overall": "🌆 {city1} vs {city2}: Which city's hotels deliver better experiences? We analyzed {total:,} reviews.",
    "star_value": "⭐ 5-star price, 3-star experience? We compared {total:,} reviews across star categories.",
    "star_service": "🛎️ Do more stars mean better service? Not always. Here's what the data shows.",
    "star_overall": "⭐ 3-star vs 4-star vs 5-star: Which gives the best bang for your buck?",
    "persona_business": "💼 Business travelers have ONE non-negotiable. Most hotels get it wrong.",
    "persona_couple": "💑 What couples want vs what families want: The gap is wider than you think.",
    "aspect_gap": "📊 The #1 aspect where {leader} beats {laggard}? It's not what you'd expect.",
    "competitive": "⚔️ Your competitor's weakness is hiding in plain sight. We found it in {total:,} reviews."
}

# ═══════════════════════════════════════════════════════════════
# CREDENTIALS & CLIENT
# ═══════════════════════════════════════════════════════════════
def get_credentials():
    gcp_creds = os.environ.get("GCP_CREDENTIALS_JSON", "")
    if not gcp_creds:
        return None
    gcp_creds = gcp_creds.strip().strip('"').strip("'")
    try:
        if gcp_creds.startswith("{"):
            creds_dict = json.loads(gcp_creds)
        else:
            padding = 4 - len(gcp_creds) % 4
            if padding != 4:
                gcp_creds += "=" * padding
            creds_dict = json.loads(base64.b64decode(gcp_creds).decode('utf-8'))
        return service_account.Credentials.from_service_account_info(creds_dict)
    except Exception as e:
        st.error(f"Credential error: {e}")
        return None

@st.cache_resource
def get_bq_client():
    credentials = get_credentials()
    if credentials:
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    try:
        return bigquery.Client(project=PROJECT)
    except:
        return None

client = get_bq_client()

# ═══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════
def get_color_for_score(score: float) -> str:
    idx = min(int(score / 100 * (len(TEAL_PALETTE) - 1)), len(TEAL_PALETTE) - 1)
    return TEAL_PALETTE[idx]

def format_hook(hook_template: str, **kwargs) -> str:
    """Format hook template with actual values."""
    try:
        return hook_template.format(**kwargs)
    except KeyError:
        return hook_template

# ═══════════════════════════════════════════════════════════════
# DATA QUERIES
# ═══════════════════════════════════════════════════════════════
@st.cache_data(ttl=3600)
def get_metadata():
    if client is None:
        return pd.DataFrame()
    query = f"""
    SELECT DISTINCT 
        pl.Name AS hotel_name, 
        pd.Brand, 
        pl.Star_Category AS star_category, 
        pl.City
    FROM `{PROJECT}.{DATASET}.product_list` pl
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE pd.Brand IS NOT NULL AND pl.Name IS NOT NULL AND pl.City IS NOT NULL
    """
    try:
        return client.query(query).to_dataframe()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=600)
def fetch_comparison_data(compare_by: str, selected_items: tuple, date_from: str = None, date_to: str = None):
    """
    Fetch data aggregated by comparison dimension (Brand/City/Star).
    Returns mention PERCENTAGES, excludes Unknown persona values.
    """
    if client is None or not selected_items:
        return pd.DataFrame()
    
    items_sql = "', '".join([str(item).replace("'", "''") for item in selected_items])
    
    date_filter = ""
    if date_from and date_to:
        date_filter = f"AND e.Review_date BETWEEN '{date_from}' AND '{date_to}'"
    
    # Determine grouping column
    if compare_by == "Brand":
        group_col = "pd.Brand"
        where_col = "pd.Brand"
    elif compare_by == "City":
        group_col = "pl.City"
        where_col = "pl.City"
    elif compare_by == "Star Category":
        group_col = "pl.Star_Category"
        where_col = "pl.Star_Category"
    else:  # Hotel
        group_col = "pl.Name"
        where_col = "pl.Name"
    
    query = f"""
    WITH filtered_data AS (
        SELECT 
            {group_col} AS compare_group,
            pl.Name AS hotel_name,
            pd.Brand,
            pl.Star_Category,
            pl.City,
            s.aspect_id,
            s.treemap_name AS phrase,
            s.sentiment_type,
            e.Review_date,
            e.inferred_gender AS gender,
            e.traveler_type,
            e.stay_purpose,
            COUNT(*) AS mention_count
        FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
        JOIN `{PROJECT}.{DATASET}.product_user_review_sentiment` s ON e.id = s.user_review_id
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {where_col} IN ('{items_sql}')
        {date_filter}
        -- Exclude Unknown persona values
        AND LOWER(IFNULL(e.inferred_gender, 'unknown')) != 'unknown'
        AND LOWER(IFNULL(e.traveler_type, 'unknown')) != 'unknown'
        AND LOWER(IFNULL(e.stay_purpose, 'unknown')) != 'unknown'
        GROUP BY {group_col}, pl.Name, pd.Brand, pl.Star_Category, pl.City,
                 s.aspect_id, s.treemap_name, s.sentiment_type,
                 e.Review_date, e.inferred_gender, e.traveler_type, e.stay_purpose
    )
    SELECT * FROM filtered_data
    """
    try:
        df = client.query(query).to_dataframe()
        df["aspect"] = df["aspect_id"].map(ASPECT_MAP).fillna("Other")
        df["mention_count"] = pd.to_numeric(df["mention_count"], errors="coerce").fillna(0).astype(int)
        
        if "Review_date" in df.columns:
            df["Review_date"] = pd.to_datetime(df["Review_date"], errors="coerce")
        
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

# ═══════════════════════════════════════════════════════════════
# STAGE 1: QUERY AGENT (Comparison Mode)
# ═══════════════════════════════════════════════════════════════
def run_query_agent(compare_by: str, selected_items: list, date_from: str, date_to: str) -> dict:
    """Stage 1: Fetch and aggregate data by comparison dimension."""
    if not selected_items or len(selected_items) < 2:
        return {"success": False, "error": "Select at least 2 items to compare", "data": None}
    
    df = fetch_comparison_data(compare_by, tuple(selected_items), date_from, date_to)
    
    if df.empty:
        return {"success": False, "error": "No data found for selection", "data": None}
    
    total_mentions = df['mention_count'].sum()
    
    # Calculate satisfaction % per compare_group per aspect
    satisfaction_data = []
    for group in df['compare_group'].unique():
        group_df = df[df['compare_group'] == group]
        group_total = group_df['mention_count'].sum()
        
        for aspect in df['aspect'].unique():
            aspect_df = group_df[group_df['aspect'] == aspect]
            pos = aspect_df[aspect_df['sentiment_type'].str.lower() == 'positive']['mention_count'].sum()
            neg = aspect_df[aspect_df['sentiment_type'].str.lower() == 'negative']['mention_count'].sum()
            total = pos + neg
            sat_pct = round(pos / total * 100, 1) if total > 0 else 0
            mention_pct = round(total / group_total * 100, 1) if group_total > 0 else 0
            
            satisfaction_data.append({
                'compare_group': str(group),
                'aspect': aspect,
                'satisfaction_pct': sat_pct,
                'mention_pct': mention_pct,
                'positive_count': pos,
                'negative_count': neg,
                'total_mentions': total
            })
    
    satisfaction_df = pd.DataFrame(satisfaction_data)
    
    # Overall satisfaction per group
    overall_satisfaction = {}
    for group in df['compare_group'].unique():
        group_df = df[df['compare_group'] == group]
        pos = group_df[group_df['sentiment_type'].str.lower() == 'positive']['mention_count'].sum()
        neg = group_df[group_df['sentiment_type'].str.lower() == 'negative']['mention_count'].sum()
        overall_satisfaction[str(group)] = round(pos / (pos + neg) * 100, 1) if (pos + neg) > 0 else 0
    
    # Persona breakdown per group (percentages)
    persona_data = {}
    for col in ['traveler_type', 'stay_purpose', 'gender']:
        if col in df.columns:
            persona_data[col] = {}
            for group in df['compare_group'].unique():
                group_df = df[df['compare_group'] == group]
                col_total = group_df.groupby(col)['mention_count'].sum()
                col_pct = (col_total / col_total.sum() * 100).round(1)
                persona_data[col][str(group)] = col_pct.to_dict()
    
    # Top phrases per group
    phrases_by_group = {}
    for group in df['compare_group'].unique():
        group_df = df[df['compare_group'] == group]
        pos_phrases = group_df[group_df['sentiment_type'].str.lower() == 'positive'].groupby('phrase')['mention_count'].sum().nlargest(8)
        neg_phrases = group_df[group_df['sentiment_type'].str.lower() == 'negative'].groupby('phrase')['mention_count'].sum().nlargest(8)
        phrases_by_group[str(group)] = {
            'positive': pos_phrases.to_dict(),
            'negative': neg_phrases.to_dict()
        }
    
    # Find winners and gaps
    aspect_winners = {}
    for aspect in satisfaction_df['aspect'].unique():
        aspect_data = satisfaction_df[satisfaction_df['aspect'] == aspect]
        if not aspect_data.empty:
            winner_idx = aspect_data['satisfaction_pct'].idxmax()
            loser_idx = aspect_data['satisfaction_pct'].idxmin()
            winner = aspect_data.loc[winner_idx]
            loser = aspect_data.loc[loser_idx]
            gap = winner['satisfaction_pct'] - loser['satisfaction_pct']
            aspect_winners[aspect] = {
                'winner': winner['compare_group'],
                'winner_score': winner['satisfaction_pct'],
                'loser': loser['compare_group'],
                'loser_score': loser['satisfaction_pct'],
                'gap': round(gap, 1)
            }
    
    return {
        "success": True,
        "compare_by": compare_by,
        "groups": [str(g) for g in df['compare_group'].unique()],
        "data": df,
        "satisfaction_df": satisfaction_df,
        "overall_satisfaction": overall_satisfaction,
        "persona_data": persona_data,
        "phrases_by_group": phrases_by_group,
        "aspect_winners": aspect_winners,
        "total_mentions": total_mentions,
        "date_range": f"{date_from} to {date_to}" if date_from else "All time",
        "hotel_count": df['hotel_name'].nunique()
    }

# ═══════════════════════════════════════════════════════════════
# STAGE 2: INSIGHT EXTRACTOR (Comparative Insights)
# ═══════════════════════════════════════════════════════════════
def run_insight_extractor(query_result: dict) -> dict:
    """Stage 2: Extract comparative insights across groups."""
    if not query_result.get('success'):
        return {"success": False, "insights": [], "error": query_result.get('error')}
    
    insights = []
    compare_by = query_result['compare_by']
    groups = query_result['groups']
    overall_sat = query_result['overall_satisfaction']
    aspect_winners = query_result['aspect_winners']
    satisfaction_df = query_result['satisfaction_df']
    
    # 1. Overall Winner Insight
    sorted_groups = sorted(overall_sat.items(), key=lambda x: x[1], reverse=True)
    leader = sorted_groups[0]
    laggard = sorted_groups[-1]
    gap = leader[1] - laggard[1]
    
    insights.append({
        "type": "overall_winner",
        "title": f"🏆 Overall Winner: {leader[0]}",
        "description": f"{leader[0]} leads with {leader[1]}% satisfaction vs {laggard[0]} at {laggard[1]}% (Gap: {gap:.1f}%)",
        "leader": leader[0],
        "leader_score": leader[1],
        "laggard": laggard[0],
        "laggard_score": laggard[1],
        "gap": gap,
        "icon": "🏆"
    })
    
    # 2. Biggest Competitive Gaps
    sorted_aspects = sorted(aspect_winners.items(), key=lambda x: x[1]['gap'], reverse=True)
    for aspect, data in sorted_aspects[:3]:
        if data['gap'] > 5:
            insights.append({
                "type": "aspect_gap",
                "title": f"{ASPECT_ICONS.get(aspect, '📊')} {aspect}: {data['winner']} dominates",
                "description": f"{data['winner']} scores {data['winner_score']}% vs {data['loser']} at {data['loser_score']}% (Gap: {data['gap']}%)",
                "aspect": aspect,
                "winner": data['winner'],
                "winner_score": data['winner_score'],
                "loser": data['loser'],
                "gap": data['gap'],
                "icon": ASPECT_ICONS.get(aspect, "📊")
            })
    
    # 3. Surprise findings (lower star beats higher)
    if compare_by == "Star Category" and len(groups) > 1:
        for aspect, data in aspect_winners.items():
            try:
                winner_stars = int(data['winner'])
                loser_stars = int(data['loser'])
                if winner_stars < loser_stars and data['gap'] > 10:
                    insights.append({
                        "type": "surprise",
                        "title": f"😮 Surprise: {winner_stars}★ beats {loser_stars}★ on {aspect}",
                        "description": f"Lower-tier hotels outperform on {aspect} by {data['gap']}%!",
                        "icon": "😮"
                    })
            except:
                pass
    
    # 4. Persona Insights
    persona_data = query_result.get('persona_data', {})
    if 'traveler_type' in persona_data:
        business_pct = {}
        for group, type_data in persona_data['traveler_type'].items():
            business_pct[group] = type_data.get('Business', 0) + type_data.get('business', 0)
        
        if business_pct:
            business_leader = max(business_pct.items(), key=lambda x: x[1])
            if business_leader[1] > 0:
                insights.append({
                    "type": "persona",
                    "title": f"💼 Business Traveler Favorite: {business_leader[0]}",
                    "description": f"{business_leader[1]:.1f}% of {business_leader[0]}'s reviews come from Business travelers",
                    "icon": "💼"
                })
    
    # 5. Complaints Comparison
    phrases_by_group = query_result.get('phrases_by_group', {})
    if len(groups) >= 2:
        group1, group2 = groups[0], groups[1]
        neg1 = list(phrases_by_group.get(group1, {}).get('negative', {}).keys())[:3]
        neg2 = list(phrases_by_group.get(group2, {}).get('negative', {}).keys())[:3]
        
        if neg1 or neg2:
            insights.append({
                "type": "complaints",
                "title": "⚠️ Top Complaints Comparison",
                "description": f"{group1}: {', '.join(neg1[:2]) if neg1 else 'N/A'} | {group2}: {', '.join(neg2[:2]) if neg2 else 'N/A'}",
                "icon": "⚠️"
            })
    
    # 6. USPs to Promote
    for group in groups[:2]:
        group_strengths = []
        for aspect, data in aspect_winners.items():
            if data['winner'] == group and data['gap'] > 10:
                group_strengths.append(f"{aspect} ({data['winner_score']}%)")
        
        if group_strengths:
            insights.append({
                "type": "usp",
                "title": f"✨ {group} USPs to Promote",
                "description": f"Winning aspects: {', '.join(group_strengths[:3])}",
                "group": group,
                "strengths": group_strengths,
                "icon": "✨"
            })
    
    # Generate LLM summary
    llm_summary = generate_comparison_summary(query_result, insights)
    
    return {
        "success": True,
        "insights": insights,
        "llm_summary": llm_summary,
        "query_result": query_result
    }

def generate_comparison_summary(query_result: dict, insights: list) -> str:
    """Generate LLM-powered comparison summary."""
    if client is None:
        return "AI summary unavailable"
    
    groups = query_result['groups']
    overall_sat = query_result['overall_satisfaction']
    compare_by = query_result['compare_by']
    
    context = f"""
    Analyze this hotel comparison and write 3 punchy insights for LinkedIn:
    
    Comparison: {compare_by}
    Groups: {', '.join(groups)}
    Reviews: {query_result['total_mentions']:,}
    
    Satisfaction: {json.dumps(overall_sat)}
    
    Gaps: {json.dumps({k: v for k, v in list(query_result['aspect_winners'].items())[:5]})}
    
    Write 3 insights that make hospitality pros stop scrolling.
    Include specific %. Be provocative but accurate. Under 150 words.
    """
    
    try:
        sql = f"""
        SELECT ml_generate_text_llm_result 
        FROM ML.GENERATE_TEXT(
            MODEL `{PROJECT}.{DATASET}.gemini_flash_model`,
            (SELECT @prompt AS prompt),
            STRUCT(0.4 AS temperature, 400 AS max_output_tokens, TRUE AS flatten_json_output)
        )
        """
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("prompt", "STRING", context)]
        )
        result = client.query(sql, job_config=job_cfg).to_dataframe()
        return result["ml_generate_text_llm_result"].iloc[0].strip()
    except:
        return f"• {groups[0]} leads with {overall_sat.get(groups[0], 0)}% satisfaction\n• Key gaps found across aspects\n• Actionable insights for competitive positioning"

# ═══════════════════════════════════════════════════════════════
# STAGE 3: CHART GENERATOR
# ═══════════════════════════════════════════════════════════════
def run_chart_generator(insight_result: dict) -> dict:
    """Stage 3: Generate comparison visualizations."""
    if not insight_result.get('success'):
        return {"success": False, "charts": [], "error": "No insights to visualize"}
    
    charts = []
    query_result = insight_result['query_result']
    satisfaction_df = query_result['satisfaction_df']
    groups = query_result['groups']
    compare_by = query_result['compare_by']
    overall_sat = query_result['overall_satisfaction']
    
    # 1. Overall Satisfaction Bar
    sorted_groups = sorted(overall_sat.items(), key=lambda x: x[1], reverse=True)
    fig_overall = go.Figure(go.Bar(
        x=[g[1] for g in sorted_groups],
        y=[str(g[0]) for g in sorted_groups],
        orientation='h',
        marker=dict(color=[POSITIVE_COLOR if i == 0 else TEAL_PALETTE[5] for i in range(len(sorted_groups))]),
        text=[f"{g[1]}%" for g in sorted_groups],
        textposition='outside'
    ))
    fig_overall.update_layout(
        title=f"Overall Satisfaction: {compare_by} Comparison",
        height=300, margin=dict(l=120, r=60, t=60, b=40),
        xaxis=dict(title="Satisfaction %", range=[0, 105]),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
    )
    charts.append({"type": "bar_overall", "title": "Overall Satisfaction", "figure": fig_overall,
                   "description": f"Head-to-head comparison across {len(groups)} {compare_by.lower()}s"})
    
    # 2. Aspect Heatmap
    pivot_df = satisfaction_df.pivot(index='aspect', columns='compare_group', values='satisfaction_pct').fillna(0)
    pivot_df = pivot_df[[str(g[0]) for g in sorted_groups if str(g[0]) in pivot_df.columns]]
    
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=pivot_df.values, x=pivot_df.columns,
        y=[f"{ASPECT_ICONS.get(a, '•')} {a}" for a in pivot_df.index],
        colorscale=[[0, '#fecaca'], [0.5, '#fef3c7'], [1, '#a7f3d0']],
        text=[[f"{v:.0f}%" for v in row] for row in pivot_df.values],
        texttemplate="%{text}", textfont={"size": 11}
    ))
    fig_heatmap.update_layout(
        title=f"Aspect Breakdown: {compare_by} Comparison",
        height=400, margin=dict(l=150, r=20, t=60, b=60), paper_bgcolor='rgba(0,0,0,0)'
    )
    charts.append({"type": "heatmap", "title": "Aspect Heatmap", "figure": fig_heatmap,
                   "description": "Satisfaction across all 8 aspects"})
    
    # 3. Radar Overlay
    fig_radar = go.Figure()
    colors = ['#0d9488', '#f59e0b', '#8b5cf6', '#ef4444']
    
    for i, group in enumerate(groups[:4]):
        group_data = satisfaction_df[satisfaction_df['compare_group'] == str(group)]
        aspects = group_data['aspect'].tolist()
        scores = group_data['satisfaction_pct'].tolist()
        if aspects and scores:
            fig_radar.add_trace(go.Scatterpolar(
                r=scores + [scores[0]], theta=aspects + [aspects[0]],
                fill='toself', fillcolor=f'rgba({int(colors[i][1:3], 16)}, {int(colors[i][3:5], 16)}, {int(colors[i][5:7], 16)}, 0.2)',
                line=dict(color=colors[i], width=2), name=str(group)
            ))
    
    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=True, title="Aspect Profile Overlay", height=400, paper_bgcolor='rgba(0,0,0,0)'
    )
    charts.append({"type": "radar", "title": "Aspect Radar", "figure": fig_radar,
                   "description": "Overlapping profiles show strengths and weaknesses"})
    
    # 4. Gap Analysis
    aspect_winners = query_result['aspect_winners']
    gap_data = sorted(aspect_winners.items(), key=lambda x: x[1]['gap'], reverse=True)[:6]
    
    fig_gaps = go.Figure()
    for aspect, data in gap_data:
        fig_gaps.add_trace(go.Bar(
            name=aspect, x=[data['gap']], y=[f"{ASPECT_ICONS.get(aspect, '•')} {aspect}"],
            orientation='h', marker_color=TEAL_PALETTE[5],
            text=[f"+{data['gap']}% ({data['winner']})"], textposition='outside'
        ))
    
    fig_gaps.update_layout(
        title="Biggest Competitive Gaps", height=300,
        margin=dict(l=140, r=100, t=60, b=40),
        xaxis=dict(title="Gap %", range=[0, max([d[1]['gap'] for d in gap_data]) * 1.3 if gap_data else 50]),
        showlegend=False, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
    )
    charts.append({"type": "gaps", "title": "Competitive Gaps", "figure": fig_gaps,
                   "description": "Aspects with largest satisfaction differences"})
    
    # 5. Persona Mix (if available)
    persona_data = query_result.get('persona_data', {})
    if 'traveler_type' in persona_data and len(groups) >= 2:
        traveler_types = set()
        for group_data in persona_data['traveler_type'].values():
            traveler_types.update(group_data.keys())
        
        if traveler_types:
            fig_persona = go.Figure()
            for i, group in enumerate(groups[:3]):
                group_traveler = persona_data['traveler_type'].get(str(group), {})
                fig_persona.add_trace(go.Bar(
                    name=str(group), x=list(traveler_types),
                    y=[group_traveler.get(t, 0) for t in traveler_types], marker_color=colors[i]
                ))
            
            fig_persona.update_layout(
                title="Traveler Mix Comparison", barmode='group', height=300,
                yaxis=dict(title="% of Reviews"), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
            )
            charts.append({"type": "persona", "title": "Traveler Mix", "figure": fig_persona,
                           "description": "Who stays where? Audience distribution"})
    
    return {"success": True, "charts": charts, "insight_result": insight_result}

# ═══════════════════════════════════════════════════════════════
# STAGE 4: ARTICLE WRITER
# ═══════════════════════════════════════════════════════════════
def run_article_writer(chart_result: dict, hook_type: str = "brand_overall", tone: str = "professional") -> dict:
    """Stage 4: Generate LinkedIn article from comparison insights."""
    if not chart_result.get('success'):
        return {"success": False, "article": None, "error": "No charts to write about"}
    
    insight_result = chart_result['insight_result']
    query_result = insight_result['query_result']
    insights = insight_result['insights']
    
    groups = query_result['groups']
    compare_by = query_result['compare_by']
    overall_sat = query_result['overall_satisfaction']
    total = query_result['total_mentions']
    
    sorted_groups = sorted(overall_sat.items(), key=lambda x: x[1], reverse=True)
    leader = sorted_groups[0][0]
    laggard = sorted_groups[-1][0]
    
    hook_template = HOOK_LIBRARY.get(hook_type, HOOK_LIBRARY['brand_overall'])
    hook_vars = {
        'brand1': groups[0] if len(groups) > 0 else "Brand A",
        'brand2': groups[1] if len(groups) > 1 else "Brand B",
        'city1': groups[0] if len(groups) > 0 else "City A",
        'city2': groups[1] if len(groups) > 1 else "City B",
        'total': total, 'leader': leader, 'laggard': laggard, 'change': "15"
    }
    hook = format_hook(hook_template, **hook_vars)
    
    gap_insights = [i for i in insights if i['type'] == 'aspect_gap'][:2]
    
    article_prompt = f"""
    Write a LinkedIn article comparing {compare_by}s in hospitality.
    
    HOOK (use exactly): {hook}
    
    DATA:
    - Groups: {', '.join(groups)}
    - Reviews: {total:,}
    - Winner: {leader} at {overall_sat.get(leader, 0)}%
    - Runner-up: {laggard} at {overall_sat.get(laggard, 0)}%
    - Gap: {sorted_groups[0][1] - sorted_groups[-1][1]:.1f}%
    
    FINDINGS: {insight_result.get('llm_summary', '')}
    
    GAPS: {json.dumps([{{'aspect': i.get('aspect',''), 'winner': i.get('winner',''), 'gap': i.get('gap',0)}} for i in gap_insights])}
    
    REQUIREMENTS:
    1. Start with hook exactly
    2. Add context sentence
    3. Finding #1 with specific %
    4. Finding #2 with gap data
    5. "📊 The Takeaway:" section
    6. End with question
    7. Add 5 hashtags
    
    TONE: {tone}
    LENGTH: 1,200-1,400 characters
    """
    
    try:
        sql = f"""
        SELECT ml_generate_text_llm_result 
        FROM ML.GENERATE_TEXT(
            MODEL `{PROJECT}.{DATASET}.gemini_flash_model`,
            (SELECT @prompt AS prompt),
            STRUCT(0.7 AS temperature, 800 AS max_output_tokens, TRUE AS flatten_json_output)
        )
        """
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("prompt", "STRING", article_prompt)]
        )
        result = client.query(sql, job_config=job_cfg).to_dataframe()
        article_text = result["ml_generate_text_llm_result"].iloc[0].strip()
    except:
        article_text = f"""{hook}

We analyzed {total:,} guest reviews to find out which {compare_by.lower()} delivers the best experience.

📊 Finding #1: {leader} leads with {overall_sat.get(leader, 0)}% satisfaction
That's {sorted_groups[0][1] - sorted_groups[-1][1]:.1f}% ahead of {laggard}.

📊 Finding #2: The biggest battleground? {gap_insights[0]['aspect'] if gap_insights else 'Service'}
{gap_insights[0]['winner'] if gap_insights else leader} dominates with a {gap_insights[0]['gap'] if gap_insights else 10}% lead.

📊 The Takeaway:
For {laggard}, the path forward is clear: focus on {gap_insights[0]['aspect'] if gap_insights else 'basics'}.
For {leader}, maintain the edge but watch for challengers.

💬 Which {compare_by.lower()} delivers the best guest experience in your opinion?

#HospitalityIndustry #HotelManagement #GuestExperience #DataDriven #Smaartbrand
"""
    
    hashtags = re.findall(r'#\w+', article_text)
    
    return {
        "success": True,
        "article": {
            "text": article_text,
            "hook_used": hook,
            "word_count": len(article_text.split()),
            "char_count": len(article_text),
            "hashtags": hashtags[:8],
            "comparison": {"type": compare_by, "groups": groups, "winner": leader, "total_reviews": total}
        },
        "charts": chart_result['charts'],
        "insights": insights
    }

# ═══════════════════════════════════════════════════════════════
# MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════
def main():
    st.markdown("""
    <div class="main-header">
        <h1>🚀 Smaartbrand Intelligence Pipeline</h1>
        <p>Compare Brands • Cities • Star Categories → LinkedIn-Ready Insights</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Session state
    for key in ['pipeline_stage', 'query_result', 'insight_result', 'chart_result', 'article_result']:
        if key not in st.session_state:
            st.session_state[key] = 0 if key == 'pipeline_stage' else None
    
    # Stage Indicator
    stages = ["1. Compare", "2. Insights", "3. Charts", "4. Article"]
    cols = st.columns(4)
    for i, (col, stage) in enumerate(zip(cols, stages)):
        status = "complete" if i < st.session_state.pipeline_stage else ("active" if i == st.session_state.pipeline_stage else "pending")
        icon = "✓" if status == "complete" else str(i + 1)
        color = "#10b981" if status == "complete" else ("#0d9488" if status == "active" else "#e5e7eb")
        col.markdown(f'<div style="text-align:center;"><div style="width:36px;height:36px;border-radius:50%;background:{color};color:{"white" if status != "pending" else "#9ca3af"};display:inline-flex;align-items:center;justify-content:center;font-weight:bold;">{icon}</div><div style="font-size:11px;">{stage}</div></div>', unsafe_allow_html=True)
    
    st.divider()
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 🎯 Controls")
        if st.button("🔄 Reset", use_container_width=True):
            for key in ['pipeline_stage', 'query_result', 'insight_result', 'chart_result', 'article_result']:
                st.session_state[key] = 0 if key == 'pipeline_stage' else None
            st.rerun()
        
        st.divider()
        meta = get_metadata()
        
        if not meta.empty:
            st.markdown("### 📊 Compare By")
            compare_by = st.radio("", ["Brand", "City", "Star Category"], horizontal=True)
            
            st.divider()
            
            if compare_by == "Brand":
                options = sorted(meta["Brand"].dropna().unique().tolist())
                selected_items = st.multiselect("Select Brands", options, default=options[:2] if len(options) >= 2 else options)
                st.caption("💡 ITC vs Taj = great content!")
            elif compare_by == "City":
                options = sorted(meta["City"].dropna().unique().tolist())
                selected_items = st.multiselect("Select Cities", options, default=options[:2] if len(options) >= 2 else options)
                st.caption("💡 Bangalore vs Mumbai = high engagement")
            else:
                options = sorted(meta["star_category"].dropna().unique().tolist())
                selected_items = st.multiselect("Select Stars", options, default=options[:2] if len(options) >= 2 else options,
                                                format_func=lambda x: f"{'⭐' * int(x)}")
                selected_items = [int(x) for x in selected_items]
            
            st.divider()
            st.markdown("### 📅 Date Range")
            c1, c2 = st.columns(2)
            date_from = c1.date_input("From", datetime.now() - timedelta(days=180))
            date_to = c2.date_input("To", datetime.now())
            
            st.divider()
            st.markdown("### ✍️ Article")
            hook_map = {"Brand": ["brand_overall", "brand_dining", "brand_staff", "brand_value"],
                        "City": ["city_overall", "city_business", "city_leisure"],
                        "Star Category": ["star_overall", "star_value", "star_service"]}
            hook_type = st.selectbox("Hook", hook_map.get(compare_by, ["brand_overall"]),
                                     format_func=lambda x: x.replace("_", " ").title())
            tone = st.selectbox("Tone", ["professional", "provocative", "analytical"])
        else:
            st.warning("⚠️ No metadata")
            selected_items, compare_by = [], "Brand"
            date_from, date_to = datetime.now() - timedelta(days=180), datetime.now()
            hook_type, tone = "brand_overall", "professional"
    
    # Main Area
    col1, col2 = st.columns([2.5, 1])
    
    with col1:
        # STAGE 0: SETUP
        if st.session_state.pipeline_stage == 0:
            st.markdown("## 1️⃣ Select & Compare")
            
            if len(selected_items) >= 2:
                st.markdown("### 🆚 Your Comparison")
                preview_cols = st.columns(len(selected_items[:4]))
                for i, (c, item) in enumerate(zip(preview_cols, selected_items[:4])):
                    c.markdown(f'<div class="compare-card"><h3>{compare_by}</h3><div class="metric">{item}</div></div>', unsafe_allow_html=True)
                
                st.markdown(f'<div class="vs-text">{" vs ".join([str(s) for s in selected_items[:4]])}</div>', unsafe_allow_html=True)
                st.divider()
                
                if st.button("🚀 Run Comparison", type="primary", use_container_width=True):
                    with st.spinner("Analyzing..."):
                        result = run_query_agent(compare_by, selected_items, str(date_from), str(date_to))
                        if result['success']:
                            st.session_state.query_result = result
                            st.session_state.pipeline_stage = 1
                            st.success(f"✅ {result['total_mentions']:,} mentions from {result['hotel_count']} hotels")
                            st.rerun()
                        else:
                            st.error(result['error'])
            else:
                st.warning(f"⚠️ Select at least 2 {compare_by.lower()}s")
        
        # STAGE 1: INSIGHTS
        elif st.session_state.pipeline_stage == 1:
            st.markdown("## 2️⃣ Extract Insights")
            qr = st.session_state.query_result
            
            sorted_groups = sorted(qr['overall_satisfaction'].items(), key=lambda x: x[1], reverse=True)
            cols = st.columns(len(sorted_groups[:4]))
            for i, (c, (group, score)) in enumerate(zip(cols, sorted_groups[:4])):
                badge = "🥇" if i == 0 else ("🥈" if i == 1 else "")
                c.markdown(f'<div class="compare-card" style="{"background:linear-gradient(135deg,#10b981,#059669);" if i==0 else ""}"><h3>{badge} {group}</h3><div class="metric">{score}%</div></div>', unsafe_allow_html=True)
            
            st.divider()
            c1, c2 = st.columns(2)
            if c1.button("🔍 Extract Insights", type="primary", use_container_width=True):
                with st.spinner("Finding gaps..."):
                    result = run_insight_extractor(qr)
                    if result['success']:
                        st.session_state.insight_result = result
                        st.session_state.pipeline_stage = 2
                        st.rerun()
            if c2.button("← Back", use_container_width=True):
                st.session_state.pipeline_stage = 0
                st.rerun()
        
        # STAGE 2: CHARTS
        elif st.session_state.pipeline_stage == 2:
            st.markdown("## 3️⃣ Generate Charts")
            ir = st.session_state.insight_result
            
            st.markdown("### 🎯 Key Findings")
            for ins in ir['insights'][:5]:
                badge = f"<span class='winner-badge'>GAP: {ins.get('gap',0)}%</span>" if ins['type'] == 'aspect_gap' else ""
                st.markdown(f'<div class="insight-card"><div class="insight-title">{ins["icon"]} {ins["title"]} {badge}</div>{ins["description"]}</div>', unsafe_allow_html=True)
            
            if ir.get('llm_summary'):
                with st.expander("🤖 AI Analysis", expanded=True):
                    st.markdown(ir['llm_summary'])
            
            st.divider()
            c1, c2 = st.columns(2)
            if c1.button("📊 Generate Charts", type="primary", use_container_width=True):
                with st.spinner("Creating visuals..."):
                    result = run_chart_generator(ir)
                    if result['success']:
                        st.session_state.chart_result = result
                        st.session_state.pipeline_stage = 3
                        st.rerun()
            if c2.button("← Back", use_container_width=True):
                st.session_state.pipeline_stage = 1
                st.rerun()
        
        # STAGE 3: ARTICLE
        elif st.session_state.pipeline_stage == 3:
            st.markdown("## 4️⃣ Write Article")
            cr = st.session_state.chart_result
            
            tabs = st.tabs([c['title'] for c in cr['charts'][:4]])
            for tab, chart in zip(tabs, cr['charts'][:4]):
                with tab:
                    st.plotly_chart(chart['figure'], use_container_width=True)
            
            st.divider()
            c1, c2 = st.columns(2)
            if c1.button("✍️ Generate Article", type="primary", use_container_width=True):
                with st.spinner("Writing..."):
                    result = run_article_writer(cr, hook_type, tone)
                    if result['success']:
                        st.session_state.article_result = result
                        st.session_state.pipeline_stage = 4
                        st.rerun()
            if c2.button("← Back", use_container_width=True):
                st.session_state.pipeline_stage = 2
                st.rerun()
        
        # STAGE 4: OUTPUT
        elif st.session_state.pipeline_stage == 4:
            st.markdown("## ✅ Your LinkedIn Article")
            ar = st.session_state.article_result
            article = ar['article']
            
            st.markdown('<p style="text-align:center;"><span class="linkedin-badge">📱 Ready for LinkedIn</span></p>', unsafe_allow_html=True)
            comp = article['comparison']
            st.markdown(f"**{comp['type']}:** {', '.join(comp['groups'][:3])} | **Winner:** {comp['winner']} | **Reviews:** {comp['total_reviews']:,}")
            
            st.divider()
            st.markdown(f'<div class="article-preview">{article["text"]}</div>', unsafe_allow_html=True)
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Characters", f"{article['char_count']:,}")
            c2.metric("Words", article['word_count'])
            c3.metric("Hashtags", len(article['hashtags']))
            
            st.divider()
            edited = st.text_area("Edit Article", article['text'], height=300)
            
            c1, c2, c3, c4 = st.columns(4)
            if c1.button("📋 Copy", use_container_width=True):
                st.code(edited)
            c2.download_button("⬇️ Download", edited, f"linkedin_{compare_by.lower()}.txt", use_container_width=True)
            if c3.button("🔄 Regenerate", use_container_width=True):
                st.session_state.pipeline_stage = 3
                st.rerun()
            if c4.button("🆕 New", type="primary", use_container_width=True):
                for k in ['pipeline_stage', 'query_result', 'insight_result', 'chart_result', 'article_result']:
                    st.session_state[k] = 0 if k == 'pipeline_stage' else None
                st.rerun()
            
            st.divider()
            st.markdown("### 📊 Charts for Post")
            cc = st.columns(2)
            for i, ch in enumerate(ar['charts'][:2]):
                with cc[i]:
                    st.plotly_chart(ch['figure'], use_container_width=True)
    
    with col2:
        st.markdown("### 💡 Tips")
        tips = {
            0: "**Compare By:**\n- Brand vs Brand\n- City vs City\n- Star Category",
            1: "**Data:**\n- Unknown excluded ✓\n- Mention % used ✓",
            2: "**Insights:**\n- Biggest gaps = best content",
            3: "**Charts:**\n- Use heatmap for multi-aspect",
            4: "**Post:**\n- Best: Tue-Thu 8-10 AM\n- Add chart as image"
        }
        st.info(tips.get(st.session_state.pipeline_stage, tips[0]))
        
        if st.session_state.query_result:
            st.divider()
            qr = st.session_state.query_result
            st.metric("Reviews", f"{qr['total_mentions']:,}")
            st.metric("Hotels", qr['hotel_count'])
    
    st.divider()
    st.markdown('<div style="text-align:center;color:#888;font-size:12px;">🚀 Smaartbrand Pipeline | Hotels → Auto (Q2) → Mobile (Q3) | © Acquink 2026</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()
