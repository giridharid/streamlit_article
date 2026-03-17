import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.graph_objects as go
import json
import os
import numpy as np
import base64
import re

# ─────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────
st.set_page_config(page_title="Smaartbrand Intelligence", page_icon="🏨", layout="wide")

# ─────────────────────────────────────────
# STYLES
# ─────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.main-header { text-align: center; padding: 24px; background: linear-gradient(135deg, #1e3a5f 0%, #0d9488 100%); border-radius: 12px; margin-bottom: 20px; }
.main-header h1 { color: white; margin: 0; font-size: 1.9rem; }
.main-header p { color: rgba(255,255,255,0.85); margin: 8px 0 0 0; font-size: 0.9rem; }
.compare-card { background: linear-gradient(135deg, #0d9488 0%, #115e59 100%); padding: 20px; border-radius: 12px; color: white; text-align: center; margin: 8px 0; }
.compare-card h3 { margin: 0 0 8px 0; font-size: 1rem; opacity: 0.9; }
.compare-card .metric { font-size: 2.2rem; font-weight: 700; }
.insight-card { background: #f8fafc; border-left: 4px solid #0d9488; padding: 16px; margin: 12px 0; border-radius: 0 8px 8px 0; }
.insight-title { font-weight: 600; color: #1e3a5f; margin-bottom: 8px; }
.winner-badge { background: #10b981; color: white; padding: 2px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; }
.article-preview { background: white; border: 1px solid #e5e7eb; border-radius: 12px; padding: 24px; margin: 16px 0; white-space: pre-wrap; line-height: 1.6; }
.linkedin-badge { background: #0077b5; color: white; padding: 4px 12px; border-radius: 20px; font-size: 12px; }
.vs-text { font-size: 1.5rem; font-weight: 700; color: #f59e0b; text-align: center; padding: 10px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# CREDENTIALS (exact copy from working app)
# ─────────────────────────────────────────
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
        return bigquery.Client(project="gen-lang-client-0143536012")
    except:
        return None

client = get_bq_client()

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────
PROJECT = "gen-lang-client-0143536012"
DATASET = "analyst"

ASPECT_MAP = {1: "Dining", 2: "Cleanliness", 3: "Amenities", 4: "Staff",
              5: "Room", 6: "Location", 7: "Value for Money", 8: "General"}
ASPECT_ICONS = {"Dining": "🍽️", "Cleanliness": "🧹", "Amenities": "🏊", "Staff": "👨‍💼",
                "Room": "🛏️", "Location": "📍", "Value for Money": "💰", "General": "⭐"}
TEAL_SCALE = ["#f0fdfa", "#99f6e4", "#5eead4", "#2dd4bf", "#14b8a6", "#0d9488", "#0f766e", "#115e59"]

HOOK_LIBRARY = {
    "brand_overall": "🏆 {brand1} vs {brand2}: The ultimate showdown. {total:,} guest reviews reveal a clear winner.",
    "brand_dining": "🍽️ {brand1} vs {brand2}: Who serves breakfast better? We analyzed {total:,} reviews.",
    "brand_staff": "👨‍💼 Guests forgive bad WiFi. They never forgive rude staff. {brand1} vs {brand2}.",
    "brand_value": "💰 {brand1} charges premium prices. But do guests feel they get premium service?",
    "city_overall": "🌆 {city1} vs {city2}: Which city's hotels deliver better experiences?",
    "city_business": "💼 {city1} vs {city2}: Where do Business travelers complain more?",
    "city_leisure": "🏖️ Leisure travelers love {city1}. But {city2} is catching up fast.",
    "star_overall": "⭐ 3-star vs 4-star vs 5-star: Which gives the best bang for your buck?",
    "star_value": "⭐ 5-star price, 3-star experience? We compared {total:,} reviews.",
    "star_service": "🛎️ Do more stars mean better service? Not always.",
}

def get_color_for_score(score):
    idx = min(int(score / 100 * (len(TEAL_SCALE) - 1)), len(TEAL_SCALE) - 1)
    return TEAL_SCALE[idx]

# ─────────────────────────────────────────
# DATA QUERIES (exact copy from working app)
# ─────────────────────────────────────────
@st.cache_data(ttl=3600)
def get_metadata():
    if client is None:
        return pd.DataFrame()
    query = f"""
    SELECT DISTINCT pl.Name AS hotel_name, pd.Brand, pl.Star_Category AS star_category, pl.City
    FROM `{PROJECT}.{DATASET}.product_list` pl
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE pd.Brand IS NOT NULL AND pl.Name IS NOT NULL AND pl.City IS NOT NULL
    """
    try:
        return client.query(query).to_dataframe()
    except:
        return pd.DataFrame()

# NO CACHE - for debugging
def fetch_data_debug(hotel_names: list):
    """Fetch with debug output - no cache, using parameterized query"""
    if client is None:
        return pd.DataFrame(), "No client"
    if not hotel_names:
        return pd.DataFrame(), "No hotel names provided"
    
    # Use parameterized query to avoid SQL injection and special character issues
    query = f"""
    SELECT pl.Name AS hotel_name, pd.Brand, pl.Star_Category, pl.City,
           s.aspect_id, s.treemap_name AS phrase, s.sentiment_type, 
           e.Review_date,
           e.inferred_gender AS gender,
           e.traveler_type,
           e.stay_purpose,
           COUNT(*) AS mention_count
    FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
    JOIN `{PROJECT}.{DATASET}.product_user_review_sentiment` s ON e.id = s.user_review_id
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE pl.Name IN UNNEST(@hotel_names)
    GROUP BY pl.Name, pd.Brand, pl.Star_Category, pl.City, s.aspect_id, s.treemap_name, s.sentiment_type, e.Review_date, e.inferred_gender, e.traveler_type, e.stay_purpose
    """
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("hotel_names", "STRING", list(hotel_names))
            ]
        )
        df = client.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            return df, f"Query returned 0 rows for {len(hotel_names)} hotels"
        df["aspect"] = df["aspect_id"].map(ASPECT_MAP).fillna("Other")
        df["mention_count"] = pd.to_numeric(df["mention_count"], errors="coerce").fillna(0).astype(int)
        return df, f"Success: {len(df)} rows"
    except Exception as e:
        return pd.DataFrame(), f"Query error: {str(e)}"

# ─────────────────────────────────────────
# PIPELINE FUNCTIONS
# ─────────────────────────────────────────
def filter_unknown(df):
    original_len = len(df)
    for col in ['gender', 'traveler_type', 'stay_purpose']:
        if col in df.columns:
            df = df[~df[col].astype(str).str.lower().isin(['unknown', 'nan', 'none', ''])]
    return df

def run_query_agent(compare_by, selected_items, hotel_names):
    if not hotel_names:
        return {"success": False, "error": "No hotels selected", "debug": "hotel_names list is empty"}
    
    df, debug_msg = fetch_data_debug(hotel_names)
    
    if df.empty:
        return {"success": False, "error": "No data found", "debug": debug_msg, "hotels_tried": hotel_names[:10]}
    
    df = filter_unknown(df)
    if df.empty:
        return {"success": False, "error": "No data after filtering unknowns", "debug": "All rows filtered out"}
    
    group_col = {"Brand": "Brand", "City": "City", "Star Category": "Star_Category"}.get(compare_by, "hotel_name")
    
    sat_data = []
    for grp in df[group_col].unique():
        gdf = df[df[group_col] == grp]
        for asp in df['aspect'].unique():
            adf = gdf[gdf['aspect'] == asp]
            pos = adf[adf['sentiment_type'].str.lower() == 'positive']['mention_count'].sum()
            neg = adf[adf['sentiment_type'].str.lower() == 'negative']['mention_count'].sum()
            sat_data.append({'compare_group': str(grp), 'aspect': asp,
                             'satisfaction_pct': round(pos/(pos+neg)*100, 1) if pos+neg > 0 else 0})
    sat_df = pd.DataFrame(sat_data)
    
    overall = {}
    for grp in df[group_col].unique():
        gdf = df[df[group_col] == grp]
        pos = gdf[gdf['sentiment_type'].str.lower() == 'positive']['mention_count'].sum()
        neg = gdf[gdf['sentiment_type'].str.lower() == 'negative']['mention_count'].sum()
        overall[str(grp)] = round(pos/(pos+neg)*100, 1) if pos+neg > 0 else 0
    
    winners = {}
    for asp in sat_df['aspect'].unique():
        adf = sat_df[sat_df['aspect'] == asp]
        if len(adf) > 1:
            w, l = adf.loc[adf['satisfaction_pct'].idxmax()], adf.loc[adf['satisfaction_pct'].idxmin()]
            winners[asp] = {'winner': w['compare_group'], 'winner_score': w['satisfaction_pct'],
                            'loser': l['compare_group'], 'loser_score': l['satisfaction_pct'],
                            'gap': round(w['satisfaction_pct'] - l['satisfaction_pct'], 1)}
    
    return {"success": True, "compare_by": compare_by, "groups": [str(g) for g in df[group_col].unique()],
            "satisfaction_df": sat_df, "overall_satisfaction": overall, "aspect_winners": winners,
            "total_mentions": int(df['mention_count'].sum()), "hotel_count": df['hotel_name'].nunique(),
            "debug": debug_msg}

def run_insight_extractor(qr):
    if not qr.get('success'):
        return {"success": False, "insights": []}
    insights = []
    sg = sorted(qr['overall_satisfaction'].items(), key=lambda x: x[1], reverse=True)
    if sg:
        insights.append({"type": "overall_winner", "title": f"🏆 Winner: {sg[0][0]}",
                         "description": f"{sg[0][0]} leads with {sg[0][1]}% vs {sg[-1][0]} at {sg[-1][1]}%",
                         "icon": "🏆", "gap": sg[0][1] - sg[-1][1]})
    for asp, d in sorted(qr['aspect_winners'].items(), key=lambda x: x[1]['gap'], reverse=True)[:3]:
        if d['gap'] > 5:
            insights.append({"type": "aspect_gap", "title": f"{ASPECT_ICONS.get(asp,'📊')} {asp}: {d['winner']} leads",
                             "description": f"{d['winner']} {d['winner_score']}% vs {d['loser']} {d['loser_score']}%",
                             "icon": ASPECT_ICONS.get(asp, "📊"), "aspect": asp, "winner": d['winner'], "gap": d['gap']})
    return {"success": True, "insights": insights, "query_result": qr}

def run_chart_generator(ir):
    if not ir.get('success'):
        return {"success": False, "charts": []}
    qr = ir['query_result']
    sat_df, overall, compare_by = qr['satisfaction_df'], qr['overall_satisfaction'], qr['compare_by']
    charts = []
    
    sg = sorted(overall.items(), key=lambda x: x[1], reverse=True)
    fig1 = go.Figure(go.Bar(x=[g[1] for g in sg], y=[str(g[0]) for g in sg], orientation='h',
                            marker_color=['#10b981' if i==0 else '#0d9488' for i in range(len(sg))],
                            text=[f"{g[1]}%" for g in sg], textposition='outside'))
    fig1.update_layout(title=f"Overall: {compare_by}", height=300, margin=dict(l=150,r=60,t=60,b=40),
                       xaxis=dict(range=[0,105]), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
    charts.append({"title": "Overall", "figure": fig1})
    
    if not sat_df.empty:
        pivot = sat_df.pivot(index='aspect', columns='compare_group', values='satisfaction_pct').fillna(0)
        fig2 = go.Figure(go.Heatmap(z=pivot.values, x=pivot.columns.tolist(),
                                     y=[f"{ASPECT_ICONS.get(a,'•')} {a}" for a in pivot.index],
                                     colorscale=[[0,'#fecaca'],[0.5,'#fef3c7'],[1,'#a7f3d0']],
                                     text=[[f"{v:.0f}%" for v in row] for row in pivot.values],
                                     texttemplate="%{text}", textfont={"size":11}))
        fig2.update_layout(title="Aspect Breakdown", height=400, margin=dict(l=150,r=20,t=60,b=60), paper_bgcolor='rgba(0,0,0,0)')
        charts.append({"title": "Heatmap", "figure": fig2})
    
    fig3 = go.Figure()
    colors = ['#0d9488', '#f59e0b', '#8b5cf6', '#ef4444']
    for i, grp in enumerate(qr['groups'][:4]):
        gd = sat_df[sat_df['compare_group'] == str(grp)]
        if not gd.empty:
            aspects, scores = gd['aspect'].tolist(), gd['satisfaction_pct'].tolist()
            fig3.add_trace(go.Scatterpolar(r=scores+[scores[0]], theta=aspects+[aspects[0]],
                                           fill='toself', line=dict(color=colors[i%4], width=2), name=str(grp)))
    fig3.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0,100])),
                       showlegend=True, title="Radar", height=400, paper_bgcolor='rgba(0,0,0,0)')
    charts.append({"title": "Radar", "figure": fig3})
    
    return {"success": True, "charts": charts, "insight_result": ir}

def run_article_writer(cr, hook_type, compare_by):
    if not cr.get('success'):
        return {"success": False}
    qr = cr['insight_result']['query_result']
    insights = cr['insight_result']['insights']
    groups, overall, total = qr['groups'], qr['overall_satisfaction'], qr['total_mentions']
    
    sg = sorted(overall.items(), key=lambda x: x[1], reverse=True)
    leader, laggard = sg[0][0], sg[-1][0]
    leader_score, laggard_score = sg[0][1], sg[-1][1]
    
    hook_template = HOOK_LIBRARY.get(hook_type, HOOK_LIBRARY['brand_overall'])
    try:
        hook = hook_template.format(brand1=groups[0] if groups else "A", brand2=groups[1] if len(groups)>1 else "B",
                                    city1=groups[0] if groups else "A", city2=groups[1] if len(groups)>1 else "B", total=total)
    except:
        hook = hook_template
    
    gap_insights = [i for i in insights if i['type'] == 'aspect_gap'][:2]
    
    article = f"""{hook}

We analyzed {total:,} guest reviews to find which {compare_by.lower()} delivers the best experience.

📊 Finding #1: {leader} leads with {leader_score}% satisfaction
That's {leader_score - laggard_score:.1f}% ahead of {laggard}.

📊 Finding #2: The biggest battleground? {gap_insights[0]['aspect'] if gap_insights else 'Service'}
{gap_insights[0]['winner'] if gap_insights else leader} dominates with a {gap_insights[0]['gap'] if gap_insights else 10}% lead.

📊 The Takeaway:
For {laggard}, focus on {gap_insights[0]['aspect'] if gap_insights else 'basics'}.
For {leader}, maintain the edge but watch for challengers.

💬 Which {compare_by.lower()} delivers the best experience?

#HospitalityIndustry #HotelManagement #GuestExperience #DataDriven #Smaartbrand"""
    
    return {"success": True, "article": {"text": article, "word_count": len(article.split()), "char_count": len(article),
                                          "comparison": {"type": compare_by, "groups": groups, "winner": leader, "total_reviews": total}},
            "charts": cr['charts'], "insights": insights}

# ─────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────
st.markdown('<div class="main-header"><h1>🚀 Smaartbrand Intelligence Pipeline</h1><p>Compare Brands • Cities • Stars → LinkedIn-Ready Insights</p></div>', unsafe_allow_html=True)

for key in ['pipeline_stage', 'query_result', 'insight_result', 'chart_result', 'article_result']:
    if key not in st.session_state:
        st.session_state[key] = 0 if key == 'pipeline_stage' else None

stages = ["1. Compare", "2. Insights", "3. Charts", "4. Article"]
scols = st.columns(4)
for i, (col, stage) in enumerate(zip(scols, stages)):
    status = "complete" if i < st.session_state.pipeline_stage else ("active" if i == st.session_state.pipeline_stage else "pending")
    icon = "✓" if status == "complete" else str(i+1)
    color = "#10b981" if status == "complete" else ("#0d9488" if status == "active" else "#e5e7eb")
    col.markdown(f'<div style="text-align:center;"><div style="width:36px;height:36px;border-radius:50%;background:{color};color:{"white" if status!="pending" else "#9ca3af"};display:inline-flex;align-items:center;justify-content:center;font-weight:bold;">{icon}</div><div style="font-size:11px;">{stage}</div></div>', unsafe_allow_html=True)

st.divider()

# SIDEBAR
with st.sidebar:
    st.markdown("### 🎯 Controls")
    if st.button("🔄 Reset", use_container_width=True):
        for key in ['pipeline_stage', 'query_result', 'insight_result', 'chart_result', 'article_result']:
            st.session_state[key] = 0 if key == 'pipeline_stage' else None
        st.rerun()
    
    st.divider()
    meta = get_metadata()
    selected_hotels = []
    selected_items = []
    compare_by = "Brand"
    hook_type = "brand_overall"
    
    if not meta.empty:
        st.success(f"✅ Metadata: {len(meta)} hotels")
        
        compare_by = st.radio("Compare By", ["Brand", "City", "Star Category"], horizontal=True)
        st.divider()
        
        if compare_by == "Brand":
            brands = sorted(meta["Brand"].dropna().unique().tolist())
            selected_items = st.multiselect("Select Brands", brands, default=brands[:2] if len(brands)>=2 else brands)
            if selected_items:
                selected_hotels = meta[meta["Brand"].isin(selected_items)]["hotel_name"].unique().tolist()
        elif compare_by == "City":
            cities = sorted(meta["City"].dropna().unique().tolist())
            selected_items = st.multiselect("Select Cities", cities, default=cities[:2] if len(cities)>=2 else cities)
            if selected_items:
                selected_hotels = meta[meta["City"].isin(selected_items)]["hotel_name"].unique().tolist()
        else:
            stars = sorted(meta["star_category"].dropna().unique().tolist())
            selected_items = st.multiselect("Select Stars", stars, default=stars[:2] if len(stars)>=2 else stars,
                                            format_func=lambda x: f"{'⭐'*int(x)}")
            if selected_items:
                selected_hotels = meta[meta["star_category"].isin(selected_items)]["hotel_name"].unique().tolist()
        
        if selected_hotels:
            st.success(f"✓ {len(selected_hotels)} hotels")
            # Debug: Show first few hotel names
            with st.expander("🔍 Debug: Hotel names"):
                for h in selected_hotels[:10]:
                    st.caption(h)
                if len(selected_hotels) > 10:
                    st.caption(f"... and {len(selected_hotels) - 10} more")
        
        st.divider()
        hook_map = {"Brand": ["brand_overall","brand_dining","brand_staff","brand_value"],
                    "City": ["city_overall","city_business","city_leisure"],
                    "Star Category": ["star_overall","star_value","star_service"]}
        hook_type = st.selectbox("Hook Style", hook_map.get(compare_by, ["brand_overall"]),
                                 format_func=lambda x: x.replace("_"," ").title())
    else:
        st.warning("⚠️ No metadata")

# MAIN
col1, col2 = st.columns([2.5, 1])

with col1:
    if st.session_state.pipeline_stage == 0:
        st.markdown("## 1️⃣ Select & Compare")
        if len(selected_items) >= 2 and selected_hotels:
            st.markdown("### 🆚 Your Comparison")
            pcols = st.columns(min(len(selected_items), 4))
            for i, (c, item) in enumerate(zip(pcols, selected_items[:4])):
                c.markdown(f'<div class="compare-card"><h3>{compare_by}</h3><div class="metric">{item}</div></div>', unsafe_allow_html=True)
            st.markdown(f'<div class="vs-text">{" vs ".join([str(s) for s in selected_items[:4]])}</div>', unsafe_allow_html=True)
            st.divider()
            
            if st.button("🚀 Run Comparison", type="primary", use_container_width=True):
                with st.spinner(f"Analyzing {len(selected_hotels)} hotels..."):
                    result = run_query_agent(compare_by, selected_items, selected_hotels)
                    if result['success']:
                        st.session_state.query_result = result
                        st.session_state.pipeline_stage = 1
                        st.rerun()
                    else:
                        st.error(f"❌ {result.get('error', 'Unknown error')}")
                        # Debug info
                        st.warning(f"🔍 Debug: {result.get('debug', 'No debug info')}")
                        if result.get('hotels_tried'):
                            st.code(f"Hotels tried: {result['hotels_tried']}")
        else:
            st.warning(f"⚠️ Select at least 2 {compare_by.lower()}s from sidebar")
    
    elif st.session_state.pipeline_stage == 1:
        st.markdown("## 2️⃣ Extract Insights")
        qr = st.session_state.query_result
        sg = sorted(qr['overall_satisfaction'].items(), key=lambda x: x[1], reverse=True)
        gcols = st.columns(min(len(sg), 4))
        for i, (c, (grp, score)) in enumerate(zip(gcols, sg[:4])):
            badge = "🥇" if i==0 else ("🥈" if i==1 else "")
            c.markdown(f'<div class="compare-card" style="{"background:linear-gradient(135deg,#10b981,#059669);" if i==0 else ""}"><h3>{badge} {grp}</h3><div class="metric">{score}%</div></div>', unsafe_allow_html=True)
        st.divider()
        c1, c2 = st.columns(2)
        if c1.button("🔍 Extract Insights", type="primary", use_container_width=True):
            result = run_insight_extractor(qr)
            if result['success']:
                st.session_state.insight_result = result
                st.session_state.pipeline_stage = 2
                st.rerun()
        if c2.button("← Back", use_container_width=True):
            st.session_state.pipeline_stage = 0
            st.rerun()
    
    elif st.session_state.pipeline_stage == 2:
        st.markdown("## 3️⃣ Generate Charts")
        ir = st.session_state.insight_result
        for ins in ir['insights'][:5]:
            badge = f"<span class='winner-badge'>GAP: {ins.get('gap',0):.0f}%</span>" if ins['type']=='aspect_gap' else ""
            st.markdown(f'<div class="insight-card"><div class="insight-title">{ins.get("icon","")} {ins["title"]} {badge}</div>{ins["description"]}</div>', unsafe_allow_html=True)
        st.divider()
        c1, c2 = st.columns(2)
        if c1.button("📊 Generate Charts", type="primary", use_container_width=True):
            result = run_chart_generator(ir)
            if result['success']:
                st.session_state.chart_result = result
                st.session_state.pipeline_stage = 3
                st.rerun()
        if c2.button("← Back", use_container_width=True):
            st.session_state.pipeline_stage = 1
            st.rerun()
    
    elif st.session_state.pipeline_stage == 3:
        st.markdown("## 4️⃣ Write Article")
        cr = st.session_state.chart_result
        tabs = st.tabs([c['title'] for c in cr['charts']])
        for tab, chart in zip(tabs, cr['charts']):
            with tab:
                st.plotly_chart(chart['figure'], use_container_width=True)
        st.divider()
        c1, c2 = st.columns(2)
        if c1.button("✍️ Generate Article", type="primary", use_container_width=True):
            result = run_article_writer(cr, hook_type, compare_by)
            if result['success']:
                st.session_state.article_result = result
                st.session_state.pipeline_stage = 4
                st.rerun()
        if c2.button("← Back", use_container_width=True):
            st.session_state.pipeline_stage = 2
            st.rerun()
    
    elif st.session_state.pipeline_stage == 4:
        st.markdown("## ✅ Your LinkedIn Article")
        ar = st.session_state.article_result
        article = ar['article']
        st.markdown('<p style="text-align:center;"><span class="linkedin-badge">📱 Ready for LinkedIn</span></p>', unsafe_allow_html=True)
        st.markdown(f"**{article['comparison']['type']}:** {', '.join(article['comparison']['groups'][:3])} | **Winner:** {article['comparison']['winner']} | **Reviews:** {article['comparison']['total_reviews']:,}")
        st.divider()
        st.markdown(f'<div class="article-preview">{article["text"]}</div>', unsafe_allow_html=True)
        mc1, mc2 = st.columns(2)
        mc1.metric("Characters", f"{article['char_count']:,}")
        mc2.metric("Words", article['word_count'])
        st.divider()
        edited = st.text_area("Edit Article", article['text'], height=300)
        bc1, bc2, bc3 = st.columns(3)
        bc1.download_button("⬇️ Download", edited, f"linkedin_{compare_by.lower()}.txt", use_container_width=True)
        if bc2.button("🔄 Regenerate", use_container_width=True):
            st.session_state.pipeline_stage = 3
            st.rerun()
        if bc3.button("🆕 New", type="primary", use_container_width=True):
            for k in ['pipeline_stage','query_result','insight_result','chart_result','article_result']:
                st.session_state[k] = 0 if k=='pipeline_stage' else None
            st.rerun()
        st.divider()
        st.markdown("### 📊 Charts")
        ccols = st.columns(2)
        for i, ch in enumerate(ar['charts'][:2]):
            with ccols[i]:
                st.plotly_chart(ch['figure'], use_container_width=True)

with col2:
    st.markdown("### 💡 Tips")
    tips = {0: "Select Brand/City/Star\nto compare", 1: "Review satisfaction\nscores", 2: "See key gaps", 3: "Pick chart for post", 4: "Copy & post!"}
    st.info(tips.get(st.session_state.pipeline_stage, tips[0]))
    if st.session_state.query_result:
        st.divider()
        st.metric("Reviews", f"{st.session_state.query_result['total_mentions']:,}")
        st.metric("Hotels", st.session_state.query_result['hotel_count'])

st.divider()
st.markdown('<div style="text-align:center;color:#888;font-size:12px;">🚀 Smaartbrand Pipeline | © Acquink 2026</div>', unsafe_allow_html=True)
