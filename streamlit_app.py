import streamlit as st
import pandas as pd
from src.pss.storage.postgres.client import PostgresClient
from src.pss.llm.holdings import BIT_CAPITAL_HOLDINGS

st.set_page_config(page_title="PSS - Polymarket Signal Scanner", layout="wide")

st.title("📊 PSS: Polymarket Signal Scanner")
st.markdown("Automated Intelligence Pipeline for BIT Capital")

@st.cache_resource
def get_db_client():
    return PostgresClient()

db_client = get_db_client()

def load_data():
    with db_client._get_connection() as conn:
        query = """
            SELECT 
                m.question,
                m.description,
                lc.direction,
                lc.weighted_score,
                lc.llm_confidence,
                m.probability,
                lc.tickers,
                lc.sectors,
                lc.reasoning,
                lc.foundational_details,
                lc.circumstances,
                m.volume,
                m.liquidity,
                m.category,
                m.price_change_day,
                m.price_change_week,
                lc.classified_at
            FROM markets m
            INNER JOIN llm_classifications lc ON m.id = lc.market_id
            WHERE lc.is_relevant = true
            ORDER BY lc.weighted_score DESC;
        """
        return pd.read_sql(query, conn)

def load_stats():
    with db_client._get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM markets")
            total_markets = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM markets WHERE processed = false")
            unprocessed = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM llm_classifications WHERE is_relevant = true")
            relevant = cur.fetchone()[0]
            return total_markets, unprocessed, relevant

# Sidebar - Stats & Controls
st.sidebar.header("Pipeline Status")
total, pending, relevant = load_stats()
st.sidebar.metric("Total Markets", total)
st.sidebar.metric("Pending Classification", pending)
st.sidebar.metric("Relevant Signals", relevant)

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Main Tabs
tab1, tab2, tab3 = st.tabs(["🎯 Signal Browser", "🏢 BIT Capital Holdings", "📈 Scanner Logs"])

with tab1:
    st.header("Active Market Signals")
    df = load_data()
    
    if df.empty:
        st.info("No relevant signals found yet. Run the ingestion and classification DAGs.")
    else:
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            direction_filter = st.multiselect("Direction", options=df['direction'].unique(), default=df['direction'].unique())
        with col2:
            score_threshold = st.slider("Min Weighted Score", 0.0, 1.0, 0.3)
        
        filtered_df = df[(df['direction'].isin(direction_filter)) & (df['weighted_score'] >= score_threshold)]
        
        # Display Table
        display_cols = ['question', 'direction', 'weighted_score', 'llm_confidence', 'probability', 'tickers']
        st.dataframe(
            filtered_df[display_cols],
            column_config={
                "weighted_score": st.column_config.ProgressColumn("Score", format="%.2f", min_value=0, max_value=1),
                "probability": st.column_config.NumberColumn("Prob", format="%.2f"),
                "llm_confidence": st.column_config.NumberColumn("Conf", format="%.2f"),
            },
            use_container_width=True,
            hide_index=True
        )

        # Detailed View
        st.divider()
        st.subheader("Signal Deep Dive")
        selected_question = st.selectbox("Select a market to inspect:", filtered_df['question'].unique())
        
        if selected_question:
            detail = filtered_df[filtered_df['question'] == selected_question].iloc[0]
            
            st.subheader(f"❓ {detail['question']}")
            st.info(detail['description'])

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Direction", detail['direction'])
            c2.metric("Weighted Score", f"{detail['weighted_score']:.2f}")
            c3.metric("LLM Confidence", f"{detail['llm_confidence']:.2f}")
            c4.metric("Current Prob", f"{detail['probability']:.2f}")
            
            p1, p2, p3 = st.columns(3)
            p1.metric("24h Price Change", f"{detail['price_change_day']*100:+.2f}%")
            p2.metric("7d Price Change", f"{detail['price_change_week']*100:+.2f}%")
            p3.metric("Liquidity", f"${detail['liquidity']:,.0f}")

            st.markdown(f"**Tickers:** {', '.join(detail['tickers'])}")
            st.markdown(f"**Sectors:** {', '.join(detail['sectors'])}")
            
            with st.expander("📝 Reasoning & Analysis", expanded=True):
                st.write(detail['reasoning'])
            
            with st.expander("🌍 Circumstances"):
                st.write(detail['circumstances'])

            with st.expander("🏗️ Foundational Details"):
                st.write(detail['foundational_details'])
            
            with st.expander("📊 Market Metrics"):
                st.write(f"**Total Volume:** ${detail['volume']:,.2f}")
                st.write(f"**Category:** {detail['category']}")
                st.write(f"**Classified At:** {detail['classified_at']}")

with tab2:
    st.header("BIT Capital Portfolio Focus")
    st.write("Current tickers and sectors the LLM uses for relevance filtering.")
    
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Holdings (Tickers)")
        for ticker, desc in BIT_CAPITAL_HOLDINGS['tickers'].items():
            st.markdown(f"**{ticker}**: {desc}")
            
    with col_b:
        st.subheader("Priority Sectors")
        for sector in BIT_CAPITAL_HOLDINGS['sectors']:
            st.markdown(f"- {sector}")
        
        st.subheader("Macro Themes")
        for theme in BIT_CAPITAL_HOLDINGS['macro_themes']:
            st.markdown(f"- {theme}")

with tab3:
    st.header("Classification Logs")
    with db_client._get_connection() as conn:
        log_query = """
            SELECT m.question, lpr.pass_number, lpr.is_relevant, lpr.confidence, lpr.reason, lpr.created_at
            FROM llm_pass_results lpr
            JOIN markets m ON lpr.market_id = m.id
            ORDER BY lpr.created_at DESC
            LIMIT 50;
        """
        logs_df = pd.read_sql(log_query, conn)
        st.table(logs_df)

st.sidebar.divider()
st.sidebar.caption("PSS v0.1.0 | Internal Use Only")
