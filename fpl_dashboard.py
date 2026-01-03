import streamlit as st
from langchain_anthropic import ChatAnthropic
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv
from scripts.fetch_fpl_team import (
    fetch_team_info, 
    get_current_squad,
    save_user_team
)
from scripts.utils.infer_season import infer_season

load_dotenv()

# ============================================================================
# PAGE CONFIG & CUSTOM CSS
# ============================================================================

st.set_page_config(
    page_title="Fanap - FPL AI Assistant",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern design
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Global Styles */
    * {
        font-family: 'Inter', sans-serif;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Main container */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }
    
    /* Hero section */
    .hero {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 3rem 2rem;
        border-radius: 20px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 10px 40px rgba(0,0,0,0.1);
    }
    
    .hero h1 {
        font-size: 3rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
    }
    
    .hero p {
        font-size: 1.2rem;
        opacity: 0.9;
    }
    
    /* Stats cards */
    .stat-card {
        background: white;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.07);
        border-left: 4px solid #667eea;
        margin-bottom: 1rem;
        transition: transform 0.2s, box-shadow 0.2s;
    }
    
    .stat-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 15px rgba(0,0,0,0.1);
    }
    
    .stat-card h3 {
        color: #667eea;
        font-size: 0.9rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .stat-card .value {
        font-size: 2rem;
        font-weight: 700;
        color: #1a202c;
    }
    
    .stat-card .subtitle {
        color: #718096;
        font-size: 0.85rem;
        margin-top: 0.25rem;
    }
    
    /* Chat messages */
    .stChatMessage {
        background: white;
        border-radius: 15px;
        padding: 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        transition: transform 0.2s, box-shadow 0.2s;
        width: 100%;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #f7fafc 0%, #edf2f7 100%);
    }
    
    [data-testid="stSidebar"] .stButton > button {
        background: white;
        color: #667eea;
        border: 2px solid #667eea;
    }
    
    [data-testid="stSidebar"] .stButton > button:hover {
        background: #667eea;
        color: white;
    }
    
    /* Data tables */
    .dataframe {
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 4px 6px rgba(0,0,0,0.07);
    }
    
    /* Expanders */
    .streamlit-expanderHeader {
        background: #f7fafc;
        border-radius: 10px;
        font-weight: 600;
    }
    
    /* Input fields */
    .stTextInput > div > div > input {
        border-radius: 10px;
        border: 2px solid #e2e8f0;
        padding: 0.75rem;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #667eea;
        box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px 10px 0 0;
        padding: 1rem 2rem;
        font-weight: 600;
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        color: #667eea;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# DATABASE & LLM SETUP
# ============================================================================

@st.cache_resource
def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        st.error("❌ DATABASE_URL not found")
        st.stop()
    return psycopg2.connect(database_url, sslmode='require')

@st.cache_resource
def get_llm():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        st.error("❌ ANTHROPIC_API_KEY not found")
        st.stop()
    return ChatAnthropic(
        model="claude-sonnet-4-20250514",
        api_key=api_key,
        temperature=0
    )

def execute_query(query):
    """Execute SQL query and return DataFrame"""
    conn = get_db_connection()
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        return {"error": str(e)}

# ============================================================================
# HERO SECTION
# ============================================================================

st.markdown("""
<div class="hero">
    <h1>⚽ Fanap</h1>
    <p>Your AI-Powered Fantasy Premier League Assistant</p>
</div>
""", unsafe_allow_html=True)

# ============================================================================
# SIDEBAR - FPL TEAM CONNECTION
# ============================================================================

with st.sidebar:
    st.markdown("### 🔗 Connect Your FPL Team")
    
    # Check if team is already connected
    if 'fpl_team_id' not in st.session_state:
        st.session_state.fpl_team_id = None
    
    if st.session_state.fpl_team_id:
        # Team is connected
        team_info = fetch_team_info(st.session_state.fpl_team_id)
        
        st.success(f"✅ Connected: {team_info['name']}")
        st.caption(f"Rank: {team_info['summary_overall_rank']:,}")
        st.caption(f"Points: {team_info['summary_overall_points']}")
        
        if st.button("🔄 Refresh Team Data"):
            save_user_team(st.session_state.fpl_team_id)
            st.rerun()
        
        if st.button("❌ Disconnect"):
            st.session_state.fpl_team_id = None
            st.rerun()
    
    else:
        # Team not connected
        team_id_input = st.text_input(
            "Enter your FPL Team ID",
            placeholder="123456",
            help="Find it in your FPL URL: fantasy.premierleague.com/entry/YOUR_ID/event/X"
        )
        
        if st.button("🔗 Connect Team"):
            if team_id_input:
                try:
                    team_info = fetch_team_info(int(team_id_input))
                    save_user_team(int(team_id_input))
                    st.session_state.fpl_team_id = int(team_id_input)
                    st.success(f"✅ Connected: {team_info['name']}")
                    st.rerun()
                except:
                    st.error("❌ Invalid Team ID")
    
    st.divider()
    
    # Quick actions
    st.markdown("### 💡 Quick Actions")
    
    actions = {
        "📊 My Team Analysis": "Analyze my current team's performance and upcoming fixtures",
        "🎯 Captain Picks": "Who should I captain this gameweek based on fixtures and form?",
        "💰 Transfer Suggestions": "Suggest transfers based on my team and upcoming fixtures",
        "🔍 Find Differentials": "Show me differential picks owned by less than 5% of managers",
        "📈 Price Changes": "Which players are likely to rise or fall in price?",
    }
    
    for action, query in actions.items():
        if st.button(action, use_container_width=True, key=action):
            if st.session_state.fpl_team_id or "My Team" not in action:
                st.session_state.quick_action = query
            else:
                st.warning("⚠️ Connect your FPL team first!")

# ============================================================================
# MAIN CONTENT - TABS
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "💬 AI Assistant",
    "🏆 My Team",
    "📊 Stats & Insights",
    "⚽ Players"
])

# TAB 1: AI ASSISTANT
with tab1:
    st.markdown("### Ask Anything About FPL")
    
    # Initialize chat
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Display chat
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask about players, fixtures, transfers..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("🤔 Analyzing..."):
                # TODO: Add your AI query logic here
                response = "AI response here (connect to your existing logic)"
                st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})

# TAB 2: MY TEAM
with tab2:
    if st.session_state.fpl_team_id:
        st.markdown("### Your Current Squad")
        
        current_gw = 18  # TODO: Get from database
        squad = get_current_squad(st.session_state.fpl_team_id, current_gw)
        
        # Display squad by position
        positions = ['GK', 'DEF', 'MID', 'FWD']
        
        for pos in positions:
            st.markdown(f"#### {pos}")
            pos_players = [p for p in squad if p['position'] == pos]
            
            cols = st.columns(len(pos_players))
            for idx, player in enumerate(pos_players):
                with cols[idx]:
                    captain_badge = "Ⓒ " if player['is_captain'] else "ⓥ " if player['is_vice_captain'] else ""
                    st.markdown(f"""
                    <div class="stat-card">
                        <h3>{captain_badge}{player['web_name']}</h3>
                        <div class="subtitle">{player['team']}</div>
                        <div class="subtitle">£{player['price']}m</div>
                    </div>
                    """, unsafe_allow_html=True)
    else:
        st.info("👈 Connect your FPL team in the sidebar to see your squad!")

# TAB 3: STATS & INSIGHTS
with tab3:
    st.markdown("### League Insights")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Top Scorer", "Haaland", "23 goals")
    with col2:
        st.metric("Top Assists", "Salah", "15 assists")
    with col3:
        st.metric("Most Points", "Palmer", "156 pts")
    with col4:
        st.metric("Best Value", "Mbeumo", "8.2 pts/£m")

# TAB 4: PLAYERS
with tab4:
    st.markdown("### Player Database")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        position_filter = st.selectbox("Position", ["All", "GK", "DEF", "MID", "FWD"])
    with col2:
        max_price = st.slider("Max Price (£m)", 4.0, 15.0, 15.0, 0.5)
    with col3:
        min_points = st.number_input("Min Points", 0, 200, 0)
    
    # TODO: Add player table with filters

st.markdown("---")
st.caption("🤖 Powered by Claude Sonnet 4 | 📊 Data from Official FPL API")