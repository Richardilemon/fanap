"""
Fanap - Comprehensive FPL AI Assistant
Combines AI chat, team analysis, player insights, and more
"""
import streamlit as st
from langchain_anthropic import ChatAnthropic
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from dotenv import load_dotenv
from datetime import datetime

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

# Initialize session state
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = 0  # Default to AI Assistant

# Custom CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', sans-serif;
    }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    .block-container {
        padding-top: 2rem;
        max-width: 1400px;
    }
    
    .hero {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2.5rem 2rem;
        border-radius: 20px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 10px 40px rgba(0,0,0,0.1);
    }
    
    .hero h1 {
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
    }
    
    .hero p {
        font-size: 1.1rem;
        opacity: 0.9;
        margin: 0.5rem 0 0 0;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.6rem 1.5rem;
        font-weight: 600;
        transition: all 0.2s;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
    }
    
    [data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
        color: #667eea;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# DATABASE & LLM SETUP
# ============================================================================

def get_db_connection():
    """Get a fresh database connection"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        st.error("❌ DATABASE_URL not found in .env")
        st.stop()
    return psycopg2.connect(database_url, sslmode='require')

@st.cache_resource
def get_llm():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        st.error("❌ ANTHROPIC_API_KEY not found in .env")
        st.stop()
    return ChatAnthropic(
        model="claude-sonnet-4-20250514",
        api_key=api_key,
        temperature=0
    )

@st.cache_data(ttl=3600)
def execute_query(query):
    """Execute SQL query and return DataFrame"""
    try:
        conn = get_db_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Query error: {str(e)}")
        return None

@st.cache_data(ttl=3600)
def get_database_schema():
    """Get database schema information"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
    """)
    
    schema = {}
    for table, column, dtype in cursor.fetchall():
        if table not in schema:
            schema[table] = []
        schema[table].append(f"{column} ({dtype})")
    
    cursor.close()
    return schema

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

@st.cache_data(ttl=3600)
def get_top_scorers(limit=10):
    """Get top scorers this season"""
    query = f"""
        SELECT 
            player_name,
            SUM(goals_scored) as total_goals,
            COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0) as games_played,
            ROUND(SUM(goals_scored)::numeric / NULLIF(COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0), 0), 2) as goals_per_game
        FROM player_gameweek_stats
        WHERE season = '2025-26'
        GROUP BY player_name
        HAVING SUM(goals_scored) > 0
        ORDER BY total_goals DESC
        LIMIT {limit};
    """
    return execute_query(query)

@st.cache_data(ttl=3600)
def get_top_assisters(limit=10):
    """Get top assist providers"""
    query = f"""
        SELECT 
            player_name,
            SUM(assists) as total_assists,
            COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0) as games_played,
            ROUND(SUM(assists)::numeric / NULLIF(COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0), 0), 2) as assists_per_game
        FROM player_gameweek_stats
        WHERE season = '2025-26'
        GROUP BY player_name
        HAVING SUM(assists) > 0
        ORDER BY total_assists DESC
        LIMIT {limit};
    """
    return execute_query(query)

@st.cache_data(ttl=3600)
def get_top_points(limit=10):
    """Get highest points scorers"""
    query = f"""
        SELECT 
            player_name,
            player_team_name as team,
            SUM(total_points) as total_points,
            SUM(minutes) as minutes,
            COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0) as games
        FROM player_gameweek_stats
        WHERE season = '2025-26'
        GROUP BY player_name, player_team_name
        ORDER BY total_points DESC
        LIMIT {limit};
    """
    return execute_query(query)

@st.cache_data(ttl=3600)
def get_player_form(player_name, last_n_games=5):
    """Get player's recent form"""
    query = f"""
        SELECT 
            gameweek,
            opponent_team_name as opponent,
            total_points as points,
            goals_scored as goals,
            assists,
            minutes,
            bonus
        FROM player_gameweek_stats
        WHERE player_name ILIKE '%{player_name}%'
          AND season = '2025-26'
        ORDER BY gameweek DESC
        LIMIT {last_n_games};
    """
    return execute_query(query)

@st.cache_data(ttl=3600)
def get_team_clean_sheets():
    """Get teams with most clean sheets"""
    query = """
        SELECT 
            player_team_name as team,
            SUM(clean_sheets) as clean_sheets,
            COUNT(DISTINCT gameweek) as games
        FROM player_gameweek_stats
        WHERE season = '2025-26'
          AND player_team_name IS NOT NULL
        GROUP BY player_team_name
        ORDER BY clean_sheets DESC
        LIMIT 10;
    """
    return execute_query(query)

@st.cache_data(ttl=3600)
def get_hidden_gems(max_ownership=5.0):
    """Find low-owned players performing well"""
    query = f"""
        WITH latest_ownership AS (
            SELECT DISTINCT ON (player_name)
                player_name,
                player_team_name as team,
                selected::numeric / 10000000 * 100 as ownership_pct,
                value / 10.0 as price,
                gameweek
            FROM player_gameweek_stats
            WHERE season = '2025-26'
              AND selected > 0
              AND player_team_name IS NOT NULL
            ORDER BY player_name, gameweek DESC
        ),
        player_stats AS (
            SELECT 
                player_name,
                SUM(total_points) as total_points,
                SUM(goals_scored) as goals,
                SUM(assists) as assists,
                ROUND(AVG(expected_goals), 2) as avg_xg,
                ROUND(AVG(expected_assists), 2) as avg_xa
            FROM player_gameweek_stats
            WHERE season = '2025-26'
              AND gameweek >= (SELECT MAX(gameweek) - 4 FROM player_gameweek_stats WHERE season = '2025-26')
            GROUP BY player_name
        )
        SELECT 
            ps.player_name,
            lo.team,
            lo.price as current_price,
            ROUND(lo.ownership_pct, 2) as ownership,
            ps.total_points as points_last_5,
            ps.goals,
            ps.assists,
            ps.avg_xg,
            ps.avg_xa
        FROM player_stats ps
        JOIN latest_ownership lo ON ps.player_name = lo.player_name
        WHERE lo.ownership_pct < {max_ownership}
          AND ps.total_points > 20
        ORDER BY ps.total_points DESC
        LIMIT 15;
    """
    return execute_query(query)

@st.cache_data(ttl=3600)
def compare_players(player1, player2):
    """Compare two players' stats - returns in requested order"""
    query = f"""
        SELECT 
            player_name,
            SUM(total_points) as points,
            SUM(goals_scored) as goals,
            SUM(assists) as assists,
            SUM(minutes) as minutes,
            SUM(bonus) as bonus,
            ROUND(AVG(expected_goals), 2) as avg_xg,
            ROUND(AVG(expected_assists), 2) as avg_xa,
            COUNT(DISTINCT gameweek) FILTER (WHERE minutes > 0) as games_played,
            CASE 
                WHEN player_name ILIKE '%{player1}%' THEN 1
                ELSE 2
            END as player_order
        FROM player_gameweek_stats
        WHERE season = '2025-26'
          AND (player_name ILIKE '%{player1}%' OR player_name ILIKE '%{player2}%')
        GROUP BY player_name
        ORDER BY player_order;
    """
    result = execute_query(query)
    
    # Remove the helper column before returning
    if result is not None and 'player_order' in result.columns:
        result = result.drop(columns=['player_order'])
    
    return result

@st.cache_data(ttl=3600)
def get_player_fixtures(player_name, next_n=3):
    """Get player's upcoming fixtures using game_weeks deadline"""
    query = f"""
        WITH player_team_info AS (
            -- Get player's team code, then convert to team id
            SELECT t.id as team_id, t.name as team_name
            FROM players p
            JOIN teams t ON p.team_code = t.code AND t.season = '2025-26'
            WHERE (p.first_name || ' ' || p.second_name) ILIKE '%{player_name}%'
            LIMIT 1
        ),
        next_gameweeks AS (
            -- Get next 3 gameweeks based on deadline
            SELECT gameweek
            FROM game_weeks
            WHERE season = '2025-26'
              AND deadline > NOW()
            ORDER BY deadline ASC
            LIMIT {next_n}
        ),
        player_fixtures AS (
            SELECT 
                f.gameweek,
                f.team_h_code,
                f.team_a_code,
                f.team_h_difficulty,
                f.team_a_difficulty,
                pti.team_id as player_team_id
            FROM fixtures f
            CROSS JOIN player_team_info pti
            WHERE f.season = '2025-26'
              AND f.gameweek IN (SELECT gameweek FROM next_gameweeks)
              AND (f.team_h_code = pti.team_id OR f.team_a_code = pti.team_id)
        )
        SELECT 
            pf.gameweek,
            CASE 
                WHEN pf.team_h_code = pf.player_team_id THEN 'H'
                ELSE 'A'
            END as venue,
            CASE 
                WHEN pf.team_h_code = pf.player_team_id THEN 
                    (SELECT name FROM teams WHERE id = pf.team_a_code AND season = '2025-26')
                ELSE 
                    (SELECT name FROM teams WHERE id = pf.team_h_code AND season = '2025-26')
            END as opponent,
            CASE 
                WHEN pf.team_h_code = pf.player_team_id THEN pf.team_h_difficulty
                ELSE pf.team_a_difficulty
            END as difficulty
        FROM player_fixtures pf
        ORDER BY pf.gameweek;
    """
    return execute_query(query)

@st.cache_data(ttl=3600)
def get_team_fixtures(team_name, next_n=3):
    """Get team's upcoming fixtures using game_weeks deadline"""
    query = f"""
        WITH team_info AS (
            -- Get team id from team name
            SELECT id
            FROM teams
            WHERE name ILIKE '%{team_name}%'
              AND season = '2025-26'
            LIMIT 1
        ),
        next_gameweeks AS (
            -- Get next 3 gameweeks based on deadline
            SELECT gameweek
            FROM game_weeks
            WHERE season = '2025-26'
              AND deadline > NOW()
            ORDER BY deadline ASC
            LIMIT {next_n}
        ),
        team_fixtures AS (
            SELECT 
                f.gameweek,
                f.team_h_code,
                f.team_a_code,
                f.team_h_difficulty,
                f.team_a_difficulty,
                ti.id as team_id
            FROM fixtures f
            CROSS JOIN team_info ti
            WHERE f.season = '2025-26'
              AND f.gameweek IN (SELECT gameweek FROM next_gameweeks)
              AND (f.team_h_code = ti.id OR f.team_a_code = ti.id)
        )
        SELECT 
            tf.gameweek,
            CASE 
                WHEN tf.team_h_code = tf.team_id THEN 'H'
                ELSE 'A'
            END as venue,
            CASE 
                WHEN tf.team_h_code = tf.team_id THEN 
                    (SELECT name FROM teams WHERE id = tf.team_a_code AND season = '2025-26')
                ELSE 
                    (SELECT name FROM teams WHERE id = tf.team_h_code AND season = '2025-26')
            END as opponent,
            CASE 
                WHEN tf.team_h_code = tf.team_id THEN tf.team_h_difficulty
                ELSE tf.team_a_difficulty
            END as difficulty
        FROM team_fixtures tf
        ORDER BY tf.gameweek;
    """
    return execute_query(query)

def ask_fpl_question(question):
    """Convert natural language to SQL using Claude"""
    llm = get_llm()
    schema = get_database_schema()
    
    schema_text = "\n".join([
        f"**{table}**: {', '.join(cols[:10])}..."
        for table, cols in schema.items()
    ])
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", f"""You are an expert SQL query writer for a Fantasy Premier League database.

DATABASE SCHEMA:
{schema_text}

CRITICAL RULES:
1. Return ONLY the SQL query, no explanations or markdown
2. Use PostgreSQL syntax
3. ALWAYS use season='2025-26' for current season
4. ALWAYS add LIMIT clause (default 20)
5. For player names: player_name (already combined first + last)
6. Use ILIKE for case-insensitive matching
7. Current gameweek is around 20

Return ONLY the SQL query."""),
        ("user", "{question}")
    ])
    
    chain = prompt | llm | StrOutputParser()
    sql_query = chain.invoke({"question": question})
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    
    result = execute_query(sql_query)
    
    return {
        "sql": sql_query,
        "data": result
    }

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
# SIDEBAR
# ============================================================================

with st.sidebar:
    st.markdown("### 💡 Quick Actions")
    
    if st.button("🏆 Top Scorers", use_container_width=True, key="btn_scorers"):
        st.session_state.active_tab = 1
        st.session_state.focus_section = "scorers"
        st.rerun()
    
    if st.button("🎯 Top Assisters", use_container_width=True, key="btn_assisters"):
        st.session_state.active_tab = 1
        st.session_state.focus_section = "assisters"
        st.rerun()
    
    if st.button("💯 Most Points", use_container_width=True, key="btn_points"):
        st.session_state.active_tab = 1
        st.session_state.focus_section = "points"
        st.rerun()
    
    if st.button("🛡️ Best Defenses", use_container_width=True, key="btn_defense"):
        st.session_state.active_tab = 1
        st.session_state.focus_section = "defense"
        st.rerun()
    
    if st.button("💎 Hidden Gems", use_container_width=True, key="btn_gems"):
        st.session_state.active_tab = 4
        st.rerun()
    
    st.divider()
    
    st.markdown("### 🔍 Quick Search")
    quick_player = st.text_input("Search player", placeholder="Type player name...", key="sidebar_search")
    if st.button("🔍 Search", use_container_width=True) and quick_player:
        st.session_state.active_tab = 2
        st.session_state.quick_player_search = quick_player
        st.rerun()
    
    st.divider()
    
    st.markdown("### 📋 Database Info")
    schema = get_database_schema()
    st.caption(f"📊 {len(schema)} tables")
    
    with st.expander("View Schema"):
        for table in schema.keys():
            st.write(f"• {table}")

# ============================================================================
# MAIN TABS WITH PROGRAMMATIC CONTROL
# ============================================================================

tab_names = [
    "💬 AI Assistant",
    "🏆 League Leaders",
    "🔍 Player Analysis",
    "⚔️ Compare Players",
    "💎 Hidden Gems"
]

# Create tabs but control which one is active
selected_tab = st.radio(
    "Navigation",
    range(len(tab_names)),
    format_func=lambda x: tab_names[x],
    index=st.session_state.active_tab,
    horizontal=True,
    label_visibility="collapsed"
)

# Update active tab when user clicks
st.session_state.active_tab = selected_tab

# ============================================================================
# TAB CONTENT BASED ON SELECTION
# ============================================================================

if selected_tab == 0:
    # AI ASSISTANT
    st.markdown("### Ask Anything About FPL")
    
    with st.expander("💡 Example Questions"):
        examples = [
            "Who are the top 10 goal scorers this season?",
            "Show me defenders with the most clean sheets",
            "Which players have scored in the last 3 gameweeks?",
            "Compare Haaland and Salah's stats",
            "Find midfielders under £8m with good expected goals",
            "Show me Arsenal's recent form",
        ]
        for ex in examples:
            if st.button(ex, key=f"example_{ex}", use_container_width=True):
                st.session_state.ai_question = ex
    
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            
            if message["role"] == "assistant" and "sql" in message:
                with st.expander("🔍 SQL Query"):
                    st.code(message["sql"], language="sql")
                
                if "data" in message and message["data"] is not None:
                    if len(message["data"]) > 0:
                        st.dataframe(message["data"], use_container_width=True, hide_index=True)
    
    if "ai_question" in st.session_state:
        prompt = st.session_state.ai_question
        del st.session_state.ai_question
    else:
        prompt = None
    
    if prompt or (prompt := st.chat_input("Ask about players, stats, fixtures...")):
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("🤔 Analyzing..."):
                try:
                    result = ask_fpl_question(prompt)
                    
                    if result["data"] is not None and len(result["data"]) > 0:
                        response = f"**Found {len(result['data'])} result(s)**"
                        st.markdown(response)
                        st.dataframe(result["data"], use_container_width=True, hide_index=True)
                        
                        with st.expander("🔍 SQL Query"):
                            st.code(result["sql"], language="sql")
                        
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": response,
                            "sql": result["sql"],
                            "data": result["data"]
                        })
                    else:
                        st.info("No results found for your query.")
                        
                except Exception as e:
                    st.error(f"Error: {str(e)}")

elif selected_tab == 1:
    # LEAGUE LEADERS
    st.markdown("### 🏆 League Leaders")
    
    # Check if there's a focus section from sidebar
    focus = st.session_state.get('focus_section', None)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### 🎯 Top Scorers")
        scorers = get_top_scorers(10)
        if scorers is not None:
            st.dataframe(scorers, use_container_width=True, hide_index=True)
            if focus == "scorers":
                st.success("👆 Showing top scorers!")
    
    with col2:
        st.markdown("#### 🅰️ Top Assisters")
        assisters = get_top_assisters(10)
        if assisters is not None:
            st.dataframe(assisters, use_container_width=True, hide_index=True)
            if focus == "assisters":
                st.success("👆 Showing top assisters!")
    
    with col3:
        st.markdown("#### 💯 Most Points")
        points = get_top_points(10)
        if points is not None:
            st.dataframe(points, use_container_width=True, hide_index=True)
            if focus == "points":
                st.success("👆 Showing most points!")
    
    st.divider()
    
    # Clean sheets section
    if focus == "defense":
        st.info("📊 Showing defensive stats below")
    
    st.markdown("#### 🛡️ Best Defenses (Clean Sheets)")
    clean_sheets = get_team_clean_sheets()
    if clean_sheets is not None:
        col1, col2 = st.columns([2, 1])
        with col1:
            st.dataframe(clean_sheets, use_container_width=True, hide_index=True)
        with col2:
            fig = px.pie(clean_sheets, values='clean_sheets', names='team', title='Clean Sheets Distribution')
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Clear focus after showing
    if 'focus_section' in st.session_state:
        del st.session_state.focus_section
    
    # Visualizations
    st.markdown("### 📊 Visual Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if scorers is not None and len(scorers) > 0:
            fig = px.bar(
                scorers.head(10),
                x='player_name',
                y='total_goals',
                title='Top 10 Goal Scorers',
                color='total_goals',
                color_continuous_scale='Blues'
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if assisters is not None and len(assisters) > 0:
            fig = px.bar(
                assisters.head(10),
                x='player_name',
                y='total_assists',
                title='Top 10 Assist Providers',
                color='total_assists',
                color_continuous_scale='Greens'
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

elif selected_tab == 2:
    # PLAYER ANALYSIS
    st.markdown("### 🔍 Analyze Any Player")
    
    # Check for quick search
    default_player = st.session_state.get('quick_player_search', '')
    
    player_name = st.text_input("Enter player name", value=default_player, placeholder="e.g., Erling Haaland")
    
    # Clear quick search after using
    if 'quick_player_search' in st.session_state:
        del st.session_state.quick_player_search
    
    if player_name:
        query = f"""
            SELECT 
                player_name,
                player_team_name as team,
                SUM(total_points) as total_points,
                SUM(goals_scored) as goals,
                SUM(assists) as assists,
                SUM(minutes) as minutes,
                COUNT(DISTINCT gameweek) as games_played,
                ROUND(AVG(expected_goals), 2) as avg_xg,
                ROUND(AVG(expected_assists), 2) as avg_xa,
                ROUND(AVG(ict_index), 1) as avg_ict
            FROM player_gameweek_stats
            WHERE player_name ILIKE '%{player_name}%'
              AND season = '2025-26'
            GROUP BY player_name, player_team_name
            LIMIT 1;
        """
        
        stats = execute_query(query)
        
        if stats is not None and len(stats) > 0:
            player_data = stats.iloc[0]
            
            st.markdown(f"## {player_data['player_name']}")
            st.caption(f"Team: {player_data['team']}")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Points", int(player_data['total_points']))
            with col2:
                st.metric("Goals", int(player_data['goals']))
            with col3:
                st.metric("Assists", int(player_data['assists']))
            with col4:
                st.metric("Games Played", int(player_data['games_played']))
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Avg xG", player_data['avg_xg'])
            with col2:
                st.metric("Avg xA", player_data['avg_xa'])
            with col3:
                st.metric("Avg ICT", player_data['avg_ict'])
            
            st.divider()
            
            st.markdown("#### 📈 Recent Form (Last 5 Games)")
            form = get_player_form(player_name, 5)
            
            if form is not None and len(form) > 0:
                st.dataframe(form, use_container_width=True, hide_index=True)
                
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=form['gameweek'],
                    y=form['points'],
                    name='Points',
                    marker_color='#667eea',
                    text=form['points'],
                    textposition='outside'
                ))
                fig.update_layout(
                    title=f"{player_data['player_name']} - Points by Gameweek",
                    xaxis_title="Gameweek",
                    yaxis_title="Points",
                    showlegend=False,
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"🔍 No player found matching '{player_name}'. Try a different name.")
    else:
        st.info("💡 Try searching for players like: Haaland, Salah, Palmer, Saka, Watkins")

elif selected_tab == 3:
    # COMPARE PLAYERS
    st.markdown("### ⚔️ Compare Two Players")
    
    col1, col2 = st.columns(2)
    
    with col1:
        player1 = st.text_input("Player 1", placeholder="e.g., Haaland")
    
    with col2:
        player2 = st.text_input("Player 2", placeholder="e.g., Salah")
    
    if player1 and player2:
        comparison = compare_players(player1, player2)
        
        if comparison is not None and len(comparison) == 2:
            st.markdown("#### 📊 Head-to-Head Stats")
            
            col1, col2 = st.columns(2)
            
            for idx, (col, row) in enumerate(zip([col1, col2], comparison.itertuples())):
                with col:
                    st.markdown(f"### {row.player_name}")
                    st.metric("Total Points", int(row.points))
                    st.metric("Goals", int(row.goals))
                    st.metric("Assists", int(row.assists))
                    st.metric("Avg xG", row.avg_xg)
                    st.metric("Avg xA", row.avg_xa)
                    st.metric("Games", int(row.games_played))
            
            st.divider()
            
            # NEXT 3 FIXTURES
            st.markdown("#### 📅 Upcoming Fixtures (Next 3 Games)")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**{comparison.iloc[0]['player_name']}**")
                fixtures1 = get_player_fixtures(player1, 3)
                
                if fixtures1 is not None and len(fixtures1) > 0:
                    # Format for better display
                    display_fixtures = fixtures1.copy()
                    display_fixtures['venue'] = display_fixtures['venue'].apply(lambda x: '🏠 Home' if x == 'H' else '✈️ Away')
                    display_fixtures['difficulty'] = display_fixtures['difficulty'].apply(lambda x: {
                        1: "🟢 Easy (1)",
                        2: "🟢 Easy (2)",
                        3: "🟡 Medium (3)",
                        4: "🟠 Hard (4)",
                        5: "🔴 Very Hard (5)"
                    }.get(x, f"⚪ ({x})"))
                    
                    # Rename columns for clarity
                    display_fixtures = display_fixtures.rename(columns={
                        'gameweek': 'GW',
                        'venue': 'Venue',
                        'opponent': 'Opponent',
                        'difficulty': 'Difficulty'
                    })
                    
                    st.dataframe(
                        display_fixtures[['GW', 'Venue', 'Opponent', 'Difficulty']], 
                        use_container_width=True, 
                        hide_index=True
                    )
                else:
                    st.info("No upcoming fixtures available")

            with col2:
                st.markdown(f"**{comparison.iloc[1]['player_name']}**")
                fixtures2 = get_player_fixtures(player2, 3)
                
                if fixtures2 is not None and len(fixtures2) > 0:
                    # Format for better display
                    display_fixtures = fixtures2.copy()
                    display_fixtures['venue'] = display_fixtures['venue'].apply(lambda x: '🏠 Home' if x == 'H' else '✈️ Away')
                    display_fixtures['difficulty'] = display_fixtures['difficulty'].apply(lambda x: {
                        1: "🟢 Easy (1)",
                        2: "🟢 Easy (2)",
                        3: "🟡 Medium (3)",
                        4: "🟠 Hard (4)",
                        5: "🔴 Very Hard (5)"
                    }.get(x, f"⚪ ({x})"))
                    
                    # Rename columns for clarity
                    display_fixtures = display_fixtures.rename(columns={
                        'gameweek': 'GW',
                        'venue': 'Venue',
                        'opponent': 'Opponent',
                        'difficulty': 'Difficulty'
                    })
                    
                    st.dataframe(
                        display_fixtures[['GW', 'Venue', 'Opponent', 'Difficulty']], 
                        use_container_width=True, 
                        hide_index=True
                    )
                else:
                    st.info("No upcoming fixtures available")
            
            st.divider()
            
            st.markdown("#### 📈 Visual Comparison")
            
            metrics = ['points', 'goals', 'assists', 'bonus']
            
            fig = go.Figure(data=[
                go.Bar(name=comparison.iloc[0]['player_name'], 
                       x=metrics, 
                       y=[comparison.iloc[0][m] for m in metrics],
                       marker_color='#667eea'),
                go.Bar(name=comparison.iloc[1]['player_name'], 
                       x=metrics, 
                       y=[comparison.iloc[1][m] for m in metrics],
                       marker_color='#764ba2')
            ])
            
            fig.update_layout(barmode='group', title="Stats Comparison")
            st.plotly_chart(fig, use_container_width=True)
        elif comparison is not None and len(comparison) == 1:
            st.warning("⚠️ Only found one player. Try different names.")
        else:
            st.info("Enter two player names to compare.")

elif selected_tab == 4:
    # HIDDEN GEMS
    st.markdown("### 💎 Find Hidden Gems")
    st.caption("Low-owned players performing well in the last 5 gameweeks")
    
    max_ownership = st.slider("Max Ownership %", 1.0, 10.0, 5.0, 0.5)
    
    gems = get_hidden_gems(max_ownership)
    
    if gems is not None and len(gems) > 0:
        st.markdown("#### 📊 Hidden Gems List")
        
        # Display each player with their fixtures
        for idx, player in gems.iterrows():
            with st.expander(f"**{player['player_name']}** - {player['team']} (£{player['current_price']:.1f}m) - {player['points_last_5']} pts"):
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    st.markdown("##### 📈 Stats (Last 5 GWs)")
                    st.metric("Total Points", int(player['points_last_5']))
                    st.metric("Goals", int(player['goals']))
                    st.metric("Assists", int(player['assists']))
                    st.metric("Avg xG", player['avg_xg'])
                    st.metric("Avg xA", player['avg_xa'])
                    st.metric("Ownership", f"{player['ownership']:.2f}%")
                
                with col2:
                    st.markdown("##### 📅 Next 3 Fixtures")
                    
                    fixtures = get_team_fixtures(player['team'], 3)
                    
                    if fixtures is not None and len(fixtures) > 0:
                        # Format for display
                        display_fixtures = fixtures.copy()
                        display_fixtures['venue'] = display_fixtures['venue'].apply(lambda x: '🏠' if x == 'H' else '✈️')
                        display_fixtures['difficulty'] = display_fixtures['difficulty'].apply(lambda x: {
                            1: "🟢", 2: "🟢", 3: "🟡", 4: "🟠", 5: "🔴"
                        }.get(x, "⚪") + f" ({x})")
                        
                        display_fixtures = display_fixtures.rename(columns={
                            'gameweek': 'GW',
                            'venue': 'V',
                            'opponent': 'Opponent',
                            'difficulty': 'Diff'
                        })
                        
                        st.dataframe(
                            display_fixtures[['GW', 'V', 'Opponent', 'Diff']], 
                            use_container_width=True, 
                            hide_index=True
                        )
                    else:
                        st.info("No upcoming fixtures available")
        
        st.divider()
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Players Found", len(gems))
        with col2:
            avg_price = gems['current_price'].mean()
            st.metric("Avg Price", f"£{avg_price:.1f}m")
        with col3:
            avg_points = gems['points_last_5'].mean()
            st.metric("Avg Points (L5)", f"{avg_points:.1f}")
        
        st.divider()
        
        # Visualization
        st.markdown("#### 📊 Visual Analysis")
        fig = px.scatter(
            gems,
            x='ownership',
            y='points_last_5',
            size='current_price',
            color='assists',
            hover_name='player_name',
            hover_data={
                'team': True,
                'current_price': ':.1f',
                'ownership': ':.2f',
                'goals': True,
                'assists': True
            },
            title='Ownership vs Performance',
            labels={
                'ownership': 'Ownership %', 
                'points_last_5': 'Points (Last 5 GWs)',
                'current_price': 'Price (£m)'
            }
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
    else:
        st.info("No hidden gems found with current filters.")

# ============================================================================
# FOOTER
# ============================================================================

st.divider()
col1, col2, col3 = st.columns(3)

with col1:
    st.caption("🤖 Powered by Claude Sonnet 4")
with col2:
    st.caption("📊 Official FPL Data")
with col3:
    st.caption(f"🕒 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")