import streamlit as st
from langchain_anthropic import ChatAnthropic
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="FPL Assistant",
    page_icon="⚽",
    layout="wide"
)

# Database connection
@st.cache_resource
def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        st.error("❌ DATABASE_URL not found")
        st.stop()
    return psycopg2.connect(database_url, sslmode='require')

# LLM
@st.cache_resource
def get_llm():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        st.error("❌ ANTHROPIC_API_KEY not found")
        st.info("""
        **How to fix:**
        1. Add to `.env`: `ANTHROPIC_API_KEY=sk-ant-api03-your-key-here`
        2. Get key from: https://console.anthropic.com/
        3. Restart Streamlit
        """)
        st.stop()
    
    return ChatAnthropic(
        model="claude-sonnet-4-20250514",
        api_key=api_key,
        temperature=0
    )

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

def execute_query(query):
    """Execute SQL query and return DataFrame"""
    conn = get_db_connection()
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        return {"error": str(e)}

def ask_fpl_question(question):
    """Convert natural language to SQL and execute"""
    llm = get_llm()
    schema = get_database_schema()
    
    # Build schema description
    schema_text = "\n".join([
        f"**{table}**: {', '.join(cols)}"
        for table, cols in schema.items()
    ])
    
    # Create prompt for SQL generation
    prompt = ChatPromptTemplate.from_messages([
        ("system", f"""You are an expert SQL query writer for a Fantasy Premier League database.

DATABASE SCHEMA:
{schema_text}

IMPORTANT RULES:
1. Return ONLY the SQL query, no explanations
2. Use PostgreSQL syntax
3. Always use season='2025-26' for current season queries
4. For player names, use: first_name || ' ' || second_name
5. Limit results to 20 rows unless specified
6. Use ILIKE for case-insensitive string matching

COMMON PATTERNS:
- Top scorers: SELECT player_name, SUM(goals_scored) as goals FROM player_gameweek_stats WHERE season='2025-26' GROUP BY player_name ORDER BY goals DESC LIMIT 10
- Player search: WHERE player_name ILIKE '%name%'
- Team fixtures: FROM fixtures WHERE (team_h_code=X OR team_a_code=X) AND season='2025-26'
- Join players: JOIN players p ON pgs.player_name = p.first_name || ' ' || p.second_name

Return ONLY the SQL query."""),
        ("user", "{question}")
    ])
    
    chain = prompt | llm | StrOutputParser()
    
    # Get SQL from Claude
    sql_query = chain.invoke({"question": question})
    
    # Clean up (remove markdown if present)
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    
    # Execute query
    result = execute_query(sql_query)
    
    if isinstance(result, dict) and "error" in result:
        return {
            "sql": sql_query,
            "error": result["error"],
            "data": None
        }
    
    return {
        "sql": sql_query,
        "data": result,
        "error": None
    }

# UI
st.title("⚽ FPL Assistant")
st.markdown("Ask questions about Fantasy Premier League in plain English!")

# Sidebar
with st.sidebar:
    st.header("💡 Example Questions")
    
    examples = [
        "Who are the top 10 goal scorers this season?",
        "Which defenders have the most clean sheets?",
        "Show me all penalties taken this season",
        "What are Arsenal's next 5 fixtures?",
        "Which midfielders under £7m have scored in the last 3 gameweeks?",
        "Who takes penalties for Manchester City?",
        "Compare Haaland and Salah's goals this season",
        "Show me players with the most assists",
        "Which teams have the easiest upcoming fixtures?",
        "Find budget forwards under £6m who have scored recently",
    ]
    
    for example in examples:
        if st.button(example, key=example, use_container_width=True):
            st.session_state.question = example

    st.divider()
    
    # Database info
    with st.expander("📊 Available Data"):
        schema = get_database_schema()
        for table, cols in schema.items():
            st.write(f"**{table}**")
            st.caption(f"{len(cols)} columns")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # Show SQL if it's an assistant message with SQL
        if message["role"] == "assistant" and "sql" in message:
            with st.expander("🔍 SQL Query"):
                st.code(message["sql"], language="sql")

# Handle example question from sidebar
if "question" in st.session_state:
    prompt = st.session_state.question
    del st.session_state.question
else:
    prompt = None

# Chat input
if prompt or (prompt := st.chat_input("Ask about players, fixtures, stats...")):
    # User message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Assistant response
    with st.chat_message("assistant"):
        with st.spinner("🤔 Analyzing your question..."):
            try:
                result = ask_fpl_question(prompt)
                
                if result["error"]:
                    # Error occurred
                    error_msg = f"❌ Query failed: {result['error']}"
                    st.error(error_msg)
                    with st.expander("🔍 SQL Query (failed)"):
                        st.code(result["sql"], language="sql")
                    
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": error_msg,
                        "sql": result["sql"]
                    })
                else:
                    # Success!
                    df = result["data"]
                    
                    # Format response
                    if len(df) == 0:
                        answer = "No results found for your query."
                    else:
                        answer = f"**Found {len(df)} result(s):**"
                    
                    st.markdown(answer)
                    
                    # Show data table
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    
                    # Show SQL
                    with st.expander("🔍 SQL Query Used"):
                        st.code(result["sql"], language="sql")
                    
                    # Download button
                    if len(df) > 0:
                        csv = df.to_csv(index=False)
                        st.download_button(
                            "📥 Download CSV",
                            csv,
                            "fpl_data.csv",
                            "text/csv",
                            key=f"download_{len(st.session_state.messages)}"
                        )
                    
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": answer,
                        "sql": result["sql"]
                    })
                
            except Exception as e:
                error_msg = f"❌ Error: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })

# Footer
st.divider()
st.caption("🤖 Powered by Claude Sonnet 4 | 📊 Official FPL Data")