import streamlit as st
import json
import time
import yfinance as yf
import plotly.graph_objects as go
from confluent_kafka import Producer, Consumer

st.set_page_config(page_title="Kafka Stock AI Terminal", layout="wide", initial_sidebar_state="expanded")
st.title("🤖 Live Event-Driven AI Stock Terminal")

# =====================================================================
# 1. SIDEBAR SECURE CLOUD AUTHENTICATION
# =====================================================================
st.sidebar.header("🔐 Cluster Verification")
api_secret_input = st.sidebar.text_input("Enter Kafka API Secret (Password):", type="password")

def get_kafka_config(secret_password):
    if not secret_password:
        st.sidebar.warning("⚠️ Please provide your API Secret Password to connect.")
        return None
    try:
        return {
            'bootstrap.servers': st.secrets["KAFKA_BOOTSTRAP_SERVER"],
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': st.secrets["KAFKA_API_KEY"],
            'sasl.password': secret_password,
            'socket.timeout.ms': 45000,
            'session.timeout.ms': 45000,
        }
    except Exception as e:
        st.sidebar.error(f"❌ Check your Streamlit Cloud Secrets: {e}")
        return None

KAFKA_CONFIG = get_kafka_config(api_secret_input)

# =====================================================================
# 2. RUNTIME SESSION MEMORY POOLS
# =====================================================================
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "active_ticker" not in st.session_state:
    st.session_state.active_ticker = "RELIANCE.NS"
if "active_tf" not in st.session_state:
    st.session_state.active_tf = "1d"
if "awaiting_response" not in st.session_state:
    st.session_state.awaiting_response = False

def send_user_message(prompt, current_ticker, current_tf):
    if not KAFKA_CONFIG: return
    try:
        producer = Producer(KAFKA_CONFIG)
        payload = {
            "user_message": prompt,
            "current_ticker": current_ticker,
            "current_tf": current_tf,
            "timestamp": time.time()
        }
        producer.produce('stock-requests', key=current_ticker.encode('utf-8'), value=json.dumps(payload).encode('utf-8'))
        producer.flush()
        st.session_state.awaiting_response = True
    except Exception as e:
        st.error(f"Kafka Network Disruption: {str(e)}")

# =====================================================================
# 3. INTERFACE WORKSPACE RENDERING
# =====================================================================
if KAFKA_CONFIG:
    col1, col2 = st.columns(2)

    # --- LEFT PANEL: CONVERSATION STREAM ---
    with col1:
        st.subheader("💬 Live Conversation Stream")
        
        # Display existing chat history
        for msg in st.session_state.chat_history:
            with st.chat_message(msg["role"]):
                st.write(msg["text"])
                
        # Handle chat submission entry
        if user_input := st.chat_input("Ask about dividends, news, or request shifts..."):
            st.session_state.chat_history.append({"role": "user", "text": user_input})
            send_user_message(user_input, st.session_state.active_ticker, st.session_state.active_tf)
            st.rerun()

    # --- RIGHT PANEL: AUTO-REFRESHING VISUALIZER & BACKGROUND CONSUMER ---
    with col2:
        st.subheader("📈 Dynamic Analytics Pipeline")
        
        # 1. UI Control Configuration Elements
        st.session_state.active_ticker = st.text_input("Active Ticker Token:", value=st.session_state.active_ticker).upper()
        
        c1, c2 = st.columns(2)
        with c1:
            st.session_state.active_tf = st.selectbox("Active Window Scale:", ["1d", "5d", "1mo", "3mo", "6mo", "1y", "5y", "10y"], index=5)
        with c2:
            # New indicator selector widget adding technical analytical layers
            technical_indicator = st.selectbox("Technical Analysis Overlay:", ["None", "20-Day SMA", "50-Day EMA"])

        # 2. Interactive Plotly Graphic Assembly 
        if st.session_state.active_ticker:
            try:
                import yfinance as yf
                import plotly.graph_objects as go
                
                stock_engine = yf.Ticker(st.session_state.active_ticker)
                
                # Fetch baseline timeframe tracking metrics
                if st.session_state.active_tf == "1d":
                    df = stock_engine.history(period="1d", interval="5m")
                elif st.session_state.active_tf == "5d":
                    df = stock_engine.history(period="5d", interval="15m")
                else:
                    df = stock_engine.history(period=st.session_state.active_tf)
                
                if not df.empty:
                    # Strip out explicit timezone offsets to maintain rendering stability
                    if df.index.tz is not None:
                        df.index = df.index.tz_localize(None)

                    fig = go.Figure()
                    
                    # Primary Asset Close Path Trace
                    fig.add_trace(go.Scatter(
                        x=df.index, y=df['Close'], mode='lines', 
                        name=f"{st.session_state.active_ticker} Close",
                        line=dict(color='#00bc8c', width=2),
                        fill='tozeroy' if st.session_state.active_tf in ["1d", "5d"] else None,
                        fillcolor='rgba(0, 188, 140, 0.08)'
                    ))
                    
                    # Programmatic Technical Trend Calculations (Applied dynamically if enough rows exist)
                    if technical_indicator == "20-Day SMA" and len(df) >= 20:
                        df['SMA20'] = df['Close'].rolling(window=20).mean()
                        fig.add_trace(go.Scatter(
                            x=df.index, y=df['SMA20'], mode='lines',
                            name="20 SMA", line=dict(color='#f39c12', width=1.5, dash='dash')
                        ))
                    elif technical_indicator == "50-Day EMA" and len(df) >= 50:
                        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
                        fig.add_trace(go.Scatter(
                            x=df.index, y=df['EMA50'], mode='lines',
                            name="50 EMA", line=dict(color='#e74c3c', width=1.5, dash='dot')
                        ))

                    fig.update_layout(
                        title=f"{st.session_state.active_ticker} Telemetry Window ({st.session_state.active_tf})",
                        template="plotly_dark",
                        xaxis_title="Timeline Axis", yaxis_title="Price Metrics",
                        margin=dict(l=15, r=15, t=35, b=15), height=360
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning(f"No asset trends discovered for ticker target: '{st.session_state.active_ticker}'")
            except Exception as chart_err:
                st.error(f"Visualizer compilation fault: {str(chart_err)}")
