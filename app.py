import streamlit as st
import json
import time
from confluent_kafka import Producer, Consumer

st.set_page_config(page_title="Kafka Stock AI Terminal", layout="wide", initial_sidebar_state="expanded")
st.title("🤖 Kafka Event-Driven AI Stock Terminal")

# =====================================================================
# 1. SIDEBAR CONFIGURATION & SECURE AUTHENTICATION UI
# =====================================================================
st.sidebar.header("🔐 Authentication")

# Variable is defined first so it securely exists in memory
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
    except KeyError as e:
        st.sidebar.error(f"❌ Missing configuration key in Streamlit secrets: {e}")
        return None

# Build runtime config dictionary mapping
KAFKA_CONFIG = get_kafka_config(api_secret_input)

# =====================================================================
# 2. STATE MANAGER INITIALIZATION
# =====================================================================
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "active_ticker" not in st.session_state:
    st.session_state.active_ticker = "GOOG"
if "active_tf" not in st.session_state:
    st.session_state.active_tf = "1y"

def send_user_message(prompt, current_ticker, current_tf):
    if not KAFKA_CONFIG:
        return
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
    except Exception as e:
        st.error(f"Failed to produce message to pipeline: {str(e)}")

# =====================================================================
# 3. INTERFACE WORKSPACE RENDERING
# =====================================================================
if KAFKA_CONFIG:
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("💬 Live Conversation Stream")
        
        # Render historical interaction elements
        for msg in st.session_state.chat_history:
            with st.chat_message(msg["role"]):
                st.write(msg["text"])
                
        # Accept text submissions
        if user_input := st.chat_input("Ask about dividends, news, or change tickers..."):
            st.session_state.chat_history.append({"role": "user", "text": user_input})
            send_user_message(user_input, st.session_state.active_ticker, st.session_state.active_tf)
            st.rerun()

    with col2:
        st.subheader("📈 Dynamic Analytics Pipeline")
        
        # 1. Synchronize UI inputs and add "1d" options cleanly
        st.session_state.active_ticker = st.text_input("Active Ticker Token:", value=st.session_state.active_ticker).upper()
        
        st.session_state.active_tf = st.selectbox(
            "Active Window Scale:", 
            ["1d", "5d", "1mo", "3mo", "6mo", "1y", "5y", "10y"], 
            index=5 # Defaults to 1y tracking baseline
        )

        # =====================================================================
        # ✅ UPGRADED LIVE CHART ENGINE WITH EXTRA GRANULAR INTRA-DAY HANDLING
        # =====================================================================
        if st.session_state.active_ticker:
            try:
                import yfinance as yf
                import plotly.graph_objects as go
                
                stock_engine = yf.Ticker(st.session_state.active_ticker)
                
                # If the user selects a 1-day or 5-day horizon view, pull high-frequency data
                if st.session_state.active_tf == "1d":
                    df = stock_engine.history(period="1d", interval="5m")
                elif st.session_state.active_tf == "5d":
                    df = stock_engine.history(period="5d", interval="15m")
                else:
                    df = stock_engine.history(period=st.session_state.active_tf)
                
                if not df.empty:
                    # Construct clean interactive Plotly layout tracking paths
                    fig = go.Figure()
                    
                    # Choose a line style or area fill based on time horizon velocity
                    fig.add_trace(go.Scatter(
                        x=df.index, 
                        y=df['Close'], 
                        mode='lines', 
                        name=st.session_state.active_ticker,
                        line=dict(color='#00bc8c', width=2),
                        fill='tozeroy' if st.session_state.active_tf in ["1d", "5d"] else None,
                        fillcolor='rgba(0, 188, 140, 0.1)'
                    ))
                    
                    fig.update_layout(
                        title=f"{st.session_state.active_ticker} Close Tracking ({st.session_state.active_tf})",
                        template="plotly_dark",
                        xaxis_title="Timeline / Date-Time Window",
                        yaxis_title="Asset Value Price (INR)",
                        margin=dict(l=20, r=20, t=40, b=20),
                        height=350
                    )
                    
                    # Display the updated chart canvas
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning(f"No active data points found for ticker token '{st.session_state.active_ticker}' over a '{st.session_state.active_tf}' horizon.")
            except Exception as chart_err:
                st.error(f"Failed to draw telemetry graph: {str(chart_err)}")

        # ---------------------------------------------------------------------
        # ASYNCHRONOUS KAFKA CONSUMER LOOP
        # ---------------------------------------------------------------------
        try:
            consumer = Consumer({
                **KAFKA_CONFIG,
                'group.id': 'streamlit-ui-group',
                'auto.offset.reset': 'latest'
            })
            consumer.subscribe(['stock-results'])
            
            msg = consumer.poll(timeout=0.2)
            if msg is not None and not msg.error():
                response_data = json.loads(msg.value().decode('utf-8'))
                
                # Update session states dynamically out of backend extraction pipelines
                st.session_state.chat_history.append({"role": "assistant", "text": response_data["agent_reply"]})
                st.session_state.active_ticker = response_data["updated_ticker"]
                st.session_state.active_tf = response_data["updated_tf"]
                consumer.close()
                st.rerun()
                
            consumer.close()
        except Exception as e:
            st.caption(f"Waiting for backend data pipelines stream... ({str(e)})")

else:
    st.info("👋 Enter your Kafka API Secret in the sidebar panel to unlock the live workspace terminal window.")
