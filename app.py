import streamlit as st
import json
import time
import yfinance as yf
import plotly.graph_objects as go
from confluent_kafka import Producer, Consumer

# Enforce clean dark-mode presentation styling natively
st.set_page_config(page_title="Kafka Stock AI Terminal", layout="wide", initial_sidebar_state="expanded")
st.title("🤖 Live Event-Driven AI Stock Terminal")

# =====================================================================
# 1. SIDEBAR SECURE CLOUD AUTHENTICATION
# =====================================================================
st.sidebar.header("🔐 Cluster Verification")

# Web User Inputs its distinct session pass
api_secret_input = st.sidebar.text_input("Enter Kafka API Secret (Password):", type="password")

def get_kafka_config(secret_password):
    if not secret_password:
        st.sidebar.warning("⚠️ Please provide your API Secret Password to connect.")
        return None
    try:
        # Pulls structural connection strings securely out of your App Settings Secrets panel
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
        st.sidebar.error(f"❌ Core Secret Keys not configured in Streamlit Cloud Dashboard: {e}")
        return None

KAFKA_CONFIG = get_kafka_config(api_secret_input)

# =====================================================================
# 2. RUNTIME SESSION MEMORY POOLS
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
        st.error(f"Kafka Producer Network Disruption: {str(e)}")

# =====================================================================
# 3. MULTI-COLUMN INTERFACE LAYOUT RENDER
# =====================================================================
if KAFKA_CONFIG:
    col1, col2 = st.columns([1, 1])

    # --- LEFT SECTION: CONVERSATION FRAME ---
    with col1:
        st.subheader("💬 Live Conversation Stream")
        
        # Display existing interaction history elements blocks
        for msg in st.session_state.chat_history:
            with st.chat_message(msg["role"]):
                st.write(msg["text"])
                
        # Accept text input commands natively
        if user_input := st.chat_input("Ask about dividends, news, or request shifts..."):
            st.session_state.chat_history.append({"role": "user", "text": user_input})
            send_user_message(user_input, st.session_state.active_ticker, st.session_state.active_tf)
            st.rerun()

    # --- RIGHT SECTION: CHART VISUALIZER ---
    with col2:
        st.subheader("📈 Dynamic Analytics Pipeline")
        
        # Capture input parameters from users layout widgets
        st.session_state.active_ticker = st.text_input("Active Ticker Token:", value=st.session_state.active_ticker).upper()
        st.session_state.active_tf = st.selectbox("Active Window Scale:", ["1d", "5d", "1mo", "3mo", "6mo", "1y", "5y", "10y"], index=5)

        # Plotly Telemetry Graph Execution Box
        if st.session_state.active_ticker:
            try:
                stock_engine = yf.Ticker(st.session_state.active_ticker)
                
                # Dynamic High-Frequency Sampling for short horizons
                if st.session_state.active_tf == "1d":
                    df = stock_engine.history(period="1d", interval="5m")
                elif st.session_state.active_tf == "5d":
                    df = stock_engine.history(period="5d", interval="15m")
                else:
                    df = stock_engine.history(period=st.session_state.active_tf)
                
                if not df.empty:
                    # ✅ FIXED: Force immediate stripping of timezone offsets to prevent chart crashes
                    if df.index.tz is not None:
                        df.index = df.index.tz_localize(None)

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df.index, 
                        y=df['Close'], 
                        mode='lines', 
                        name=st.session_state.active_ticker,
                        line=dict(color='#00bc8c', width=2),
                        fill='tozeroy' if st.session_state.active_tf in ["1d", "5d"] else None,
                        fillcolor='rgba(0, 188, 140, 0.08)'
                    ))
                    fig.update_layout(
                        title=f"{st.session_state.active_ticker} Performance History ({st.session_state.active_tf})",
                        template="plotly_dark",
                        xaxis_title="Timeline Interval",
                        yaxis_title="Close Price",
                        margin=dict(l=15, r=15, t=35, b=15),
                        height=340
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning(f"No pricing indexes located for symbol token: '{st.session_state.active_ticker}'")
            except Exception as chart_err:
                st.error(f"Visualizer Rendering Error: {str(chart_err)}")

        # ---------------------------------------------------------------------
        # ✅ WEB INTERFACE ASYNCHRONOUS POLL CARRIER
        # ---------------------------------------------------------------------
        # If the last message came from the user, wait dynamically for the backend worker's reply
        if len(st.session_state.chat_history) > 0 and st.session_state.chat_history[-1]["role"] == "user":
            with st.spinner("⏳ Streamlit Cloud awaiting events from Kafka channel matrix..."):
                try:
                    consumer = Consumer({
                        **KAFKA_CONFIG,
                        'group.id': 'streamlit-cloud-group',
                        'auto.offset.reset': 'latest',
                        'enable.auto.commit': True
                    })
                    consumer.subscribe(['stock-results'])
                    
                    # Safe web polling: check for updates iteratively over 8 seconds max
                    msg_payload = None
                    for _ in range(40):
                        msg = consumer.poll(timeout=0.2)
                        if msg is not None and not msg.error():
                            msg_payload = msg
                            break
                        time.sleep(0.1)

                    if msg_payload:
                        response_data = json.loads(msg_payload.value().decode('utf-8'))
                        # Re-hydrate layout session states out of message boundaries
                        st.session_state.chat_history.append({"role": "assistant", "text": response_data["agent_reply"]})
                        st.session_state.active_ticker = response_data["updated_ticker"]
                        st.session_state.active_tf = response_data["updated_tf"]
                        consumer.close()
                        st.rerun()
                    
                    consumer.close()
                except Exception as kafka_err:
                    st.caption(f"Awaiting pipeline updates... ({str(kafka_err)})")
else:
    st.info("👋 Enter your Kafka API Secret in the sidebar panel to unlock the live workspace terminal window.")
