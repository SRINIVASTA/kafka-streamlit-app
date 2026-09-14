import streamlit as st
import yfinance as yf
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError

# 1. Web Page Layout Setup
st.set_page_config(page_title="Agentic AI Kafka Engine", layout="centered")
st.title("🤖 Agentic AI Financial Stream Engine")
st.subheader("Welcome, Appala Srinivas!")

# 2. Sidebar Configuration
st.sidebar.header("🔐 Authentication")
api_secret_input = st.sidebar.text_input("Enter Kafka API Secret (Password):", type="password")

st.sidebar.markdown("---")
st.sidebar.header("📅 Select Date Range")
default_start = datetime.today() - timedelta(days=60) # Expanded window for Agent analysis
default_end = datetime.today()

selected_dates = st.sidebar.date_input(
    "Choose History Horizon:",
    value=(default_start, default_end),
    max_value=datetime.today()
)

def get_kafka_config():
    if not api_secret_input:
        st.sidebar.warning("⚠️ Please provide your API Secret Password to connect.")
        return None
    return {
        'bootstrap.servers': st.secrets["KAFKA_BOOTSTRAP_SERVER"],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': st.secrets["KAFKA_API_KEY"],
        'sasl.password': api_secret_input,
        'socket.timeout.ms': 45000,
        'session.timeout.ms': 45000,
    }

TOPIC = "topic_0"

# 3. Main Screen Selector
st.markdown("### 🔍 Instruct AI Agent")
target_ticker = st.text_input("Assign a Ticker Symbol to the AI Agent:", value="AAPL").upper().strip()

if len(selected_dates) == 2:
    start_date, end_date = selected_dates
else:
    st.stop()

if st.button(f"🤖 Activate Agent for {target_ticker}"):
    kafka_config = get_kafka_config()
    if kafka_config:
        
        # --- PHASE A: AGENT INGESTION (Producer) ---
        try:
            producer = Producer(kafka_config)
            with st.spinner(f"Agent collecting market data for {target_ticker}..."):
                stock = yf.Ticker(target_ticker)
                data = stock.history(start=start_date, end=end_date, interval="1d")
                
            if data.empty:
                st.error("❌ Agent could not find data metrics.")
                st.stop()
                
            for date, row in data.iterrows():
                payload = {
                    "ticker": target_ticker,
                    "price": round(row['Close'], 2),
                    "timestamp": str(date.date()) 
                }
                producer.produce(TOPIC, key=target_ticker, value=json.dumps(payload))
            producer.flush()
            
        except Exception as e:
            st.error(f"Agent Ingestion Error: {e}")
            st.stop()

        # --- PHASE B: AGENT SCANNING (Consumer) ---
        try:
            consumer = Consumer(consumer_config := kafka_config.copy())
            consumer_config.update({
                'group.id': f'agent-group-{int(time.time())}',
                'auto.offset.reset': 'earliest'
            })
            consumer = Consumer(consumer_config)
            consumer.subscribe([TOPIC])
            
            history_pool = []
            start_time = time.time()
            
            with st.spinner("Agent listening to Kafka stream partitions..."):
                while time.time() - start_time < 5.0:
                    msg = consumer.poll(timeout=0.2)
                    if msg is None or msg.error():
                        continue
                    try:
                        parsed_payload = json.loads(msg.value().decode('utf-8'))
                        if parsed_payload.get("ticker") == target_ticker:
                            ts_string = parsed_payload["timestamp"].strip()
                            payload_date = datetime.strptime(ts_string.split()[0], "%Y-%m-%d").date()
                            if start_date <= payload_date <= end_date:
                                parsed_payload["timestamp"] = str(payload_date)
                                history_pool.append(parsed_payload)
                    except Exception:
                        continue
            consumer.close()

            # --- PHASE C: AGENT COGNITION & ACTION (Decision Engine) ---
            st.markdown("---")
            st.markdown(f"### 📡 AI Agent Execution Dashboard: {target_ticker}")
            
            if history_pool:
                df = pd.DataFrame(history_pool).drop_duplicates(subset=['timestamp']).sort_values(by="timestamp")
                
                # Render chart first
                st.line_chart(data=df, x="timestamp", y="price", use_container_width=True)
                
                # Agent Intelligence Calculations
                latest_price = df['price'].iloc[-1]
                previous_price = df['price'].iloc[-2] if len(df) > 1 else latest_price
                short_sma = df['price'].rolling(window=min(5, len(df))).mean().iloc[-1]
                long_sma = df['price'].rolling(window=min(20, len(df))).mean().iloc[-1]
                
                # Agent Decision Matrix
                if latest_price > short_sma and short_sma > long_sma:
                    signal = "🟢 STRONG BUY"
                    confidence = "High (Bullish Crossover detected)"
                    reasoning = f"The current price (${latest_price}) is trading above its short-term average (${round(short_sma, 2)}), demonstrating clean upward momentum."
                elif latest_price < short_sma and short_sma < long_sma:
                    signal = "🔴 STRONG SELL"
                    confidence = "High (Bearish Breakdown detected)"
                    reasoning = f"The price has dropped below both key support averages. The agent detects downward heavy distribution."
                else:
                    signal = "🟡 HOLD"
                    confidence = "Medium (Consolidation status)"
                    reasoning = f"The asset is moving sideways. The price is hovering close to its long-term baseline (${round(long_sma, 2)})."

                # Render Agent Insights to UI
                st.markdown("#### 🤖 Agent Report Summary")
                
                col1, col2 = st.columns(2)
                col1.metric("Latest Streamed Price", f"${latest_price:,}")
                col2.metric("Agent Action Signal", signal)
                
                # Display reasoning box
                st.info(f"🧠 **Agent Reasoning:** {reasoning} \n\n🎯 **Confidence Interval:** {confidence}")
                
            else:
                st.warning("⚠️ Agent could not fetch clean partition streams.")

        except Exception as e:
            st.error(f"Agent Execution Failure: {e}")
