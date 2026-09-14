import streamlit as st
import yfinance as yf
import json
import time
import random
from confluent_kafka import Producer, Consumer, KafkaError

# 1. Web Page Layout Setup
st.set_page_config(page_title="Kafka Dynamic Ticker Engine", layout="centered")
st.title("📈 Dynamic Real-Time Financial Tracker")
st.subheader("Welcome, Appala Srinivas!")

# 2. Secret Key Setup (Hybrid Mode)
st.sidebar.header("🔐 Authentication")
api_secret_input = st.sidebar.text_input(
    "Enter Kafka API Secret (Password):", 
    type="password"
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

# 3. Dynamic User Inputs
st.markdown("### 🔍 Select Asset Target")
target_ticker = st.text_input("Type any Stock or Crypto Ticker Symbol (e.g., AAPL, BTC-USD, MSFT, INFY):", value="AAPL").upper().strip()

# 4. Integrated Live Stream Processing Engine
if st.button(f"⚡ Establish Kafka Tunnel for {target_ticker}"):
    kafka_config = get_kafka_config()
    if kafka_config:
        # --- PHASE A: PRODUCTION (Fetch data & write to Kafka) ---
        try:
            producer = Producer(kafka_config)
            current_price = None
            
            with st.spinner(f"Pulling fresh market positions for {target_ticker}..."):
                try:
                    stock = yf.Ticker(target_ticker)
                    data = stock.history(period="1d", interval="1m")
                    if not data.empty:
                        current_price = round(data.iloc[-1]['Close'], 2)
                except Exception:
                    pass 
            
            if current_price is None:
                current_price = round(random.uniform(150.0, 350.0), 2)
                st.caption(f"ℹ️ Market Holiday/Closed. Running simulated price feed for `{target_ticker}`.")

            payload = {
                "ticker": target_ticker,
                "price": current_price,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            }

            producer.produce(TOPIC, key=target_ticker, value=json.dumps(payload))
            producer.flush()
            st.toast(f"✅ Data event broadcasted to Confluent Cluster for {target_ticker}!")
            
        except Exception as e:
            st.error(f"Producer Failure: {e}")
            st.stop()

        # --- PHASE B: CONSUMPTION (Read data back from Kafka) ---
        consumer_config = kafka_config.copy()
        consumer_config.update({
            'group.id': f'st-dynamic-group-{int(time.time())}',
            'auto.offset.reset': 'earliest'
        })

        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([TOPIC])
            
            found_payload = None
            start_time = time.time()
            
            with st.spinner("Streaming event records back from Kafka partitions..."):
                while time.time() - start_time < 5.0:
                    msg = consumer.poll(timeout=0.5)
                    if msg is None:
                        continue
                    if msg.error():
                        continue
                    
                    # SAFETY FILTER: Skip old plain text logs that are not JSON strings
                    try:
                        parsed_payload = json.loads(msg.value().decode('utf-8'))
                        if isinstance(parsed_payload, dict) and parsed_payload.get("ticker") == target_ticker:
                            found_payload = parsed_payload
                    except json.JSONDecodeError:
                        continue # Safely ignore old "Hackathon Test 1!" text data and keep reading
            
            consumer.close()

            # --- PHASE C: RENDER TO USER INTERFACE ---
            st.markdown("---")
            st.markdown("### 📡 Live Kafka Feed Monitor")
            if found_payload:
                st.success(f"🎉 Successfully captured data packet from Confluent Cloud!")
                st.metric(
                    label=f"🚀 Ticker: {found_payload['ticker']}", 
                    value=f"${found_payload['price']:,}", 
                    delta=f"Log Timestamp: {found_payload['timestamp'].split()[-1]}"
                )
            else:
                st.warning(f"⚠️ Sent successfully, but connection timed out before capturing `{target_ticker}` from the broker partition pool. Please click the button again!")

        except Exception as e:
            st.error(f"Consumer Failure: {e}")
