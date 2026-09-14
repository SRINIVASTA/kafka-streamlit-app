import streamlit as st
import json
import time
from confluent_kafka import Consumer, KafkaError

st.set_page_config(page_title="Kafka Live Market Ticker", layout="wide")
st.title("📈 Real-Time Multi-Cloud Financial Tracker")
st.subheader("Welcome, Appala Srinivas! Powered by Apache Kafka & Confluent Cloud")

# Initialize UI session states for storing incoming data streams
if "ticker_prices" not in st.session_state:
    st.session_state.ticker_prices = {"BTC-USD": [], "AAPL": [], "RELIANCE.NS": []}

# 1. Hybrid Password Authentication Setup
st.sidebar.header("🔐 Authentication")
api_secret_input = st.sidebar.text_input(
    "Enter Kafka API Secret (Password):", 
    type="password"
)

def get_kafka_config():
    if not api_secret_input:
        st.sidebar.warning("⚠️ Please provide your API Secret Password to establish streaming tunnels.")
        return None
    return {
        'bootstrap.servers': st.secrets["KAFKA_BOOTSTRAP_SERVER"],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': st.secrets["KAFKA_API_KEY"],
        'sasl.password': api_secret_input,
    }

TOPIC = "topic_0"

# 2. Main Area: Live Metric Cards Layout
st.markdown("### 📊 Market Live Tickers")
cols = st.columns(3)

# Display visual placeholders that will hold our streaming data values
metrics_places = {
    "BTC-USD": cols[0].empty(),
    "AAPL": cols[1].empty(),
    "RELIANCE.NS": cols[2].empty()
}

# 3. Dynamic Stream Poller Engine
st.markdown("---")
st.markdown("### 📡 Live Engine Command Center")
if st.button("Start Live Data Stream Catching"):
    kafka_config = get_kafka_config()
    if kafka_config:
        consumer_config = kafka_config.copy()
        consumer_config.update({
            'group.id': f'streamlit-financial-{int(time.time())}',
            'auto.offset.reset': 'latest' # Catch live numbers streaming right now!
        })
        
        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([TOPIC])
            
            st.toast("⚡ Handshaking completed. Connected to Confluent Stream Engine!")
            status_container = st.empty()
            
            # Poll loop running dynamically for 30 cycles to demonstrate live streaming
            for i in range(30):
                status_container.write(f"🔄 Actively Polling Kafka Broker Lanes... Cycle {i+1}/30")
                msg = consumer.poll(timeout=1.0)
                
                if msg is not None and not msg.error():
                    # Parse the incoming JSON event record package sent by data_producer.py
                    payload = json.loads(msg.value().decode('utf-8'))
                    ticker = payload["ticker"]
                    price = payload["price"]
                    timestamp = payload["timestamp"]
                    
                    # Update local application memory states
                    st.session_state.ticker_prices[ticker].append(price)
                    
                    # Render updated metric card live on user screen instantly
                    metrics_places[ticker].metric(
                        label=f"🚀 {ticker}", 
                        value=f"${price:,}" if "BTC" in ticker or "AAPL" in ticker else f"₹{price:,}",
                        delta=f"Updated: {timestamp.split()[-1]}"
                    )
                time.sleep(0.5)
                
            consumer.close()
            status_container.success("✅ Stream monitoring window finished successfully.")
        except Exception as e:
            st.error(f"Streaming Interrupted: {e}")
