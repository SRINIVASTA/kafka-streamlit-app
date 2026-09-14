import streamlit as st
import yfinance as yf
import json
import time
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError

# 1. Web Page Layout Setup
st.set_page_config(page_title="Kafka Historical Ticker Engine", layout="centered")
st.title("📈 Historical Trend Stream Engine")
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
st.markdown("### 🔍 Historical Chart Target")
target_ticker = st.text_input("Type any Ticker (e.g., AAPL, BTC-USD, MSFT):", value="AAPL").upper().strip()

if st.button(f"📊 Fetch & Stream History for {target_ticker}"):
    kafka_config = get_kafka_config()
    if kafka_config:
        
        # --- PHASE A: PRODUCTION (Fetch 30-Day History & Stream to Kafka) ---
        try:
            producer = Producer(kafka_config)
            
            with st.spinner(f"Pulling 30 days of historical rows for {target_ticker}..."):
                stock = yf.Ticker(target_ticker)
                # Pull daily interval historical trend positions
                data = stock.history(period="30d", interval="1d")
                
            if data.empty:
                st.error(f"❌ Could not retrieve historical records for `{target_ticker}`.")
                st.stop()
                
            # Stream historical log rows chronologically into Kafka
            with st.spinner("Streaming data packets into Kafka Cloud..."):
                for date, row in data.iterrows():
                    payload = {
                        "ticker": target_ticker,
                        "price": round(row['Close'], 2),
                        "timestamp": str(date.date()) # Date format string
                    }
                    producer.produce(TOPIC, key=target_ticker, value=json.dumps(payload))
                producer.flush()
            st.toast("✅ Historical records successfully broadcasted to Confluent Cluster!")
            
        except Exception as e:
            st.error(f"Producer Failure: {e}")
            st.stop()

        # --- PHASE B: CONSUMPTION (Read back logs from Kafka) ---
        consumer_config = kafka_config.copy()
        consumer_config.update({
            'group.id': f'st-chart-group-{int(time.time())}',
            'auto.offset.reset': 'earliest'
        })

        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([TOPIC])
            
            history_pool = []
            start_time = time.time()
            
            with st.spinner("Extracting stream records out of Kafka partitions..."):
                while time.time() - start_time < 5.0:
                    msg = consumer.poll(timeout=0.2)
                    if msg is None or msg.error():
                        continue
                    
                    try:
                        parsed_payload = json.loads(msg.value().decode('utf-8'))
                        if isinstance(parsed_payload, dict) and parsed_payload.get("ticker") == target_ticker:
                            history_pool.append(parsed_payload)
                    except json.JSONDecodeError:
                        continue
                        
            consumer.close()

            # --- PHASE C: RENDER HISTORICAL DATA CHART ---
            st.markdown("---")
            st.markdown(f"### 📡 Live Kafka Feed Chart: {target_ticker}")
            
            if history_pool:
                # Convert list of JSON logs into a structured Dataframe
                df = pd.DataFrame(history_pool)
                # Deduplicate and sort values by date cleanly
                df = df.drop_duplicates(subset=['timestamp']).sort_values(by="timestamp")
                
                # Render a gorgeous native line chart
                st.line_chart(data=df, x="timestamp", y="price", use_container_width=True)
                st.success(f"🎉 Successfully mapped historical trend curves from Confluent Broker logs!")
            else:
                st.warning("⚠️ Connection completed, but no data records matching this asset symbol were found in the current stream partition sweep window.")

        except Exception as e:
            st.error(f"Consumer Failure: {e}")
