import streamlit as st
import yfinance as yf
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError

# 1. Web Page Layout Setup
st.set_page_config(page_title="Kafka Historical Ticker Engine", layout="centered")
st.title("📈 Historical Trend Stream Engine")
st.subheader("Welcome, Appala Srinivas!")

# 2. Secret Key Setup & Date Configuration (Sidebar Panel)
st.sidebar.header("🔐 Authentication")
api_secret_input = st.sidebar.text_input(
    "Enter Kafka API Secret (Password):", 
    type="password"
)

st.sidebar.markdown("---")
st.sidebar.header("📅 Select Date Range")

# Default historical parameters (from 30 days ago up until today)
default_start = datetime.today() - timedelta(days=30)
default_end = datetime.today()

# Streamlit Date Picker inside the Sidebar
selected_dates = st.sidebar.date_input(
    "Choose History Horizon:",
    value=(default_start, default_end),
    max_value=datetime.today(),
    help="Select the starting date and ending date for your data log query."
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

# 3. Main Screen Viewport: Target Selector
st.markdown("### 🔍 Historical Chart Target")
target_ticker = st.text_input("Type any Ticker (e.g., AAPL, BTC-USD, MSFT):", value="AAPL").upper().strip()

# Make sure user selected a valid start and end tuple before running
if len(selected_dates) == 2:
    start_date, end_date = selected_dates
    st.sidebar.caption(f"📅 **Query Window:** `{start_date}` to `{end_date}`")
else:
    st.sidebar.info("💡 Please choose both a start and end date on the calendar map above.")
    st.stop()

if st.button(f"📊 Fetch & Stream History for {target_ticker}"):
    kafka_config = get_kafka_config()
    if kafka_config:
        
        # --- PHASE A: PRODUCTION (Fetch Dynamic Range & Stream to Kafka) ---
        try:
            producer = Producer(kafka_config)
            
            with st.spinner(f"Pulling rows for {target_ticker} from {start_date} to {end_date}..."):
                stock = yf.Ticker(target_ticker)
                # Query historical rows based directly on user's calendar selection
                data = stock.history(start=start_date, end=end_date, interval="1d")
                
            if data.empty:
                st.error(f"❌ No historical market activity found for `{target_ticker}` within that range.")
                st.stop()
                
            # Stream historical log rows chronologically into Kafka
            with st.spinner(f"Streaming {len(data)} data packets into Kafka Cloud..."):
                for date, row in data.iterrows():
                    payload = {
                        "ticker": target_ticker,
                        "price": round(row['Close'], 2),
                        "timestamp": str(date.date()) 
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
                        # Filter for the target ticker and ensure date falls inside our query horizon
                        if isinstance(parsed_payload, dict) and parsed_payload.get("ticker") == target_ticker:
                            payload_date = datetime.strptime(parsed_payload["timestamp"], "%Y-%m-%d").date()
                            if start_date <= payload_date <= end_date:
                                history_pool.append(parsed_payload)
                    except json.JSONDecodeError:
                        continue
                        
            consumer.close()

            # --- PHASE C: RENDER HISTORICAL DATA CHART ---
            st.markdown("---")
            st.markdown(f"### 📡 Live Kafka Feed Chart: {target_ticker}")
            
            if history_pool:
                df = pd.DataFrame(history_pool)
                # Deduplicate and sort values by date cleanly
                df = df.drop_duplicates(subset=['timestamp']).sort_values(by="timestamp")
                
                # Render line chart trend module
                st.line_chart(data=df, x="timestamp", y="price", use_container_width=True)
                st.success(f"🎉 Successfully mapped historical trend curves from Confluent Broker logs!")
            else:
                st.warning(f"⚠️ Connection completed, but no data records matching `{target_ticker}` within that date range were found in the partition logs.")

        except Exception as e:
            st.error(f"Consumer Failure: {e}")
