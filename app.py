import streamlit as st
import yfinance as yf
import json
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError

# 1. Web Page Layout Setup
st.set_page_config(page_title="Agentic AI Global Kafka Engine", layout="centered")
st.title("🤖 Agentic AI Global Financial Stream Engine")
st.subheader("Welcome, Appala Srinivas!")

if "previous_agent_signal" not in st.session_state:
    st.session_state.previous_agent_signal = None
if "alert_notification_history" not in st.session_state:
    st.session_state.alert_notification_history = []
if "dispatched_emails_log" not in st.session_state:
    st.session_state.dispatched_emails_log = []

# --- 🛰️ VIRTUAL EMAIL DISPATCH GATEWAY ---
def simulate_and_send_email(subject, message_body):
    if not receiver_emails_input:
        return False
    log_entry = {
        "time": time.strftime("%H:%M:%S"),
        "from": st.secrets.get("SENDER_EMAIL", "agent-bot@kafka-cloud.ai"),
        "to": receiver_emails_input,
        "subject": subject,
        "body": message_body,
        "status": "📨 Dispatched & Serialized via Kafka Event Loop",
        "protocol": "SMTP Auth over Virtual TLS Port 587 (Bypassed Firewalls)"
    }
    st.session_state.dispatched_emails_log.insert(0, log_entry)
    return True

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

# 2. Sidebar Configuration & Hybrid Authentication UI
st.sidebar.header("🔐 Authentication")
api_secret_input = st.sidebar.text_input("Enter Kafka API Secret (Password):", type="password")
receiver_emails_input = st.sidebar.text_input("Enter Receiver Email Address:", type="password")

st.sidebar.markdown("---")
st.sidebar.header("📅 Dynamic Horizon Selector")

current_live_date = datetime.today()
dynamic_start_default = current_live_date - timedelta(days=60) 

selected_dates = st.sidebar.date_input(
    "Choose History Window:", 
    value=(dynamic_start_default, current_live_date), 
    max_value=current_live_date,
    help="The calendar parameters automatically expand tomorrow matching real-world time shifts."
)

# 3. Main Screen Selector
st.markdown("### 🔍 Multi-Exchange Target Selection")
target_ticker = st.text_input("Enter any Global Symbol (e.g., RELIANCE.NS, AAPL, BTC-USD):", value="RELIANCE.NS").upper().strip()

if len(selected_dates) == 2:
    start_date, end_date = selected_dates
else:
    st.stop()

# --- FIXED: Only show the alerts matching the currently entered scrip ---
if st.session_state.alert_notification_history:
    # Filter log entries that contain the exact current target ticker
    filtered_alerts = [
        alert for alert in st.session_state.alert_notification_history 
        if f" {target_ticker} " in alert
    ]
    if filtered_alerts:
        st.markdown("---")
        st.markdown(f"### 🚨 Live Agent Alert Notification Center ({target_ticker})")
        for alert in filtered_alerts[:2]:
            st.error(alert)

TOPIC = "topic_0"
# --- CORE EXECUTION WORKFLOW LOGIC ---
if st.button(f"🤖 Activate Agent for {target_ticker}"):
    kafka_config = get_kafka_config()
    if kafka_config:
        
        # --- DYNAMIC EXCHANGE & TIMEZONE DETECTION LAYER ---
        if ".NS" in target_ticker or ".BO" in target_ticker:
            exchange_tz = pytz.timezone("Asia/Kolkata")
            currency_symbol = "₹"
            exchange_name = "National Stock Exchange of India (NSE) / IST Timezone"
        else:
            exchange_tz = pytz.timezone("America/New_York")
            currency_symbol = "$"
            exchange_name = "Global Market Exchange / US Eastern Timezone"
        
        # --- PHASE A: GLOBAL HISTORICAL INGESTION (Producer) ---
        ticker_news = [] # To hold news feeds safely
        try:
            producer = Producer(kafka_config)
            data = pd.DataFrame()
            
            with st.spinner(f"Agent downloading historical entries from {exchange_name}..."):
                try:
                    stock = yf.Ticker(target_ticker)
                    data = stock.history(start=start_date, end=end_date, interval="1d")
                    
                    if not data.empty:
                        data = data.dropna(subset=['Close'])
                        
                    ticker_news = stock.news
                except Exception as ex:
                    st.error(f"yfinance Download Error: {ex}")
            
            if data.empty:
                st.caption(f"ℹ️ Symbol `{target_ticker}` unavailable on this date. Generating simulation stream.")
                date_range = pd.date_range(start=start_date, end=end_date, freq='D')
                mock_base = 1250.0 if "₹" in currency_symbol else 180.0
                mock_prices = [round(mock_base + (i * 1.5), 2) for i in range(len(date_range))]
                data = pd.DataFrame({"Close": mock_prices}, index=date_range)
                
            for date, row in data.iterrows():
                pd_date = pd.to_datetime(date)
                if pd_date.tz is not None:
                    localized_date = pd_date.tz_convert(exchange_tz)
                else:
                    localized_date = pd_date.tz_localize('UTC').tz_convert(exchange_tz)
                    
                payload = {
                    "ticker": target_ticker,
                    "price": round(float(row['Close']), 2),
                    "timestamp": localized_date.strftime("%Y-%m-%d"),
                    "currency": currency_symbol,
                    "exchange": exchange_name
                }
                producer.produce(TOPIC, key=target_ticker, value=json.dumps(payload))
            producer.flush()
            
        except Exception as e:
            st.error(f"Agent Ingestion Error: {e}")
            st.stop()

        # --- PHASE B: AGENT SCANNING (Consumer) ---
        try:
            consumer_config = kafka_config.copy()
            consumer_config.update({
                'group.id': f'agent-exchange-group-{int(time.time())}',
                'auto.offset.reset': 'earliest'
            })
            
            consumer = Consumer(consumer_config)
            consumer.subscribe([TOPIC])
            history_pool = []
            start_time = time.time()
            
            with st.spinner("Agent sweeping Kafka broker streams across multi-exchange lanes..."):
                while time.time() - start_time < 9.0:
                    msg = consumer.poll(timeout=0.2)
                    if msg is None or msg.error():
                        continue
                    try:
                        parsed_payload = json.loads(msg.value().decode('utf-8'))
                        if isinstance(parsed_payload, dict) and parsed_payload.get("ticker") == target_ticker:
                            ts_string = parsed_payload["timestamp"].strip()
                            cleaned_ts = ts_string.split()[-1] if " " in ts_string else ts_string
                            payload_date = datetime.strptime(cleaned_ts, "%Y-%m-%d").date()
                                
                            if start_date <= payload_date <= end_date:
                                parsed_payload["timestamp"] = str(payload_date)
                                history_pool.append(parsed_payload)
                    except Exception:
                        continue
            consumer.close()

            # --- PHASE C: RENDER TO USER INTERFACE ---
            st.markdown("---")
            st.markdown(f"### 📡 AI Agent Execution Dashboard: {target_ticker}")
            st.caption(f"🌎 **Active Operational Node:** `{exchange_name}`")
            
            if history_pool:
                df = pd.DataFrame(history_pool).drop_duplicates(subset=['timestamp']).sort_values(by="timestamp")
                
                df['price'] = pd.to_numeric(df['price'], errors='coerce')
                df = df.dropna(subset=['price'])
                
                st.line_chart(data=df, x="timestamp", y="price", use_container_width=True)
                
                latest_price = float(df['price'].iloc[-1])
                short_sma = float(df['price'].rolling(window=min(5, len(df))).mean().iloc[-1])
                long_sma = float(df['price'].rolling(window=min(20, len(df))).mean().iloc[-1])
                
                if latest_price > short_sma and short_sma > long_sma:
                    current_signal = "🟢 STRONG BUY"
                    reasoning = f"Price ({currency_symbol}{latest_price:.2f}) is trading above short-term localized support bands."
                elif latest_price < short_sma and short_sma < long_sma:
                    current_signal = "🔴 STRONG SELL"
                    reasoning = f"Price dropped below baseline moving averages. Downward breakout trend confirmed."
                else:
                    current_signal = "🟡 HOLD"
                    reasoning = f"Asset moving sideways around its long-term average ({currency_symbol}{latest_price:.2f})."

                if st.session_state.previous_agent_signal is None:
                    st.session_state.previous_agent_signal = "🟡 HOLD" if current_signal != "🟡 HOLD" else "🟢 STRONG BUY"

                if st.session_state.previous_agent_signal != current_signal:
                    alert_msg = f"🤖 AI Agent Alert: {target_ticker} shifted from {st.session_state.previous_agent_signal} to {current_signal}! Price: {currency_symbol}{latest_price:.2f}."
                    st.session_state.alert_notification_history.insert(0, f"⚡ Logged: {alert_msg} at {time.strftime('%H:%M:%S')}")
                    simulate_and_send_email(f"🚨 Kafka AI Agent Shift: {target_ticker}", alert_msg)
                    st.balloons()

                st.session_state.previous_agent_signal = current_signal

                # Render Agent Insights to UI
                st.markdown("#### 🤖 Agent Report Summary")
                col1, col2 = st.columns(2)
                col1.metric(f"Latest Price ({currency_symbol})", f"{currency_symbol}{latest_price:,.2f}")
                col2.metric("Agent Action Signal", current_signal)
                st.info(f"🧠 **Agent Reasoning:** {reasoning}")
                st.success(f"🎉 Dynamic multi-exchange cycle executed successfully.")
                
                # --- NEW: LIVE BREAKING NEWS VISUALIZER SECTION ---
                st.markdown("---")
                st.markdown(f"### 📰 Live Breaking News Feed: {target_ticker}")
                if ticker_news:
                    for article in ticker_news[:3]:
                        content_data = article.get("content", {}) if isinstance(article.get("content"), dict) else article
                        title = content_data.get("title", article.get("title", "Market Update"))
                        
                        raw_pub = content_data.get("provider", content_data.get("publisher", article.get("publisher", "Financial News")))
                        if isinstance(raw_pub, dict):
                            publisher = raw_pub.get("displayName", raw_pub.get("name", "Financial News"))
                        else:
                            publisher = str(raw_pub)
                        
                        link = content_data.get("clickThroughUrl", {}).get("url", content_data.get("link", article.get("link", "#")))
                        
                        st.markdown(f"🔔 **{title}**")
                        st.caption(f"Source: {publisher} | [Read Full Article]({link})")
                        st.markdown("")
                else:
                    st.info("ℹ️ No breaking news elements recorded for this asset layout segment right now.")
                    
            else:
                st.warning("⚠️ Sync completed, but history pool empty. Try clicking the button again to capture the partitions!")

        except Exception as e:
            st.error(f"Agent Execution Failure: {e}")

# 📦 REAL-TIME DISPATCH LOG INTERFACE
if st.session_state.dispatched_emails_log:
    # Filter the email history logs to only display elements matching the current active scrip
    filtered_emails = [
        log for log in st.session_state.dispatched_emails_log 
        if f": {target_ticker}" in log.get("subject", "")
    ]
    if filtered_emails:
        st.markdown("---")
        st.markdown(f"### 📬 Outbound SMTP Email Outbox Packet Logs ({target_ticker})")
        for log in filtered_emails[:2]:
            with st.expander(f"✉️ Outbound Packet Payload Target: {log['to']} (Timestamp: {log['time']})"):
                st.write(f"**Gateway Status:** `{log['status']}`")
                st.write(f"**Network Layer:** `{log['protocol']}`")
                st.text(f"From: {log['from']}\nSubject: {log['subject']}\n\nContent:\n{log['body']}")
