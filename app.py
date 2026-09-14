import streamlit as st
import yfinance as yf
import json
import time
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError

# 1. Web Page Layout Setup
st.set_page_config(page_title="Agentic AI Kafka Engine", layout="centered")
st.title("🤖 Agentic AI Financial Stream Engine")
st.subheader("Welcome, Appala Srinivas!")

if "previous_agent_signal" not in st.session_state:
    st.session_state.previous_agent_signal = None
if "alert_notification_history" not in st.session_state:
    st.session_state.alert_notification_history = []

# 2. Sidebar Configuration & Hybrid Authentication UI
st.sidebar.header("🔐 Authentication")
api_secret_input = st.sidebar.text_input(
    "Enter Kafka API Secret (Password):", 
    type="password"
)

# Target Receiver Email Input on screen, masked like a password field
receiver_emails_input = st.sidebar.text_input(
    "Enter Receiver Email Address:", 
    type="password",
    help="Type the target email address where the alert should be sent."
)

st.sidebar.markdown("---")
st.sidebar.header("📅 Select Date Range")
default_start = datetime.today() - timedelta(days=60) 
default_end = datetime.today()

selected_dates = st.sidebar.date_input("Choose History Horizon:", value=(default_start, default_end), max_value=datetime.today())

# --- 🛰️ AUTOMATED EMAIL UTILITY FUNCTION ---
def send_email_alert(subject, message_body):
    """Fires real secure emails using Python's SMTP library and Gmail App Tunnels"""
    if not receiver_emails_input:
        st.sidebar.error("❌ Email transmission aborted: No receiver email provided on screen.")
        return False
        
    try:
        msg = MIMEText(message_body)
        msg['Subject'] = subject
        msg['From'] = st.secrets["SENDER_EMAIL"]
        msg['To'] = receiver_emails_input  # Fed directly from your screen input box
        
        # Connect to Gmail SMTP Secure Server Port
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(st.secrets["SENDER_EMAIL"], st.secrets["EMAIL_APP_PASSWORD"])
            server.sendmail(st.secrets["SENDER_EMAIL"], [receiver_emails_input], msg.as_string())
        return True
    except Exception as e:
        st.sidebar.error(f"Email Gateway Error: {e}")
        return False

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

# 3. Main Screen Selector
st.markdown("### 🔍 Instruct AI Agent")
target_ticker = st.text_input("Assign a Ticker Symbol to the AI Agent:", value="AAPL").upper().strip()

if len(selected_dates) == 2:
    start_date, end_date = selected_dates
else:
    st.stop()

if st.session_state.alert_notification_history:
    st.markdown("---")
    st.markdown("### 🚨 Live Agent Alert Notification Center")
    for alert in st.session_state.alert_notification_history[:3]:
        st.error(alert)

TOPIC = "topic_0"

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
            consumer_config = kafka_config.copy()
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
                            payload_date = datetime.strptime(ts_string.split(), "%Y-%m-%d").date()
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
                st.line_chart(data=df, x="timestamp", y="price", use_container_width=True)
                
                latest_price = df['price'].iloc[-1]
                short_sma = df['price'].rolling(window=min(5, len(df))).mean().iloc[-1]
                long_sma = df['price'].rolling(window=min(20, len(df))).mean().iloc[-1]
                
                if latest_price > short_sma and short_sma > long_sma:
                    current_signal = "🟢 STRONG BUY"
                    reasoning = f"Price (${latest_price}) is trading above short-term average (${round(short_sma, 2)})."
                elif latest_price < short_sma and short_sma < long_sma:
                    current_signal = "🔴 STRONG SELL"
                    reasoning = f"Price has dropped below support lines. Structural downward distribution detected."
                else:
                    current_signal = "🟡 HOLD"
                    reasoning = f"Asset consolidating sideways near baseline average (${round(long_sma, 2)})."

                # 🔥 TRIGGER AUTOMATED EMAIL NOTIFICATION ON SIGNAL SHIFT
                if st.session_state.previous_agent_signal is not None and st.session_state.previous_agent_signal != current_signal:
                    alert_msg = f"🤖 AI Agent Alert: {target_ticker} shifted from {st.session_state.previous_agent_signal} to {current_signal}! Latest Price: ${latest_price}."
                    
                    st.session_state.alert_notification_history.insert(0, f"⚡ Logged: {alert_msg} at {time.strftime('%H:%M:%S')}")
                    
                    # Fire Email Gateway
                    if receiver_emails_input:
                        with st.spinner("Dispatching live email alert pipeline..."):
                            email_status = send_email_alert(f"🚨 Kafka AI Agent Shift: {target_ticker}", alert_msg)
                            if email_status:
                                st.toast(f"📧 Alert email routed cleanly to {receiver_emails_input}!", icon="📬")
                    else:
                        st.sidebar.warning("⚠️ Signal shifted, but email could not be sent because the input box was blank.")
                            
                    st.balloons()

                st.session_state.previous_agent_signal = current_signal

                # Render Agent Insights to UI
                st.markdown("#### 🤖 Agent Report Summary")
                col1, col2 = st.columns(2)
                col1.metric("Latest Streamed Price", f"${latest_price:,}")
                col2.metric("Agent Action Signal", current_signal)
                st.info(f"🧠 **Agent Reasoning:** {reasoning}")
                st.success(f"🎉 Agent cycle completed.")
            else:
                st.warning("⚠️ Connection completed, but no data records matching parameters were caught.")

        except Exception as e:
            st.error(f"Agent Execution Failure: {e}")
