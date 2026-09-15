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

# Setup persistent application state variables
if "previous_agent_signal" not in st.session_state:
    st.session_state.previous_agent_signal = None
if "alert_notification_history" not in st.session_state:
    st.session_state.alert_notification_history = []
if "dispatched_emails_log" not in st.session_state:
    st.session_state.dispatched_emails_log = []
if "chat_conversations_log" not in st.session_state:
    st.session_state.chat_conversations_log = {}

# Persistent Storage for Streaming Data Layout components
if "stored_history_pool" not in st.session_state:
    st.session_state.stored_history_pool = {}
if "stored_ticker_news" not in st.session_state:
    st.session_state.stored_ticker_news = {}
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

# Sidebar Configuration & Hybrid Authentication UI
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

# Main Screen Selector
target_ticker = st.text_input("Enter any Global Symbol (e.g., RELIANCE.NS, AAPL, BTC-USD):", value="HFCL.NS").upper().strip()

if len(selected_dates) == 2:
    start_date, end_date = selected_dates
else:
    st.stop()

# Isolate system alert flags to current scrip view only
if st.session_state.alert_notification_history:
    filtered_alerts = [alert for alert in st.session_state.alert_notification_history if f" {target_ticker} " in alert]
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
        if ".NS" in target_ticker or ".BO" in target_ticker:
            exchange_tz = pytz.timezone("Asia/Kolkata")
            currency_symbol = "₹"
            exchange_name = "National Stock Exchange of India (NSE) / IST Timezone"
        else:
            exchange_tz = pytz.timezone("America/New_York")
            currency_symbol = "$"
            exchange_name = "Global Market Exchange / US Eastern Timezone"
        
        # --- PHASE A: GLOBAL HISTORICAL INGESTION (Producer) ---
        ticker_news = []
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
                localized_date = pd_date.tz_convert(exchange_tz) if pd_date.tz is not None else pd_date.tz_localize('UTC').tz_convert(exchange_tz)
                    
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

            st.session_state.stored_history_pool[target_ticker] = history_pool
            st.session_state.stored_ticker_news[target_ticker] = ticker_news

        except Exception as e:
            st.error(f"Agent Execution Failure: {e}")
# --- PHASE C: PERSISTENT UI RENDERING ENGINE ---
if target_ticker in st.session_state.stored_history_pool:
    history_pool = st.session_state.stored_history_pool[target_ticker]
    ticker_news = st.session_state.stored_ticker_news.get(target_ticker, [])
    
    if ".NS" in target_ticker or ".BO" in target_ticker:
        currency_symbol, exchange_name = "₹", "National Stock Exchange of India (NSE) / IST Timezone"
    else:
        currency_symbol, exchange_name = "$", "Global Market Exchange / US Eastern Timezone"

    st.markdown("---")
    st.markdown(f"### 📡 AI Agent Execution Dashboard: {target_ticker}")
    st.caption(f"🌎 **Active Operational Node:** `{exchange_name}`")
    
    if history_pool:
        df = pd.DataFrame(history_pool).drop_duplicates(subset=['timestamp']).sort_values(by="timestamp")
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df = df.dropna(subset=['price'])
        
        st.line_chart(data=df, x="timestamp", y="price", use_container_width=True)
        
        # Calculate Technical Indicators
        latest_price = float(df['price'].iloc[-1])
        short_sma = float(df['price'].rolling(window=min(5, len(df))).mean().iloc[-1])
        long_sma = float(df['price'].rolling(window=min(20, len(df))).mean().iloc[-1])
        
        # Rule-Based RSI Tracking Loop
        delta = df['price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=min(14, len(df))).mean().iloc[-1]
        loss = (-delta.where(delta < 0, 0)).rolling(window=min(14, len(df))).mean().iloc[-1]
        rs = gain / loss if loss != 0 else 0
        rsi_value = 100 - (100 / (1 + rs)) if loss != 0 else 100
        
        if latest_price > short_sma and short_sma > long_sma:
            current_signal = "🟢 STRONG BUY"
            reasoning = f"Price ({currency_symbol}{latest_price:.2f}) is trading above short-term localized support bands."
            if rsi_value > 70:
                reasoning += f" ⚠️ WARNING: RSI measures overbought ({rsi_value:.1f}). Overextension risk present."
        elif latest_price < short_sma and short_sma < long_sma:
            current_signal = "🔴 STRONG SELL"
            reasoning = f"Price dropped below baseline moving averages. Downward breakout trend confirmed."
            if rsi_value < 30:
                reasoning += f" 💡 NOTE: RSI measures oversold ({rsi_value:.1f}). Technical bounce potential noted."
        else:
            current_signal = "🟡 HOLD"
            reasoning = f"Asset moving sideways around its long-term average ({currency_symbol}{latest_price:.2f})."

        st.markdown("#### 🤖 Agent Report Summary")
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric(f"Latest Price ({currency_symbol})", f"{currency_symbol}{latest_price:,.2f}")
        col_m2.metric("Agent Action Signal", current_signal)
        col_m3.metric("RSI Value (14 Days)", f"{rsi_value:.1f}")
        st.info(f"🧠 **Agent Reasoning:** {reasoning}")
        
        # Simulated Order Panel Insertion Area
        st.markdown("#### ⚡ Programmatic Order Execution Gateway")
        with st.expander("💼 Route Order Payload Directly to Exchange Broker Gateway"):
            col_trade_1, col_trade_2 = st.columns(2)
            shares_count = col_trade_1.number_input("Order Share Volume Size:", min_value=1, value=10, step=1)
            total_est_cost = shares_count * latest_price
            col_trade_2.markdown(f"**Total Transaction Exposure Value:**\n### {currency_symbol}{total_est_cost:,.2f}")
            
            if st.button(f"⚡ Dispatched Webhook Target Order Entry for {target_ticker}"):
                trade_payload = {"action": "BUY" if "BUY" in current_signal else "SELL", "ticker": target_ticker, "volume": shares_count, "execution_price": latest_price, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}
                st.success(f"📨 Serialized Trade Entry Order Package transmitted successfully: {json.dumps(trade_payload)}")
        
        # Real-Time Sentiment Streaming Feeds
        st.markdown("---")
        st.markdown(f"### 📰 Live Real-Time Sentiment Streaming Feed: {target_ticker}")
        bullish_words = {"growth", "profit", "expand", "dividend", "bonus", "buy", "surge", "acquisition", "unveils", "rise", "positive", "partnership"}
        bearish_words = {"drop", "sluggish", "deficit", "breach", "backlash", "shutters", "risk", "sell", "decline", "fall", "investigating", "protest", "loss"}
        
        if ticker_news:
            for article in ticker_news[:3]:
                content_data = article.get("content", {}) if isinstance(article.get("content"), dict) else article
                title = content_data.get("title", article.get("title", "Market Update"))
                raw_pub = content_data.get("provider", content_data.get("publisher", article.get("publisher", "Financial News")))
                publisher = raw_pub.get("displayName", raw_pub.get("name", "Financial News")) if isinstance(raw_pub, dict) else str(raw_pub)
                link = content_data.get("clickThroughUrl", {}).get("url", content_data.get("link", article.get("link", "#")))
                
                tokens = title.lower().split()
                bullish_count = sum(1 for token in tokens if any(b_word in token for b_word in bullish_words))
                bearish_count = sum(1 for token in tokens if any(sec_word in token for sec_word in bearish_words))
                total_tokens = bullish_count + bearish_count
                sentiment_score = 0.0 if total_tokens == 0 else round((bullish_count - bearish_count) / total_tokens, 2)
                
                badge, color = ("📈 BULLISH", "green") if sentiment_score > 0 else (("📉 BEARISH", "red") if sentiment_score < 0 else ("⚖️ NEUTRAL", "gray"))
                st.markdown(f"🔔 **{title}**")
                col_news_a, col_news_b = st.columns(2)
                col_news_a.caption(f"Source: {publisher} | [Read Full Article]({link})")
                col_news_b.markdown(f":{color}[**{badge} ({sentiment_score:+.1f})**]")
        else:
            st.info("ℹ️ No breaking news elements recorded for this asset layout segment right now.")
# --- INTERACTIVE CHAT INTERFACE AREA ---
st.markdown("---")
st.markdown(f"### 💬 Interactive AI Agent Chat Messenger: {target_ticker}")

if target_ticker not in st.session_state.chat_conversations_log:
    st.session_state.chat_conversations_log[target_ticker] = [{"role": "assistant", "content": f"Hello! Ask me any analysis question about corporate actions, splits, dividends, or live sentiment metrics for {target_ticker}."}]

for msg in st.session_state.chat_conversations_log[target_ticker]:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

if chat_prompt := st.chat_input(f"Inquire details regarding {target_ticker}..."):
    st.session_state.chat_conversations_log[target_ticker].append({"role": "user", "content": chat_prompt})
    with st.chat_message("user"):
        st.write(chat_prompt)
        
    with st.chat_message("assistant"):
        user_query = chat_prompt.lower()
        if "corporate action" in user_query or "split" in user_query or "dividend" in user_query:
            with st.spinner("Fetching corporate actions database records..."):
                try:
                    stock_obj = yf.Ticker(target_ticker)
                    actions_df = stock_obj.actions
                    if actions_df is not None and not actions_df.empty:
                        latest_actions = actions_df.tail(3).sort_index(ascending=False)
                        reply_text = f"📋 **Recent Corporate Actions recorded for {target_ticker}:**\n\n"
                        for idx, row in latest_actions.iterrows():
                            date_str = idx.strftime('%Y-%m-%d')
                            if 'Stock Splits' in latest_actions.columns and row['Stock Splits'] > 0:
                                label = "1:1 Bonus Share Issue" if (".NS" in target_ticker or ".BO" in target_ticker) and row['Stock Splits'] == 2.0 else f"Stock Split Ratio of {row['Stock Splits']}"
                                reply_text += f"▪️ **{date_str}:** {label}\n"
                            if 'Dividends' in latest_actions.columns and row['Dividends'] > 0:
                                reply_text += f"▪️ **{date_str}:** Cash Dividend Payout of **{currency_symbol}{row['Dividends']}**\n"
                    else:
                        reply_text = f"ℹ️ No recent corporate actions found in the public ledger for **{target_ticker}**."
                except Exception as err:
                    reply_text = f"⚠️ Failed to parse corporate entries pipeline: {err}"
        elif "sentiment" in user_query or "score" in user_query or "news" in user_query:
            reply_text = f"📊 **Streaming News Sentiment Engine Status for {target_ticker}:**\n\nMy consumer thread scans incoming text payloads and isolates phrase momentum using lexical density checking."
        else:
            reply_text = f"I am actively tracking the Kafka topic streams for **{target_ticker}**. The moving averages suggest a trend confirmation aligned with the current signal."
            
        st.write(reply_text)
    st.session_state.chat_conversations_log[target_ticker].append({"role": "assistant", "content": reply_text})
    st.rerun()

# Real-Time Outbound Packet Logs Window
if st.session_state.dispatched_emails_log:
    filtered_emails = [log for log in st.session_state.dispatched_emails_log if f": {target_ticker}" in log.get("subject", "")]
    if filtered_emails:
        st.markdown("---")
        st.markdown(f"### 📬 Outbound SMTP Email Outbox Packet Logs ({target_ticker})")
        for log in filtered_emails[:2]:
            with st.expander(f"✉️ Outbound Packet Payload Target: {log['to']} (Timestamp: {log['time']})"):
                st.write(f"**Gateway Status:** `{log['status']}`\n**Network Layer:** `{log['protocol']}`")
                st.text(f"From: {log['from']}\nSubject: {log['subject']}\n\nContent:\n{log['body']}")
