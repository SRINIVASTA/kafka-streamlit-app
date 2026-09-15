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
if "chat_conversations_log" not in st.session_state:
    st.session_state.chat_conversations_log = {}

# Persistent Storage for Streaming Data Layout components
if "stored_history_pool" not in st.session_state:
    st.session_state.stored_history_pool = {}
if "stored_ticker_news" not in st.session_state:
    st.session_state.stored_ticker_news = {}

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
st.markdown("### 🔍 Multi-Exchange Target Selection")
target_ticker = st.text_input("Enter any Global Symbol (e.g., RELIANCE.NS, AAPL, BTC-USD):", value="HFCL.NS").upper().strip()

if len(selected_dates) == 2:
    start_date, end_date = selected_dates
else:
    st.stop()

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
                data = pd.DataFrame({"Close": mock_prices, "High": [x*1.02 for x in mock_prices], "Low": [x*0.98 for x in mock_prices]}, index=date_range)
                
            for date, row in data.iterrows():
                pd_date = pd.to_datetime(date)
                localized_date = pd_date.tz_convert(exchange_tz) if pd_date.tz is not None else pd_date.tz_localize('UTC').tz_convert(exchange_tz)
                    
                payload = {
                    "ticker": target_ticker,
                    "price": round(float(row['Close']), 2),
                    "high": round(float(row['High']), 2) if 'High' in row and not pd.isna(row['High']) else round(float(row['Close']) * 1.02, 2),
                    "low": round(float(row['Low']), 2) if 'Low' in row and not pd.isna(row['Low']) else round(float(row['Close']) * 0.98, 2),
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

            # Store the consumed partitions safely to state parameters
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
        
        # FIXED: Explicitly coerce all data streams to numeric values to prevent ₹nan errors
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['high'] = pd.to_numeric(df.get('high', df['price'] * 1.02), errors='coerce')
        df['low'] = pd.to_numeric(df.get('low', df['price'] * 0.98), errors='coerce')
        df = df.dropna(subset=['price', 'high', 'low'])
        
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
        
        # Floor Trader Pivot Point Levels
        last_high = float(df['high'].iloc[-1])
        last_low = float(df['low'].iloc[-1])
        pivot_point = (last_high + last_low + latest_price) / 3
        r1_level = (2 * pivot_point) - last_low
        s1_level = (2 * pivot_point) - last_high
        r2_level = pivot_point + (last_high - last_low)
        s2_level = pivot_point - (last_high - last_low)
        
        if latest_price > short_sma and short_sma > long_sma:
            current_signal, signal_type = "🟢 STRONG BUY", "BUY"
            reasoning = f"Price ({currency_symbol}{latest_price:.2f}) is trading above short-term localized support bands."
        elif latest_price < short_sma and short_sma < long_sma:
            current_signal, signal_type = "🔴 STRONG SELL", "SELL"
            reasoning = f"Price dropped below baseline moving averages. Downward breakout trend confirmed."
        else:
            current_signal, signal_type = "🟡 HOLD", "HOLD"
            reasoning = f"Asset moving sideways around its long-term average ({currency_symbol}{latest_price:.2f})."

        # Live News Sentiment Stream variables
        bullish_words = {"growth", "profit", "expand", "dividend", "bonus", "buy", "surge", "acquisition", "unveils", "rise", "positive", "partnership"}
        bearish_words = {"drop", "sluggish", "deficit", "breach", "backlash", "shutters", "risk", "sell", "decline", "fall", "investigating", "protest", "loss"}
        
        net_news_score = 0.0
        news_count = 0
        news_rendered_list = []
        
        if ticker_news:
            for article in ticker_news[:3]:
                content_data = article.get("content", {}) if isinstance(article.get("content"), dict) else article
                title = content_data.get("title", article.get("title", "Market Update"))
                raw_pub = content_data.get("provider", content_data.get("publisher", article.get("publisher", "Financial News")))
                publisher = raw_pub.get("displayName", raw_pub.get("name", "Financial News")) if isinstance(raw_pub, dict) else str(raw_pub)
                link = content_data.get("clickThroughUrl", {}).get("url", content_data.get("link", article.get("link", "#")))
                
                tokens = title.lower().split()
                b_c = sum(1 for t in tokens if any(bw in t for bw in bullish_words))
                br_c = sum(1 for t in tokens if any(brw in t for brw in bearish_words))
                total_t = b_c + br_c
                score = 0.0 if total_t == 0 else round((b_c - br_c) / total_t, 2)
                
                net_news_score += score
                news_count += 1
                
                badge, color = ("📈 BULLISH", "green") if score > 0 else (("📉 BEARISH", "red") if score < 0 else ("⚖️ NEUTRAL", "gray"))
                news_rendered_list.append((title, publisher, link, badge, color, score))
        
        avg_sentiment = round(net_news_score / news_count, 2) if news_count > 0 else 0.0
        
        # Signal Divergence Guardrails
        divergence_alert = None
        if signal_type == "BUY" and avg_sentiment < -0.2:
            divergence_alert = "⚠️ **AGENT DIVERGENCE WARNING:** Price chart signals a BUY, but media streaming sentiment is heavily BEARISH. Watch out for traps!"
        elif signal_type == "SELL" and avg_sentiment > 0.2:
            divergence_alert = "💡 **AGENT ACCUMULATION ALERT:** Chart signals a SELL, but media streaming sentiment is highly BULLISH. Reversal signature suspected."

        st.markdown("#### 🤖 Agent Report Summary")
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric(f"Latest Price ({currency_symbol})", f"{currency_symbol}{latest_price:,.2f}")
        col_m2.metric("Agent Action Signal", current_signal)
        col_m3.metric("RSI Value (14 Days)", f"{rsi_value:.1f}")
        st.info(f"🧠 **Agent Reasoning:** {reasoning}")
        
        if divergence_alert:
            st.warning(divergence_alert)

        # Pivot Point Dashboard Grid
        st.markdown("#### 📊 Quantitative Volatility Floor Grid")
        col_p1, col_p2, col_p3, col_p4 = st.columns(4)
        col_p1.metric("Resistance 2 (R2)", f"{currency_symbol}{r2_level:.2f}")
        col_p2.metric("Resistance 1 (R1)", f"{currency_symbol}{r1_level:.2f}")
        col_p3.metric("Support 1 (S1)", f"{currency_symbol}{s1_level:.2f}")
        col_p4.metric("Support 2 (S2)", f"{currency_symbol}{s2_level:.2f}")
        
        st.markdown("---")
        st.markdown(f"### 📰 Live Real-Time Sentiment Streaming Feed: {target_ticker} (Avg Mood: {avg_sentiment:+.2f})")
        if news_rendered_list:
            for item in news_rendered_list:
                st.markdown(f"🔔 **{item[0]}**")
                col_n_a, col_n_b = st.columns(2)
                col_n_a.caption(f"Source: {item[1]} | [Read Full Article]({item[2]})")
                col_n_b.markdown(f":{item[4]}[**{item[3]} ({item[5]:+.1f})**]")
        else:
            st.info("ℹ️ No breaking news elements recorded for this asset layout segment right now.")
# --- INTERACTIVE CHAT INTERFACE AREA ---
st.markdown("---")
st.markdown(f"### 💬 Interactive AI Agent Chat Messenger: {target_ticker}")

if target_ticker not in st.session_state.chat_conversations_log:
    st.session_state.chat_conversations_log[target_ticker] = [{"role": "assistant", "content": f"Hello! Ask me any analysis question about corporate actions, splits, dividends, or live pivot grid statistics for {target_ticker}."}]

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
        elif "pivot" in user_query or "resistance" in user_query or "support" in user_query:
            reply_text = f"📊 **Pivot Level Mathematical Explanation for {target_ticker}:**\n\nMy engine runs the standard Floor Trader Volatility Formula to calculate floor lines. Resistance layers (R1/R2) represent high-volume target ceilings where sellers historically supply liquidity, while Support levels (S1/S2) reflect price target floors where buyers frequently step in to defend momentum."
        else:
            reply_text = f"I am actively tracking the Kafka topic streams for **{target_ticker}**. The moving averages suggest a trend confirmation aligned with the current signal."
            
        st.write(reply_text)
    st.session_state.chat_conversations_log[target_ticker].append({"role": "assistant", "content": reply_text})
    st.rerun()
