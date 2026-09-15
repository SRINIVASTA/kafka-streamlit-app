import os
import sys
import json
import yfinance as yf
from confluent_kafka import Consumer, Producer
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

# Load API key environment credentials locally or via server systems
# export GOOGLE_API_KEY="AIzaSy..."
# Or if running locally, it can fallback to reading your .streamlit/secrets.toml
try:
    with open(".streamlit/secrets.toml", "r") as f:
        for line in f:
            if "GOOGLE_API_KEY" in line:
                os.environ["GOOGLE_API_KEY"] = line.split("=")[1].replace('"', '').strip()
except Exception:
    pass

client = genai.Client()
MODEL_ID = 'gemini-2.5-flash'

# =====================================================================
# SECURE WORKER KAFKA REGISTRATION (Reads layout passwords)
# =====================================================================
def get_worker_kafka_config():
    # Attempt to read secure pipeline configurations out of local runtime maps
    bootstrap_server = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
    api_key = os.environ.get("KAFKA_API_KEY")
    api_secret = os.environ.get("KAFKA_API_SECRET") # Pass your cluster password here

    # Try fallback parsing out of local files if env context is empty
    if not bootstrap_server:
        try:
            with open(".streamlit/secrets.toml", "r") as f:
                content = f.read()
                # Simple parsing fallback for dev verification
                for line in content.splitlines():
                    if "KAFKA_BOOTSTRAP_SERVER" in line: bootstrap_server = line.split("=")[1].replace('"', '').strip()
                    if "KAFKA_API_KEY" in line: api_key = line.split("=")[1].replace('"', '').strip()
        except Exception:
            print("❌ Failure configuring pipeline connection keys.")
            sys.exit(1)

    # In production workers, your password should be passed securely into env parameters
    if not api_secret:
        api_secret = input("🔑 Enter Kafka API Secret (Password) for Worker Process: ").strip()

    return {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': api_key,
        'sasl.password': api_secret,
        'socket.timeout.ms': 45000,
        'session.timeout.ms': 45000,
    }

KAFKA_CONFIG = get_worker_kafka_config()

# =====================================================================
# SCHEMAS & UTILITY TOOLS
# =====================================================================
class AgentDashboardSignal(BaseModel):
    ticker: str = Field(description="The structural uppercase stock ticker token identified from context.")
    time_frame: str = Field(description="The duration period requested by user. e.g. '1y'.")

def get_stock_market_data(ticker: str, period: str = "1mo") -> dict:
    """Tool function pulling live metrics, dividend records, and news out of yfinance."""
    try:
        stock = yf.Ticker(ticker.upper())
        info = stock.info
        current_price = info.get('regularMarketPrice') or info.get('currentPrice') or "N/A"
        dividend_yield = info.get('dividendYield', 0)
        div_percentage = f"{dividend_yield * 100:.2f}%" if dividend_yield else "0.00%"
        
        raw_news = stock.news[:2] if stock.news else []
        news_list = [n.get("title") for n in raw_news]
        
        return {
            "ticker": ticker.upper(),
            "current_price": f"{current_price} {info.get('currency', 'USD')}",
            "dividend_yield": div_percentage,
            "latest_market_news": news_list
        }
    except Exception as e:
        return {"error": str(e)}

# Initialize conversational configuration layer
chat_config = types.GenerateContentConfig(
    system_instruction="You are a market analyst terminal. Use get_stock_market_data for stock calculations.",
    tools=[get_stock_market_data],
    temperature=0.2
)
chat = client.chats.create(model=MODEL_ID, config=chat_config)

# =====================================================================
# BACKGROUND PIPELINE RUNTIME ENGINE
# =====================================================================
def run_event_processor():
    consumer = Consumer({
        **KAFKA_CONFIG,
        'group.id': 'gemini-worker-group',
        'auto.offset.reset': 'latest'
    })
    producer = Producer(KAFKA_CONFIG)
    consumer.subscribe(['stock-requests'])
    
    print("\n⚙️ Gemini Kafka Worker Online. Streaming calculations active...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue
                
            # 1. Capture incoming message events out of the broker stream
            event_payload = json.loads(msg.value().decode('utf-8'))
            user_msg = event_payload["user_message"]
            current_ticker = event_payload["current_ticker"]
            current_tf = event_payload["current_tf"]
            
            # Inject active workspace boundaries into the context structure
            contextualised_message = (
                f"User request: '{user_msg}'. Context: Active view is '{current_ticker}' "
                f"with timeframe '{current_tf}'. Apply context if the user request is generic."
            )
            
            # 2. Fire conversational execution turn allowing unrestricted tool handling
            response = chat.send_message(contextualised_message)
            agent_reply = response.text
            
            # 3. Running secondary visual extraction logic layer safely
            extraction_prompt = f"""
            Analyze the text context and extract the target stock ticker symbol and requested timeframe period.
            Text Window: "{user_msg} {agent_reply}"
            Fallback variables if unstated: Ticker={current_ticker}, Timeframe={current_tf}.
            """
            
            extract_response = client.models.generate_content(
                model=MODEL_ID,
                contents=extraction_prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=AgentDashboardSignal,
                    temperature=0.1
                )
            )
            
            data_signal = json.loads(extract_response.text)
            
            # 4. Pack resulting payload block and write it back onto the output pipeline channel
            result_payload = {
                "agent_reply": agent_reply,
                "updated_ticker": data_signal.get("ticker", current_ticker).upper(),
                "updated_tf": data_signal.get("time_frame", current_tf)
            }
            
            producer.produce('stock-results', value=json.dumps(result_payload).encode('utf-8'))
            producer.flush()
            print(f"🚀 Successfully processed streaming loop metrics for target: {current_ticker}")
            
    except KeyboardInterrupt:
        print("\n👋 Shutting down background worker loop gracefully.")
    finally:
        consumer.close()

if __name__ == '__main__':
    run_event_processor()
