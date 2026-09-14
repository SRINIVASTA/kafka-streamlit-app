import time
import json
import yfinance as yf
from confluent_kafka import Producer

# 1. Confluent Cloud Credentials (Replace with your actual keys)
# Note: For hackathons, running this locally can read from a secure terminal env.
KAFKA_CONFIG = {
    'bootstrap.servers': 'YOUR_BOOTSTRAP_SERVER_ADDRESS', 
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'YOUR_API_KEY',
    'sasl.password': 'YOUR_API_SECRET_PASSWORD', # Put your text secret here
}

TOPIC = "topic_0"
TICKERS = ["BTC-USD", "AAPL", "RELIANCE.NS"] # Tracking Crypto, US Stocks, and Indian Market

try:
    producer = Producer(KAFKA_CONFIG)
    print("🚀 Kafka Financial Data Engine Started...")
    
    while True:
        for ticker in TICKERS:
            # Fetch the latest live ticker price using yfinance
            stock = yf.Ticker(ticker)
            data = stock.history(period="1d", interval="1m")
            
            if not data.empty:
                latest_row = data.iloc[-1]
                current_price = round(latest_row['Close'], 2)
                
                # Construct JSON event payload
                payload = {
                    "ticker": ticker,
                    "price": current_price,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                }
                
                # Produce to Kafka (Kafka automatically balances across your 5 partitions)
                producer.produce(
                    TOPIC, 
                    key=ticker, 
                    value=json.dumps(payload),
                    callback=lambda err, msg: print(f"✅ Sent {ticker} to Partition {msg.partition()} at Offset {msg.offset()}") if err is None else print(f"❌ Error: {err}")
                )
        
        producer.flush()
        print("⏰ Sleeping for 10 seconds before next market sweep...")
        time.sleep(10) # Adjust interval as needed

except KeyboardInterrupt:
    print("\n👋 Stopping Data Engine...")
