import streamlit as st
from confluent_kafka import Producer, Consumer, KafkaError

# 1. Web Page Layout Setup
st.set_page_config(page_title="Kafka Streamlit Engine", layout="centered")
st.title("🚀 Kafka Live Stream Engine")
st.subheader("Welcome, Appala Srinivas!")

# 2. Secret Key Setup (Hybrid Mode)
# Server and Key come automatically from your Streamlit TOML secrets panel.
# The API Secret Password will be typed into the UI box by you.
st.sidebar.header("🔐 Authentication")
api_secret_input = st.sidebar.text_input(
    "Enter Kafka API Secret (Password):", 
    type="password", 
    help="Type or paste your Confluent API Secret here."
)

# Build configuration dictionary dynamically
def get_kafka_config():
    if not api_secret_input:
        st.sidebar.warning("⚠️ Please enter your API Secret Password to connect.")
        return None
        
    return {
        'bootstrap.servers': st.secrets["KAFKA_BOOTSTRAP_SERVER"],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': st.secrets["KAFKA_API_KEY"],
        'sasl.password': api_secret_input,  # Fed directly from your password input box
    }

TOPIC = "topic_0"

# 3. Sidebar Panel: Sending Messages (Producer)
st.sidebar.markdown("---")
st.sidebar.header("📥 Produce Message")
user_message = st.sidebar.text_input("Type a message to send:", "Hello from Streamlit Web!")

if st.sidebar.button("Send to Kafka Cloud"):
    kafka_config = get_kafka_config()
    if kafka_config:
        try:
            producer = Producer(kafka_config)
            
            def delivery_report(err, msg):
                if err is not None:
                    st.sidebar.error(f"❌ Failed: {err}")
                else:
                    st.sidebar.success(f"✅ Sent to topic '{msg.topic()}': Offset {msg.offset()}")
                    
            producer.produce(TOPIC, value=user_message, callback=delivery_report)
            producer.flush()
        except Exception as e:
            st.sidebar.error(f"Error initializing Producer: {e}")

# 4. Main Screen Panel: Reading Messages (Consumer)
st.header("📡 Live Stream Receiver")
st.write("Click the button below to fetch the latest message sitting in your Confluent Cloud cluster.")

if st.button("Check for New Messages"):
    kafka_config = get_kafka_config()
    if kafka_config:
        # Add consumer specific configurations to the base configuration
        consumer_config = kafka_config.copy()
        consumer_config.update({
            'group.id': 'streamlit-web-group',
            'auto.offset.reset': 'earliest'
        })
        
        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([TOPIC])
            
            # Poll Kafka for a message (wait up to 3 seconds)
            msg = consumer.poll(timeout=3.0)
            
            if msg is None:
                st.warning("⚠️ No new messages found in the cluster right now.")
            elif msg.error():
                st.error(f"❌ Kafka Error: {msg.error()}")
            else:
                st.info("🎉 Successfully fetched a message from the cloud!")
                st.success(f"📩 **Message Content:** {msg.value().decode('utf-8')}")
                
            consumer.close()
        except Exception as e:
            st.error(f"Error initializing Consumer: {e}")
