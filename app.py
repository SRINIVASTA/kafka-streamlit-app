import streamlit as st
import time
from confluent_kafka import Producer, Consumer, KafkaError

# 1. Web Page Layout Setup
st.set_page_config(page_title="Kafka Streamlit Engine", layout="centered")
st.title("🚀 Kafka Live Stream Engine")
st.subheader("Welcome, Appala Srinivas!")

# Initialize session memory state for messages
if "status_log" not in st.session_state:
    st.session_state.status_log = []

# 2. Secret Key Setup (Hybrid Mode)
st.sidebar.header("🔐 Authentication")
api_secret_input = st.sidebar.text_input(
    "Enter Kafka API Secret (Password):", 
    type="password", 
    help="Type or paste your Confluent API Secret here."
)

def get_kafka_config():
    if not api_secret_input:
        st.sidebar.warning("⚠️ Please enter your API Secret Password to connect.")
        return None
        
    return {
        'bootstrap.servers': st.secrets["KAFKA_BOOTSTRAP_SERVER"],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': st.secrets["KAFKA_API_KEY"],
        'sasl.password': api_secret_input,
    }

TOPIC = "topic_0"

# 3. Sidebar Panel: Sending Messages (Producer)
st.sidebar.markdown("---")
st.sidebar.header("📥 Produce Message")
user_message = st.sidebar.text_input("Type a message to send:", "Hackathon Test 1!")

if st.sidebar.button("Send to Kafka Cloud"):
    kafka_config = get_kafka_config()
    if kafka_config:
        try:
            producer = Producer(kafka_config)
            
            def delivery_report(err, msg):
                if err is not None:
                    st.session_state.status_log.insert(0, f"❌ Failed to send: {err}")
                else:
                    st.session_state.status_log.insert(0, f"✅ Sent successfully! Message: '{user_message}' (Offset: {msg.offset()})")
                    
            producer.produce(TOPIC, value=user_message, callback=delivery_report)
            producer.flush()
        except Exception as e:
            st.session_state.status_log.insert(0, f"❌ Error initializing Producer: {e}")

# Display active delivery actions
if st.session_state.status_log:
    st.info("📨 **Producer Log Activity:**")
    for log in st.session_state.status_log[:3]: # Show last 3 events
        st.write(log)
    st.markdown("---")

# 4. Main Screen Panel: Reading Messages (Consumer)
st.header("📡 Live Stream Receiver")
st.write("Click the button below to fetch messages from your Confluent Cloud cluster.")

if st.button("Check for New Messages"):
    kafka_config = get_kafka_config()
    if kafka_config:
        consumer_config = kafka_config.copy()
        consumer_config.update({
            'group.id': f'streamlit-group-{int(time.time())}',
            'auto.offset.reset': 'earliest'
        })
        
        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([TOPIC])
            
            # Poll Kafka for a message (wait up to 4 seconds)
            msg = consumer.poll(timeout=4.0)
            
            if msg is None:
                st.warning("⚠️ No messages found in the stream right now.")
            elif msg.error():
                st.error(f"❌ Kafka Error: {msg.error()}")
            else:
                st.success("🎉 Successfully fetched a message from the cloud!")
                st.info(f"📩 **Message Content:** {msg.value().decode('utf-8')}")
                
            consumer.close()
        except Exception as e:
            st.error(f"Error initializing Consumer: {e}")
