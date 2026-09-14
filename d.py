import streamlit as st
import time
from confluent_kafka import Consumer, KafkaError, Producer

# 1. Web Page Layout Setup
st.set_page_config(page_title="Kafka Streamlit Engine", layout="centered")
st.title("🚀 Kafka Live Stream Engine")
st.subheader("Welcome, Appala Srinivas!")

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
        # TWEAK 1: Prevent timeouts over cloud networks
        'socket.timeout.ms': 45000,
        'session.timeout.ms': 45000,
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
                    st.session_state.status_log.insert(0, f"✅ Sent successfully! Message: '{user_message}' (Offset: {msg.offset()} on Partition: {msg.partition()})")
                    
            producer.produce(TOPIC, value=user_message, callback=delivery_report)
            producer.flush()
        except Exception as e:
            st.session_state.status_log.insert(0, f"❌ Error initializing Producer: {e}")

# Display active delivery actions
if st.session_state.status_log:
    st.info("📨 **Producer Log Activity:**")
    for log in st.session_state.status_log[:3]:
        st.write(log)
    st.markdown("---")

# 4. Main Screen Panel: Reading Messages (Consumer)
st.header("📡 Live Stream Receiver")
st.write("Click the button below to sweep and download all messages sitting in your Confluent Cloud cluster.")

if st.button("Check for New Messages"):
    kafka_config = get_kafka_config()
    if kafka_config:
        consumer_config = kafka_config.copy()
        consumer_config.update({
            # TWEAK 2: Use a fresh, shorter group name to make broker coordination instant
            'group.id': f'st-sweep-{int(time.time() % 100000)}', 
            'auto.offset.reset': 'earliest',
            # TWEAK 3: Optimize for faster metadata retrieval across partitions
            'api.version.request': True
        })
        
        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([TOPIC])
            
            all_messages = []
            start_time = time.time()
            
            # Spend up to 5 seconds pulling records out of the stream
            with st.spinner("Connecting and sweeping all Kafka partitions..."):
                while time.time() - start_time < 5.0:
                    msg = consumer.poll(timeout=0.5)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            st.error(f"❌ Kafka Error: {msg.error()}")
                            break
                    
                    decoded_val = msg.value().decode('utf-8')
                    all_messages.append({
                        "text": decoded_val,
                        "partition": msg.partition(),
                        "offset": msg.offset()
                    })
            
            consumer.close()
            
            # 5. Render Results to UI
            if all_messages:
                st.success(f"🎉 Successfully fetched {len(all_messages)} messages from the cloud!")
                for index, item in enumerate(all_messages):
                    st.markdown(f"📦 **[{index+1}] Message:** `{item['text']}` — *(Partition: {item['partition']}, Offset: {item['offset']})*")
            else:
                st.warning("⚠️ Stream window completed with no records. If this persists, the cloud container network is taking longer to handshake. Try clicking again in a few seconds!")
                
        except Exception as e:
            st.error(f"Error initializing Consumer: {e}")
