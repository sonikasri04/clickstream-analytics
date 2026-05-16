import json, time, random, uuid
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

PAGES = ["/home", "/products", "/products/detail", "/cart",
         "/checkout", "/checkout/payment", "/confirmation",
         "/blog", "/about", "/pricing"]

FUNNEL = ["/products", "/products/detail", "/cart",
          "/checkout", "/checkout/payment", "/confirmation"]

EVENT_TYPES = ["page_view", "click", "scroll", "form_submit"]
DEVICES     = ["desktop", "mobile", "tablet"]

producer = Producer({"bootstrap.servers": "localhost:9092"})

def make_event(session_id, user_id, page_url):
    return {
        "event_id":    str(uuid.uuid4()),
        "user_id":     user_id,
        "session_id":  session_id,
        "event_type":  random.choices(EVENT_TYPES, weights=[50, 30, 15, 5])[0],
        "page_url":    page_url,
        "referrer":    random.choice(["google", "direct", "twitter", "email", ""]),
        "device":      random.choice(DEVICES),
        "country":     fake.country_code(),
        "element_id":  random.choice(["btn-cta", "nav-link", "product-card", ""]),
        "duration_ms": random.randint(100, 15000),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }

def simulate_session():
    user_id    = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    if random.random() < 0.3:
        pages = random.sample(PAGES, k=random.randint(1, 4))
    else:
        drop  = random.randint(1, len(FUNNEL))
        pages = FUNNEL[:drop]
    return session_id, user_id, pages

print("Simulator starting — targeting ~200 events/sec")
batches = 0

while True:
    for _ in range(random.randint(5, 10)):
        session_id, user_id, pages = simulate_session()
        for page in pages:
            event = make_event(session_id, user_id, page)
            producer.produce(
                "clickstream-raw",
                key=event["user_id"],
                value=json.dumps(event).encode()
            )
    producer.poll(0)
    batches += 1
    if batches % 100 == 0:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] batches={batches}")
    time.sleep(0.05)