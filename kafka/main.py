import json
import time
import random
from faker import Faker
from datetime import datetime
from confluent_kafka import SerializingProducer


def generate_sales_transactions():
    fake = Faker()
    user = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(
            ["product1", "product2", "product3", "product4", "product5", "product6"]
        ),
        "productName": random.choice(
            ["laptop", "mobile", "tablet", "watch", "headphone", "speaker"]
        ),
        "productCategory": random.choice(
            ["electronic", "fashion", "grocery", "home", "beauty", "sports"]
        ),
        "productPrice": round(random.uniform(10, 1000), 2),
        "productQuantity": random.randint(1, 10),
        "productBrand": random.choice(
            ["apple", "samsung", "oneplus", "mi", "boat", "sony"]
        ),
        "currency": random.choice(["USD", "GBP"]),
        "customerId": user["username"],
        "transactionDate": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        "paymentMethod": random.choice(
            ["credit_card", "debit_card", "online_transfer"]
        ),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")


def main():
    topic = "sales_transactions"

    producer = SerializingProducer(
        {
            "bootstrap.servers": "localhost:9092",
        }
    )

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 120:

        try:
            sales_transaction = generate_sales_transactions()
            sales_transaction["totalAmount"] = (
                sales_transaction["productPrice"] * sales_transaction["productQuantity"]
            )

            print(sales_transaction)
            producer.produce(
                topic=topic,
                key=sales_transaction["transactionId"],
                value=json.dumps(sales_transaction),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            time.sleep(5)
        except BufferError as e:
            print("Local producer queue is full, consider increasing the queue size")
            time.sleep(1)
        except Exception as e:
            print("Error while generating sales transaction", e)
        except KeyboardInterrupt:
            print("Program exicted with CTRL + C")
            exit(0)


if __name__ == "__main__":
    main()
