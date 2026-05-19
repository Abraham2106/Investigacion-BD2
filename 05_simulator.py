import random
import time
import uuid
import argparse
import requests
URL = "http://localhost:8000/orders"
countries = ["CR","US","MX","AR","BR","CO","CL","PE","PA","GT","SV","HN","NI"]
products = ["Laptop","Mouse","Keyboard", "Monitor", "Phone","Tablet","Webcam", "CPU"]


def generate_order():
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": f"user-{random.randint(1, 20)}",
        "country_code": random.choice(countries),
        "product": random.choice(products),
        "amount": round(random.uniform(10, 5000), 2)
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=1)
    parser.add_argument("--total", type=int, default=20)
    args = parser.parse_args()
    delay = 1 / args.rate
    if args.total > 0:
        for _ in range(args.total):
            order = generate_order()
            response = requests.post(URL, json=order)
            print(response.status_code, response.json())
            time.sleep(delay)
    else:
        print("Simulador en ejecución INFINITA (Ctrl+C para cancelar)...")
        while True:
            order = generate_order()
            response = requests.post(URL, json=order)
            print(response.status_code, response.json())
            time.sleep(delay)
if __name__ == "__main__":
    main()