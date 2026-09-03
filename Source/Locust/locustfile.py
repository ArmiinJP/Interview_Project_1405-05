from locust import HttpUser, task
import random
import uuid


locations = [
    ("Iran", "Tehran"),
    ("Iran", "Mashhad"),
    ("Iran", "Isfahan"),
    ("Iran", "Shiraz"),
    ("Germany", "Berlin"),
    ("Germany", "Munich"),
    ("Germany", "Hamburg"),
    ("United Kingdom", "London"),
    ("United Kingdom", "Manchester"),
    ("United Kingdom", "Birmingham"),
    ("France", "Paris"),
    ("France", "Lyon"),
    ("France", "Marseille"),
    ("Canada", "Toronto"),
    ("Canada", "Vancouver"),
    ("Canada", "Montreal"),
]


promo_codes = [
    "",
    "SUMMER10",
    "WELCOME",
    "FALL15"
]


def generate_payload():

    country, city = random.choice(locations)

    return {
        "user_id": str(uuid.uuid4()),

        "products": [
            {
                "product_id": str(uuid.uuid4()),
                "quantity": random.randint(1, 5),
                "price_per_unit": round(
                    random.uniform(10, 500),
                    2
                ),
                "weight_per_unit": round(
                    random.uniform(0.1, 10),
                    2
                ),
            }
            for _ in range(random.randint(1, 5))
        ],

        "destination": {
            "country": country,
            "city": city,
            "postal_code": str(
                random.randint(10000, 99999)
            ),
        },

        "promo_code": random.choice(
            promo_codes
        ),
    }


class ShopUser(HttpUser):
    # wait_time = between(0.1, 0.5)
    @task
    def calculate_price(self):
        self.client.post(
            "/calculate-price",
            json=generate_payload(), 
            timeout=100
            )