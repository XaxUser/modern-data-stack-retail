import pandas as pd
from faker import Faker
import random
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# ==========================================
# 0. CONFIGURATION AZURE
# ==========================================
print("Chargement des variables d'environnement...")
load_dotenv()
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "landing-zone"

if not CONNECTION_STRING:
    raise ValueError("ERREUR : La chaîne de connexion Azure est introuvable.")

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def upload_to_azure(local_path, blob_path):
    blob_client = container_client.get_blob_client(blob_path)
    with open(local_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"☁️ Succès Azure : {blob_path}")

# ==========================================
# 1. INITIALISATION
# ==========================================
fake = Faker('fr_FR')
Faker.seed(42)
random.seed(42)

os.makedirs("landing_zone_temp/dimensions/stores", exist_ok=True)
os.makedirs("landing_zone_temp/dimensions/products", exist_ok=True)
os.makedirs("landing_zone_temp/dimensions/customers", exist_ok=True)

print("\n--- Génération des Dimensions Initiales ---")

# ==========================================
# 2. MAGASINS (Doit correspondre à l'ancien script !)
# ==========================================
print("Génération des Magasins...")
store_ids = ['MAG_TOULOUSE_CENTRE', 'MAG_PARIS_NORD', 'MAG_LYON_SUD', 'WEB_FRANCE', 'DRIVE_TOULOUSE']
stores = []
for s_id in store_ids:
    stores.append({
        'store_id': s_id,
        'store_name': s_id.replace('_', ' ').title(),
        'city': s_id.split('_')[1].title() if len(s_id.split('_')) > 1 else 'National',
        'store_type': 'Web' if 'WEB' in s_id else ('Drive' if 'DRIVE' in s_id else 'Boutique Physique')
    })
df_stores = pd.DataFrame(stores)
local_store_path = "landing_zone_temp/dimensions/stores/stores_master.csv"
df_stores.to_csv(local_store_path, index=False)
upload_to_azure(local_store_path, "dimensions/stores/stores_master.csv")

# ==========================================
# 3. PRODUITS (PRD_00001 à PRD_00500)
# ==========================================
print("Génération du catalogue Produits...")
categories = {'Electronique': ['Smartphone', 'Laptop'], 'Vetements': ['T-shirt', 'Jeans'], 'Maison': ['Lampe', 'Canapé']}
products = []
for i in range(1, 501):
    cat = random.choice(list(categories.keys()))
    products.append({
        'product_id': f"PRD_{str(i).zfill(5)}",
        'product_name': f"{random.choice(categories[cat])} {fake.word().capitalize()}",
        'category': cat,
        'unit_price': round(random.uniform(2.99, 899.99), 2)
    })
df_products = pd.DataFrame(products)
local_product_path = "landing_zone_temp/dimensions/products/products_master.csv"
df_products.to_csv(local_product_path, index=False)
upload_to_azure(local_product_path, "dimensions/products/products_master.csv")

# ==========================================
# 4. CLIENTS (Chargement initial)
# ==========================================
print("Génération de la base Clients (Initial Load)...")
customers = []
# On génère une base initiale de 10 000 clients avec le format CUST_XXXXX
for _ in range(10000):
    customers.append({
        'customer_id': f"CUST_{random.randint(10000, 99999)}",
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'country': 'France',
        'segment': random.choice(['Standard', 'Premium', 'VIP'])
    })
# On supprime les éventuels doublons d'ID générés par le hasard
df_customers = pd.DataFrame(customers).drop_duplicates(subset=['customer_id'])
local_cust_path = "landing_zone_temp/dimensions/customers/customers_initial.csv"
df_customers.to_csv(local_cust_path, index=False)
upload_to_azure(local_cust_path, "dimensions/customers/customers_initial.csv")

print("\n🚀 TERMINÉ ! Les dimensions sont dans Azure. Prêt pour les jointures !")