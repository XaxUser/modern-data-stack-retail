import pandas as pd
import numpy as np
from faker import Faker
import uuid
import os
from datetime import datetime, timedelta
import random
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt
import tempfile

# ==========================================
# 0. CONFIGURATION SÉCURISÉE AZURE
# ==========================================
print("Chargement des variables d'environnement...")
load_dotenv()
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "landing-zone"

if not CONNECTION_STRING:
    raise ValueError("ERREUR : La chaîne de connexion Azure est introuvable.")

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# ==========================================
# 1. INITIALISATION & RÉFÉRENTIELS
# ==========================================
fake = Faker('fr_FR')
# On utilise le dossier temporaire officiel du Cloud Azure
LOCAL_SALES_DIR = tempfile.gettempdir()
LOCAL_CUST_DIR = tempfile.gettempdir()

os.makedirs(LOCAL_SALES_DIR, exist_ok=True)
os.makedirs(LOCAL_CUST_DIR, exist_ok=True) 

STORES = ['MAG_TOULOUSE_CENTRE', 'MAG_PARIS_NORD', 'MAG_LYON_SUD', 'WEB_FRANCE', 'DRIVE_TOULOUSE']
PAYMENT_METHODS = ['CARTE_BANCAIRE', 'ESPECES', 'PAYPAL', 'CARTE_CADEAU', 'APPLE_PAY']
PRODUCTS = [f"PRD_{str(i).zfill(5)}" for i in range(1, 501)]
PRODUCT_PRICES = {prod: round(random.uniform(2.99, 899.99), 2) for prod in PRODUCTS}

# ==========================================
# 2. FONCTION D'ENVOI CIBLÉE
# ==========================================
@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def upload_to_azure(file_path, azure_path):
    blob_client = container_client.get_blob_client(azure_path)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"☁️ Succès de l'upload Azure : {azure_path}")

# ==========================================
# 3. MOTEUR DE GÉNÉRATION INCRÉMENTALE
# ==========================================
def generate_incremental_data():
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # --- A. GÉNÉRATION DES NOUVEAUX CLIENTS ---
    num_new_customers = random.randint(1, 10)
    new_customers = []
    new_customer_ids = []
    
    for _ in range(num_new_customers):
        c_id = f"CUST_NEW_{uuid.uuid4().hex[:6].upper()}"
        new_customer_ids.append(c_id)
        
        new_customers.append({
            "customer_id": c_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "country": "France",
            "segment": random.choice(['Standard', 'Premium', 'VIP'])
        })
    
    columns_customers = ['customer_id', 'first_name', 'last_name', 'email', 'country', 'segment']
    df_new_cust = pd.DataFrame(new_customers, columns=columns_customers)
    
    cust_filename = f"customers_inc_{timestamp_str}.csv"
    cust_local_path = os.path.join(LOCAL_CUST_DIR, cust_filename)
    df_new_cust.to_csv(cust_local_path, index=False)
    
    print(f"Transfert de {num_new_customers} nouveaux clients vers Azure...")
    upload_to_azure(cust_local_path, f"dimensions/customers/{cust_filename}")
    
    # --- B. GÉNÉRATION DES VENTES DU JOUR ---
    num_sales = random.randint(50, 500)
    print(f"\nGénération de {num_sales} nouvelles ventes...")
    
    products_sold = np.random.choice(PRODUCTS, num_sales)
    
    existing_simulated_ids = [f"CUST_{str(random.randint(10000, 99999))}" for _ in range(50)]
    all_possible_customers = existing_simulated_ids + new_customer_ids + [None]*20
    
    data = {
        "transaction_id": [str(uuid.uuid4()) for _ in range(num_sales)],
        "timestamp": [(datetime.now() - timedelta(minutes=random.randint(1, 60))).isoformat() for _ in range(num_sales)],
        "store_id": np.random.choice(STORES, num_sales, p=[0.2, 0.3, 0.2, 0.2, 0.1]),
        "product_id": products_sold,
        "quantity": np.random.choice([1, 1, 1, 2, 2, 3, 5], num_sales),
        "unit_price": [PRODUCT_PRICES[p] for p in products_sold],
        "payment_method": np.random.choice(PAYMENT_METHODS, num_sales),
        "customer_id": [random.choice(all_possible_customers) for _ in range(num_sales)]
    }
    
    columns_sales = ['transaction_id', 'timestamp', 'store_id', 'product_id', 'quantity', 'unit_price', 'payment_method', 'customer_id']
    df_sales = pd.DataFrame(data, columns=columns_sales)
    
    # Injection d'anomalies (retours)
    df_sales.loc[df_sales.sample(frac=0.02).index, 'quantity'] *= -1
    
    sales_filename = f"sales_inc_{timestamp_str}.csv"
    sales_local_path = os.path.join(LOCAL_SALES_DIR, sales_filename)
    df_sales.to_csv(sales_local_path, index=False)
    
    print("Transfert des ventes vers Azure en cours...")
    upload_to_azure(sales_local_path, f"ventes/{sales_filename}")

# ==========================================
# 4. POINT D'ENTRÉE DU SCRIPT
# ==========================================
if __name__ == "__main__":
    print("Démarrage du script incrémental (Exécution unique)...")
    generate_incremental_data()
    print("\n✅ Terminé ! Les fichiers incrémentaux sont dans le Data Lake.")