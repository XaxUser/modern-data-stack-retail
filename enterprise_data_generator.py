import pandas as pd
import numpy as np
from faker import Faker
import uuid
import os
import time
from datetime import datetime, timedelta
import random
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt

# ==========================================
# 0. CONFIGURATION SÉCURISÉE AZURE
# ==========================================
print("Chargement des variables d'environnement...")
load_dotenv() # Lit le fichier .env
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "landing-zone"

if not CONNECTION_STRING:
    raise ValueError("ERREUR : La chaîne de connexion Azure est introuvable.")

# Connexion au Data Lake
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# ==========================================
# 1. INITIALISATION & RÉFÉRENTIELS
# ==========================================
fake = Faker('fr_FR')
LOCAL_DIR = 'landing_zone/ventes'
os.makedirs(LOCAL_DIR, exist_ok=True) 

print("Génération des catalogues de référence...")
STORES = ['MAG_TOULOUSE_CENTRE', 'MAG_PARIS_NORD', 'MAG_LYON_SUD', 'WEB_FRANCE', 'DRIVE_TOULOUSE']
PAYMENT_METHODS = ['CARTE_BANCAIRE', 'ESPECES', 'PAYPAL', 'CARTE_CADEAU', 'APPLE_PAY']
PRODUCTS = [f"PRD_{str(i).zfill(5)}" for i in range(1, 501)]
PRODUCT_PRICES = {prod: round(random.uniform(2.99, 899.99), 2) for prod in PRODUCTS}

# ==========================================
# 2. FONCTION D'ENVOI RÉSILIENTE (Fault Tolerance)
# ==========================================
# S'il y a une coupure réseau, ça attend 2s, puis 4s, puis 8s... jusqu'à 5 essais.
@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def upload_to_azure(file_path, file_name):
    """Envoie le fichier vers le Data Lake Azure de manière sécurisée"""
    # On crée un chemin virtuel dans Azure (ex: ventes/fichier.csv)
    blob_client = container_client.get_blob_client(f"ventes/{file_name}")
    
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"☁️  Succès de l'upload Azure : {file_name} est dans le Data Lake !")

# ==========================================
# 3. MOTEUR DE GÉNÉRATION MASSIVE 
# ==========================================
def generate_batch(batch_id, num_rows=50000):
    print(f"\n Création du batch {batch_id} ({num_rows} lignes)...")
    
    products_sold = np.random.choice(PRODUCTS, num_rows)
    data = {
        "transaction_id": [str(uuid.uuid4()) for _ in range(num_rows)],
        "timestamp": [(datetime.now() - timedelta(minutes=random.randint(1, 10000))).isoformat() for _ in range(num_rows)],
        "store_id": np.random.choice(STORES, num_rows, p=[0.2, 0.3, 0.2, 0.2, 0.1]),
        "product_id": products_sold,
        "quantity": np.random.choice([1, 1, 1, 2, 2, 3, 5, 10], num_rows),
        "unit_price": [PRODUCT_PRICES[p] for p in products_sold],
        "payment_method": np.random.choice(PAYMENT_METHODS, num_rows)
    }
    df = pd.DataFrame(data)
    
    # Injection de "Sale Donnée"
    df['customer_id'] = [f"CUST_{random.randint(10000, 99999)}" if random.random() > 0.4 else None for _ in range(num_rows)]
    df.loc[df.sample(frac=0.02).index, 'quantity'] *= -1 # Retours
    df.loc[df.sample(frac=0.005).index, 'unit_price'] = 99999.99 # Bugs
    
    # Sauvegarde du fichier localement (Le Buffer)
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"sales_batch_{batch_id}_{timestamp_str}.csv"
    filepath = os.path.join(LOCAL_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f" Sauvegardé en local ({round(os.path.getsize(filepath)/(1024*1024), 2)} MB)")
    
    # Envoi vers le Cloud
    print(f" Transfert vers Azure en cours...")
    upload_to_azure(filepath, filename)

# ==========================================
# 4. LANCEMENT DU FLUX 
# ==========================================
if __name__ == "__main__":
    print(" Démarrage du simulateur vers AZURE...")
    print(" Appuyez sur CTRL+C pour arrêter.")
    
    try:
        batch_number = 1
        while True:
            generate_batch(batch_number, num_rows=50000)
            batch_number += 1
            time.sleep(3) 
            
    except KeyboardInterrupt:
        print("\n Simulation arrêtée. Vos données sont bien au chaud dans le Cloud !")