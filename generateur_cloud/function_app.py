import azure.functions as func
import logging
from incremental_data_generator import generate_incremental_data

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def GenerateurVentesNuit(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("Le timer a pris du retard !")

    logging.info("⏳ Déveil de la fonction Azure : Début de la génération de données...")
    
    try:
        # Lancement de ton usine à données
        generate_incremental_data()
        logging.info("✅ Génération terminée avec succès ! Les données sont dans le Data Lake.")
    except Exception as e:
        logging.error(f"❌ Erreur lors de la génération : {str(e)}")