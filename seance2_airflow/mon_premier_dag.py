
"""
1. Copier ce fichier dans le dossier dags/.
2. Dans l'interface, activer le DAG puis le déclencher (Trigger).
3. Lire le journal de la tâche « charger ».
4. Ajouter une tâche « controler » entre « transformer » et « charger »,
   qui lève ValueError si n < 100. Que voit-on dans la vue Grid ?
"""

from datetime import datetime
from airflow.sdk import dag, task

@dag(schedule="@daily", start_date=datetime(2026, 1, 1),
     catchup=False, tags=["initiation"])
def mon_premier_dag():

    @task
    def extraire() -> dict:
        return {"nb_patients": 40}  # Modifié : 150 → 40

    @task
    def transformer(stats: dict) -> int:
        return stats["nb_patients"] * 2

    # Nouvelle tâche : contrôle
    @task
    def controler(n: int) -> int:
        if n < 100:
            raise ValueError("Nombre de patients inférieur à 100")
        return n

    @task
    def charger(n: int):
        print(f"Chargement de {n} lignes")

    # Nouvel enchaînement
    charger(controler(transformer(extraire())))

mon_premier_dag()
