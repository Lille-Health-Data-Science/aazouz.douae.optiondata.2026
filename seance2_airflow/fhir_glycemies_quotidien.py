"""TP séance 2 · Cas d'usage : le flux FHIR quotidien des glycémies.

extraire (API FHIR, pagination) → aplatir (Parquet) → valider (Pandera) → charger (DuckDB, idempotent)
Le guide pas à pas est dans TD.md (partie 04).

Avant de commencer :
  - copier ce fichier dans dags/ ;
  - créer le pool api_fhir : Admin › Pools › + , nom api_fhir, 3 places.
Sans réseau : déclencher avec le paramètre source = fichier (formulaire « Trigger »).
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

DONNEES = Path(__file__).resolve().parents[1] / "donnees"
BASE = "https://hapi.fhir.org/baseR5"  # OK (panne volontaire (TD.md, 4.4) : mettre une mauvaise URL)
UNITES = ["g/L", "mg/dL", "mmol/L"]
EXEMPLE = DONNEES / "exemple" / "glycemies_exemple.ndjson"     # repli sans réseau (données de la séance 1)
glycemies = Asset("glycemies")


def jour(data_interval_start) -> str:
    """Le jour traité ; une exécution manuelle n'a pas d'intervalle : on prend la veille."""
    return (data_interval_start or datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def alerter(context):
    """Appelée quand une tâche échoue définitivement (après ses retries)."""
    ti = context["ti"]
    message = f"ALERTE {datetime.now():%H:%M:%S} · {ti.dag_id}.{ti.task_id} en échec : {context.get('exception')}"
    print(message)
    with open(DONNEES / "alertes.log", "a") as f:
        f.write(message + "\n")


@dag(schedule=CronTriggerTimetable("0 6 * * *", timezone="Europe/Paris", interval=timedelta(days=1)),
     start_date=datetime(2026, 11, 1), catchup=False, tags=["fhir", "tp"],
     default_args={"on_failure_callback": alerter},
     params={"source": Param("api", enum=["api", "fichier"],
                             description="api : serveur FHIR public ; fichier : données d'exemple, sans réseau")})
def fhir_glycemies_quotidien():

    # En production : retries=5, retry_delay=2 min ; raccourci ici pour voir la panne pendant le TP
    @task(pool="api_fhir", retries=3, retry_delay=timedelta(seconds=20))
    def extraire(params=None, data_interval_start=None) -> str:
        """Étape 1 : glycémies (LOINC 2345-7) modifiées depuis le début de l'intervalle, toutes pages."""
        import requests

        chemin = DONNEES / "brut" / f"glycemies_{jour(data_interval_start)}.ndjson"
        chemin.parent.mkdir(parents=True, exist_ok=True)
        if params["source"] == "fichier":
            chemin.write_text(EXEMPLE.read_text())
            print("Source fichier :", EXEMPLE)
            return str(chemin)

        url = f"{BASE}/Observation"
        requete = {"code": "http://loinc.org|2345-7", "_count": 100,
                   "_lastUpdated": f"ge{jour(data_interval_start)}"}
        ressources = []

        # À COMPLÉTER (étape 1) : boucle de pagination de la séance 1, qui remplit `ressources`
        #   - r = requests.get(url, params=requete, headers={"Accept": "application/fhir+json"}, timeout=30)
        #   - r.raise_for_status() : une erreur HTTP fait échouer la tâche, donc déclenche un retry
        #   - ajouter les ressources de bundle["entry"], suivre le lien « next », puis requete = None
        while url:
            r = requests.get(
                url,
                params=requete,
                headers={"Accept": "application/fhir+json"},
                timeout=30
            )

            # Vérifier que la requête HTTP a réussi
            r.raise_for_status()
            bundle = r.json()

            # Récupérer les observations de la page
            for entree in bundle.get("entry", []):
                if "resource" in entree:
                    ressources.append(entree["resource"])

            # Chercher le lien vers la page suivante
            url_suivante = None

            for lien in bundle.get("link", []):
                if lien.get("relation") == "next":
                    url_suivante = lien.get("url")
                    break

            url = url_suivante

            # Les paramètres sont déjà inclus dans les liens suivants
            requete = None

        chemin.write_text("".join(json.dumps(o) + "\n" for o in ressources))
        print(len(ressources), "ressources écrites dans", chemin)
        return str(chemin)                            # XCom : un chemin, pas les données

    @task
    def aplatir(chemin: str) -> str:
        """Étape 2 : une ligne par Observation (id, patient, date, statut, valeur, unite), en Parquet."""
        import pandas as pd
        import json
        from pathlib import Path

        lignes = []

        with open(chemin, "r", encoding="utf-8") as fichier:
            for ligne in fichier:
                if not ligne.strip():
                    continue

                obs = json.loads(ligne)
                quantite = obs.get("valueQuantity") or {}

                lignes.append({
                    "id": obs.get("id"),
                    "patient": (obs.get("subject") or {}).get("reference"),
                    "date": obs.get("effectiveDateTime"),
                    "statut": obs.get("status"),
                    "valeur": quantite.get("value"),
                    "unite": quantite.get("unit")
                })

        df = pd.DataFrame(lignes)

        sortie = Path(chemin).with_suffix(".parquet")
        df.to_parquet(sortie, index=False)

        print(f"{len(df)} lignes écrites en Parquet")
        print("Unités :", df["unite"].value_counts(dropna=False).to_dict())

        return str(sortie)


    @task
    def valider(chemin: str) -> str:
        """Étape 3 : schéma Pandera ; que faire des lignes invalides ?"""
        import pandas as pd
        import pandera.pandas as pa
        from pathlib import Path

        # Lire les données Parquet
        df = pd.read_parquet(chemin)

        # Définir les règles de validation
        schema = pa.DataFrameSchema({
            "patient": pa.Column(str, nullable=False),
            "valeur": pa.Column(float, checks=pa.Check.gt(0), nullable=False, coerce=True),
            "unite": pa.Column(
                str,
                checks=pa.Check.isin(["g/L", "mg/dL", "mmol/L"]),
                nullable=False
            )
        }, strict=False)

        # Repérer les lignes invalides
        erreurs = schema.validate(df, lazy=True) if False else None

        masque = (
            df["patient"].notna()
            & df["valeur"].notna()
            & (pd.to_numeric(df["valeur"], errors="coerce") > 0)
            & df["unite"].isin(["g/L", "mg/dL", "mmol/L"])
        )

        df_valides = df.loc[masque].copy()
        df_rejetees = df.loc[~masque].copy()

        # Valider les lignes conservées avec Pandera
        df_valides = schema.validate(df_valides)

        # Enregistrer les données valides
        sortie = Path(chemin).with_name(
            Path(chemin).stem + "_valides.parquet"
        )

        df_valides.to_parquet(sortie, index=False)

        print(f"Lignes initiales : {len(df)}")
        print(f"Lignes valides : {len(df_valides)}")
        print(f"Lignes rejetées : {len(df_rejetees)}")

        return str(sortie)



    @task(outlets=[glycemies])
    def charger(chemin: str, data_interval_start=None):
        """Étape 4 : charger les glycémies dans DuckDB sans doublons."""
        import duckdb
        from pathlib import Path

        base = Path("/opt/airflow/donnees/entrepot.duckdb")
        base.parent.mkdir(parents=True, exist_ok=True)

        con = duckdb.connect(str(base))

        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS glycemies (
                    id VARCHAR,
                    patient VARCHAR,
                    date TIMESTAMP,
                    statut VARCHAR,
                    valeur_mg_dl DOUBLE,
                    unite VARCHAR
                )
            """)

            # Lire les données validées et convertir en mg/dL
            con.execute("""
                CREATE OR REPLACE TEMP TABLE nouvelles AS
                SELECT
                    id,
                    patient,
                    TRY_CAST(date AS TIMESTAMP) AS date,
                    statut,
                    CASE
                        WHEN unite = 'mmol/L' THEN valeur * 18.0182
                        WHEN unite = 'g/L' THEN valeur * 100.0
                        WHEN unite = 'mg/dL' THEN valeur
                    END AS valeur_mg_dl,
                    unite
                FROM read_parquet(?)
            """, [chemin])

            # Supprimer les anciennes lignes correspondant aux IDs importés
            con.execute("""
                DELETE FROM glycemies
                WHERE id IN (SELECT id FROM nouvelles)
            """)

            # Insérer les données actualisées
            con.execute("""
                INSERT INTO glycemies
                SELECT * FROM nouvelles
            """)

            total = con.execute(
                "SELECT COUNT(*) FROM glycemies"
            ).fetchone()[0]

            print("Glycémies chargées :", con.execute(
                "SELECT COUNT(*) FROM nouvelles"
            ).fetchone()[0])
            print("Total dans DuckDB :", total)

        finally:
            con.close()

    charger(valider(aplatir(extraire())))


fhir_glycemies_quotidien()
