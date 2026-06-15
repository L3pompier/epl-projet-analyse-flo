import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any, List, Optional
import numpy as np
import pandas as pd
from app.core.config import DATA_PATH, CACHE_TTL
from app.core.referentiel import get_departement, get_type_formation, get_niveau

logger = logging.getLogger(__name__)

def read_parquet_or_csv(chemin: Path) -> pd.DataFrame:
    chemin_csv = chemin.with_suffix(".csv")
    if chemin.exists():
        try:
            return pd.read_parquet(chemin)
        except Exception as erreur:
            logger.warning(f"Lecture parquet échouée ({chemin}) : {erreur} — tentative CSV")
    if chemin_csv.exists():
        return pd.read_csv(chemin_csv)
    raise FileNotFoundError(f"Fichier introuvable : {chemin} ou {chemin_csv}")

def validate_schema(df: pd.DataFrame) -> None:
    colonnes_requises = {
        "annee", "semestre", "carte", "anonymat", "ue",
        "credit", "nom_prenoms", "sexe", "note", "cohorte", "filiere",
    }
    manquantes = colonnes_requises - set(df.columns)
    if manquantes:
        raise ValueError(f"Colonnes manquantes : {manquantes}")

# Schéma minimal pour un DataFrame vide (permet le démarrage sans données)
_COLONNES_VIDE = [
    "annee", "semestre", "carte", "anonymat", "ue", "credit",
    "nom_prenoms", "sexe", "note", "cohorte", "filiere",
    "departement", "type_formation", "niveau",
]

def _df_vide() -> pd.DataFrame:
    """Retourne un DataFrame vide mais valide pour un démarrage sans données."""
    return pd.DataFrame(columns=_COLONNES_VIDE)


def load_data() -> pd.DataFrame:
    """
    Charge les données depuis le fichier Parquet/CSV.
    Si le fichier est absent, retourne un DataFrame vide avec le bon schéma
    plutôt que de lever une exception — le serveur démarre et les endpoints
    retournent des réponses vides, l'upload d'un fichier initialise les données.
    """
    try:
        df = read_parquet_or_csv(DATA_PATH)
    except FileNotFoundError:
        logger.warning(
            "Aucun fichier de données trouvé (%s). "
            "Le serveur démarre en mode vide — uploadez un fichier via /api/data/upload.",
            DATA_PATH,
        )
        return _df_vide()

    try:
        validate_schema(df)
    except ValueError as e:
        logger.error("Schéma invalide : %s — démarrage en mode vide.", e)
        return _df_vide()

    if "note" in df.columns:
        if df["note"].dtype == object:
            df["note"] = df["note"].str.replace(",", ".", regex=False)
        df["note"] = pd.to_numeric(df["note"], errors="coerce")
    if "credit" in df.columns:
        if df["credit"].dtype == object:
            df["credit"] = df["credit"].str.replace(",", ".", regex=False)
        df["credit"] = pd.to_numeric(df["credit"], errors="coerce").fillna(1).astype(int)

    # Enrichissement via le référentiel
    df["departement"]    = df["filiere"].apply(lambda f: get_departement(str(f).upper()))
    df["type_formation"] = df.apply(
        lambda row: get_type_formation(str(row["filiere"]).upper(), pd.to_numeric(row["semestre"], errors="coerce")), axis=1
    )
    df["niveau"]         = df.apply(
        lambda row: get_niveau(str(row["filiere"]).upper(), pd.to_numeric(row["semestre"], errors="coerce")), axis=1
    )

    for col in ["ue", "annee", "cohorte", "sexe", "filiere", "departement", "type_formation", "niveau"]:
        if col in df.columns:
            df[col] = df[col].astype("category")
    return df

def obtenir_mtime_fichier() -> float:
    for chemin in (DATA_PATH, DATA_PATH.with_suffix(".csv")):
        if chemin.exists():
            return chemin.stat().st_mtime
    return 0.0

class CacheDonnees:
    df:          Optional[pd.DataFrame] = None
    fingerprint: Optional[str]          = None
    timestamp:   float                  = 0.0
    file_mtime:  float                  = 0.0

cache_donnees = CacheDonnees()

def df_fingerprint(df: pd.DataFrame) -> str:
    try:
        metadonnees = {
            "shape":   df.shape,
            "columns": list(df.columns),
            "sums":    df.select_dtypes(include=[np.number]).sum().to_dict(),
            "counts":  df.nunique().to_dict(),
        }
        return hashlib.md5(json.dumps(metadonnees, sort_keys=True).encode()).hexdigest()
    except Exception:
        return f"{len(df)}-nohash"

def get_cached_data() -> pd.DataFrame:
    mtime_actuel    = obtenir_mtime_fichier()
    cache_expire    = (time.time() - cache_donnees.timestamp) > CACHE_TTL
    fichier_modifie = mtime_actuel != cache_donnees.file_mtime

    if cache_donnees.df is None or cache_expire or fichier_modifie:
        logger.info("Rechargement des données")
        cache_donnees.df          = load_data()
        cache_donnees.fingerprint = df_fingerprint(cache_donnees.df)
        cache_donnees.timestamp   = time.time()
        cache_donnees.file_mtime  = mtime_actuel
        if cache_donnees.df.empty:
            logger.info("DataFrame vide — nouvelle tentative au prochain appel ou après upload.")
    return cache_donnees.df

def apply_filters_to_df(
    df_base:  pd.DataFrame,
    annee:    Optional[str] = None,
    semestre: Optional[str] = None,
    cohorte:  Optional[str] = None,
    sexe:     Optional[str] = None,
    ue:       Optional[str] = None,
    filiere:  Optional[str] = None,
    departement: Optional[str] = None,
    type_formation: Optional[str] = None,
    niveau:   Optional[str] = None,
) -> pd.DataFrame:
    if df_base is None or df_base.empty:
        return pd.DataFrame()

    # Application des filtres de manière vectorisée 
    df_f = df_base
    
    def _to_list(val: Any) -> List[str]:
        if val is None or val == "": return []
        if isinstance(val, (list, tuple)): return [str(x).strip() for x in val if str(x).strip()]
        return [s.strip() for s in str(val).split(",") if s.strip()]

    annees = _to_list(annee)
    if annees: df_f = df_f[df_f["annee"].isin(annees)]
    
    semestres = _to_list(semestre)
    if semestres: df_f = df_f[df_f["semestre"].astype(str).isin(semestres)]
    
    cohortes = _to_list(cohorte)
    if cohortes: df_f = df_f[df_f["cohorte"].isin(cohortes)]
    
    sexes = [s.upper() for s in _to_list(sexe) if s.upper() in {"M", "F"}]
    if sexes: df_f = df_f[df_f["sexe"].isin(sexes)]
    
    ues = _to_list(ue)
    if ues: df_f = df_f[df_f["ue"].isin(ues)]
    
    # Filtres hiérarchiques
    depts = [s.upper() for s in _to_list(departement)]
    if depts: df_f = df_f[df_f["departement"].isin(depts)]
    
    types = [s.upper() for s in _to_list(type_formation)]
    if types: df_f = df_f[df_f["type_formation"].isin(types)]
    
    niveaux = [s.capitalize() for s in _to_list(niveau)]
    if niveaux: df_f = df_f[df_f["niveau"].isin(niveaux)]
        
    filieres = [s.upper() for s in _to_list(filiere)]
    if filieres: df_f = df_f[df_f["filiere"].isin(filieres)]

    return df_f
