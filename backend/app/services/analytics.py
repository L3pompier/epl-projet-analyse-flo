"""
analytics.py
Toutes les fonctions de calcul statistique académique.
"""
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import warnings
import logging
from app.core.config import SEUIL_REUSSITE, BUCKETS, BUCKET_LABELS

logger = logging.getLogger(__name__)

_UE_VIDE = pd.DataFrame(columns=[
    "ue", "semestre", "moyenne", "taux_reussite", "effectif", "credit",
    "mediane_note", "std_note", "min_note", "max_note",
    "q1_note", "q3_note", "iqr_note", "departement", "filiere"
])



def _taux_reussite(x: pd.Series) -> float:
    return (x >= SEUIL_REUSSITE).mean() * 100

def _std(x: pd.Series) -> float:
    return x.std(ddof=1) if len(x) > 1 else 0.0

def _q1(x: pd.Series) -> float: return x.quantile(0.25)
def _q3(x: pd.Series) -> float: return x.quantile(0.75)

STATS_AGG_MAP = {
    "moyenne":       ("note", "mean"),
    "taux_reussite": ("note", _taux_reussite),
    "mediane_note":  ("note", "median"),
    "std_note":      ("note", _std),
    "min_note":      ("note", "min"),
    "max_note":      ("note", "max"),
    "q1_note":       ("note", _q1),
    "q3_note":       ("note", _q3),
}


def calculer_stats_notes(notes: pd.Series) -> Dict[str, Optional[float]]:
    """Calcule l'ensemble des indicateurs statistiques sur une série de notes."""
    notes = notes.dropna()
    if notes.empty:
        return {
            "moyenne": None, "taux_reussite": None, "mediane": None,
            "ecart_type": None, "variance": None,
            "q1": None, "q3": None, "iqr": None,
            "min": None, "max": None, "effectif": 0,
        }

    n = len(notes)
    q1 = round(float(_q1(notes)), 2)
    q3 = round(float(_q3(notes)), 2)

    return {
        "moyenne":       round(float(notes.mean()), 2),
        "taux_reussite": round(float(_taux_reussite(notes)), 2),
        "mediane":       round(float(notes.median()), 2),
        "ecart_type":    round(float(_std(notes)), 2),
        "variance":      round(float(notes.var(ddof=1)), 2) if n > 1 else 0.0,
        "q1":            q1,
        "q3":            q3,
        "iqr":           round(q3 - q1, 2),
        "min":           round(float(notes.min()), 2),
        "max":           round(float(notes.max()), 2),
        "effectif":      n,
    }


def calculer_stats_df(df: pd.DataFrame) -> Dict[str, Any]:
    """Calcule les stats globales d'un DataFrame filtré."""
    notes = df["note"].dropna() if "note" in df.columns else pd.Series(dtype=float)
    stats = calculer_stats_notes(notes)
    stats["effectif_exact"] = int(df["anonymat"].nunique()) if "anonymat" in df.columns and not df.empty else 0
    return stats



def buckets_counts(serie: pd.Series) -> Dict[str, int]:
    if serie is None or serie.empty:
        return dict.fromkeys(BUCKET_LABELS, 0)
    tranches = pd.cut(serie.dropna(), BUCKETS, include_lowest=True, labels=BUCKET_LABELS)
    return tranches.value_counts().sort_index().to_dict()


def _credit_modal(x: pd.Series):
    vals = x.dropna()
    if vals.empty: return x.iloc[0] if len(x) > 0 else np.nan
    mode = vals.mode()
    return mode.iloc[0] if not mode.empty else vals.iloc[0]


def tableau_ue(df_local: pd.DataFrame) -> pd.DataFrame:
    """Agrégats statistiques complets par (UE, semestre)."""
    if df_local is None or df_local.empty:
        return _UE_VIDE.copy()

    df = df_local.copy()
    df["credit"] = df.get("credit", 1).fillna(1)
    df_valide = df.dropna(subset=["note"])
    if df_valide.empty:
        return _UE_VIDE.copy()

    df_valide = df_valide.copy()
    df_valide["credit"] = df_valide.groupby(["ue", "semestre"], observed=False)["credit"].transform(_credit_modal)

    agg = (
        df_valide.groupby(["ue", "semestre"], observed=False, dropna=False)
        .agg(
            **STATS_AGG_MAP,
            effectif     =("anonymat", "nunique"),
            credit_val   =("credit", "first"),
            departement  =("departement", "first"),
            filiere      =("filiere", "first"),
        )
        .reset_index()
    )
    
    agg = agg.rename(columns={"credit_val": "credit"})
    agg["iqr_note"] = (agg["q3_note"] - agg["q1_note"]).round(2)
    
    cols_to_round = ["moyenne", "taux_reussite", "mediane_note", "std_note", "min_note", "max_note", "q1_note", "q3_note"]
    agg[cols_to_round] = agg[cols_to_round].round(2)
    
    agg["effectif"] = agg["effectif"].fillna(0).astype(int)
    agg["credit"]   = agg["credit"].fillna(0).astype(int)
    
    return agg


def top_bottom_ue(df_local: pd.DataFrame, n: int = 10, by: str = "taux_reussite", agg_df: pd.DataFrame = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    agg = agg_df if agg_df is not None else tableau_ue(df_local)
    if agg.empty: return agg, agg
    return agg.sort_values(by, ascending=False).head(n), agg.sort_values(by, ascending=True).head(n)


def ue_difficiles(df_local: pd.DataFrame, seuil_taux: float = 50.0, seuil_moyenne: float = 10.0, agg_df: pd.DataFrame = None) -> pd.DataFrame:
    agg = agg_df if agg_df is not None else tableau_ue(df_local)
    return agg[(agg["taux_reussite"] < seuil_taux) & (agg["moyenne"] < seuil_moyenne)]


def etudiants_a_risque(df_local: pd.DataFrame, n: int = None) -> List[Dict]:
    """Étudiants dont la moyenne est sous le seuil."""
    if df_local.empty: return []
    g = df_local.groupby("anonymat").agg(
        moyenne=("note", "mean"),
        nom_prenoms=("nom_prenoms", "first"),
        departement=("departement", "first"),
    ).reset_index()
    risques = g[g["moyenne"] < SEUIL_REUSSITE].sort_values("moyenne")
    if n is not None: risques = risques.head(n)
    return risques.to_dict(orient="records")


def vide_etudiant(anonymat_id: str = None) -> Dict:
    return {
        "anonymat": anonymat_id, "carte": None, "nom_prenoms": None,
        "sexe": None, "cohorte": None, "filiere": None, "parcours": [],
        "moyenne_globale": None, "taux_reussite_global": None,
        "credits_valides": None, "credits_total": None,
        "rang": None, "nb_cohorte": None,
    }


def calculer_parcours_etudiant(etudiant_df: pd.DataFrame, anonymat_id: str, df_global: pd.DataFrame = None) -> Dict:
    """Calcule le profil détaillé d'un étudiant et son classement."""
    if etudiant_df.empty: return vide_etudiant(anonymat_id)
    df_compare = df_global if df_global is not None else etudiant_df
    
    etudiant = etudiant_df[etudiant_df["anonymat"] == anonymat_id].copy()
    if etudiant.empty: return vide_etudiant(anonymat_id)

    etudiant["credit"] = etudiant.get("credit", 1).fillna(1)
    etudiant = etudiant.dropna(subset=["note"])

    parcours, credits_valides_total = [], 0
    for semestre, groupe in etudiant.groupby("semestre"):
        cr = groupe["credit"].sum()
        moy = float((groupe["note"] * groupe["credit"]).sum() / cr) if cr > 0 else float(groupe["note"].mean())
        cv = int(groupe[groupe["note"] >= SEUIL_REUSSITE]["credit"].sum())
        credits_valides_total += cv
        parcours.append({
            "semestre": int(semestre), "moyenne": round(moy, 2),
            "credits": int(cr), "credits_valides": cv,
            "nombre_ues": len(groupe),
            "details_ues": groupe[["ue", "note", "credit"]].to_dict(orient="records"),
        })

    cr_total = etudiant["credit"].sum()
    moy_globale = round(float((etudiant["note"] * etudiant["credit"]).sum() / cr_total), 2) if cr_total > 0 else 0.0

    def _first(col): return etudiant[col].iloc[0] if col in etudiant.columns else None

    # Ranking
    rang, nb_cohorte, palmares = None, None, []
    coh_val = _first("cohorte")
    if coh_val is not None and not df_compare.empty:
        mask = df_compare["cohorte"].astype(str).str.strip() == str(coh_val).strip()
        fil_val = _first("filiere")
        if fil_val: mask &= df_compare["filiere"].astype(str).str.strip() == str(fil_val).strip()
        
        df_coh = df_compare[mask]
        if not df_coh.empty:
            stats_coh = df_coh.groupby("anonymat", observed=True).agg(
                moyenne=("note", "mean"),
                nom_prenoms=("nom_prenoms", "first"),
                departement=("departement", "first")
            ).round(2).sort_values("moyenne", ascending=False).reset_index()
            
            nb_cohorte = len(stats_coh)
            pos = stats_coh[stats_coh["anonymat"] == anonymat_id].index
            if len(pos) > 0: rang = int(pos[0]) + 1
            
            stats_coh["rang"] = range(1, nb_cohorte + 1)
            palmares = stats_coh.to_dict(orient="records")

    return {
        **vide_etudiant(anonymat_id),
        "carte": _first("carte"), "nom_prenoms": _first("nom_prenoms"),
        "sexe": _first("sexe"), "cohorte": int(pd.to_numeric(coh_val, errors="coerce")) if coh_val is not None else None,
        "filiere": _first("filiere"),
        "parcours": sorted(parcours, key=lambda x: x["semestre"]),
        "moyenne_globale": moy_globale,
        "taux_reussite_global": round(float(_taux_reussite(etudiant["note"])), 2),
        "credits_total": int(cr_total), "credits_valides": credits_valides_total,
        "rang": rang, "nb_cohorte": nb_cohorte,
        "palmares_cohorte": palmares[:100]
    }


def calculer_performance_par_dimension(df: pd.DataFrame, dimension: str, include_effectif: bool = False) -> List[Dict]:
    if df.empty or dimension not in df.columns: return []
    
    agg_dict = {"score": ("note", "mean")}
    if include_effectif:
        agg_dict["effectif"] = ("anonymat", "nunique")
    
    perf = (
        df.groupby(dimension, observed=True)
        .agg(**agg_dict)
        .round(2)
        .reset_index()
    )
    if dimension == "departement" and "score" in perf.columns:
        pass

    return perf.sort_values("score", ascending=False).to_dict(orient="records")
