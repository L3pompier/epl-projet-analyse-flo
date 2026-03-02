"""
script.py
Fonctions métier de l'application :
  - Chargement et validation des données
  - Agrégations statistiques (tableau UE, parcours étudiant…)
  - Génération des graphiques (matplotlib / seaborn)
  - Gestion du cache mémoire et disque
"""

from __future__ import annotations

import copy
import hashlib
import io
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import base64
import matplotlib
matplotlib.use("Agg")          # Pas d'affichage graphique (mode serveur)
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

from config import (
    BASE_DIR, DATA_PATH, REPORTS_DIR, FIGURES_DIR, FIGURES_TMP,
    CACHE_TTL, BUCKETS, BUCKET_LABELS, SEUIL_REUSSITE, FIG_DPI,
)

#Création des dossiers nécessaires
FIGURES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_TMP.mkdir(parents=True, exist_ok=True)

#Configuration du journal (logs)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Thème graphique commun
sns.set_theme(style="whitegrid", palette="muted", font_scale=1.05)

# Palette de couleurs 
PALETTE_MAIN   = "Blues_d"
COLOR_REUSSITE = "#2ecc71"
COLOR_ECHEC    = "#e74c3c"
COLOR_MEAN     = "#e67e22"
COLOR_MEDIAN   = "#3498db"


# CHARGEMENT ET VALIDATION DES DONNÉES

def read_parquet_or_csv(chemin: Path) -> pd.DataFrame:
    """
    Lit un fichier Parquet ; si absent ou illisible, tente le CSV équivalent.
    Lève FileNotFoundError si aucun des deux n'existe.
    """
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
    """
    Vérifie que le DataFrame contient bien toutes les colonnes attendues.
    Lève ValueError si des colonnes sont manquantes.
    """
    colonnes_requises = {
        "annee", "semestre", "carte", "anonymat", "ue",
        "credit", "nom_prenoms", "sexe", "note", "cohorte",
    }
    manquantes = colonnes_requises - set(df.columns)
    if manquantes:
        raise ValueError(f"Colonnes manquantes dans les données : {manquantes}")


def load_data() -> pd.DataFrame:
    """
    Charge et valide le fichier de données principal.
    Convertit les colonnes catégorielles pour économiser la mémoire.
    """
    df = read_parquet_or_csv(DATA_PATH)
    validate_schema(df)
    for col in ["ue", "annee", "cohorte", "sexe"]:
        if col in df.columns:
            df[col] = df[col].astype("category")
    return df

def obtenir_mtime_fichier() -> float:
    """Retourne la date de modification du fichier de données (0 si absent)."""
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



def get_cached_data() -> pd.DataFrame:
    """
    Point d'entrée unique pour accéder aux données.
    Recharge automatiquement
    """
    mtime_actuel    = obtenir_mtime_fichier()
    cache_expire    = (time.time() - cache_donnees.timestamp) > CACHE_TTL
    fichier_modifie = mtime_actuel != cache_donnees.file_mtime

    if cache_donnees.df is None or cache_expire or fichier_modifie:
        logger.info("Rechargement des données")
        cache_donnees.df          = load_data()
        cache_donnees.fingerprint = df_fingerprint(cache_donnees.df)
        cache_donnees.timestamp   = time.time()
        cache_donnees.file_mtime  = mtime_actuel
        logger.info(
            f"Données rechargées : {cache_donnees.df.shape[0]} lignes, "
            f"{cache_donnees.df.shape[1]} colonnes"
        )
    return cache_donnees.df


# UTILITAIRES DIVERS

def df_fingerprint(df: pd.DataFrame) -> str:
    """
    Calcule un hash MD5 représentant l'état du DataFrame.
    Utilisé pour détecter si les données ont changé (validation cache disque).
    """
    try:
        metadonnees = {
            "shape":   df.shape,
            "columns": list(df.columns),
            "sums":    df.select_dtypes(include=[np.number]).sum().to_dict(),
            "counts":  df.nunique().to_dict(),
            "mtime":   obtenir_mtime_fichier(),
        }
        return hashlib.md5(json.dumps(metadonnees, sort_keys=True).encode()).hexdigest()
    except Exception:
        return f"{len(df)}-nohash"

def fig_to_base64(fig: plt.Figure) -> str:
    """Encode une figure matplotlib en image PNG base64 (pour affichage web)."""
    tampon = io.BytesIO()
    fig.savefig(tampon, format="png", bbox_inches="tight")
    plt.close(fig)
    return f"data:image/png;base64,{base64.b64encode(tampon.getvalue()).decode('utf-8')}"



def buckets_counts(serie: pd.Series) -> Dict[str, int]:
    """
    Répartit les notes dans les tranches définies dans config.py.
    Retourne un dictionnaire {label_tranche: nombre_étudiants}.
    """
    if serie is None or serie.empty:
        return dict.fromkeys(BUCKET_LABELS, 0)
    tranches = pd.cut(serie.dropna(), BUCKETS, include_lowest=True, labels=BUCKET_LABELS)
    return tranches.value_counts().sort_index().to_dict()


def build_figure_subpath(
    dossier: Path,
    annee, semestre, cohorte, ue,
    vue: str,
    nom_fichier: str,
) -> Path:
    """
    Construit et crée le sous-dossier d'une figure en fonction des filtres.
    Ex : figures/annee_2024/semestre_1/cohorte_Tout/ue_Tout/vue_histogram/
    """
    parties = [
        f"annee_{annee or 'Tout'}",
        f"semestre_{semestre if semestre is not None else 'Tout'}",
        f"cohorte_{cohorte if cohorte is not None else 'Tout'}",
        f"ue_{ue or 'Tout'}",
        f"vue_{vue}",
    ]
    sous_dossier = dossier.joinpath(*parties)
    sous_dossier.mkdir(parents=True, exist_ok=True)
    return sous_dossier / nom_fichier

# FILTRAGE CENTRALISÉ

def apply_filters_to_df(
    df_base:  pd.DataFrame,
    annee:    Optional[str] = None,
    semestre: Optional[int] = None,
    cohorte:  Optional[int] = None,
    sexe:     Optional[str] = None,
    ue:       Optional[str] = None,
) -> pd.DataFrame:
    """
    Applique les filtres demandés sur le DataFrame de base.
    Retourne un DataFrame vide si un filtre est invalide.
    """
    if df_base is None:
        return pd.DataFrame()

    df_f = df_base

    if annee:
        df_f = df_f[df_f["annee"] == annee]

    if semestre is not None:
        try:
            df_f = df_f[df_f["semestre"] == int(semestre)]
        except (ValueError, TypeError):
            logger.warning(f"Filtre semestre invalide : {semestre}")
            return df_f.iloc[0:0]

    if cohorte is not None:
        try:
            df_f = df_f[df_f["cohorte"] == int(cohorte)]
        except (ValueError, TypeError):
            logger.warning(f"Filtre cohorte invalide : {cohorte}")
            return df_f.iloc[0:0]

    if sexe:
        sexe_norm = str(sexe).upper()
        if sexe_norm in {"M", "F"}:
            df_f = df_f[df_f["sexe"] == sexe_norm]
        else:
            logger.warning(f"Filtre sexe invalide : {sexe}")
            return df_f.iloc[0:0]

    if ue:
        df_f = df_f[df_f["ue"] == ue]

    return df_f

# AGRÉGATIONS STATISTIQUES

# DataFrame vide de référence 
_UE_VIDE = pd.DataFrame(columns=["ue", "semestre", "moyenne", "taux_reussite", "effectif", "credit"])


def tableau_ue(df_local: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les statistiques par UE et par semestre :
    moyenne, taux de réussite, effectif, crédits.
    """
    if df_local is None or df_local.empty:
        return _UE_VIDE.copy()

    df = df_local.copy()
    df["note"]   = pd.to_numeric(df.get("note"),   errors="coerce")
    df["credit"] = pd.to_numeric(df.get("credit"), errors="coerce").fillna(1)
    df_valide    = df.dropna(subset=["note"])

    if df_valide.empty:
        return _UE_VIDE.copy()

    # Vérification : chaque UE doit avoir un seul nombre de crédits par semestre
    credits_incoherents = df_valide.groupby(["ue", "semestre"], observed=False)["credit"].nunique(dropna=True)
    if (credits_incoherents > 1).any():
        nb = int((credits_incoherents > 1).sum())
        raise ValueError(f"Crédits incohérents pour {nb} couple(s) UE/semestre")

    agregats = (
        df_valide.groupby(["ue", "semestre"], observed=False, dropna=False)
        .agg(
            moyenne       =("note",     "mean"),
            taux_reussite =("note",     lambda x: (x >= SEUIL_REUSSITE).mean() * 100),
            effectif      =("anonymat", "nunique"),
            credit        =("credit",   "first"),
        )
        .reset_index()
    )

    agregats["moyenne"]       = agregats["moyenne"].round(2)
    agregats["taux_reussite"] = agregats["taux_reussite"].round(2)
    agregats["effectif"]      = agregats["effectif"].fillna(0).astype(int)
    agregats["credit"]        = agregats["credit"].apply(lambda x: int(x) if pd.notna(x) else 0)
    return agregats


def top_bottom_ue(
    df_local: pd.DataFrame,
    n: int = 10,
    by: str = "taux_reussite",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retourne les n meilleures et n pires UE selon le critère (by) choisi
    (par défaut : taux de réussite).
    """
    agg = tableau_ue(df_local)
    if agg.empty:
        return agg, agg
    meilleures = agg.sort_values(by, ascending=False).head(n)
    pires      = agg.sort_values(by, ascending=True).head(n)
    return meilleures, pires


def ue_difficiles(
    df_local: pd.DataFrame,
    seuil_taux: float = 50.0,
    seuil_moyenne: float = 10.0,
) -> pd.DataFrame:
    """
    Retourne les UE considérées comme difficiles :
    taux de réussite < seuil_taux ET moyenne < seuil_moyenne.
    """
    agg = tableau_ue(df_local)
    return agg[
        (agg["taux_reussite"] < seuil_taux) &
        (agg["moyenne"]       < seuil_moyenne)
    ]


def calculer_parcours_etudiant(df_local: pd.DataFrame, anonymat_id: str) -> dict:
    """
    Calcule le profil académique complet d'un étudiant sur tous les semestres :
    moyennes pondérées, crédits validés, liste des UE par semestre.
    """
    resultat_vide = {
        "anonymat": anonymat_id, "carte": None, "nom_prenoms": None,
        "sexe": None, "cohorte": None, "parcours": [],
        "moyenne_globale": None, "taux_reussite_global": None,
        "credits_valides": None, "credits_total": None,
    }

    etudiant = df_local[df_local["anonymat"] == anonymat_id].copy()
    if etudiant.empty:
        return resultat_vide

    etudiant["note"]   = pd.to_numeric(etudiant["note"],   errors="coerce")
    etudiant["credit"] = pd.to_numeric(etudiant["credit"], errors="coerce").fillna(1)
    etudiant = etudiant.dropna(subset=["note"])

    parcours = []
    credits_valides_total = 0

    for semestre, groupe in etudiant.groupby("semestre"):
        credits_sem  = groupe["credit"].sum()
        # Moyenne pondérée par les crédits
        moyenne_sem  = (
            (groupe["note"] * groupe["credit"]).sum() / credits_sem
            if credits_sem > 0
            else groupe["note"].mean()
        )
        credits_val  = int(groupe[groupe["note"] >= SEUIL_REUSSITE]["credit"].sum())
        credits_valides_total += credits_val

        parcours.append({
            "semestre":        int(semestre),
            "moyenne":         round(float(moyenne_sem), 2),
            "credits":         int(credits_sem),
            "credits_valides": credits_val,
            "nombre_ues":      len(groupe),
            "details_ues":     groupe[["ue", "note", "credit"]].to_dict(orient="records"),
        })

    credits_total  = etudiant["credit"].sum()
    moyenne_globale = round(
        float((etudiant["note"] * etudiant["credit"]).sum() / credits_total), 2
    )

    def premiere_valeur(colonne):
        return etudiant[colonne].iloc[0] if colonne in etudiant.columns else None

    return {
        **resultat_vide,
        "carte":               premiere_valeur("carte"),
        "nom_prenoms":         premiere_valeur("nom_prenoms"),
        "sexe":                premiere_valeur("sexe"),
        "cohorte":             int(premiere_valeur("cohorte")) if "cohorte" in etudiant.columns else None,
        "parcours":            sorted(parcours, key=lambda x: x["semestre"]),
        "moyenne_globale":     moyenne_globale,
        "taux_reussite_global": round(float((etudiant["note"] >= SEUIL_REUSSITE).mean() * 100), 2),
        "credits_total":       int(credits_total),
        "credits_valides":     credits_valides_total,
    }

# CACHE MÉMOIRE (tableaux et chemins de figures)

TABLEAU_CACHE: Dict[str, Tuple[List[Dict[str, Any]], float]] = {}
FIGURE_CACHE: Dict[str, Tuple[str, float, str]] = {}



def make_cache_key(annee, semestre, cohorte, sexe, ue, vue: str) -> str:
    """Construit une clé de cache unique à partir des paramètres de filtre."""
    return (
        f"a={annee or ''}|s={semestre if semestre is not None else ''}"
        f"|c={cohorte if cohorte is not None else ''}|x={sexe or ''}"
        f"|ue={ue or ''}|v={vue}"
    )


def set_cached(cache: dict, key: str, valeur: Any, fingerprint: str):
    """Enregistre une valeur, le temps actuel et l'empreinte des données."""
    cache[key] = (valeur, time.time(), fingerprint)

def get_cached(cache: dict, key: str, current_fingerprint: str) -> Optional[Any]:
    """
    Récupère une valeur seulement si :
    1. Elle existe
    2. Le TTL n'est pas expiré
    3. L'empreinte correspond
    """
    entree = cache.get(key)
    if not entree:
        return None
    
    valeur, horodatage, cached_fp = entree
    
    # Vérification du temps
    if time.time() - horodatage > CACHE_TTL:
        del cache[key]
        return None
    
    # Vérification de l'empreinte 
    if cached_fp != current_fingerprint:
        del cache[key]
        return None
        
    return valeur

# Cache tableau (agrégats JSON) 

def set_cached_tableau(key: str, donnees: List[Dict[str, Any]], fingerprint: str):
    """Enregistre un tableau et son empreinte de données."""
    # On stocke le triplet : (données, temps_actuel, empreinte)
    TABLEAU_CACHE[key] = (copy.deepcopy(donnees), time.time(), fingerprint)
    
def get_cached_tableau(key: str, fp: str):
    valeur = get_cached(TABLEAU_CACHE, key, fp)
    return copy.deepcopy(valeur) if valeur else None


# Cache figure (PNG) 

def set_cached_figure(key: str, chemin_fichier: str, fingerprint: str = ""):
    FIGURE_CACHE[key] = (chemin_fichier, time.time(), fingerprint)

def get_cached_figure(key: str, current_fingerprint: str) -> Optional[str]:
    """
    Récupère le chemin d'une figure en cache si l'empreinte est valide.
    Vérifie également que le fichier existe physiquement sur le disque.
    """
    chemin = get_cached(FIGURE_CACHE, key, current_fingerprint)
    
    if chemin and not Path(chemin).exists():
        if key in FIGURE_CACHE:
            del FIGURE_CACHE[key]
        return None
        
    return chemin

# Invalidation  

def clear_cache(all_keys: bool = True, key: Optional[str] = None):
    """
    Vide le cache mémoire et/ou les figures sur disque.
    Si all_keys=True, supprime tout. Si key est fourni, supprime cette entrée.
    """
    if all_keys:
        TABLEAU_CACHE.clear()
        FIGURE_CACHE.clear()
        for motif in ["**/*.png", "**/*.meta.json"]:
            for fichier in FIGURES_DIR.glob(motif):   # FIGURES_DIR depuis config
                try:
                    fichier.unlink(missing_ok=True)
                except Exception as erreur:
                    logger.error(f"Impossible de supprimer {fichier} : {erreur}")
        for dossier in sorted(FIGURES_DIR.glob("**/"), reverse=True):
            try:
                if dossier != FIGURES_DIR and dossier.is_dir() and not any(dossier.iterdir()):
                    dossier.rmdir()
            except Exception:
                continue
    elif key:
        TABLEAU_CACHE.pop(key, None)
        entree = FIGURE_CACHE.pop(key, None)
        if entree:
            chemin = Path(entree[0])
            chemin.unlink(missing_ok=True)
            chemin.with_suffix(".meta.json").unlink(missing_ok=True)

# SAUVEGARDE ATOMIQUE DES FIGURES SUR DISQUE

def save_figure_atomic(fig: plt.Figure, chemin_cible: Path, meta: dict, fmt: str = "png"):
    """
    Sauvegarde une figure de façon atomique (écriture dans un fichier temporaire
    puis déplacement) pour éviter les fichiers corrompus en cas d'erreur.
    Génère également un fichier .meta.json associé (TTL, empreinte des données…).
    """
    params    = meta.get("params", {})
    vue       = params.get("view", "inconnu")
    key_cache = make_cache_key(
        params.get("annee"), params.get("semestre"), params.get("cohorte"),
        params.get("sexe"), params.get("ue"), vue
    )

    digest     = hashlib.md5(key_cache.encode()).hexdigest()
    nom_fichier = f"fig_{digest}.{fmt}"
    chemin_final = build_figure_subpath(
        FIGURES_DIR,
        params.get("annee"), params.get("semestre"),
        params.get("cohorte"), params.get("ue"),
        vue, nom_fichier
    )

    ts_ms      = int(time.time() * 1000)
    FIGURES_TMP.mkdir(parents=True, exist_ok=True)
    tmp_img    = FIGURES_TMP / f"{nom_fichier}.tmp-{ts_ms}"
    tmp_meta   = FIGURES_TMP / f"fig_{digest}.meta.json.tmp-{ts_ms}"

    try:
        fig.savefig(tmp_img, format=fmt, bbox_inches="tight")
        tmp_img.replace(chemin_final)

        meta_complete = {
            **meta,
            "generated_at_ts":  int(time.time()),
            "ttl_seconds":      meta.get("ttl_seconds", CACHE_TTL),
            "data_fingerprint": meta.get("data_fingerprint"),
            "params":           params,
            "source_path":      chemin_final.name,
        }
        tmp_meta.write_text(json.dumps(meta_complete))
        tmp_meta.replace(chemin_final.with_suffix(".meta.json"))

    finally:
        plt.close(fig)
        tmp_img.unlink(missing_ok=True)
        tmp_meta.unlink(missing_ok=True)

# GRAPHIQUES

def render_insufficient_data_image(
    message: str = "Données insuffisantes",
    largeur: int = 800,
    hauteur: int = 400,
) -> plt.Figure:
    """Retourne une figure neutre affichant un message d'avertissement."""
    fig, ax = plt.subplots(figsize=(largeur / FIG_DPI, hauteur / FIG_DPI))
    ax.text(0.5, 0.5, message, ha="center", va="center",
            fontsize=13, color="#7f8c8d", wrap=True, transform=ax.transAxes)
    ax.set_facecolor("#f9f9f9")
    ax.axis("off")
    plt.tight_layout()
    return fig


def _guard(df: pd.DataFrame, msg: str, l: int = 900, h: int = 450):
    """Retourne une figure placeholder si le DataFrame est vide, sinon None."""
    if df is None or df.empty:
        return render_insufficient_data_image(msg, l, h)
    return None

def plot_hist_generic(df_in: pd.DataFrame) -> plt.Figure:
    """Histogramme de la distribution des notes avec lignes moyenne et médiane."""
    garde = _guard(df_in, "Pas assez de données pour l'histogramme")
    if garde:
        return garde

    notes       = df_in["note"].dropna()
    moyenne     = notes.mean()
    mediane     = notes.median()

    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=FIG_DPI)
    sns.histplot(notes, bins=BUCKETS, kde=True, color="#5b9bd5",
                    edgecolor="white", linewidth=0.6, ax=ax)

    if not np.isnan(moyenne):
        ax.axvline(moyenne, color=COLOR_MEAN, linestyle="--", linewidth=1.8,
                    label=f"Moyenne = {moyenne:.2f}")
    if not np.isnan(mediane):
        ax.axvline(mediane, color=COLOR_MEDIAN, linestyle=":", linewidth=1.8,
                    label=f"Médiane = {mediane:.2f}")

    ax.axvline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="-.", linewidth=1.4,
                label=f"Seuil réussite = {SEUIL_REUSSITE}")
    ax.set(title="Distribution des notes", xlabel="Note", ylabel="Nombre d'étudiants")
    ax.xaxis.set_major_locator(mticker.MultipleLocator(2))
    ax.legend(frameon=False)
    plt.tight_layout()
    return fig


def plot_box_generic(df_in: pd.DataFrame) -> plt.Figure:
    """Boxplot horizontal des notes (min, Q1, médiane, Q3, max)."""
    garde = _guard(df_in, "Pas assez de données pour le boxplot")
    if garde:
        return garde

    notes = df_in["note"].dropna()
    fig, ax = plt.subplots(figsize=(8, 3.5), dpi=FIG_DPI)
    sns.boxplot(x=notes, color="#aec6e8", width=0.45,
                flierprops=dict(marker="o", markerfacecolor="#e74c3c", markersize=4, alpha=0.6),
                ax=ax)
    ax.axvline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="--", linewidth=1.4,
                label=f"Seuil réussite = {SEUIL_REUSSITE}")

    stats = notes.describe()
    for val, label in [(stats["25%"], "Q1"), (stats["50%"], "Md"), (stats["75%"], "Q3")]:
        ax.text(val, 0.35, f"{label}\n{val:.1f}", ha="center", va="bottom",
                fontsize=8, color="#2c3e50")

    ax.set(title="Distribution des notes (boxplot)", xlabel="Note")
    ax.yaxis.set_visible(False)
    ax.legend(frameon=False)
    plt.tight_layout()
    return fig


def plot_box_by_sex(df_in: pd.DataFrame) -> plt.Figure:
    """Violin + boxplot superposés, un groupe par sexe, avec effectifs affichés."""
    garde = _guard(df_in, "Pas assez de données pour la comparaison par sexe")
    if garde:
        return garde

    df_plot = df_in.dropna(subset=["note", "sexe"]).copy()
    df_plot["sexe"] = df_plot["sexe"].astype(str).str.upper()
    sexes   = sorted(df_plot["sexe"].unique())
    palette = {"M": "#5b9bd5", "F": "#f08080"}
    palette = {s: palette.get(s, "#95a5a6") for s in sexes}

    fig, ax = plt.subplots(figsize=(7, 5), dpi=FIG_DPI)
    sns.violinplot(data=df_plot, x="sexe", y="note", palette=palette,
                    inner=None, cut=0, linewidth=0.8, ax=ax)
    sns.boxplot(data=df_plot, x="sexe", y="note", palette=palette,
                width=0.18, linewidth=1.2,
                flierprops=dict(marker=".", color="grey", alpha=0.5), ax=ax)
    ax.axhline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="--", linewidth=1.4,
                label=f"Seuil réussite = {SEUIL_REUSSITE}")

    # Affichage des effectifs sous chaque groupe
    effectifs = (
        df_plot.groupby("sexe")["anonymat"].nunique()
        if "anonymat" in df_plot.columns
        else df_plot.groupby("sexe")["note"].count()
    )
    y_bas = df_plot["note"].min() - 1.2
    for i, s in enumerate(sexes):
        ax.text(i, y_bas, f"n = {effectifs.get(s, 0)}", ha="center",
                va="top", fontsize=9, color="#555")

    ax.set(title="Distribution des notes par sexe", xlabel="Sexe", ylabel="Note")
    ax.legend(frameon=False)
    plt.tight_layout()
    return fig


def plot_courbe_moyenne_par_sexe(df_in: pd.DataFrame) -> plt.Figure:
    """Courbe de la moyenne pondérée par crédit, par sexe et par semestre."""
    garde = _guard(df_in, "Pas assez de données pour la courbe par sexe")
    if garde:
        return garde

    df_plot = df_in.dropna(subset=["note", "sexe", "semestre"]).copy()
    df_plot["sexe"] = df_plot["sexe"].astype(str).str.upper()

    def moyenne_ponderee(groupe: pd.DataFrame) -> float:
        poids = groupe["credit"].fillna(1)
        return float(np.average(groupe["note"], weights=poids)) if poids.sum() > 0 else float(groupe["note"].mean())

    moyennes = (
        df_plot.groupby(["sexe", "semestre"], observed=False)
        .apply(moyenne_ponderee, include_groups=False)
        .reset_index(name="moyenne")
    )

    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=FIG_DPI)
    sns.lineplot(data=moyennes, x="semestre", y="moyenne", hue="sexe",
                    marker="o", linewidth=2, ax=ax)
    ax.axhline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="--", linewidth=1.2,
                alpha=0.7, label=f"Seuil = {SEUIL_REUSSITE}")

    semestres = sorted(moyennes["semestre"].unique())
    ax.set_xticks(semestres)
    ax.set_xlim(min(semestres) - 0.4, max(semestres) + 0.4)
    ax.set(title="Évolution de la moyenne par sexe et semestre",
            xlabel="Semestre", ylabel="Moyenne pondérée")
    ax.legend(title="Sexe", frameon=False)
    plt.tight_layout()
    return fig



def plot_heatmap_ue_semestre(df_in: pd.DataFrame) -> plt.Figure:
    """Heatmap : UE (lignes) × semestre (colonnes) → moyenne des notes."""
    garde = _guard(df_in, "Pas assez de données pour la heatmap", 1000, 800)
    if garde:
        return garde

    pivot  = df_in.pivot_table(index="ue", columns="semestre", values="note", aggfunc="mean", observed=False)
    nb_ues = len(pivot)
    hauteur = max(6, min(nb_ues * 0.35, 24))   # Hauteur adaptative

    fig, ax = plt.subplots(figsize=(10, hauteur), dpi=FIG_DPI)
    sns.heatmap(pivot, cmap="YlGnBu", annot=(nb_ues <= 40), fmt=".1f",
                linewidths=0.4, linecolor="#e0e0e0",
                cbar_kws={"label": "Moyenne", "shrink": 0.6}, ax=ax)
    ax.set(title="Heatmap des moyennes — UE × Semestre",
            xlabel="Semestre", ylabel="UE")
    ax.tick_params(axis="y", labelsize=max(6, 10 - nb_ues // 10))
    plt.tight_layout()
    return fig


def plot_courbe_cohortes(df_in: pd.DataFrame) -> plt.Figure:
    """Courbe de la moyenne par semestre, une ligne par cohorte."""
    garde = _guard(df_in, "Pas assez de données pour la courbe par cohorte")
    if garde:
        return garde

    courbes = (
        df_in.groupby(["cohorte", "semestre"], observed=False, as_index=False)["note"]
        .mean()
        .rename(columns={"note": "moyenne"})
    )

    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=FIG_DPI)
    sns.lineplot(data=courbes, x="semestre", y="moyenne", hue="cohorte",
                    marker="o", linewidth=2, ax=ax)
    ax.axhline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="--", linewidth=1.2,
                alpha=0.7, label=f"Seuil = {SEUIL_REUSSITE}")

    semestres = sorted(courbes["semestre"].unique())
    ax.set_xticks(semestres)
    ax.set_xlim(min(semestres) - 0.4, max(semestres) + 0.4)
    ax.set(title="Évolution des moyennes par cohorte",
            xlabel="Semestre", ylabel="Moyenne")
    ax.legend(title="Cohorte", frameon=False)
    plt.tight_layout()
    return fig


def plot_evolution_moyenne_by_annee(df_in: pd.DataFrame) -> plt.Figure:
    """Barres groupées : moyenne pondérée par semestre et par année académique."""
    garde = _guard(df_in, "Pas assez de données pour l'évolution des moyennes")
    if garde:
        return garde

    def moyenne_ponderee(groupe: pd.DataFrame) -> float:
        poids = groupe["credit"].fillna(1)
        return float(np.average(groupe["note"], weights=poids)) if poids.sum() > 0 else float(groupe["note"].mean())

    moyennes  = (
        df_in.groupby(["annee", "semestre"])
        .apply(moyenne_ponderee)
        .reset_index(name="moyenne")
    )
    annees    = sorted(moyennes["annee"].unique())
    semestres = sorted(moyennes["semestre"].unique())

    largeur_barre = 0.8 / len(annees)
    positions     = np.arange(len(semestres))

    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=FIG_DPI)
    for i, annee in enumerate(annees):
        data_annee    = moyennes[moyennes["annee"] == annee]
        pos_barres    = positions + i * largeur_barre
        barres        = ax.bar(pos_barres, data_annee["moyenne"], largeur_barre,
                        label=annee, alpha=0.8)
        for barre, valeur in zip(barres, data_annee["moyenne"]):
            ax.text(barre.get_x() + barre.get_width() / 2, barre.get_height() + 0.1,
                    f"{valeur:.1f}", ha="center", va="bottom", fontsize=8)

    ax.axhline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="--", linewidth=1.2,
            alpha=0.7, label=f"Seuil = {SEUIL_REUSSITE}")
    ax.set_xticks(positions + largeur_barre * (len(annees) - 1) / 2)
    ax.set_xticklabels(semestres)
    ax.set(title="Évolution des moyennes par année académique",
        xlabel="Semestre", ylabel="Moyenne pondérée")
    ax.legend(title="Année", frameon=False)
    plt.tight_layout()
    return fig


def plot_evolution_taux_by_semestre(df_in: pd.DataFrame) -> plt.Figure:
    """Courbe du taux de réussite (%) par semestre, avec zones colorées."""
    garde = _guard(df_in, "Pas assez de données pour le taux de réussite par semestre")
    if garde:
        return garde

    agg = (
        df_in.groupby("semestre")
        .agg(taux=("note", lambda x: (x >= SEUIL_REUSSITE).mean() * 100))
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=FIG_DPI)
    sns.lineplot(data=agg, x="semestre", y="taux",
                marker="o", color="#5b9bd5", linewidth=2.2, ax=ax)
    ax.axhline(50, color=COLOR_ECHEC, linestyle="--", linewidth=1.2, label="50 % (référence)")
    ax.fill_between(agg["semestre"], agg["taux"], 50,
                    where=agg["taux"] >= 50, alpha=0.12, color=COLOR_REUSSITE)
    ax.fill_between(agg["semestre"], agg["taux"], 50,
                    where=agg["taux"] < 50,  alpha=0.12, color=COLOR_ECHEC)

    for _, ligne in agg.iterrows():
        ax.annotate(f"{ligne['taux']:.1f}%",
                    xy=(ligne["semestre"], ligne["taux"]),
                    xytext=(0, 8), textcoords="offset points",
                    ha="center", fontsize=8.5, color="#2c3e50")

    semestres = sorted(agg["semestre"].unique())
    ax.set_xticks(semestres)
    ax.set_xlim(min(semestres) - 0.4, max(semestres) + 0.4)
    ax.set(title="Évolution du taux de réussite par semestre",
        xlabel="Semestre", ylabel="Taux de réussite (%)", ylim=(0, 105))
    ax.legend(frameon=False)
    plt.tight_layout()
    return fig


def plot_validation_global(df_in: pd.DataFrame) -> plt.Figure:
    """Barres colorées du taux de validation par cohorte."""
    garde = _guard(df_in, "Pas assez de données pour la validation globale")
    if garde:
        return garde

    agg = (
        df_in.groupby("cohorte", observed=True)
        .agg(taux=("note", lambda x: (x >= SEUIL_REUSSITE).mean() * 100))
        .reset_index()
        .sort_values("cohorte")
    )
    couleurs = [COLOR_REUSSITE if t >= 50 else COLOR_ECHEC for t in agg["taux"]]

    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=FIG_DPI)
    barres = ax.bar(agg["cohorte"].astype(str), agg["taux"],
                    color=couleurs, edgecolor="white", linewidth=0.8)
    ax.axhline(50, color="#555", linestyle="--", linewidth=1.2, label="50 % (référence)")

    for barre, val in zip(barres, agg["taux"]):
        ax.text(barre.get_x() + barre.get_width() / 2,
                barre.get_height() + 1.2, f"{val:.1f}%",
                ha="center", va="bottom", fontsize=9)

    ax.set(title="Taux de validation global par cohorte",
        xlabel="Cohorte", ylabel="Taux de réussite (%)", ylim=(0, 110))
    ax.legend(frameon=False)
    plt.tight_layout()
    return fig


def donut(df_in: pd.DataFrame) -> plt.Figure:
    """
    Double anneau (donut) :
      - Anneau externe : taux global réussite / échec
      - Anneau interne : déclinaison par sexe (M / F)
    """
    garde = _guard(df_in, "Pas assez de données pour le donut", 1200, 600)
    if garde:
        return garde

    df_plot = df_in.dropna(subset=["note"]).copy()
    df_plot["sexe"] = df_plot["sexe"].astype(str).str.upper() if "sexe" in df_plot.columns else "?"
    reussi = df_plot["note"] >= SEUIL_REUSSITE

    nb_reussis  = int(reussi.sum())
    nb_echoues  = int((~reussi).sum())
    total       = nb_reussis + nb_echoues

    def nb(condition):
        return int(condition.sum())

    vals_ext    = [nb_reussis, nb_echoues]
    labels_ext  = ["Réussite", "Échec"]
    couleurs_ext = [COLOR_REUSSITE, COLOR_ECHEC]

    vals_int    = [
        nb(reussi  & (df_plot["sexe"] == "M")),
        nb(reussi  & (df_plot["sexe"] == "F")),
        nb(~reussi & (df_plot["sexe"] == "M")),
        nb(~reussi & (df_plot["sexe"] == "F")),
    ]
    labels_int  = ["Réussite M", "Réussite F", "Échec M", "Échec F"]
    couleurs_int = ["#27ae60", "#82e0aa", "#c0392b", "#f1948a"]

    fig, (ax_donut, ax_legende) = plt.subplots(1, 2, figsize=(14, 7), dpi=FIG_DPI)

    formater = lambda pct: f"{pct:.1f}%" if pct > 3 else ""

    # Anneau externe
    wedges_ext, _, textes_ext = ax_donut.pie(
        vals_ext, labels=None, autopct=formater, colors=couleurs_ext,
        radius=1.0, startangle=90,
        wedgeprops=dict(width=0.30, edgecolor="white", linewidth=2),
        pctdistance=0.85,
    )
    for t in textes_ext:
        t.set_fontsize(11)
        t.set_color("white")
        t.set_fontweight("bold")

    # Anneau interne
    wedges_int, _, textes_int = ax_donut.pie(
        vals_int, labels=None, autopct=formater, colors=couleurs_int,
        radius=0.68, startangle=90,
        wedgeprops=dict(width=0.28, edgecolor="white", linewidth=1.5),
        pctdistance=0.80,
    )
    for t in textes_int:
        t.set_fontsize(9)

    # Texte central
    pct_global = nb_reussis / total * 100 if total else 0
    ax_donut.text(0,  0.08, f"{pct_global:.1f}%", ha="center", va="center",
                fontsize=20, fontweight="bold", color=COLOR_REUSSITE)
    ax_donut.text(0, -0.18, "réussite", ha="center", va="center",
                fontsize=11, color="#555")
    ax_donut.set_title("Réussite & Échec — global et par sexe", fontsize=13, pad=12)
    ax_donut.set(aspect="equal")

    # Légende dans le panneau de droite
    tous_wedges  = wedges_ext + wedges_int
    tous_labels  = labels_ext + labels_int
    toutes_vals  = vals_ext   + vals_int
    labels_legende = [f"{l} ({v/total*100:.1f}%)" for l, v in zip(tous_labels, toutes_vals)]
    ax_legende.legend(tous_wedges, labels_legende, loc="center", frameon=False,
                    fontsize=11, title="Légende", title_fontsize=12)
    ax_legende.axis("off")

    plt.tight_layout()
    return fig
