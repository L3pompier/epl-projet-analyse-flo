from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import seaborn as sns
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from config import (
    CACHE_TTL, FIGURES_DIR, SEUIL_REUSSITE,
)
from src.script import (
    # Données
    get_cached_data, df_fingerprint, load_data, cache_donnees,
    # Filtrage
    apply_filters_to_df,
    # Agrégations
    buckets_counts, tableau_ue, top_bottom_ue, ue_difficiles,
    calculer_parcours_etudiant,
    # Figures
    plot_heatmap_ue_semestre, plot_courbe_cohortes,
    plot_hist_generic, plot_box_generic, plot_box_by_sex,
    plot_evolution_taux_by_semestre, plot_evolution_moyenne_by_annee,
    plot_courbe_moyenne_par_sexe, plot_validation_global, donut,
    build_figure_subpath, save_figure_atomic,
    # Cache
    make_cache_key, get_cached_figure, set_cached_figure,
    get_cached_tableau, set_cached_tableau, clear_cache,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("main-api")
sns.set_theme(style="whitegrid")


# MODÈLES DE DONNÉES (Pydantic)

class ErrorResponse(BaseModel):
    """Format standardisé des erreurs renvoyées par l'API."""
    error:  str
    detail: Optional[str] = None
    code:   int


class HealthResponse(BaseModel):
    """Réponse de l'endpoint de santé."""
    status:              str
    rows:                int
    indicateurs_charges: bool


class UEStat(BaseModel):
    """Statistiques complètes d'une UE."""
    ue:                  str
    semestre:            Optional[int]
    credit:              Optional[int]
    moyenne:             Optional[float]
    taux_reussite:       Optional[float]
    effectif:            Optional[int]
    isDifficile:         Optional[bool]
    min_note:            Optional[float] = None
    max_note:            Optional[float] = None
    std_note:            Optional[float] = None
    mediane_note:        Optional[float] = None
    nombre_admis:        Optional[int]   = None
    pourcentage_admis:   Optional[float] = None
    nombre_ajournes:     Optional[int]   = None
    pourcentage_ajournes: Optional[float] = None


class EtudiantParcours(BaseModel):
    """Profil académique complet d'un étudiant."""
    anonymat:              str
    carte:                 Optional[str]   = None
    nom_prenoms:           Optional[str]   = None
    sexe:                  Optional[str]   = None
    cohorte:               Optional[int]   = None
    parcours:              List[Dict[str, Any]]
    moyenne_globale:       Optional[float] = None
    taux_reussite_global:  Optional[float] = None
    credits_total:         Optional[int]   = None
    credits_valides:       Optional[int]   = None


class DashboardAggregates(BaseModel):
    """Agrégats principaux pour le tableau de bord."""
    top10:                 List[Dict[str, Any]]
    bottom10:              List[Dict[str, Any]]
    ue_difficiles:         List[Dict[str, Any]]
    tableau_ue:            List[Dict[str, Any]]
    moyenne_global:        Optional[float] = None
    taux_reussite_global:  Optional[float] = None
    effectif_exact:        Optional[int]   = None
    mediane:               Optional[float] = None
    ecart_type:            Optional[float] = None
    variance:              Optional[float] = None


# CORRESPONDANCE VUE → FONCTION DE TRACÉ

PLOT_DISPATCH: Dict[str, Any] = {
    "heatmap_ue_semestre":        plot_heatmap_ue_semestre,
    "courbe_cohortes":            plot_courbe_cohortes,
    "histogram":                  plot_hist_generic,
    "boxplot":                    plot_box_generic,
    "boxplot_by_sex":             plot_box_by_sex,
    "evolution_moyenne_by_annee": plot_evolution_moyenne_by_annee,
    "evolution_taux_by_semestre": plot_evolution_taux_by_semestre,
    "courbe_moyenne_par_sexe":    plot_courbe_moyenne_par_sexe,
    "validation_global":          plot_validation_global,
    "donut":                      donut,
}


# INITIALISATION DE L'APPLICATION

# Pré-chargement des données au démarrage du serveur
logger.info("Pré-chargement des données au démarrage…")
_preload = get_cached_data()
logger.info(f"{len(_preload)} lignes chargées")

app = FastAPI(title="API Analyse résultats académiques")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# GESTIONNAIRES D'ERREURS GLOBAUX

@app.exception_handler(ValueError)
async def gerer_erreur_validation(_: Request, exc: ValueError):
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(error="Erreur de validation", detail=str(exc), code=400).dict(),
    )


@app.exception_handler(Exception)
async def gerer_erreur_serveur(_: Request, exc: Exception):
    logger.exception(exc)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(error="Erreur interne du serveur", detail=str(exc), code=500).dict(),
    )


# ENDPOINTS SANTÉ & MÉTA

@app.get("/health", response_model=HealthResponse, summary="État de l'API")
def sante():
    """Vérifie que l'API répond et indique le nombre de lignes en mémoire."""
    df_actuel = get_cached_data()
    return HealthResponse(status="ok", rows=len(df_actuel), indicateurs_charges=True)


@app.get("/meta/disponibilites", summary="Disponibilités pour les filtres")
def disponibilites(
    annee:   Optional[str] = None,
    cohorte: Optional[int] = None,
):
    """Retourne les valeurs disponibles pour remplir les filtres du frontend."""
    df_actuel = get_cached_data()
    cohortes  = sorted(df_actuel["cohorte"].unique().tolist()) if "cohorte" in df_actuel.columns else []
    df_sub    = df_actuel.copy()
    if cohorte:
        df_sub = df_sub[df_sub["cohorte"] == cohorte]
    if annee:
        df_sub = df_sub[df_sub["annee"] == annee]
    return {
        "annees":    sorted(df_sub["annee"].unique().tolist()),
        "cohortes":  cohortes,
        "semestres": sorted(df_sub["semestre"].unique().tolist()),
    }


@app.get("/meta/ues", summary="Liste des UEs pour autocomplétion")
def liste_ues(limit: int = 1000):
    """Retourne la liste complète (ou tronquée) des UEs disponibles."""
    df_actuel = get_cached_data()
    return {"ues": sorted(df_actuel["ue"].unique().tolist())[:limit]}


# ENDPOINT UE

@app.get("/ues/{code}/stats", response_model=UEStat, summary="Statistiques d'une UE")
def stats_ue(
    code:     str,
    annee:    Optional[str] = Query(None),
    semestre: Optional[int] = Query(None),
    cohorte:  Optional[int] = Query(None),
    sexe:     Optional[str] = Query(None),
):
    """Retourne toutes les statistiques d'une UE (moyenne, taux, min/max…)."""
    df_actuel = get_cached_data()
    df_ue     = apply_filters_to_df(df_actuel, annee, semestre, cohorte, sexe)
    df_ue     = df_ue[df_ue["ue"] == code]

    if df_ue.empty:
        raise HTTPException(status_code=404, detail="UE introuvable pour ce filtre")

    notes = df_ue["note"]

    def arrondir(val):
        """Arrondit à 2 décimales, retourne None si la valeur est NaN."""
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        return round(float(val), 2)

    moyenne        = notes.mean()        if not notes.empty else None
    taux_reussite  = (notes >= SEUIL_REUSSITE).mean() * 100 if not notes.empty else None
    effectif       = int(df_ue["anonymat"].nunique()) if "anonymat" in df_ue.columns else None
    nombre_admis   = int((notes >= SEUIL_REUSSITE).sum()) if not notes.empty else None
    nombre_ajournes = int((notes < SEUIL_REUSSITE).sum())  if not notes.empty else None

    # Récupération du crédit
    credit = None
    if "credit" in df_ue.columns and not df_ue["credit"].dropna().empty:
        try:
            credit = int(df_ue["credit"].dropna().iloc[0])
        except Exception:
            pass

    semestre_val = None
    if "semestre" in df_ue.columns:
        semestres_ue = df_ue["semestre"].dropna().unique()
        if len(semestres_ue) == 1:
            semestre_val = int(semestres_ue[0])

    est_difficile = (
        (taux_reussite < 50.0 and moyenne < SEUIL_REUSSITE)
        if moyenne is not None and taux_reussite is not None
        else None
    )

    return UEStat(
        ue=code,
        semestre=semestre_val,
        credit=credit,
        moyenne=arrondir(moyenne),
        taux_reussite=arrondir(taux_reussite),
        effectif=effectif,
        isDifficile=est_difficile,
        min_note=arrondir(notes.min()       if not notes.empty else None),
        max_note=arrondir(notes.max()       if not notes.empty else None),
        std_note=arrondir(notes.std(ddof=1) if notes.size > 1  else None),
        mediane_note=arrondir(notes.median() if not notes.empty else None),
        nombre_admis=nombre_admis,
        pourcentage_admis=arrondir(taux_reussite),
        nombre_ajournes=nombre_ajournes,
        pourcentage_ajournes=arrondir(100 - taux_reussite) if taux_reussite is not None else None,
    )


# ENDPOINT DASHBOARD

@app.get(
    "/dashboard/aggregates",
    response_model=DashboardAggregates,
    summary="Agrégats globaux du dashboard",
)
def dashboard(
    annee:    Optional[str] = Query(None),
    semestre: Optional[int] = Query(None),
    cohorte:  Optional[int] = Query(None),
    sexe:     Optional[str] = Query(None),
    ue:       Optional[str] = Query(None),
):
    """
    Retourne tous les agrégats du tableau de bord pour les filtres donnés :
    top/bottom UE, UE difficiles, tableau complet et stats globales.
    """
    df_actuel  = get_cached_data()
    empreinte = cache_donnees.fingerprint
    df_filtre  = apply_filters_to_df(df_actuel, annee, semestre, cohorte, sexe, ue)

    cle_cache = make_cache_key(annee, semestre, cohorte, sexe, ue, "tableau")
    tableau_ue_filtre = get_cached_tableau(cle_cache,empreinte)
    if tableau_ue_filtre is None:
        if df_filtre.empty:
            tableau_ue_filtre = []
        else:
            try:
                ue_df = tableau_ue(df_filtre)
                tableau_ue_filtre = ue_df[ue_df["effectif"] > 0].round(2).to_dict(orient="records")
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
        set_cached_tableau(cle_cache, tableau_ue_filtre,empreinte)

    top_df, bottom_df = top_bottom_ue(df_filtre, n=10)
    difficiles_df     = ue_difficiles(df_filtre)

    # Statistiques globales
    stats = {"effectif_exact": 0}
    if not df_filtre.empty:
        stats = {
            "moyenne_global":       round(float(df_filtre["note"].mean()),   2),
            "taux_reussite_global": round(float((df_filtre["note"] >= SEUIL_REUSSITE).mean() * 100), 2),
            "effectif_exact":       int(df_filtre["anonymat"].nunique()),
            "mediane":              round(float(df_filtre["note"].median()), 2),
            "ecart_type":           round(float(df_filtre["note"].std()),    2),
            "variance":             round(float(df_filtre["note"].var()),    2) if len(df_filtre) > 1 else 0.0,
        }

    return DashboardAggregates(
        top10=top_df.round(2).to_dict(orient="records"),
        bottom10=bottom_df.round(2).to_dict(orient="records"),
        ue_difficiles=difficiles_df.round(2).to_dict(orient="records"),
        tableau_ue=tableau_ue_filtre,
        **stats,
    )


# ENDPOINT PARCOURS ÉTUDIANT

@app.get(
    "/etudiants/{id}/parcours",
    response_model=EtudiantParcours,
    summary="Profil académique d'un étudiant",
)
def parcours_etudiant(
    id:       str,
    annee:    Optional[str] = Query(None),
    semestre: Optional[int] = Query(None),
    cohorte:  Optional[int] = Query(None),
    sexe:     Optional[str] = Query(None),
):
    """
    Retourne le parcours complet d'un étudiant identifié par son numéro
    d'anonymat ou son numéro de carte.
    """
    df_actuel = get_cached_data()
    df_filtre = apply_filters_to_df(df_actuel, annee, semestre, cohorte, sexe)
    etud_df   = df_filtre[(df_filtre["anonymat"] == id) | (df_filtre["carte"] == id)].copy()

    if etud_df.empty:
        raise HTTPException(status_code=404, detail="Étudiant introuvable")

    if "nom_prenoms" in etud_df.columns:
        if etud_df["nom_prenoms"].nunique() > 1:
            noms_trouves = etud_df["nom_prenoms"].unique().tolist()
            raise HTTPException(
                status_code=409, 
                detail=f"L'identifiant '{id}' est partagé par plusieurs étudiants ({noms_trouves})"
            )

    return calculer_parcours_etudiant(etud_df, anonymat_id=etud_df["anonymat"].iloc[0])


# ENDPOINTS FIGURES

@app.get("/figures", summary="Générer ou servir une figure")
def get_figure(
    view:     str           = Query(...),
    annee:    Optional[str] = Query(None),
    semestre: Optional[int] = Query(None),
    cohorte:  Optional[int] = Query(None),
    sexe:     Optional[str] = Query(None),
    ue:       Optional[str] = Query(None),
    fmt:      str           = Query("png", regex="^(png|svg)$"),
):
    """
    Génère et retourne une figure au format PNG ou SVG.
    Utilise le cache disque pour ne pas régénérer une figure identique.
    """
    df_actuel  = get_cached_data()
    empreinte = cache_donnees.fingerprint

    df_filtre   = apply_filters_to_df(df_actuel, annee, semestre, cohorte, sexe, ue)
    cle = make_cache_key(annee, semestre, cohorte, sexe, ue, view)

    # 1. Cache RAM — vérification TTL + fingerprint
    en_cache = get_cached_figure(cle, current_fingerprint=empreinte)
    if en_cache:
        return FileResponse(en_cache, media_type=f"image/{fmt}")

    nom_fichier = f"fig_{hashlib.md5(cle.encode()).hexdigest()}.{fmt}"
    chemin      = build_figure_subpath(FIGURES_DIR, annee, semestre, cohorte, ue, view, nom_fichier)
    meta_chemin = chemin.with_suffix(".meta.json")

    # 2. Cache disque — vérification TTL + fingerprint
    if meta_chemin.exists() and chemin.exists():
        try:
            meta_disk    = json.loads(meta_chemin.read_text())
            duree_valide = time.time() - meta_disk.get("generated_at_ts", 0) <= meta_disk.get("ttl_seconds", CACHE_TTL)
            if duree_valide and meta_disk.get("data_fingerprint") == empreinte:
                set_cached_figure(cle, str(chemin), empreinte)
                return FileResponse(str(chemin), media_type=f"image/{fmt}")
        except Exception:
            pass  # Cache disque invalide → régénération

    # 3. Génération complète
    fonction_tracé = PLOT_DISPATCH.get(view)
    if fonction_tracé is None:
        raise HTTPException(status_code=400, detail=f"Vue inconnue : {view}")

    fig  = fonction_tracé(df_filtre)
    meta = {
        "params": {
            "annee": annee, "semestre": semestre, "cohorte": cohorte,
            "sexe": sexe, "ue": ue, "view": view,
        },
        "data_fingerprint": empreinte,
        "ttl_seconds": CACHE_TTL,
    }
    try:
        save_figure_atomic(fig, chemin, meta, fmt=fmt)
        set_cached_figure(cle, str(chemin), empreinte)
        return FileResponse(str(chemin), media_type=f"image/{fmt}")
    except Exception as e:
        logger.exception("Erreur sauvegarde figure : %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ENDPOINT ADMINISTRATION

@app.post("/admin/cache/clear", summary="Vider les caches et recharger les données")
def admin_vider_cache(
    all: bool           = Query(True),
    key: Optional[str] = Query(None),
):
    """
    Vide le cache mémoire et les figures sur disque.
    Le prochain appel à get_cached_data() rechargera automatiquement les données.
    """
    try:
        clear_cache(all_keys=all, key=key)
        # On force le rechargement immédiat en réinitialisant le timestamp du cache
        cache_donnees.timestamp  = 0.0
        cache_donnees.file_mtime = 0.0

        df_frais = get_cached_data()   # Rechargement immédiat
        logger.info(f"Cache vidé + données rechargées : {len(df_frais)} lignes")
        return {"status": "ok", "rows": len(df_frais)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# LANCEMENT DIRECT

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
