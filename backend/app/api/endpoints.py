"""
endpoints.py — Routes FastAPI GoodAdmin
"""
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Depends, Request
from app.core.security import get_current_user
from app.core.limiter import limiter
from fastapi.responses import FileResponse
from typing import Annotated, Optional
import numpy as np
import pandas as pd
import json, time, hashlib, logging
from pathlib import Path

from app.models.schemas import (
    ErrorResponse, HealthResponse, UEStat,
    DashboardAggregates, EtudiantParcours, UploadResult,
)
from app.services.data_processing import get_cached_data, apply_filters_to_df, cache_donnees
from app.services.analytics import (
    tableau_ue, top_bottom_ue, ue_difficiles, calculer_parcours_etudiant,
    etudiants_a_risque, calculer_performance_par_dimension,
    calculer_stats_notes, calculer_stats_df,
)
from app.services.interpreter import interpreter
from app.services.plotting import (
    PLOT_DISPATCH, make_cache_key,
    get_cached_figure, set_cached_figure,
    get_cached_tableau, set_cached_tableau,
    clear_cache, save_figure_atomic, build_figure_subpath,
)
from app.core.config import FIGURES_DIR, CACHE_TTL, SEUIL_REUSSITE, MAX_UPLOAD_SIZE

logger = logging.getLogger(__name__)
router = APIRouter()

AnneeQuery          = Annotated[Optional[str], Query()]
SemestreQuery       = Annotated[Optional[str], Query()]
CohorteQuery        = Annotated[Optional[str], Query()]
SexeQuery           = Annotated[Optional[str], Query()]
FiliereQuery        = Annotated[Optional[str], Query()]
DepartementQuery    = Annotated[Optional[str], Query()]
TypeFormationQuery  = Annotated[Optional[str], Query()]
NiveauQuery         = Annotated[Optional[str], Query()]
UeQuery             = Annotated[Optional[str], Query()]



def _figure_response(path: str, fmt: str) -> FileResponse:
    return FileResponse(path, media_type=f"image/{fmt}",
                        headers={"Cache-Control": f"private, max-age={CACHE_TTL}"})


def _arrondir(val) -> Optional[float]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return float(np.round(val, 2))





def _scores_sans_dept(df_actuel, annee, semestre, cohorte, sexe, ue,
                        filiere, type_formation, niveau) -> list:
    """Classement inter-départements."""
    df_all_depts = apply_filters_to_df(
        df_actuel, annee, semestre, cohorte, sexe, ue,
        filiere=filiere, departement=None,
        type_formation=type_formation, niveau=niveau,
    )
    return calculer_performance_par_dimension(df_all_depts, "departement")


def _get_dashboard_data(df_filtre, df_actuel, filters_dict, empreinte) -> dict:
    cle_cache = make_cache_key(
        annee=filters_dict.get("annee"),
        semestre=filters_dict.get("semestre"),
        cohorte=filters_dict.get("cohorte"),
        sexe=filters_dict.get("sexe"),
        ue=filters_dict.get("ue"),
        filiere=filters_dict.get("filiere"),
        departement=filters_dict.get("departement"),
        type_formation=filters_dict.get("type_formation"),
        niveau=filters_dict.get("niveau"),
        vue="dashboard_data"
    )

    cached_data = get_cached_tableau(cle_cache, empreinte)
    if cached_data is not None:
        return cached_data

    stats = calculer_stats_df(df_filtre)

    ue_df = tableau_ue(df_filtre)
    tableau_ue_list = ue_df[ue_df["effectif"] > 0].round(2).to_dict(orient="records")
    
    top_df, bottom_df = top_bottom_ue(df_filtre, n=10, agg_df=ue_df)
    difficiles        = ue_difficiles(df_filtre, agg_df=ue_df)
    
    result = {
        "stats":         stats,
        "tableau_ue":    tableau_ue_list,
        "top10":         top_df.round(2).to_dict(orient="records"),
        "bottom10":      bottom_df.round(2).to_dict(orient="records"),
        "ue_difficiles": difficiles.round(2).to_dict(orient="records"),
        "risques":       etudiants_a_risque(df_filtre),
        "scores_depts":  _scores_sans_dept(
            df_actuel,
            filters_dict.get("annee"),    filters_dict.get("semestre"),
            filters_dict.get("cohorte"),  filters_dict.get("sexe"),
            filters_dict.get("ue"),       filters_dict.get("filiere"),
            filters_dict.get("type_formation"), filters_dict.get("niveau"),
        ),
    }
    
    set_cached_tableau(cle_cache, result, empreinte)
    return result


#  Santé 

@router.get("/health", response_model=HealthResponse, tags=["Santé"])
def sante():
    df = get_cached_data()
    return HealthResponse(status="ok", rows=len(df), indicateurs_charges=True)


#  Comparaison

@router.get("/compare", tags=["Dashboard"], summary="Comparaison multi-entités")
def compare_entities(
    type:     Annotated[str, Query()] = "filiere",
    entites:  Annotated[str, Query()] = "",
    annee:    AnneeQuery   = None,
    cohorte:  CohorteQuery = None,
    semestre: SemestreQuery = None,
):

    df_actuel = get_cached_data()
    if df_actuel.empty:
        return {"entites": [], "type": type}

    noms = [e.strip() for e in entites.split(",") if e.strip()]
    if not noms:
        col = {"filiere": "filiere", "departement": "departement", "ue": "ue"}.get(type, "filiere")
        if col in df_actuel.columns:
            all_noms = sorted(df_actuel[col].dropna().astype(str).unique().tolist())
            noms = all_noms[:30]

    resultats = []
    for nom in noms:
        df_e = apply_filters_to_df(
            df_actuel, annee, semestre, cohorte, None,
            ue          =(nom if type == "ue"          else None),
            filiere     =(nom if type == "filiere"     else None),
            departement =(nom if type == "departement" else None),
        )
        if df_e.empty:
            continue
        notes = pd.to_numeric(df_e["note"], errors="coerce").dropna()
        if notes.empty:
            continue
        stats = calculer_stats_notes(notes)
        stats["effectif"] = int(df_e["anonymat"].nunique()) if "anonymat" in df_e.columns else len(notes)
        stats["nom"]     = nom
        stats["nb_ues"]  = int(df_e["ue"].nunique()) if "ue" in df_e.columns else 0
        resultats.append(stats)

    return {"entites": resultats, "type": type, "nb": len(resultats)}


#  Métadonnées

@router.get("/meta/disponibilites", tags=["Métadonnées"])
def disponibilites(
    annee:          AnneeQuery         = None,
    cohorte:        CohorteQuery       = None,
    filiere:        FiliereQuery       = None,
    departement:    DepartementQuery   = None,
    type_formation: TypeFormationQuery = None,
    niveau:         NiveauQuery        = None,
    semestre:       SemestreQuery      = None,
):
    df = get_cached_data()
    filters = {
        "annee": annee, "cohorte": cohorte, "filiere": filiere,
        "departement": departement, "type_formation": type_formation,
        "niveau": niveau, "semestre": semestre,
    }
    def opts(dim):
        other = {k: v for k, v in filters.items() if k != dim}
        df_tmp = apply_filters_to_df(df, **other)
        return sorted(str(x) for x in df_tmp[dim].unique() if pd.notnull(x)) if not df_tmp.empty else []

    return {
        "annees": opts("annee"), "cohortes": opts("cohorte"),
        "semestres": opts("semestre"), "filieres": opts("filiere"),
        "departements": opts("departement"), "types_formation": opts("type_formation"),
        "niveaux": opts("niveau"), "sexes": opts("sexe"),
    }


@router.get("/meta/ues", tags=["Métadonnées"])
def liste_ues(limit: int = 1000):
    df = get_cached_data()
    all_ues = sorted(df["ue"].unique().tolist())
    return {"ues": all_ues[:limit]}


#  Statistiques UE

@router.get("/ues/{code}/stats", response_model=UEStat, tags=["Statistiques UE"],
            responses={404: {"model": ErrorResponse}})
def stats_ue(
    code:           str,
    annee:          AnneeQuery         = None,
    semestre:       SemestreQuery      = None,
    cohorte:        CohorteQuery       = None,
    sexe:           SexeQuery          = None,
    filiere:        FiliereQuery       = None,
    departement:    DepartementQuery   = None,
    type_formation: TypeFormationQuery = None,
    niveau:         NiveauQuery        = None,
):
    df_actuel = get_cached_data()
    df_ue = apply_filters_to_df(
        df_actuel, annee, semestre, cohorte, sexe,
        filiere=filiere, departement=departement,
        type_formation=type_formation, niveau=niveau,
    )
    df_ue = df_ue[df_ue["ue"] == code]
    if df_ue.empty:
        raise HTTPException(status_code=404, detail="UE introuvable")

    stats = calculer_stats_notes(df_ue["note"])
    # Effectif unique par anonymat
    effectif = int(df_ue["anonymat"].nunique()) if "anonymat" in df_ue.columns else len(df_ue)
    
    # Infos spécifiques à l'UE
    credit = int(df_ue["credit"].dropna().iloc[0]) if "credit" in df_ue.columns and not df_ue["credit"].dropna().empty else None
    semestres = df_ue["semestre"].dropna().unique()
    semestre_val = int(semestres[0]) if len(semestres) == 1 else None
    
    est_difficile = (stats["taux_reussite"] is not None and stats["moyenne"] is not None 
                        and stats["taux_reussite"] < 50 and stats["moyenne"] < SEUIL_REUSSITE)

    return UEStat(
        ue=code, semestre=semestre_val, credit=credit,
        moyenne=stats["moyenne"],
        taux_reussite=stats["taux_reussite"],
        effectif=effectif,
        isDifficile=est_difficile,
        min_note=stats["min"],
        max_note=stats["max"],
        std_note=stats["ecart_type"],
        variance_note=stats["variance"],
        mediane_note=stats["mediane"],
        q1_note=stats["q1"],
        q3_note=stats["q3"],
        iqr_note=stats["iqr"],
        nombre_admis=int((df_ue["note"] >= SEUIL_REUSSITE).sum()),
        pourcentage_admis=stats["taux_reussite"],
        nombre_ajournes=int((df_ue["note"] < SEUIL_REUSSITE).sum()),
        pourcentage_ajournes=_arrondir(100 - stats["taux_reussite"] if stats["taux_reussite"] is not None else None),
    )


#  Dashboard 

@router.get("/dashboard/aggregates", response_model=DashboardAggregates, tags=["Dashboard"],
            responses={400: {"model": ErrorResponse}})
def dashboard(
    annee:          AnneeQuery         = None,
    semestre:       SemestreQuery      = None,
    cohorte:        CohorteQuery       = None,
    sexe:           SexeQuery          = None,
    filiere:        FiliereQuery       = None,
    ue:             UeQuery            = None,
    departement:    DepartementQuery   = None,
    type_formation: TypeFormationQuery = None,
    niveau:         NiveauQuery        = None,
):
    df_actuel = get_cached_data()

    df_filtre = apply_filters_to_df(
        df_actuel, annee, semestre, cohorte, sexe, ue,
        filiere=filiere, departement=departement,
        type_formation=type_formation, niveau=niveau,
    )

    f_dict = {"annee": annee, "semestre": semestre, "cohorte": cohorte, "sexe": sexe, "ue": ue,
                "filiere": filiere, "departement": departement, "type_formation": type_formation, "niveau": niveau}
    
    empreinte = cache_donnees.fingerprint
    d = _get_dashboard_data(df_filtre, df_actuel, f_dict, empreinte)
    s = d["stats"]

    return DashboardAggregates(
        top10         = d["top10"],
        bottom10      = d["bottom10"],
        ue_difficiles = d["ue_difficiles"],
        tableau_ue    = d["tableau_ue"],
        risques       = d["risques"],
        scores_depts  = d["scores_depts"],
        moyenne_global       = s.get("moyenne"),
        taux_reussite_global = s.get("taux_reussite"),
        effectif_exact       = s.get("effectif_exact", 0),
        mediane              = s.get("mediane"),
        ecart_type           = s.get("ecart_type"),
        variance             = s.get("variance"),
        q1                   = s.get("q1"),
        q3                   = s.get("q3"),
        iqr                  = s.get("iqr"),
    )


#  Parcours étudiant

@router.get("/etudiants/{id}/parcours", response_model=EtudiantParcours, tags=["Étudiants"],
            responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}})
def parcours_etudiant(
    id:             str,
    annee:          AnneeQuery         = None,
    semestre:       SemestreQuery      = None,
    cohorte:        CohorteQuery       = None,
    sexe:           SexeQuery          = None,
    filiere:        FiliereQuery       = None,
    departement:    DepartementQuery   = None,
    type_formation: TypeFormationQuery = None,
    niveau:         NiveauQuery        = None,
):
    df_actuel = get_cached_data()
    df_filtre = apply_filters_to_df(
        df_actuel, annee, semestre, cohorte, sexe,
        filiere=filiere, departement=departement,
        type_formation=type_formation, niveau=niveau,
    )
    etud_df = df_filtre[(df_filtre["anonymat"] == id) | (df_filtre["carte"] == id)].copy()
    if etud_df.empty:
        raise HTTPException(status_code=404, detail="Étudiant introuvable")
    if "nom_prenoms" in etud_df.columns and etud_df["nom_prenoms"].nunique() > 1:
        raise HTTPException(status_code=409,
            detail=f"Identifiant '{id}' partagé par plusieurs étudiants ({etud_df['nom_prenoms'].unique().tolist()})")
    return calculer_parcours_etudiant(etud_df, anonymat_id=etud_df["anonymat"].iloc[0], df_global=df_actuel)


#  Interprétation 

@router.get("/interpret", tags=["Interprétation"],
            responses={400: {"model": ErrorResponse}})
def interpret(
    context:              Annotated[str, Query(...)],
    # Stats directes (contextes ue / etudiant)
    moyenne:              Optional[float] = Query(None),
    taux_reussite:        Optional[float] = Query(None),
    effectif:             Optional[int]   = Query(None),
    mediane:              Optional[float] = Query(None),
    ecart_type:           Optional[float] = Query(None),
    variance:             Optional[float] = Query(None),
    min_note:             Optional[float] = Query(None),
    max_note:             Optional[float] = Query(None),
    mediane_note:         Optional[float] = Query(None),
    std_note:             Optional[float] = Query(None),
    nombre_admis:         Optional[int]   = Query(None),
    nombre_ajournes:      Optional[int]   = Query(None),
    pourcentage_admis:    Optional[float] = Query(None),
    pourcentage_ajournes: Optional[float] = Query(None),
    is_difficile:         Optional[bool]  = Query(None, alias="isDifficile"),
    credit:               Optional[int]   = Query(None),
    semestre:             Optional[int]   = Query(None),
    ue:                   Optional[str]   = Query(None),
    filiere:              Optional[str]   = Query(None),
    departement:          Optional[str]   = Query(None),
    moyenne_global:       Optional[float] = Query(None),
    taux_reussite_global: Optional[float] = Query(None),
    effectif_exact:       Optional[int]   = Query(None),
    q1:                   Optional[float] = Query(None),
    q3:                   Optional[float] = Query(None),
    iqr:                  Optional[float] = Query(None),
    _nb_ue_difficiles:    Optional[int]   = Query(None),
    _nb_risques:          Optional[int]   = Query(None),
    annee:          AnneeQuery         = None,
    cohorte:        CohorteQuery       = None,
    sexe:           SexeQuery          = None,
    type_formation: TypeFormationQuery = None,
    niveau:         NiveauQuery        = None,
):
    if context in ("dashboard", "filiere", "departement"):
        has_inline_stats = (
            context != "departement"
            and (moyenne_global is not None or taux_reussite_global is not None)
        )
        if has_inline_stats:
            data = {
                "moyenne_global":       moyenne_global,
                "taux_reussite_global": taux_reussite_global,
                "effectif_exact":       effectif_exact or 0,
                "mediane":              mediane,
                "ecart_type":           ecart_type,
                "variance":             variance,
                "q1":                   q1,
                "q3":                   q3,
                "iqr":                  iqr,
                "ue_difficiles":        [{}] * (_nb_ue_difficiles or 0),
                "risques":              [{}] * (_nb_risques or 0),
                "tableau_ue":           [],
                "scores_depts":         [],
                "filiere":              filiere or "",
                "departement":          departement or "",
                "annee":                annee,
                "cohorte":              cohorte,
                "semestre":             semestre,
                "type_formation":       type_formation,
                "niveau":               niveau,
            }
        else:
            df_actuel = get_cached_data()
            df_filtre = apply_filters_to_df(
                df_actuel, annee, semestre, cohorte, sexe,
                ue=ue, filiere=filiere, departement=departement,
                type_formation=type_formation, niveau=niveau,
            )
            if df_filtre.empty:
                return interpreter(context, {})

            f_dict = {"annee": annee, "semestre": semestre, "cohorte": cohorte, "sexe": sexe, "ue": ue,
                      "filiere": filiere, "departement": departement, "type_formation": type_formation, "niveau": niveau}
            empreinte = cache_donnees.fingerprint
            d = _get_dashboard_data(df_filtre, df_actuel, f_dict, empreinte)
            s = d["stats"]

            data = {
                **s,
                "moyenne_global":       s.get("moyenne"),
                "taux_reussite_global": s.get("taux_reussite"),
                "ue_difficiles":        d["ue_difficiles"],
                "risques":              d["risques"],
                "tableau_ue":           d["tableau_ue"],
                "scores_depts":         d["scores_depts"],
                "filiere":              filiere or "",
                "departement":          departement or "",
            }

    elif context in ("ue", "etudiant"):
        data = {k: v for k, v in {
            "ue": ue, "moyenne": moyenne, "taux_reussite": taux_reussite,
            "effectif": effectif, "mediane": mediane, "ecart_type": ecart_type,
            "variance": variance, "min_note": min_note, "max_note": max_note,
            "mediane_note": mediane_note, "std_note": std_note,
            "nombre_admis": nombre_admis, "nombre_ajournes": nombre_ajournes,
            "pourcentage_admis": pourcentage_admis,
            "pourcentage_ajournes": pourcentage_ajournes,
            "isDifficile": is_difficile, "credit": credit, "semestre": semestre,
        }.items() if v is not None}
    else:
        raise HTTPException(status_code=400,
            detail=f"Contexte '{context}' invalide. Acceptés : ue, dashboard, filiere, departement, etudiant")

    return interpreter(context, data)


#  Figures 

_FIGURE_FILTER_RULES = {
    "heatmap_filiere_semestre": dict(annee=None, semestre=None, niveau=None, filiere=None),
    "courbe_cohortes":          dict(annee=None, semestre=None, cohorte=None),
    "donut":                    dict(annee=None, semestre=None, sexe=None),
    "boxplot_by_sex":           dict(annee=None, semestre=None, sexe=None),
    "courbe_moyenne_par_sexe":  dict(annee=None, semestre=None, sexe=None),
    "heatmap_ue_semestre":      dict(annee=None, filiere=None, niveau=None),
}


def _build_fig_df(view, df_actuel, annee, semestre, cohorte, sexe, ue,
                  filiere, departement, type_formation, niveau):
    overrides = _FIGURE_FILTER_RULES.get(view, {})
    return apply_filters_to_df(
        df_actuel,
        annee         =overrides.get("annee",          annee),
        semestre      =overrides.get("semestre",        semestre),
        cohorte       =overrides.get("cohorte",         cohorte),
        sexe          =overrides.get("sexe",            sexe),
        ue            =overrides.get("ue",              ue),
        filiere       =overrides.get("filiere",         filiere),
        departement   =departement,
        type_formation=overrides.get("type_formation",  type_formation),
        niveau        =overrides.get("niveau",          niveau),
    )


@router.get("/figures", tags=["Visualisations"],
            responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}})
@limiter.limit("20/minute")
def get_figure(
    request:        Request,
    view:           Annotated[str, Query(...)],
    annee:          AnneeQuery         = None,
    semestre:       SemestreQuery      = None,
    cohorte:        CohorteQuery       = None,
    sexe:           SexeQuery          = None,
    ue:             UeQuery            = None,
    filiere:        FiliereQuery       = None,
    departement:    DepartementQuery   = None,
    type_formation: TypeFormationQuery = None,
    niveau:         NiveauQuery        = None,
    fmt:            Annotated[str, Query(pattern="^(png|svg)$")] = "png",
    _v:             Annotated[Optional[int], Query()] = None,
):
    df_actuel = get_cached_data()
    empreinte = cache_donnees.fingerprint
    df_filtre = apply_filters_to_df(
        df_actuel, annee, semestre, cohorte, sexe, ue,
        filiere=filiere, departement=departement,
        type_formation=type_formation, niveau=niveau,
    )
    cle = make_cache_key(annee, semestre, cohorte, sexe, ue, view,
                         filiere=filiere, departement=departement,
                         type_formation=type_formation, niveau=niveau)

    # Cache en mémoire
    en_cache = get_cached_figure(cle, current_fingerprint=empreinte)
    if en_cache and Path(en_cache).exists():
        return _figure_response(en_cache, fmt)
    elif en_cache:
        set_cached_figure(cle, None, empreinte)

    # Cache disque
    nom_fichier = f"fig_{hashlib.md5(cle.encode()).hexdigest()}.{fmt}"
    chemin      = build_figure_subpath(FIGURES_DIR, annee, semestre, cohorte, ue, view, nom_fichier, filiere=filiere)
    meta_chemin = chemin.with_suffix(".meta.json")
    if meta_chemin.exists() and chemin.exists():
        try:
            meta_disk = json.loads(meta_chemin.read_text())
            if (time.time() - meta_disk.get("generated_at_ts", 0) <= meta_disk.get("ttl_seconds", CACHE_TTL)
                    and meta_disk.get("data_fingerprint") == empreinte):
                set_cached_figure(cle, str(chemin), empreinte)
                return _figure_response(str(chemin), fmt)
        except Exception:
            pass

    if view == "student_cohorte":
        etud_id = ue
        if not etud_id:
            raise HTTPException(status_code=400, detail="Paramètre 'ue' requis (id étudiant)")
        etud_df = df_actuel[(df_actuel["anonymat"] == etud_id) | (df_actuel["carte"] == etud_id)]
        if etud_df.empty:
            raise HTTPException(status_code=404, detail="Étudiant introuvable")
        coh_val = str(etud_df["cohorte"].iloc[0])
        fil_val = str(etud_df["filiere"].iloc[0]) if "filiere" in etud_df.columns else None
        df_coh  = apply_filters_to_df(df_actuel, None, None, coh_val, None,
                                       filiere=fil_val, departement=departement)
        from app.services.plotting import plot_student_vs_cohorte as _plot_sc
        cle_sc   = make_cache_key(None, None, coh_val, None, etud_id, "student_cohorte", filiere=fil_val)
        cache_sc = get_cached_figure(cle_sc, current_fingerprint=empreinte)
        if cache_sc and Path(cache_sc).exists():
            return _figure_response(cache_sc, fmt)
        nom_sc  = f"fig_{hashlib.md5(cle_sc.encode()).hexdigest()}.{fmt}"
        ch_sc   = build_figure_subpath(FIGURES_DIR, None, None, None, None, "student_cohorte", nom_sc)
        fig_sc  = _plot_sc(etud_df, df_coh)
        save_figure_atomic(fig_sc, ch_sc, {"data_fingerprint": empreinte, "ttl_seconds": CACHE_TTL}, fmt=fmt)
        set_cached_figure(cle_sc, str(ch_sc), empreinte)
        return _figure_response(str(ch_sc), fmt)

    fn = PLOT_DISPATCH.get(view)
    if fn is None:
        raise HTTPException(status_code=400, detail=f"Vue inconnue : {view}")

    df_pour_fig = _build_fig_df(view, df_actuel, annee, semestre, cohorte, sexe, ue,
                                filiere, departement, type_formation, niveau)
    fig  = fn(df_pour_fig)
    meta = {"data_fingerprint": empreinte, "ttl_seconds": CACHE_TTL,
            "params": {"view": view, "annee": annee, "semestre": semestre,
                       "cohorte": cohorte, "filiere": filiere, "departement": departement}}
    try:
        save_figure_atomic(fig, chemin, meta, fmt=fmt)
        set_cached_figure(cle, str(chemin), empreinte)
        return _figure_response(str(chemin), fmt)
    except Exception as e:
        logger.exception("Erreur figure : %s", e)
        raise HTTPException(status_code=500, detail=str(e))


#  Administration 

@router.post("/admin/cache/clear", tags=["Administration"],
             responses={500: {"model": ErrorResponse}})
def admin_vider_cache(
    current_user: Annotated[str, Depends(get_current_user)],
    all: Annotated[bool, Query()] = True,
    key: Annotated[Optional[str], Query()] = None,
):
    try:
        clear_cache(all_keys=all, key=key)
        cache_donnees.timestamp  = 0.0
        cache_donnees.file_mtime = 0.0
        return {"status": "ok", "rows": len(get_cached_data())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/admin/upload-history", tags=["Administration"])
def admin_upload_history(
    current_user: Annotated[str, Depends(get_current_user)]
):
    from app.services.upload import get_upload_history
    return get_upload_history(limit=50)


@router.post("/admin/rollback", tags=["Administration"])
def admin_rollback(
    current_user: Annotated[str, Depends(get_current_user)]
):
    from app.services.upload import rollback_data
    try:
        return rollback_data(user=current_user)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#  Upload 

@router.post("/data/upload", response_model=UploadResult, tags=["Données"],
             responses={400: {"model": ErrorResponse}, 413: {"model": ErrorResponse}, 500: {"model": ErrorResponse}})
async def upload_data(
    current_user: Annotated[str, Depends(get_current_user)],
    file: UploadFile = File(...)
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Nom de fichier manquant")
    suffix = Path(file.filename).suffix.lower()
    if suffix not in {".csv", ".parquet", ".xlsx", ".xls"}:
        raise HTTPException(status_code=400, detail=f"Format non supporté : '{suffix}'")
    content = await file.read()
    if len(content) > MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=413,
            detail=f"Fichier trop volumineux ({len(content)/(1024*1024):.1f} MB). Max : {MAX_UPLOAD_SIZE/(1024*1024):.0f} MB")
    try:
        from app.services.upload import process_upload
        return UploadResult(**process_upload(content, file.filename))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Upload : %s", e)
        raise HTTPException(status_code=500, detail=str(e))


#  Rapport PDF 

@router.get("/reports/pdf", tags=["Rapports"],
            responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}})
@limiter.limit("5/minute")
def export_pdf(
    request:        Request,
    current_user:   Annotated[str, Depends(get_current_user)],
    annee:          AnneeQuery         = None,
    semestre:       SemestreQuery      = None,
    cohorte:        CohorteQuery       = None,
    sexe:           SexeQuery          = None,
    ue:             UeQuery            = None,
    filiere:        FiliereQuery       = None,
    departement:    DepartementQuery   = None,
    type_formation: TypeFormationQuery = None,
    niveau:         NiveauQuery        = None,
    anonymat:       Annotated[Optional[str], Query()] = None,
    _context:       Annotated[Optional[str], Query()] = None,
    _label:         Annotated[Optional[str], Query()] = None,
):
    df_actuel = get_cached_data()
    if anonymat:
        df_filtre = df_actuel[(df_actuel["anonymat"] == anonymat) | (df_actuel["carte"] == anonymat)]
    else:
        df_filtre = apply_filters_to_df(
            df_actuel, annee, semestre, cohorte, sexe, ue,
            filiere=filiere, departement=departement,
            type_formation=type_formation, niveau=niveau,
        )
    if df_filtre.empty:
        raise HTTPException(status_code=404, detail="Aucune donnée pour ces filtres")

    try:
        from app.services.pdf_export import generate_report_pdf
        labels = {
            "departement": f"Département {departement}", "filiere": f"Filière {filiere}",
            "ue": f"UE {ue}", "etudiant": _label or f"Étudiant {anonymat}", "dashboard": "Vue Globale",
        }
        ctx   = _context or "dashboard"
        label = _label or labels.get(ctx, "Rapport Académique")
        filtres_dict = {"annee": annee, "semestre": semestre, "cohorte": cohorte,
                        "sexe": sexe, "ue": ue, "filiere": filiere,
                        "departement": departement, "type_formation": type_formation, "niveau": niveau}
        
        df_extra = None
        if ctx == "etudiant" and not df_filtre.empty:
            coh_val = df_filtre["cohorte"].iloc[0] if "cohorte" in df_filtre.columns else None
            fil_val = df_filtre["filiere"].iloc[0] if "filiere" in df_filtre.columns else None
            if coh_val is not None:
                mask = df_actuel["cohorte"].astype(str).str.strip() == str(coh_val).strip()
                if fil_val:
                    mask &= df_actuel["filiere"].astype(str).str.strip() == str(fil_val).strip()
                df_extra = df_actuel[mask]

        pdf_path = generate_report_pdf(df_filtre, filtres_dict, context=ctx, label=label, df_extra=df_extra)
        return FileResponse(str(pdf_path), media_type="application/pdf",
                            filename=f"rapport_{ctx}_{int(time.time())}.pdf")
    except Exception as e:
        logger.exception("PDF : %s", e)
        raise HTTPException(status_code=500, detail=f"Erreur PDF : {e}")
