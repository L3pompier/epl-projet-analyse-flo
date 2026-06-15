from typing import Any, Dict, List, Optional
from pydantic import BaseModel

class ErrorResponse(BaseModel):
    error:  str
    detail: Optional[str] = None
    code:   int

class HealthResponse(BaseModel):
    status:              str
    rows:                int
    indicateurs_charges: bool

class UEStat(BaseModel):
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
    variance_note:       Optional[float] = None
    mediane_note:        Optional[float] = None
    q1_note:             Optional[float] = None
    q3_note:             Optional[float] = None
    iqr_note:            Optional[float] = None
    nombre_admis:        Optional[int]   = None
    pourcentage_admis:   Optional[float] = None
    nombre_ajournes:     Optional[int]   = None
    pourcentage_ajournes: Optional[float] = None

class EtudiantParcours(BaseModel):
    anonymat:              str
    carte:                 Optional[str]   = None
    nom_prenoms:           Optional[str]   = None
    sexe:                  Optional[str]   = None
    cohorte:               Optional[int]   = None
    filiere:               Optional[str]   = None
    parcours:              List[Dict[str, Any]]
    moyenne_globale:       Optional[float] = None
    taux_reussite_global:  Optional[float] = None
    credits_total:         Optional[int]   = None
    credits_valides:       Optional[int]   = None
    palmares_cohorte:      Optional[List[Dict[str, Any]]] = None
    rang:                  Optional[int]   = None
    nb_cohorte:            Optional[int]   = None

class DashboardAggregates(BaseModel):
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
    q1:                    Optional[float] = None
    q3:                    Optional[float] = None
    iqr:                   Optional[float] = None
    min:                   Optional[float] = None
    max:                   Optional[float] = None
    risques:               Optional[List[Dict[str, Any]]] = None
    scores_depts:          Optional[List[Dict[str, Any]]] = None

class UploadResult(BaseModel):
    added:         int
    updated:       int
    total:         int
    lignes_fichier: Optional[int] = None
    lignes_valides: Optional[int] = None
    errors:        List[str] = []
    warnings:      List[str] = []

