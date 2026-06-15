
from __future__ import annotations
from typing import Dict, List, Optional, Set


#   departement  : GI | GM | GC | GE
#   type         : LF (Licence Fondamentale) | LP (Licence Professionnelle)

REFERENTIEL_FILIERES: Dict[str, Dict[str, str]] = {

    # Département GI — Génie Informatique 
    "IS":    {
        "departement": "GI",
        "type":        "LF",
        "label":       "Informatique des Systèmes",
    },
    "IABD":  {
        "departement": "GI",
        "type":        "LF",
        "label":       "IA & Big Data",
    },
    "LT":    {
        "departement": "GI",
        "type":        "LF",
        "label":       "Logiciel & Télécom",
    },
    "GL":    {
        "departement": "GI",
        "type":        "LP",
        "label":       "Génie Logiciel",
    },
    "SRI":   {
        "departement": "GI",
        "type":        "LP",
        "label":       "Systèmes Réseaux & Infrastructure",
    },

    #  Département GM — Génie Mécanique
    "GM":    {
        "departement": "GM",
        "type":        "LF",
        "label":       "Génie Mécanique",
    },
    "LP-GM": {
        "departement": "GM",
        "type":        "LP",
        "label":       "Génie Mécanique Professionnel",
    },

    #  Département GC — Génie Civil 
    "GC":    {
        "departement": "GC",
        "type":        "LF",
        "label":       "Génie Civil",
    },
    "LP-GC": {
        "departement": "GC",
        "type":        "LP",
        "label":       "Génie Civil Professionnel",
    },

    #  Département GE — Génie Électrique 
    "GE":    {
        "departement": "GE",
        "type":        "LF",
        "label":       "Génie Électrique",
    },
    "LP-GE": {
        "departement": "GE",
        "type":        "LP",
        "label":       "Génie Électrique Professionnel",
    },
}


#  ENSEMBLES DÉRIVÉS (calculés une seule fois au chargement du module) 


FILIERES_LP: Set[str] = {
    code for code, info in REFERENTIEL_FILIERES.items()
    if info["type"] == "LP"
}

FILIERES_PAR_DEPARTEMENT: Dict[str, List[str]] = {}
for _code, _info in REFERENTIEL_FILIERES.items():
    FILIERES_PAR_DEPARTEMENT.setdefault(_info["departement"], []).append(_code)


FILIERES_PAR_TYPE: Dict[str, List[str]] = {}
for _code, _info in REFERENTIEL_FILIERES.items():
    FILIERES_PAR_TYPE.setdefault(_info["type"], []).append(_code)

# Liste des départements
DEPARTEMENTS: List[str] = ["GI", "GM", "GC", "GE"]

TYPES_FORMATION: List[str] = ["LF", "LP", "M"]

# Seuils semestriels
SEMESTRE_LICENCE_MIN = 1
SEMESTRE_LICENCE_MAX = 6
SEMESTRE_MASTER_MIN  = 7
SEMESTRE_MASTER_MAX  = 10


#  FONCTIONS UTILITAIRES

def get_niveau(filiere: str, semestre: int) -> str:
    """
    Déduit le niveau d'études depuis le code filière et le numéro de semestre.
    """
    if filiere not in REFERENTIEL_FILIERES:
        return "Inconnu"
    if REFERENTIEL_FILIERES[filiere]["type"] == "LP":
        return "Licence"
    return "Master" if semestre >= SEMESTRE_MASTER_MIN else "Licence"


def get_departement(filiere: str) -> Optional[str]:
    """Retourne le département d'une filière, ou None si inconnue."""
    info = REFERENTIEL_FILIERES.get(filiere)
    return info["departement"] if info else None


def get_type_formation(filiere: str, semestre: int) -> Optional[str]:
    """
    Retourne le type de formation (LF/LP/M)
    """
    niveau = get_niveau(filiere, semestre)
    if niveau == "Master":
        return "M"
        
    info = REFERENTIEL_FILIERES.get(filiere)
    return info["type"] if info else None


def get_label(filiere: str) -> str:
    """Retourne l'intitulé complet d'une filière."""
    info = REFERENTIEL_FILIERES.get(filiere)
    return info["label"] if info else filiere


def filieres_pour(
    departement: Optional[str] = None,
    type_formation: Optional[str] = None,
) -> List[str]:
    """
    Retourne la liste des filières correspondant aux critères donnés.
    Si les deux paramètres sont None, retourne toutes les filières.

    Exemples :
        filieres_pour("GI")           → ["IS", "IABD", "LT", "GL", "SRI"]
        filieres_pour("GI", "LF")     → ["IS", "IABD", "LT"]
        filieres_pour(type_formation="LP") → ["GL", "SRI", "LP-GM", "LP-GC", "LP-GE"]
    """
    resultat = []
    for code, info in REFERENTIEL_FILIERES.items():
        if departement    and info["departement"] != departement:
            continue
        if type_formation and info["type"]        != type_formation:
            continue
        resultat.append(code)
    return resultat


def valider_filiere(filiere: str) -> bool:
    """Retourne True si le code filière existe dans le référentiel."""
    return filiere in REFERENTIEL_FILIERES
