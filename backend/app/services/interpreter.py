"""
interpreter.py
Moteur d'interprétation pédagogique par règles déterministes.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional
from app.core.config import SEUIL_REUSSITE


def _niv_taux(t: Optional[float]) -> str:
    if t is None: return "info"
    if t >= 75: return "success"
    if t >= 50: return "info"
    if t >= 35: return "warning"
    return "danger"

def _niv_moy(m: Optional[float]) -> str:
    if m is None: return "info"
    if m >= 14: return "success"
    if m >= 12: return "info"
    if m >= 10: return "warning"
    return "danger"

def _q_taux(t: Optional[float]) -> str:
    if t is None: return "inconnue"
    if t >= 85: return "excellente"
    if t >= 70: return "très bonne"
    if t >= 55: return "bonne"
    if t >= 50: return "acceptable"
    if t >= 35: return "préoccupante"
    return "critique"

def _q_moy(m: Optional[float]) -> str:
    if m is None: return "non définie"
    if m >= 16: return "excellente"
    if m >= 14: return "solide"
    if m >= 12: return "satisfaisante"
    if m >= 11: return "correcte"
    if m >= 10: return "tout juste suffisante"
    if m >= 8:  return "insuffisante"
    return "très insuffisante"

def _q_disp(std: float) -> str:
    if std < 1.0: return "très serrés, presque tout le monde est au même niveau"
    if std < 2.0: return "assez homogènes"
    if std < 3.0: return "moyennement dispersés"
    if std < 4.0: return "très étalés, avec un vrai fossé entre les meilleurs et les autres"
    return "extrêmement dispersés, deux groupes opposés cohabitent visiblement"

def _n(v) -> str:
    return f"{v:.2f}/20" if v is not None else "n/d"

def _p(v) -> str:
    return f"{v:.1f} %" if v is not None else "n/d"

def _asymetrie_courte(moy: float, med: float) -> Optional[str]:
    """
    Décrit en une courte incise la forme de la distribution (moyenne vs
    médiane), pour être intégrée dans une phrase plus large plutôt que
    de constituer son propre paragraphe. Formulé en proposition complète
    (sujet + verbe) pour ne pas dépendre de l'accord du mot qui précède.
    """
    d = moy - med
    if abs(d) < 0.4:
        return None  # rien à signaler, ne pas alourdir le texte pour un non-événement
    if d > 0:
        if moy < SEUIL_REUSSITE:
            return "et même ce chiffre-là tient surtout à quelques bonnes copies, le reste est en dessous"
        return "tirés vers le haut par un petit groupe de bons résultats plus que par l'ensemble"
    if moy >= SEUIL_REUSSITE:
        return "freinés par quelques notes très basses, la majorité fait mieux que ça"
    return "et plus de la moitié du groupe est en fait sous ce chiffre déjà bas"

def interpreter_ue(s: Dict[str, Any]) -> Dict[str, Any]:
    moy  = s.get("moyenne")
    taux = s.get("taux_reussite")
    eff  = s.get("effectif")
    std  = s.get("std_note")
    med  = s.get("mediane_note")
    diff = s.get("isDifficile", False)
    code = s.get("ue", "cette UE")

    if moy is None or taux is None:
        return _vide("Données insuffisantes",
                     "Les statistiques de cette UE sont incomplètes "
                     "ou les filtres actifs ne retournent aucune note.")

    niveau = "danger" if diff else _niv_taux(taux)

    ctx = _contexte_filtres(s)
    eff_str = f" ({eff} étudiants)" if eff else ""

    # Phrase 1 : le constat global, formulé comme une lecture plutôt
    # qu'une simple répétition des deux chiffres (déjà visibles en KPI).
    phrase1 = (f"{ctx}sur l'UE {code}{eff_str}, la moyenne est {_q_moy(moy)} "
               f"et la réussite {_q_taux(taux)}.")

    # Phrase 2 : une seule lecture de la "forme" du groupe. La dispersion
    # porte l'idée principale ; l'asymétrie n'est ajoutée que si elle
    # apporte une vraie nuance (sinon elle redirait juste la dispersion
    # autrement). L'amplitude min/max n'est pas commentée séparément :
    # c'est la même information que la dispersion, redondante à l'oral.
    phrase2 = None
    if std is not None:
        phrase2 = f"Les résultats sont {_q_disp(std)}"
        asym = _asymetrie_courte(float(moy), float(med)) if (med is not None and moy is not None) else None
        if asym:
            phrase2 += f", {asym}"
        phrase2 += "."

    corps = phrase1 + (" " + phrase2 if phrase2 else "")

    # ── Conseil
    conseil = None
    if diff:
        conseil = (
            f"L'UE cumule un taux d'échec élevé et une moyenne sous {SEUIL_REUSSITE}/20 : ce n'est "
            "pas juste un mauvais semestre, c'est structurel. Ça vaut le coup de revoir le contenu ou "
            "l'évaluation, et de caler des séances de remédiation avant le rattrapage."
        )
    elif taux is not None and taux < 50:
        conseil = (
            "Plus d'un étudiant sur deux est ajourné, même si la moyenne tient encore le coup. "
            "Un coup de pouce ciblé avant le rattrapage ferait sans doute la différence."
        )
    elif std is not None and std >= 4.5:
        conseil = (
            "L'écart entre les étudiants est tel qu'ils n'arrivent visiblement pas avec les mêmes "
            "bases. Un test de positionnement en début d'UE aiderait à repérer vite ceux qui ont besoin "
            "d'un coup de main."
        )
    elif taux is not None and taux >= 80 and moy is not None and moy >= 14:
        conseil = (
            "De très bons résultats, à un niveau qui peut servir d'exemple pour le département : "
            "ça vaut le coup de documenter ce qui marche ici pour le réutiliser ailleurs."
        )
    elif taux is not None and taux >= 65:
        conseil = (
            "Des résultats corrects dans l'ensemble. Suivre les ajournés et leur donner de quoi "
            "réviser devrait suffire à grappiller quelques points la prochaine fois."
        )

    return {
        "niveau": niveau,
        "titre": _titre_ue(taux, diff),
        "corps": corps,
        "conseil": conseil,
    }

def _titre_ue(t, diff):
    if diff:       return "UE en difficulté"
    if t >= 80:    return "Très bonne performance"
    if t >= 65:    return "Bonne performance"
    if t >= 50:    return "Performance acceptable"
    if t >= 35:    return "Performance préoccupante"
    return "Performance critique"


def interpreter_dashboard(s: Dict[str, Any]) -> Dict[str, Any]:
    moy   = s.get("moyenne_global")
    taux  = s.get("taux_reussite_global")
    eff   = s.get("effectif_exact", 0)
    med   = s.get("mediane")
    std   = s.get("ecart_type")
    diff  = s.get("ue_difficiles", [])
    risq  = s.get("risques", [])

    if moy is None or taux is None:
        return _vide("Aucune donnée",
                     "Appliquez des filtres ou vérifiez que des données sont chargées.")

    niveau = _niv_taux(taux)
    nb_diff = len(diff) if isinstance(diff, list) else 0
    nb_risq = len(risq) if isinstance(risq, list) else 0

    ctx = _contexte_filtres(s)

    # Phrase 1 : constat global (taux + moyenne croisés, pas juste listés).
    phrase1 = (f"{ctx}sur {eff:,} étudiants, la réussite est {_q_taux(taux)} "
               f"et la moyenne {_q_moy(moy)}.")

    # Phrase 2 : une seule lecture de la forme du groupe. La dispersion
    # (std) et l'IQR racontent la même chose à deux échelles différentes
    # (tout le monde / le centre) — on les fond en une phrase au lieu de
    # deux paragraphes qui répéteraient la même idée.
    iqr_v = s.get("iqr")
    phrase2 = None
    if std is not None:
        phrase2 = f"Les résultats sont {_q_disp(std)}"
        if iqr_v is not None and float(iqr_v) > 6.0 and std < 3.0:
            # Cas où l'ensemble paraît homogène mais le centre se disperse :
            # une vraie nuance qui mérite d'être dite, contrairement au cas
            # général où IQR et std racontent juste la même chose.
            phrase2 += ", même si le milieu du classement reste assez étalé"
        asym = _asymetrie_courte(float(moy), float(med)) if (med is not None and moy is not None) else None
        if asym:
            phrase2 += f", {asym}"
        phrase2 += "."

    # Phrase 3 : les alertes, qui apportent une info qu'on ne lit pas déjà
    # dans les KPI (nombre d'UE et d'étudiants concrètement concernés).
    phrase3 = None
    if nb_diff > 0 and nb_risq > 0:
        phrase3 = (f"{nb_diff} UE sont en difficulté et {nb_risq} étudiants sont sous le seuil — "
                    "le détail est dans l'onglet Alertes.")
    elif nb_diff > 0:
        phrase3 = f"{nb_diff} UE sont en difficulté (taux et moyenne sous le seuil) — voir l'onglet Alertes."
    elif nb_risq > 0:
        phrase3 = f"{nb_risq} étudiants sont sous le seuil de réussite — voir l'onglet Alertes."

    corps = " ".join(p for p in (phrase1, phrase2, phrase3) if p)

    conseil = None
    if nb_diff >= 5:
        conseil = (
            f"{nb_diff} UE en difficulté en même temps, c'est trop pour être un hasard : il y a "
            "probablement une cause commune (prérequis qui manquent, programme trop chargé, "
            "évaluation mal calibrée). Un audit transversal aiderait à trancher."
        )
    elif nb_risq >= 10:
        conseil = (
            f"{nb_risq} étudiants sont sous le seuil. Un tutorat ou une remédiation ciblée "
            "vaudrait le coup, en priorité pour ceux qui cumulent plusieurs UE ajournées."
        )
    elif taux is not None and taux < 50:
        conseil = (
            "Plus d'un étudiant sur deux échoue. Ça dépasse le cas isolé : la progression et "
            "les modalités d'évaluation méritent d'être revues avant la prochaine session."
        )
    elif taux is not None and taux >= 70 and moy is not None and moy >= 12:
        if nb_diff > 0 or nb_risq > 0:
            conseil = (
                "L'ensemble tient plutôt bien la route. Le mieux à faire maintenant, c'est de se "
                "concentrer sur les quelques UE en difficulté et les étudiants déjà repérés à risque."
            )
        else:
            conseil = "Bons résultats sur l'ensemble, sans signal d'alerte particulier à traiter en priorité."
    return {
        "niveau": niveau,
        "titre": _titre_global(taux),
        "corps": corps,
        "conseil": conseil,
    }

def _titre_global(t):
    if t >= 75: return "Performance globale satisfaisante"
    if t >= 60: return "Performance globale correcte"
    if t >= 50: return "Performance globale à surveiller"
    if t >= 35: return "Performance globale préoccupante"
    return "Performance globale critique"

#  3. Filière 

def interpreter_filiere(s: Dict[str, Any]) -> Dict[str, Any]:
    moy    = s.get("moyenne_global")
    taux   = s.get("taux_reussite_global")
    eff    = s.get("effectif_exact", 0)
    std    = s.get("ecart_type")
    med    = s.get("mediane")
    ues    = s.get("tableau_ue", [])
    fil    = s.get("filiere", "cette filière")

    if moy is None or taux is None:
        return _vide("Données insuffisantes",
                     "Sélectionnez une filière pour obtenir une interprétation.")

    nb_ue   = len(ues)
    nb_diff = sum(1 for u in ues
                  if u.get("taux_reussite", 100) < 50 and u.get("moyenne", 10) < SEUIL_REUSSITE)
    pct_diff = nb_diff / nb_ue * 100 if nb_ue else 0

    best  = max(ues, key=lambda u: u.get("taux_reussite", 0), default=None)
    worst = min(ues, key=lambda u: u.get("taux_reussite", 100), default=None)

    ctx = _contexte_filtres(s, exclude="filiere")

    # Phrase 1 : constat global croisé (effectif + réussite + moyenne).
    phrase1 = (f"{ctx}la filière {fil} compte {eff:,} étudiants sur {nb_ue} UE, "
               f"avec une réussite {_q_taux(taux)} et une moyenne {_q_moy(moy)}.")

    # Phrase 2 : la forme du groupe (dispersion + asymétrie), comme pour
    # les autres contextes — une seule lecture plutôt que des commentaires
    # séparés sur chaque indicateur statistique.
    phrase2 = None
    if std is not None:
        phrase2 = f"Les résultats sont {_q_disp(std)}"
        asym = _asymetrie_courte(float(moy), float(med)) if (med is not None and moy is not None) else None
        if asym:
            phrase2 += f", {asym}"
        phrase2 += "."

    # Phrase 3 : le diagnostic UE par UE — combine la proportion en
    # difficulté ET l'écart best/worst en une lecture, plutôt que deux
    # paragraphes séparés. L'écart best/worst n'est mentionné que s'il
    # apporte une vraie info (un grand écart, pas un détail de quelques
    # points qui serait du bruit statistique).
    phrase3 = None
    if nb_diff > 0:
        # On évite "X UE sur Y sont en difficulté" quand X == Y == 1,
        # qui sonnerait faux à l'accord ("1 UE... sont").
        if nb_diff == 1:
            phrase3 = (f"1 UE sur {nb_ue} est en difficulté ({pct_diff:.0f} % du programme)"
                       if nb_ue > 1 else "La seule UE de la filière est en difficulté")
        else:
            phrase3 = f"{nb_diff} UE sur {nb_ue} sont en difficulté ({pct_diff:.0f} % du programme)"
        if best and worst and best != worst:
            gap = best.get('taux_reussite', 0) - worst.get('taux_reussite', 0)
            if gap > 30:
                phrase3 += f", avec un grand écart entre {best['ue']} et {worst['ue']}"
        phrase3 += "."
    elif best and worst and best != worst:
        gap = best.get('taux_reussite', 0) - worst.get('taux_reussite', 0)
        if gap > 30:
            phrase3 = f"L'écart entre la meilleure UE ({best['ue']}) et la plus faible ({worst['ue']}) reste marqué."

    corps = " ".join(p for p in (phrase1, phrase2, phrase3) if p)

    conseil = None
    if pct_diff > 40 and nb_ue >= 3:
        conseil = (
            f"Près de la moitié des UE ({nb_diff}/{nb_ue}) sont en difficulté dans cette filière. "
            "Ça vaut le coup de revoir la progression dans son ensemble et de regarder si certaines "
            "UE bloquent les suivantes."
        )
    elif pct_diff > 40:
        conseil = (
            f"{nb_diff} UE sur {nb_ue} {'est' if nb_diff == 1 else 'sont'} en difficulté — avec aussi "
            "peu d'UE évaluées, mieux vaut attendre d'avoir plus de données avant de tirer des "
            "conclusions définitives, mais ça vaut le coup de garder un œil dessus."
        )
    elif pct_diff > 20:
        conseil = (
            f"{nb_diff} UE {'pose' if nb_diff == 1 else 'posent'} problème. Un plan ciblé sur "
            f"{'celle-là' if nb_diff == 1 else 'celles-là'} — remédiation, supports revus, "
            "accompagnement renforcé — devrait suffire à redresser la barre."
        )
    elif taux is not None and taux >= 70:
        conseil = "Bons résultats dans l'ensemble. Reste à traiter les quelques UE encore en retrait pour viser une réussite homogène sur tout le programme."

    return {
        "niveau": _niv_taux(taux),
        "titre": f"Filière {fil} — {_q_taux(taux)}",
        "corps": corps,
        "conseil": conseil,
    }

#  4. Département

def interpreter_departement(
    dept_code: str,
    scores_depts: List[Dict],
    tableau_ue: List[Dict],
) -> Dict[str, Any]:
    target_code = dept_code.strip().upper()
    
    score_d = next((d for d in scores_depts if str(d.get("departement", "")).strip().upper() == target_code), None)
    ues_d   = [u for u in tableau_ue
               if str(u.get("departement", "")).strip().upper() == target_code]

    if not score_d:
        return _vide(f"Département {dept_code}",
                     f"Aucune donnée agrégée pour le département {dept_code} avec les filtres actifs.")

    moy_dept  = score_d.get("score", 0)
    nb_ue     = len(ues_d)
    nb_diff   = sum(1 for u in ues_d
                    if u.get("taux_reussite", 100) < 50 and u.get("moyenne", 10) < SEUIL_REUSSITE)

    classement = sorted(scores_depts, key=lambda d: d.get("score", 0), reverse=True)
    rang       = next((i + 1 for i, d in enumerate(classement)
                       if d.get("departement") == dept_code), None)
    nb_depts   = len(classement)

    taux_moyen = None
    if ues_d:
        taux_moyen = sum(u.get("taux_reussite", 0) for u in ues_d) / len(ues_d)

    # Phrase 1 : moyenne + classement, en une lecture (le classement
    # donne du sens à la moyenne, pas l'inverse — on les croise donc).
    rang_str = ""
    if rang and nb_depts > 1:
        if rang == 1:
            rang_str = ", en tête des départements"
        elif rang == nb_depts:
            rang_str = ", dernier au classement"
        else:
            rang_str = f", {rang}e sur {nb_depts}"
    phrase1 = f"Le département {dept_code} a une moyenne {_q_moy(moy_dept)} ({_n(moy_dept)}){rang_str}."

    # Si le département n'est pas en tête, on dit l'écart avec le premier
    # dans la même idée plutôt que dans un paragraphe séparé.
    if nb_depts > 1 and rang and rang > 1:
        best_dept = classement[0]
        gap = best_dept.get("score", 0) - moy_dept
        phrase1 += f" Le mieux classé ({best_dept.get('departement')}) fait {gap:.1f} pts de mieux."

    # Phrase 2 : taux de réussite consolidé + UE en difficulté, fusionnés
    # (le taux prend son sens dès qu'on sait combien d'UE tirent vers le bas).
    phrase2 = None
    if taux_moyen is not None:
        lib_ue = "l'unique UE" if nb_ue == 1 else f"les {nb_ue} UE"
        phrase2 = f"La réussite sur {lib_ue} du département est {_q_taux(taux_moyen)}"
        if nb_diff > 0:
            pct = nb_diff / nb_ue * 100 if nb_ue else 0
            phrase2 += f", tirée vers le bas par {nb_diff} UE en difficulté ({pct:.0f} %)"
        phrase2 += "."
    elif nb_diff > 0:
        pct = nb_diff / nb_ue * 100 if nb_ue else 0
        phrase2 = f"{nb_diff} UE sur {nb_ue} ({pct:.0f} %) sont en difficulté."

    corps = phrase1 + (" " + phrase2 if phrase2 else "")

    conseil = None
    if nb_diff > 3:
        conseil = (
            f"{nb_diff} UE sont en difficulté dans ce département. Une coordination entre "
            "enseignants pour harmoniser les niveaux d'exigence et partager ce qui fonctionne "
            "ailleurs ferait sans doute la différence."
        )
    elif moy_dept < SEUIL_REUSSITE:
        conseil = (
            f"La moyenne du département ({_n(moy_dept)}) est sous le seuil. Ça mérite un vrai "
            "diagnostic sur les contenus et les modalités d'évaluation, pas juste un ajustement."
        )
    elif rang == 1 and nb_depts > 1:
        conseil = (
            f"Le département {dept_code} est en tête. Ses pratiques pédagogiques valent le coup "
            "d'être documentées et partagées avec les autres départements."
        )

    return {
        "niveau": _niv_moy(moy_dept),
        "titre": f"Département {dept_code}",
        "corps": corps,
        "conseil": conseil,
    }

#  5. Étudiant

def _mention(m: Optional[float]) -> Optional[str]:
    """
    Mention LMD classique, calculée à partir de la moyenne. Pas de
    mention sous la moyenne (10/20) : on ne décerne pas de mention à un
    parcours qui n'a pas atteint le seuil de réussite.
    """
    if m is None or m < 10:
        return None
    if m >= 18: return "Excellent"
    if m >= 16: return "Très Bien"
    if m >= 14: return "Bien"
    if m >= 12: return "Assez Bien"
    return "Passable"

def interpreter_etudiant(s: Dict[str, Any]) -> Dict[str, Any]:
    moy  = s.get("moyenne_globale") or s.get("moyenne")
    taux = s.get("taux_reussite_global") or s.get("taux_reussite")
    cv   = s.get("credits_valides", 0)
    ct   = s.get("credits_total", 0)
    sems = s.get("parcours", [])

    if moy is None:
        return _vide("Parcours vide",
                     "Aucune note disponible pour cet étudiant avec les filtres actifs.")

    pct_cred = cv / ct * 100 if ct else 0

    # Phrase 1 : moyenne + mention + réussite + progression dans le
    # programme, fusionnées (les crédits validés donnent le contexte du
    # chiffre de moyenne — un 13/20 ne veut pas dire la même chose à 30 %
    # ou 90 % du parcours bouclé). La mention officielle (LMD) remplace
    # le qualificatif _q_moy quand elle existe, pour éviter de dire deux
    # choses légèrement différentes sur le même chiffre (ex: "excellente"
    # à 16/20 alors que la mention officielle est encore "Très Bien").
    mention = _mention(moy)
    if mention:
        phrase1 = f"L'étudiant a une moyenne de {_n(moy)} (mention {mention})"
    else:
        phrase1 = f"L'étudiant a une moyenne {_q_moy(moy)} ({_n(moy)})"
    phrase1 += f" et une réussite {_q_taux(taux or 0)}"
    if ct:
        if pct_cred >= 80:
            phrase1 += f", avec {pct_cred:.0f} % du programme déjà validé"
        elif pct_cred < 60:
            phrase1 += f", mais seulement {pct_cred:.0f} % des crédits sont validés pour l'instant"
        else:
            phrase1 += f" ({pct_cred:.0f} % des crédits validés)"
    phrase1 += "."

    # Phrase 2 : la tendance dans le temps — c'est une info que la
    # moyenne globale ne donne pas, donc ça reste une phrase à part,
    # mais reformulée sans les tics ("On observe...", "indiquant que...").
    phrase2 = None
    if len(sems) >= 2:
        moys_sems = [s.get("moyenne", 0) for s in sems]
        trend_tot = moys_sems[-1] - moys_sems[0]
        if trend_tot > 1.5:
            phrase2 = f"Sa courbe est nettement ascendante (+{trend_tot:.2f} pts depuis le début) : il a su monter en puissance."
        elif trend_tot > 0.5:
            phrase2 = f"Sa courbe progresse doucement (+{trend_tot:.2f} pts) — bon signe pour la suite."
        elif trend_tot < -1.5:
            phrase2 = f"Sa courbe redescend ({trend_tot:.2f} pts depuis le début), ce qui mérite qu'on en parle avec lui."
        elif abs(trend_tot) <= 0.5:
            moy_moy = sum(moys_sems) / len(moys_sems)
            if moy_moy < SEUIL_REUSSITE:
                phrase2 = "Le niveau reste stable, mais durablement sous la moyenne — il faudrait changer quelque chose."
            else:
                phrase2 = "Le niveau reste stable d'un semestre à l'autre, sans à-coups."

    corps = phrase1 + (" " + phrase2 if phrase2 else "")

    # Conseil
    conseil = None
    if moy < SEUIL_REUSSITE:
        conseil = (
            f"La moyenne ({_n(moy)}) est sous le seuil. Un point avec le responsable pédagogique "
            "serait utile, en priorisant les UE à fort coefficient et un accompagnement sur "
            "celles qui posent le plus de problème."
        )
    elif pct_cred < 60 and ct > 0:
        conseil = (
            f"Avec {pct_cred:.0f} % des crédits validés, il reste encore {ct - cv} crédits à "
            "décrocher. Mieux vaut repérer vite les UE non validées et poser un plan clair pour les rattraper."
        )
    elif moy >= 14 and pct_cred >= 80:
        conseil = "Très bon parcours — un profil à pousser vers l'excellence, la mobilité internationale ou un stage long."
    elif moy >= 12:
        conseil = "Parcours satisfaisant. Garder cette régularité devrait suffire à consolider les résultats et lisser les UE encore fragiles."

    return {
        "niveau": _niv_moy(moy),
        "titre": _titre_etudiant(moy, taux or 0),
        "corps": corps,
        "conseil": conseil,
    }

def _titre_etudiant(m, t):
    if m >= 16:         return "Profil excellent"
    if m >= 14:         return "Profil très satisfaisant"
    if m >= 12:         return "Profil satisfaisant"
    if m >= 10:         return "Profil juste passable"
    if t >= 50:         return "Profil fragile"
    return "Profil à risque"


def _vide(titre: str, corps: str) -> Dict[str, Any]:
    return {"niveau": "info", "titre": titre, "corps": corps, "conseil": None}


def _contexte_filtres(data: Dict[str, Any], exclude: Optional[str] = None) -> str:
    """
    Génère une phrase décrivant les filtres actifs pour que l'interprétation
    mentionne explicitement le périmètre analysé.
    Ex: "Pour l'année 2023-24, le semestre 2 — "

    `exclude` permet d'omettre une clé déjà utilisée comme sujet dans la
    phrase d'ouverture de la fonction appelante (ex: interpreter_filiere
    nomme déjà la filière dans sa propre phrase, pas besoin de la répéter
    ici aussi).
    """
    parties = []
    LABELS = {
        "annee":          "l'année",
        "semestre":       "le semestre",
        "cohorte":        "la cohorte",
        "filiere":        "la filière",
        "departement":    "le département",
        "type_formation": "le type de formation",
        "niveau":         "le niveau",
    }
    for cle, label in LABELS.items():
        if cle == exclude:
            continue
        val = data.get(cle)
        if val and str(val).strip():
            vals = [v.strip() for v in str(val).split(",") if v.strip()]
            if vals:
                v_list = list(vals)
                valstr = ", ".join(v_list) if len(v_list) <= 3 else f"{', '.join(v_list[:2])} +{len(v_list)-2}"
                parties.append(f"{label} {valstr}")
    if not parties:
        return ""
    return "Pour " + " · ".join(parties) + " — "


def interpreter(context: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Moteur d'interprétation pédagogique par règles déterministes."""
    if context == "departement":
        return interpreter_departement(
            dept_code    = data.get("departement", ""),
            scores_depts = data.get("scores_depts", []),
            tableau_ue   = data.get("tableau_ue", []),
        )
    dispatch = {
        "ue":        interpreter_ue,
        "dashboard": interpreter_dashboard,
        "filiere":   interpreter_filiere,
        "etudiant":  interpreter_etudiant,
    }
    fn = dispatch.get(context)
    if not fn:
        return _vide("Contexte inconnu",
                     f"Le contexte '{context}' n'est pas supporté.")
    return fn(data)
