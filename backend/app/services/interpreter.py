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
    if m >= 16: return "excellente, témoignant d'une maîtrise remarquable du programme"
    if m >= 14: return "solide, avec une très bonne acquisition des compétences"
    if m >= 12: return "satisfaisante, indiquant que les objectifs pédagogiques sont atteints"
    if m >= 11: return "correcte mais laisse une marge de progression"
    if m >= 10: return "précaire, juste au niveau requis pour la validation"
    if m >= 8:  return "insuffisante, signalant des lacunes réelles dans l'apprentissage"
    return "très insuffisante, appelant à une réévaluation complète du parcours"

def _q_disp(std: float) -> str:
    if std < 1.0: return f"exceptionnellement homogène (σ = {std:.2f}) — le groupe avance au même rythme sans disparité"
    if std < 2.0: return f"homogène (σ = {std:.2f}) — la majorité des étudiants partagent un niveau similaire"
    if std < 3.0: return f"modérée (σ = {std:.2f}) — on observe quelques écarts de niveau habituels"
    if std < 4.0: return f"hétérogène (σ = {std:.2f}) — les profils sont très disparates, certains étudiants sont largement distancés"
    return f"très hétérogène (σ = {std:.2f}) — la forte dispersion indique des profils de réussite et d'échec radicalement opposés"

def _n(v) -> str:
    return f"{v:.2f}/20" if v is not None else "n/d"

def _q_iqr(iqr: float) -> str:
    """Qualifie l'étendue inter-quartile."""
    if iqr <= 2.0:  return f"très resserrée (IQR = {iqr:.2f} pts) — le cœur de la promotion (50 %) est extrêmement soudé"
    if iqr <= 4.0:  return f"équilibrée (IQR = {iqr:.2f} pts)"
    if iqr <= 6.0:  return f"étendue (IQR = {iqr:.2f} pts) — même au sein du groupe central, les différences de réussite sont notables"
    return f"disparate (IQR = {iqr:.2f} pts) — forte hétérogénéité structurelle au sein même de la classe moyenne"

def _p(v) -> str:
    return f"{v:.1f} %" if v is not None else "n/d"

def _asymetrie(moy: float, med: float) -> str:
    d = moy - med
    if abs(d) < 0.4:
        return (f"La distribution est équilibrée : la moyenne ({_n(moy)}) "
                f"et la médiane ({_n(med)}) sont très proches. "
                "Cela indique une promotion homogène sans 'petits groupes' aux résultats extrêmes.")
    if d > 0:
        return (f"La moyenne ({_n(moy)}) est supérieure à la médiane ({_n(med)}) "
                f"(+{d:.2f} pts). Cette asymétrie positive montre qu'une minorité d'étudiants "
                "très performants tirent les résultats vers le haut, ce qui peut masquer les difficultés rencontrées par la majorité.")
    return (f"La médiane ({_n(med)}) dépasse la moyenne ({_n(moy)}) "
            f"({abs(d):.2f} pts d'écart). Cela suggère que quelques notes très basses "
            "pénalisent la moyenne globale, alors que plus de la moitié de la classe réussit plutôt bien.")

def interpreter_ue(s: Dict[str, Any]) -> Dict[str, Any]:
    moy  = s.get("moyenne")
    taux = s.get("taux_reussite")
    eff  = s.get("effectif")
    std  = s.get("std_note")
    med  = s.get("mediane_note")
    mn   = s.get("min_note")
    mx   = s.get("max_note")
    adm  = s.get("nombre_admis")
    ajr  = s.get("nombre_ajournes")
    diff = s.get("isDifficile", False)
    code = s.get("ue", "cette UE")

    if moy is None or taux is None:
        return _vide("Données insuffisantes",
                     "Les statistiques de cette UE sont incomplètes "
                     "ou les filtres actifs ne retournent aucune note.")

    niveau = "danger" if diff else _niv_taux(taux)

    paras = []

    # Phrase d'accroche
    eff_str = f" sur {eff} étudiant(s) évalué(s)" if eff else ""
    ctx = _contexte_filtres(s)
    paras.append(
        f"{ctx}l'UE {code} présente un taux de réussite de {_p(taux)} ({_q_taux(taux)}){eff_str}, "
        f"pour une moyenne de {_n(moy)} ({_q_moy(moy)})."
    )

    if med is not None and moy is not None:
        paras.append(_asymetrie(float(moy), float(med)))

    if std is not None:
        paras.append(f"La dispersion des résultats est {_q_disp(std)}. Concrètement, cela signifie que {
            'les étudiants progressent de manière très unie' if std < 1.0 
            else 'le groupe est relativement soudé' if std < 2.0
            else 'certaines disparités apparaissent entre les étudiants' if std < 3.0
            else 'le niveau est très hétérogène, nécessitant une pédagogie différenciée'
        }.")

    if mn is not None and mx is not None:
        spread = mx - mn
        paras.append(
            f"L'écart entre la meilleure note ({_n(mx)}) et la note la plus basse ({_n(mn)}) est de {spread:.2f} pts. "
            + ("Cette forte amplitude confirme que l'UE discrimine fortement les niveaux." if spread > 12
               else "Cette étendue modérée indique une certaine cohérence dans l'évaluation." if spread > 7
               else "Ce faible écart traduit une notation très resserrée.")
        )

    corps = " ".join(paras)

    q1  = s.get("q1_note")
    q3  = s.get("q3_note")
    iqr = s.get("iqr_note")
    points = []
    if adm is not None and ajr is not None:
        pct_a = taux or 0
        pct_j = 100 - pct_a
        points.append(f"Admis : {adm} ({pct_a:.1f} %)  ·  Ajournés : {ajr} ({pct_j:.1f} %)")
    if mn is not None and mx is not None:
        points.append(f"Min : {_n(mn)}  ·  Max : {_n(mx)}  ·  Étendue : {mx-mn:.2f} pts")
    if med is not None:
        points.append(f"Médiane (Q2) : {_n(med)}  ·  Moyenne : {_n(moy)}")
    if q1 is not None and q3 is not None:
        points.append(f"Q1 : {_n(q1)}  ·  Q3 : {_n(q3)}  ·  IQR : {iqr:.2f} pts")
    if std is not None:
        points.append(f"Écart-type σ : {std:.2f}  ·  Variance σ² : {std**2:.2f}")

    # ── Conseil 
    conseil = None
    if diff:
        conseil = (
            f"⚠ UE en difficulté structurelle (taux < 50 % et moyenne < {SEUIL_REUSSITE}). "
            "Actions recommandées : révision du contenu ou du mode d'évaluation, "
            "mise en place de séances de remédiation, et suivi rapproché "
            "des étudiants ajournés avant la session de rattrapage."
        )
    elif taux is not None and taux < 50:
        conseil = (
            "Le taux d'échec dépasse 50 %. Bien que la moyenne ne soit pas "
            "formellement sous le seuil, un nombre important d'étudiants n'atteint "
            "pas la validation. Un soutien ciblé avant la session de rattrapage est conseillé."
        )
    elif std is not None and std >= 4.5:
        conseil = (
            "La forte hétérogénéité des niveaux suggère des prérequis très variables. "
            "Un test de positionnement en début d'UE permettrait d'adapter le rythme "
            "et d'orienter les étudiants les plus fragiles vers des ressources complémentaires."
        )
    elif taux is not None and taux >= 80 and moy is not None and moy >= 14:
        conseil = (
            "Performance excellente. Cette UE peut servir de référence "
            "pédagogique pour le département. Les pratiques d'enseignement "
            "associées méritent d'être documentées et partagées."
        )
    elif taux is not None and taux >= 65:
        conseil = (
            "Résultats satisfaisants. Le suivi des étudiants ajournés "
            "et la disponibilité de ressources de révision permettraient "
            "d'améliorer encore ce taux lors de la prochaine session."
        )

    return {
        "niveau": niveau,
        "titre": _titre_ue(taux, diff),
        "corps": corps,
        "points": points,
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
    var   = s.get("variance")
    diff  = s.get("ue_difficiles", [])
    risq  = s.get("risques", [])

    if moy is None or taux is None:
        return _vide("Aucune donnée",
                     "Appliquez des filtres ou vérifiez que des données sont chargées.")

    niveau = _niv_taux(taux)
    nb_diff = len(diff) if isinstance(diff, list) else 0
    nb_risq = len(risq) if isinstance(risq, list) else 0

    paras = []
    ctx = _contexte_filtres(s)
    paras.append(
        f"{ctx}sur {eff:,} étudiant(s) évalué(s), le taux de réussite global s'établit à {_p(taux)} "
        f"({_q_taux(taux)}) avec une moyenne générale de {_n(moy)} ({_q_moy(moy)})."
    )

    if med is not None and moy is not None:
        paras.append(_asymetrie(float(moy), float(med)))

    if std is not None:
        paras.append(f"La distribution globale des notes est {_q_disp(std)}. "
                     "L'enseignement semble ainsi " + 
                     ("parfaitement adapté à l'ensemble du groupe." if std < 1.5 
                      else "globalement d'accès équitable pour la majorité." if std < 3.0
                      else "confronté à des écarts de niveaux qui méritent une attention par filière."))

    iqr_v = s.get("iqr")
    if iqr_v is not None:
        paras.append(f"L'étendue inter-quartile est {_q_iqr(float(iqr_v))}. "
                     "On en déduit que le 'cœur' de la promotion est " + 
                     ("très homogène dans ses performances." if float(iqr_v) < 3.0
                      else "marqué par des disparités internes significatives."))

    if nb_diff > 0:
        paras.append(
            f"{nb_diff} UE présentent simultanément un taux de réussite inférieur à 50 % "
            f"et une moyenne sous le seuil (consultables dans l'onglet Alertes)."
        )

    if nb_risq > 0:
        paras.append(
            f"{nb_risq} étudiant(s) ont une moyenne individuelle inférieure au seuil "
            f"de réussite ({SEUIL_REUSSITE}/20) et nécessitent une attention particulière "
            f"(liste consultable dans l'onglet Alertes)."
        )

    corps = " ".join(paras)

    q1  = s.get("q1")
    q3  = s.get("q3")
    iqr = s.get("iqr")
    points = []
    points.append(f"Taux de réussite : {_p(taux)}  ·  Moyenne : {_n(moy)}")
    if med is not None:
        points.append(f"Médiane (Q2) : {_n(med)}")
    if q1 is not None and q3 is not None:
        points.append(f"Q1 : {_n(q1)}  ·  Q3 : {_n(q3)}  ·  IQR : {iqr:.2f} pts")
    if std is not None:
        v = f"  ·  Variance σ² : {var:.2f}" if var is not None else ""
        points.append(f"Écart-type σ : {std:.2f}{v}")
    points.append(f"UE en difficulté : {nb_diff}  ·  Étudiants à risque : {nb_risq}")

    conseil = None
    if nb_diff >= 5:
        conseil = (
            f"{nb_diff} UE sont simultanément en difficulté. Un audit pédagogique "
            "transversal est recommandé pour identifier les causes communes "
            "(prérequis insuffisants, surcharge de programme, modalités d'évaluation inadaptées)."
        )
    elif nb_risq >= 10:
        conseil = (
            f"{nb_risq} étudiants sont sous le seuil de réussite. "
            "Un dispositif de tutorat ou de remédiation ciblée devrait être activé, "
            "en priorisant les étudiants cumulant plusieurs UE ajournées."
        )
    elif taux is not None and taux < 50:
        conseil = (
            "Le taux de réussite global est inférieur à 50 %. "
            "Une révision globale de la progression pédagogique et des modalités "
            "d'évaluation s'impose avant la prochaine session."
        )
    elif taux is not None and taux >= 70 and moy is not None and moy >= 12:
        conseil = (
            "Les résultats sont globalement satisfaisants. "
            "La priorité devrait se concentrer sur les UE en difficulté "
            "et l'accompagnement des étudiants à risque identifiés."
        )
    return {
        "niveau": niveau,
        "titre": _titre_global(taux),
        "corps": corps,
        "points": points,
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

    paras = []
    ctx = _contexte_filtres(s)
    paras.append(
        f"{ctx}la filière {fil} regroupe {eff:,} étudiant(s) répartis sur {nb_ue} UE. "
        f"Le taux de réussite moyen s'établit à {_p(taux)} ({_q_taux(taux)}) "
        f"pour une moyenne générale de {_n(moy)} ({_q_moy(moy)})."
    )

    if med is not None and moy is not None:
        paras.append(_asymetrie(float(moy), float(med)))

    if std is not None:
        paras.append(f"La dispersion des résultats est {_q_disp(std)}.")

    if nb_diff > 0:
        paras.append(
            f"{nb_diff} UE sur {nb_ue} ({pct_diff:.0f} % du programme) "
            "présentent des difficultés structurelles."
        )

    if best and worst and best != worst:
        gap = best.get('taux_reussite',0) - worst.get('taux_reussite',0)
        paras.append(
            f"On note un écart de réussite significatif ({gap:.1f} points) entre la meilleure UE ({best['ue']}) "
            f"et l'UE la plus critique ({worst['ue']}). "
            "Cette disparité suggère que certains enseignements présentent des obstacles spécifiques qui méritent d'être investigués."
        )

    corps = " ".join(paras)

    q1  = s.get("q1")
    q3  = s.get("q3")
    iqr = s.get("iqr")
    points = []
    points.append(f"Taux de réussite : {_p(taux)}  ·  Moyenne : {_n(moy)}")
    if med is not None:
        points.append(f"Médiane (Q2) : {_n(med)}")
    if q1 is not None and q3 is not None:
        points.append(f"Q1 : {_n(q1)}  ·  Q3 : {_n(q3)}  ·  IQR : {iqr:.2f} pts")
    if std is not None:
        points.append(f"Écart-type σ : {std:.2f}  ·  Variance σ² : {std**2:.2f}")
    points.append(f"{nb_ue} UE au total  ·  {nb_diff} en difficulté ({pct_diff:.0f} %)")
    if best:
        points.append(f"Meilleure UE : {best['ue']} — {_p(best.get('taux_reussite',0))}")
    if worst and worst != best:
        points.append(f"UE la plus difficile : {worst['ue']} — {_p(worst.get('taux_reussite',0))}")

    conseil = None
    if pct_diff > 40:
        conseil = (
            f"Plus de 40 % des UE ({nb_diff}/{nb_ue}) sont en difficulté. "
            "Une révision de la progression pédagogique de la filière est urgente. "
            "Il est recommandé d'analyser les corrélations entre les UE pour identifier "
            "les goulots d'étranglement dans le parcours."
        )
    elif pct_diff > 20:
        conseil = (
            f"{nb_diff} UE présentent des difficultés structurelles. "
            "Un plan d'action ciblé sur ces UE (remédiation, révision des supports, "
            "accompagnement renforcé) permettrait d'améliorer significativement les résultats."
        )
    elif taux is not None and taux >= 70:
        conseil = (
            "La filière affiche de bons résultats globaux. "
            "La priorité peut se concentrer sur les quelques UE difficiles "
            "pour tendre vers une réussite homogène sur l'ensemble du programme."
        )

    return {
        "niveau": _niv_taux(taux),
        "titre": f"Filière {fil} — {_q_taux(taux)}",
        "corps": corps,
        "points": points,
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

    paras = []
    rang_str = ""
    if rang and nb_depts > 1:
        qualif_rang = ("en tête du classement" if rang == 1
                       else "en dernière position" if rang == nb_depts
                       else f"en {rang}e position sur {nb_depts}")
        rang_str = f", le plaçant {qualif_rang}"
    paras.append(
        f"Le département {dept_code} affiche une moyenne de {_n(moy_dept)} ({_q_moy(moy_dept)})"
        f"{rang_str}."
    )

    if taux_moyen is not None:
        paras.append(
            f"Le taux de réussite moyen consolidé sur les {nb_ue} UE du département "
            f"est de {_p(taux_moyen)} ({_q_taux(taux_moyen)}). "
            "C'est un indicateur de la fluidité globale du passage des étudiants au sein de vos filières."
        )

    if nb_diff > 0:
        pct = nb_diff / nb_ue * 100 if nb_ue else 0
        paras.append(
            f"{nb_diff} UE sur {nb_ue} ({pct:.0f} %) présentent des difficultés structurelles."
        )

    if nb_depts > 1 and rang:
        best_dept = classement[0]
        if best_dept.get("departement") != dept_code:
            gap = best_dept.get("score", 0) - moy_dept
            paras.append(
                f"L'écart avec le département le mieux classé "
                f"({best_dept.get('departement')}, {_n(best_dept.get('score'))}) "
                f"est de {gap:.2f} pts."
            )

    corps = " ".join(paras)

    points = []
    if rang:
        points.append(f"Rang inter-départements : {rang}/{nb_depts}")
    points.append(f"Moyenne : {_n(moy_dept)}")
    if taux_moyen is not None:
        points.append(f"Taux de réussite moyen : {_p(taux_moyen)}")
    points.append(f"{nb_ue} UE  ·  {nb_diff} en difficulté")

    conseil = None
    if nb_diff > 3:
        conseil = (
            f"Le département {dept_code} compte {nb_diff} UE en difficulté. "
            "Une coordination entre enseignants pour harmoniser les niveaux d'exigence "
            "et partager les bonnes pratiques pédagogiques est recommandée."
        )
    elif moy_dept < SEUIL_REUSSITE:
        conseil = (
            f"La moyenne département ({_n(moy_dept)}) est sous le seuil de réussite. "
            "Un diagnostic approfondi des contenus et des modalités d'évaluation s'impose."
        )
    elif rang == 1 and nb_depts > 1:
        conseil = (
            f"Le département {dept_code} est le mieux classé. "
            "Ses pratiques pédagogiques peuvent servir de référence "
            "et être partagées avec les autres départements."
        )

    return {
        "niveau": _niv_moy(moy_dept),
        "titre": f"Département {dept_code}",
        "corps": corps,
        "points": points,
        "conseil": conseil,
    }

#  5. Étudiant

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

    paras = []
    paras.append(
        f"L'étudiant affiche une moyenne globale de {_n(moy)} ({_q_moy(moy)}) "
        f"avec un taux de réussite de {_p(taux or 0)} sur l'ensemble des UE évaluées."
    )

    if ct:
        paras.append(
            f"Il a validé {cv} crédit(s) sur {ct} au total ({pct_cred:.0f} % du programme), "
            + ("ce qui représente une progression très satisfaisante." if pct_cred >= 80
               else "témoignant d'une progression correcte." if pct_cred >= 60
               else "indiquant que des UE importantes restent à valider.")
        )

    if len(sems) >= 2:
        moys_sems = [s.get("moyenne", 0) for s in sems]
        trend_tot = moys_sems[-1] - moys_sems[0]
        if trend_tot > 1.5:
            paras.append(
                f"On observe une dynamique de progression remarquable (+{trend_tot:.2f} pts). "
                "C'est la marque d'un étudiant qui a su s'adapter aux exigences croissantes de son cursus."
            )
        elif trend_tot > 0.5:
            paras.append(
                f"La tendance est positive (+{trend_tot:.2f} pts), indiquant que l'étudiant "
                "gagne en maturité et améliore ses méthodes de travail."
            )
        elif trend_tot < -1.5:
            paras.append(
                f"La trajectoire est malheureusement descendante ({trend_tot:.2f} pts). "
                "Ce recul suggère des difficultés croissantes ou une perte de motivation qui nécessite un point de situation."
            )
        elif abs(trend_tot) <= 0.5:
            moy_moy = sum(moys_sems) / len(moys_sems)
            if moy_moy < SEUIL_REUSSITE:
                paras.append("Les résultats sont malheureusement stables sous le seuil de réussite. Un changement de méthode ou un soutien extérieur semble indispensable.")
            else:
                paras.append("L'étudiant maintient un niveau constant tout au long de son parcours, témoignant d'une bonne régularité dans son investissement.")

    corps = " ".join(paras)

    # Points
    points = []
    points.append(f"Moyenne globale : {_n(moy)}  ·  Taux réussite : {_p(taux or 0)}")
    if ct:
        points.append(f"Crédits validés : {cv}/{ct} ({pct_cred:.0f} %)")

    sems_critiques = [s for s in sems if s.get("moyenne", 10) < SEUIL_REUSSITE]
    if sems_critiques:
        nums = ", ".join(f"S{s['semestre']}" for s in sems_critiques)
        points.append(f"Semestre(s) à surveiller : {nums}")

    if sems:
        best_sem = max(sems, key=lambda s: s.get("moyenne", 0))
        points.append(f"Période de réussite maximale : Semestre {best_sem['semestre']} ({_n(best_sem.get('moyenne'))})")

    # Conseil
    conseil = None
    if moy < SEUIL_REUSSITE:
        conseil = (
            f"La moyenne globale ({_n(moy)}) est inférieure au seuil de réussite ({SEUIL_REUSSITE}/20). "
            "Un entretien de suivi avec le responsable pédagogique est recommandé. "
            "L'étudiant devrait prioriser les UE à fort coefficient et solliciter "
            "un accompagnement pour les UE les plus difficiles."
        )
    elif pct_cred < 60 and ct > 0:
        conseil = (
            f"Avec {pct_cred:.0f} % des crédits validés, l'étudiant a encore "
            f"{ct - cv} crédits à acquérir. "
            "Il devrait identifier les UE non encore validées et établir un plan de progression."
        )
    elif moy >= 14 and pct_cred >= 80:
        conseil = (
            "Excellent parcours. Cet étudiant est un candidat potentiel "
            "pour des programmes d'excellence, de mobilité internationale ou de stage long."
        )
    elif moy >= 12:
        conseil = (
            "Parcours satisfaisant. Maintenir la régularité dans le travail "
            "permettra de consolider les résultats et d'améliorer les UE encore fragiles."
        )

    return {
        "niveau": _niv_moy(moy),
        "titre": _titre_etudiant(moy, taux or 0),
        "corps": corps,
        "points": points,
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
    return {"niveau": "info", "titre": titre, "corps": corps, "points": [], "conseil": None}


def _contexte_filtres(data: Dict[str, Any]) -> str:
    """
    Génère une phrase décrivant les filtres actifs pour que l'interprétation
    mentionne explicitement le périmètre analysé.
    Ex: "Pour l'année 2023-24, le semestre 2, la filière GI"
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
