"""
upload.py
Service de traitement des fichiers uploadés (CSV, Parquet, Excel).
Validation du schéma, fusion (add/update) avec les données existantes.
"""
import logging
import shutil
import tempfile
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.core.config import DATA_PATH, UPLOAD_DIR, MAX_UPLOAD_SIZE
from app.core.referentiel import get_departement, get_type_formation, get_niveau
from app.services.data_processing import (
    validate_schema, cache_donnees, df_fingerprint, load_data
)
import json
import fcntl
from datetime import datetime
from app.core.config import DATA_DIR
HISTORY_FILE = DATA_DIR / "upload_history.jsonl"

def log_upload_event(
    filename: str,
    status: str,
    results: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    user: str = "admin"
) -> None:
    event = {
        "timestamp": int(time.time()),
        "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "filename": filename,
        "status": status,
        "user": user,
        "added": results.get("added", 0) if results else 0,
        "updated": results.get("updated", 0) if results else 0,
        "warnings": results.get("warnings", []) if results else [],
        "error": error
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
            f.flush()
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)

def get_upload_history(limit: int = 50) -> List[Dict[str, Any]]:
    if not HISTORY_FILE.exists():
        return []
    events = []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_SH)
                for line in f:
                    if line.strip():
                        events.append(json.loads(line))
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
    except Exception:
        return []
    events.reverse()
    return events[:limit]
logger = logging.getLogger(__name__)

# Colonnes requises
COLONNES_REQUISES = {
    "annee", "semestre", "carte", "anonymat", "ue",
    "credit", "nom_prenoms", "sexe", "note", "cohorte", "filiere",
}

# Clé composite pour la fusion ajout/mise à jour
MERGE_KEY = ["anonymat", "ue", "semestre", "annee"]
MAX_UPLOAD_ROWS = 1_000_000


def read_uploaded_file(content: bytes, filename: str) -> pd.DataFrame:
    """
    Lit un fichier uploadé et retourne un DataFrame.
    Supporte CSV, Parquet et Excel (.xlsx, .xls).
    """
    suffix = Path(filename).suffix.lower()
    buf = BytesIO(content)

    if suffix == ".csv":
        df = pd.read_csv(buf)
    elif suffix == ".parquet":
        df = pd.read_parquet(buf)
    elif suffix in (".xlsx", ".xls"):
        df = pd.read_excel(buf, engine="openpyxl")
    else:
        raise ValueError(
            f"Format non supporté : '{suffix}'. "
            "Formats acceptés : .csv, .parquet, .xlsx, .xls"
        )
        
    if len(df) > MAX_UPLOAD_ROWS:
        raise ValueError(f"Fichier trop conséquent : {len(df)} lignes (Maximum autorisé : {MAX_UPLOAD_ROWS}).")

    return df


def validate_uploaded_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Valide et nettoie le DataFrame uploadé.

    Travaille sur une copie interne — ne mute jamais le DataFrame appelant.
    Lève ValueError si le schéma (colonnes requises) est invalide.

    Retourne
    --------
    df_clean : pd.DataFrame
        DataFrame prêt à être enrichi et fusionné (lignes invalides supprimées,
        types numériques convertis, colonnes normalisées).
    warnings : List[str]
        Messages d'avertissement non bloquants à renvoyer au client.
    """
    warnings: List[str] = []

    # Travailler sur une copie pour ne pas modifier l'appelant
    df = df.copy()

    # 1. Normalisation des noms de colonnes (idempotent : process_upload le fait
    #    déjà en amont, mais la fonction doit rester autonome)
    df.columns = df.columns.str.strip().str.lower()

    # 2. Vérification du schéma (colonnes requises) — lève ValueError si échec
    validate_schema(df)

    # 3. Conversion et vérification des types
    df["note"] = pd.to_numeric(df["note"], errors="coerce")
    nb_notes_invalides = int(df["note"].isna().sum())
    if nb_notes_invalides > 0:
        warnings.append(f"{nb_notes_invalides} note(s) non numérique(s) ignorée(s)")

    notes_valides = df["note"].dropna()
    nb_hors_bornes = int(((notes_valides < 0) | (notes_valides > 20)).sum())
    if nb_hors_bornes > 0:
        warnings.append(f"{nb_hors_bornes} note(s) hors de la plage [0, 20]")

    df["semestre"] = pd.to_numeric(df["semestre"], errors="coerce")
    nb_sem_invalides = int(df["semestre"].isna().sum())
    if nb_sem_invalides > 0:
        warnings.append(f"{nb_sem_invalides} semestre(s) non numérique(s)")

    # Semestres hors plage réaliste [1, 12]
    sem_valides = df["semestre"].dropna()
    nb_sem_aberrants = int(((sem_valides < 1) | (sem_valides > 12)).sum())
    if nb_sem_aberrants > 0:
        warnings.append(f"{nb_sem_aberrants} semestre(s) hors plage [1-12] — sera(ont) supprimé(s)")
        df.loc[(df["semestre"] < 1) | (df["semestre"] > 12), "semestre"] = np.nan

    df["credit"] = pd.to_numeric(df["credit"], errors="coerce").fillna(1)
    df["cohorte"] = pd.to_numeric(df["cohorte"], errors="coerce")

    import datetime as _dt
    annee_max = _dt.date.today().year + 2

    def _annee_debut(val) -> int:
        if pd.isna(val):
            return -1
        s = str(val).strip()
        partie = s.split('-')[0].strip() if '-' in s else s
        try:
            return int(float(partie))
        except (ValueError, TypeError):
            return -1

    df["annee"] = df["annee"].astype(str).str.strip()
    debuts = df["annee"].apply(_annee_debut)
    masque_invalide = (debuts < 1990) | (debuts > annee_max)
    nb_annees_aberrantes = int(masque_invalide.sum())
    if nb_annees_aberrantes > 0:
        warnings.append(f"{nb_annees_aberrantes} année(s) hors plage [1990-{annee_max}] — ligne(s) supprimée(s)")
        df.loc[masque_invalide, "annee"] = np.nan

    # Codes UE vides ou contenant uniquement des espaces
    if "ue" in df.columns:
        df["ue"] = df["ue"].astype(str).str.strip()
        nb_ue_vides = int((df["ue"] == "").sum() + df["ue"].isna().sum())
        if nb_ue_vides > 0:
            warnings.append(f"{nb_ue_vides} code(s) UE vide(s) — ligne(s) supprimée(s)")
            df.loc[df["ue"] == "", "ue"] = np.nan

    # Anonymats vides
    if "anonymat" in df.columns:
        df["anonymat"] = df["anonymat"].astype(str).str.strip()
        df.loc[df["anonymat"].isin(["", "nan"]), "anonymat"] = np.nan

    # 4. Supprimer les lignes avec champs critiques manquants
    df_clean = df.dropna(subset=["note", "semestre", "anonymat", "ue", "annee"])
    nb_supprimees = len(df) - len(df_clean)
    if nb_supprimees > 0:
        warnings.append(f"{nb_supprimees} ligne(s) supprimée(s) (champs critiques manquants ou aberrants)")

    return df_clean, warnings


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Enrichit le DataFrame avec les colonnes dérivées du référentiel."""
    df = df.copy()
    df["filiere"] = df["filiere"].astype(str).str.upper()
    df["departement"] = df["filiere"].apply(lambda f: get_departement(str(f).upper()))
    df["type_formation"] = df.apply(
        lambda row: get_type_formation(str(row["filiere"]).upper(), int(row["semestre"])),
        axis=1
    )
    df["niveau"] = df.apply(
        lambda row: get_niveau(str(row["filiere"]).upper(), int(row["semestre"])),
        axis=1
    )
    return df


def _verifier_coherence_identifiants(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Vérifie la cohérence croisée carte ↔ anonymat entre le fichier importé
    et les données existantes. Deux cas rejetés :
      1. anonymat connu en base → carte doit être identique
      2. carte connue en base  → anonymat doit être identique
    """
    warnings: List[str] = []
    if existing_df.empty or "carte" not in existing_df.columns:
        return new_df, warnings

    ref_anon_to_carte = (
        existing_df.dropna(subset=["anonymat","carte"])
        .drop_duplicates(subset=["anonymat"])
        .set_index("anonymat")["carte"]
        .astype(str).str.strip()
    )
    ref_carte_to_anon = (
        existing_df.dropna(subset=["anonymat","carte"])
        .drop_duplicates(subset=["carte"])
        .set_index("carte")["anonymat"]
        .astype(str).str.strip()
    )

    new_df = new_df.copy()
    new_df["anonymat"] = new_df["anonymat"].astype(str).str.strip()
    new_df["carte"]    = new_df["carte"].astype(str).str.strip()
    mask_ok = pd.Series(True, index=new_df.index)

    # Cas 1 : anonymat connu → carte doit correspondre
    anons_connus = new_df["anonymat"].isin(ref_anon_to_carte.index)
    if anons_connus.any():
        carte_attendue = new_df.loc[anons_connus, "anonymat"].map(ref_anon_to_carte)
        incoherence    = anons_connus & (new_df["carte"] != carte_attendue)
        if incoherence.any():
            ex = new_df.loc[incoherence, ["anonymat","carte"]].iloc[0]
            warnings.append(
                f"{int(incoherence.sum())} ligne(s) rejetée(s) — anonymat '{ex['anonymat']}' "
                f"connu en base avec carte='{ref_anon_to_carte[ex['anonymat']]}' "
                f"mais carte='{ex['carte']}' fournie dans le fichier"
            )
            mask_ok &= ~incoherence

    # Cas 2 : carte connue → anonymat doit correspondre
    cartes_connues = new_df["carte"].isin(ref_carte_to_anon.index)
    if cartes_connues.any():
        anon_attendu = new_df.loc[cartes_connues, "carte"].map(ref_carte_to_anon)
        incoherence  = cartes_connues & (new_df["anonymat"] != anon_attendu)
        if incoherence.any():
            ex = new_df.loc[incoherence, ["carte","anonymat"]].iloc[0]
            warnings.append(
                f"{int(incoherence.sum())} ligne(s) rejetée(s) — carte '{ex['carte']}' "
                f"connue en base avec anonymat='{ref_carte_to_anon[ex['carte']]}' "
                f"mais anonymat='{ex['anonymat']}' fourni dans le fichier"
            )
            mask_ok &= ~incoherence

    new_df = new_df[mask_ok].reset_index(drop=True)

    # ── Vérification des attributs identitaires ──────────────────────────────
    # Deux catégories :
    #   CORRIGEABLES  : nom_prenoms, sexe — erreurs de saisie fréquentes,
    #                   la nouvelle valeur est acceptée et remplace l'ancienne.
    #   VERROUILLÉS   : cohorte, filiere — changement = fraude académique,
    #                   la ligne est rejetée silencieusement (log serveur uniquement).
    CHAMPS_CORRIGER  = ["nom_prenoms", "sexe"]
    CHAMPS_VERROUILLES = ["cohorte", "filiere"]
    CHAMPS_IDENTITE  = CHAMPS_CORRIGER + CHAMPS_VERROUILLES

    anons_presents = [c for c in CHAMPS_IDENTITE if c in existing_df.columns]
    if anons_presents and not existing_df.empty:
        ref_identite = (
            existing_df.dropna(subset=["anonymat"])
            .drop_duplicates(subset=["anonymat"])
            .set_index("anonymat")[anons_presents]
            .astype(str).apply(lambda s: s.str.strip().str.upper())
        )

        if not new_df.empty and "anonymat" in new_df.columns:
            new_df_check      = new_df.copy()
            new_df_check["anonymat"] = new_df_check["anonymat"].astype(str).str.strip()
            anons_connus_mask = new_df_check["anonymat"].isin(ref_identite.index)

            if anons_connus_mask.any():
                mask_rejeter = pd.Series(False, index=new_df_check.index)

                # ── Étape 1 : identifier les lignes avec champ verrouillé différent ──
                # Ces lignes sont rejetées ET ne bénéficient d'aucune correction,
                # car un champ verrouillé différent indique un étudiant distinct
                # (collision d'anonymat dans les données de test, ou vraie tentative).
                for champ in CHAMPS_VERROUILLES:
                    if champ not in new_df_check.columns:
                        continue
                    val_fournie  = new_df_check.loc[anons_connus_mask, champ].astype(str).str.strip().str.upper()
                    val_attendue = new_df_check.loc[anons_connus_mask, "anonymat"].map(ref_identite[champ])
                    vide         = val_fournie.isin(["", "NAN", "NONE"])
                    incoherence  = anons_connus_mask & (~vide) & (val_fournie != val_attendue)
                    if incoherence.any():
                        nb = int(incoherence.sum())
                        # Logger une seule ligne par (anonymat, champ) unique
                        seen = set()
                        for idx in new_df_check.index[incoherence]:
                            anon   = new_df_check.at[idx, "anonymat"]
                            fourni = new_df_check.at[idx, champ]
                            key    = (anon, champ)
                            if key in seen:
                                continue
                            seen.add(key)
                            attendu = ref_identite.at[anon, champ] if anon in ref_identite.index else "?"
                            logger.warning(
                                "REJET — champ verrouillé '%s' : "
                                "anonymat=%s importé='%s' base='%s'",
                                champ, anon, fourni, attendu,
                            )
                        warnings.append(
                            f"{nb} ligne(s) rejetée(s) : champ protégé '{champ}' "
                            f"ne correspond pas à l'étudiant enregistré."
                        )
                        mask_rejeter |= incoherence

                # ── Étape 2 : corrections légitimes — uniquement sur les lignes
                # dont TOUS les champs verrouillés sont identiques à la base ──
                mask_eligible = anons_connus_mask & ~mask_rejeter
                if mask_eligible.any():
                    for champ in CHAMPS_CORRIGER:
                        if champ not in new_df_check.columns:
                            continue
                        val_fournie  = new_df_check.loc[mask_eligible, champ].astype(str).str.strip().str.upper()
                        val_attendue = new_df_check.loc[mask_eligible, "anonymat"].map(ref_identite[champ])
                        vide         = val_fournie.isin(["", "NAN", "NONE"])
                        incoherence  = mask_eligible & (~vide) & (val_fournie != val_attendue)
                        if incoherence.any():
                            nb = int(incoherence.sum())
                            ex_anon = new_df_check.loc[incoherence, "anonymat"].iloc[0]
                            logger.info(
                                "Correction '%s' pour %d étudiant(s) — anonymat ex: %s",
                                champ, nb, ex_anon,
                            )
                            warnings.append(
                                f"{nb} enregistrement(s) : champ '{champ}' corrigé."
                            )
                            # Appliquer la correction dans new_df
                            new_df.loc[new_df.index[incoherence], champ] = (
                                new_df_check.loc[incoherence, "anonymat"]
                                .map(ref_identite[champ])
                                .values
                            )

                if mask_rejeter.any():
                    new_df = new_df[~mask_rejeter.values].reset_index(drop=True)

    return new_df, warnings


def merge_data(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, int, int, List[str]]:
    """
    Fusionne les données existantes avec les nouvelles données.
    Stratégie : clé composite (anonymat, ue, semestre, annee)
      - Si la clé existe → update
      - Sinon → ajout

    Retourne (df_fusionné, nb_ajoutés, nb_mis_à_jour, warnings_intégrité)
    """
    # Vérification intégrité carte ↔ anonymat avant toute fusion
    new_df, integrity_warnings = _verifier_coherence_identifiants(existing_df, new_df)

    # Normaliser les colonnes de fusion
    for col in MERGE_KEY:
        existing_df[col] = existing_df[col].astype(str).str.strip()
        new_df[col] = new_df[col].astype(str).str.strip()

    # Créer la clé composite
    existing_keys = existing_df[MERGE_KEY].apply(lambda r: tuple(r), axis=1)
    new_keys = new_df[MERGE_KEY].apply(lambda r: tuple(r), axis=1)

    # Séparer les mises à jour et les ajouts
    mask_update = new_keys.isin(set(existing_keys))
    df_updates = new_df[mask_update]
    df_adds = new_df[~mask_update]

    nb_updated = len(df_updates)
    nb_added = len(df_adds)

    if nb_updated > 0:
        update_keys_set = set(df_updates[MERGE_KEY].apply(lambda r: tuple(r), axis=1))
        mask_keep = ~existing_keys.isin(update_keys_set)
        existing_df = existing_df[mask_keep]
        existing_df = pd.concat([existing_df, df_updates], ignore_index=True)

    if nb_added > 0:
        existing_df = pd.concat([existing_df, df_adds], ignore_index=True)

    return existing_df, nb_added, nb_updated, integrity_warnings


def save_merged_data(df: pd.DataFrame) -> None:
    """
    Sauvegarde le DataFrame fusionné en parquet de manière atomique.
    Sauvegarde également un backup CSV.
    """
    # Supprimer les colonnes dérivées avant sauvegarde
    cols_derivees = {"departement", "type_formation", "niveau"}
    cols_a_sauver = [c for c in df.columns if c not in cols_derivees]
    df_save = df[cols_a_sauver].copy()

    # Convertir les catégories en chaînes pour la sauvegarde
    for col in df_save.columns:
        if df_save[col].dtype.name == "category":
            df_save[col] = df_save[col].astype(str)

    # Sauvegarde atomique
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    ts = int(time.time() * 1000)
    tmp_parquet = UPLOAD_DIR / f"data_merged_{ts}.parquet"

    try:
        # Création d'un backup sécuritaire de l'état précédent
        if DATA_PATH.exists():
            backup_path = DATA_PATH.with_name(DATA_PATH.name + ".bak")
            shutil.copy2(str(DATA_PATH), str(backup_path))
            
        df_save.to_parquet(tmp_parquet, index=False)
        # Remplacer le fichier principal
        shutil.move(str(tmp_parquet), str(DATA_PATH))
        # Sauvegarder aussi en CSV
        csv_path = DATA_PATH.with_suffix(".csv")
        df_save.to_csv(csv_path, index=False)
        logger.info(f"Données sauvegardées : {len(df_save)} lignes → {DATA_PATH}")
    except Exception:
        # Nettoyer les fichiers temporaires en cas d'erreur
        tmp_parquet.unlink(missing_ok=True)
        raise

    # Invalider le cache pour forcer le rechargement
    _invalidate_app_cache()


def process_upload(content: bytes, filename: str) -> Dict[str, Any]:
    """
    Pipeline complet de traitement d'un fichier uploadé.
    Retourne un résumé de l'opération.
    """
    # 1. Lire le fichier
    try:
        new_df = read_uploaded_file(content, filename)
        initial_count = len(new_df)

        # 2. Normaliser les noms de colonnes en amont, avant la validation
        new_df.columns = new_df.columns.str.strip().str.lower()

        # 3. Valider et nettoyer en une seule passe.
        new_df, warnings = validate_uploaded_data(new_df)

        if new_df.empty:
            res = {
                "added": 0,
                "updated": 0,
                "total": 0,
                "errors": ["Aucune ligne valide dans le fichier après nettoyage"],
                "warnings": warnings,
            }
            log_upload_event(filename, "error", res, error="Fichier vide ou invalide")
            return res

        # 4. Charger les données existantes (brutes, sans enrichissement)
        from app.services.data_processing import get_cached_data
        existing_df = get_cached_data().copy()

        # 5. Enrichir les nouvelles données
        new_df = enrich_dataframe(new_df)

        # 6. Fusionner
        merged_df, nb_added, nb_updated, integrity_warnings = merge_data(existing_df, new_df)
        warnings.extend(integrity_warnings)

        # 7. Sauvegarder
        save_merged_data(merged_df)

        res = {
            "added": nb_added,
            "updated": nb_updated,
            "total": len(merged_df),
            "lignes_fichier": initial_count,
            "lignes_valides": len(new_df),
            "errors": [],
            "warnings": warnings,
        }
        
        status = "success" if not warnings else "partial_success"
        log_upload_event(filename, status, res)
        return res

    except Exception as e:
        logger.exception("Erreur fatale d'upload pour %s : %s", filename, e)
        log_upload_event(filename, "error", error=str(e))
        raise
def _invalidate_app_cache():
    """Invalide tous les caches de l'application (données et figures)."""
    cache_donnees.timestamp   = 0.0
    cache_donnees.file_mtime  = 0.0
    cache_donnees.df          = None
    cache_donnees.fingerprint = None
    try:
        from app.services.plotting import clear_cache as clear_fig_cache
        clear_fig_cache(all_keys=True)
    except Exception as _e:
        logger.warning("Erreur lors du nettoyage du cache des figures: %s", _e)

def rollback_data(user: str = "admin") -> Dict[str, Any]:
    """
    Restaure les données à partir du fichier backup (.bak) s'il existe.
    """
    backup_path = DATA_PATH.with_name(DATA_PATH.name + ".bak")
    if not backup_path.exists():
        raise FileNotFoundError("Aucun backup disponible pour une restauration.")
    
    try:
        # 1. Remplacer le fichier principal par le backup
        shutil.copy2(str(backup_path), str(DATA_PATH))
        
        # 2. Synchroniser le CSV pour la cohérence
        df_restored = pd.read_parquet(DATA_PATH)
        csv_path = DATA_PATH.with_suffix(".csv")
        df_restored.to_csv(csv_path, index=False)
        
        # 3. Invalider les caches
        _invalidate_app_cache()
        
        # 4. Loguer l'événement
        log_upload_event("ROLLBACK_DATA", "success", {"added": 0, "updated": 0}, user=user)
        
        return {
            "status": "success",
            "message": "Données restaurées à l'état précédent avec succès.",
            "total_rows": len(df_restored)
        }
    except Exception as e:
        logger.exception("Échec du rollback : %s", e)
        log_upload_event("ROLLBACK_DATA", "error", error=str(e), user=user)
        raise
