from pathlib import Path

# Chemins 
BASE_DIR    = Path(__file__).resolve().parent.parent.parent
DATA_DIR    = BASE_DIR / "data"
DATA_PATH   = DATA_DIR / "donnees_generees.parquet"
REPORTS_DIR = BASE_DIR / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
FIGURES_TMP = FIGURES_DIR / "tmp"
UPLOAD_DIR  = DATA_DIR / "uploads"
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50 MB

# Paramètres pédagogiques
SEUIL_REUSSITE = 10          # Note minimale pour valider une UE
BUCKETS        = [0, 5, 10, 15, 20]
BUCKET_LABELS  = ["0-5", "5-10", "10-15", "15-20"]

# Paramètres de cache
CACHE_TTL      = 300         # Durée de vie du cache mémoire (secondes)

# Paramètres graphiques 
FIG_DPI = 120                # Résolution des figures exportées
