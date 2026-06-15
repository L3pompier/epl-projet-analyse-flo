from fastapi import FastAPI, Request
import os 
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
import logging

from app.api.endpoints import router
from app.api.auth import router as auth_router
from app.services.data_processing import get_cached_data
from app.models.schemas import ErrorResponse
from app.core.limiter import limiter
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("main-api")

import asyncio
from pathlib import Path
from app.core.config import FIGURES_DIR
import time
import json

async def garbage_collect_figures_loop():
    logger.info("Démarrage du service de Garbage Collection (nettoyage) des figures en arrière-plan.")
    while True:
        try:
            logger.info("Garbage Collection : Scan des figures expirées...")
            nettoyees = 0
            maintenant = time.time()
            if FIGURES_DIR.exists():
                for meta_file in FIGURES_DIR.rglob("*.meta.json"):
                    try:
                        with open(meta_file, "r", encoding="utf-8") as f:
                            meta_data = json.load(f)
                        gen_time = meta_data.get("generated_at_ts", 0)
                        ttl = meta_data.get("ttl_seconds", 3600)
                        if maintenant - gen_time > ttl:
                            image_file = meta_file.with_suffix("")
                            if image_file.suffix == '.meta': 
                                image_file = image_file.with_suffix("")
                            if image_file.exists(): image_file.unlink()
                            meta_file.unlink()
                            nettoyees += 1
                    except json.JSONDecodeError:
                        meta_file.unlink()
                    except Exception as e:
                        logger.error(f"Erreur lors du nettoyage d'un fichier : {e}")
            if nettoyees > 0:
                logger.info(f"Garbage Collection terminé : {nettoyees} figures expirées supprimées.")
        except Exception as e:
            logger.error(f"Erreur majeure dans la boucle de GC : {e}")
        await asyncio.sleep(3600)

logger.info("Pré-chargement des données au démarrage…")
_preload = get_cached_data()
logger.info(f"{len(_preload)} lignes chargées" if _preload is not None else "Aucune ligne chargée temporairement")

app = FastAPI(
    title="API GoodAdmin",
    description="API complète pour l'analyse des résultats académiques. ",
    version="1.0.0",
    docs_url="/docs", 
    redoc_url="/redoc",  
    openapi_url="/openapi.json",
    contact={
        "name": "Support API",
        "email": "support@example.com",
    },
    openapi_tags=[
        {
            "name": "Santé",
            "description": "Vérification de l'état de l'API et des données"
        },
        {
            "name": "Métadonnées",
            "description": "Informations sur les données disponibles et options de filtrage"
        },
        {
            "name": "Statistiques UE",
            "description": "Analyse détaillée des Unités d'Enseignement"
        },
        {
            "name": "Dashboard",
            "description": "Agrégats pour le tableau de bord principal"
        },
        {
            "name": "Étudiants",
            "description": "Parcours académique individuel des étudiants"
        },
        {
            "name": "Interprétation",
            "description": "Analyse pédagogique automatisée"
        },
        {
            "name": "Visualisations",
            "description": "Génération de graphiques et figures"
        },
        {
            "name": "Administration",
            "description": "Gestion du cache et maintenance"
        },
        {
            "name": "Données",
            "description": "Import et gestion des fichiers de données"
        },
        {
            "name": "Rapports",
            "description": "Export PDF et génération de rapports"
        }
    ]
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(garbage_collect_figures_loop())

_raw_origins = os.getenv("ALLOWED_ORIGINS", "*")
ALLOWED_ORIGINS: list[str] = (
    ["*"] if _raw_origins.strip() == "*"
    else [o.strip() for o in _raw_origins.split(",") if o.strip()]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=ALLOWED_ORIGINS != ["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

@app.exception_handler(ValueError)
async def gerer_erreur_validation(_: Request, exc: ValueError):
    return JSONResponse(status_code=400, content=ErrorResponse(error="Erreur de validation", detail=str(exc), code=400).model_dump())

@app.exception_handler(Exception)
async def gerer_erreur_serveur(_: Request, exc: Exception):
    err_str = str(exc).lower()
    if "anonymat" in err_str or "nom_prenoms" in err_str:
        logger.error("Exception interceptée contenant de possibles données PII. Détails masqués.")
    else:
        logger.exception("Erreur serveur inattendue : %s", type(exc).__name__)
        
    return JSONResponse(status_code=500, content=ErrorResponse(error="Erreur interne du serveur", detail="Une erreur inattendue a bloqué le processus.", code=500).model_dump())

app.include_router(auth_router, prefix="/api/auth")
app.include_router(router, prefix="/api")

frontend_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "frontend")
if os.path.exists(frontend_dir):
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True, reload_excludes=["reports", "data"])
