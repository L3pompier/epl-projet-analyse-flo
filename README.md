# GoodAdmin — Tableau de bord analytique académique

Outil web de visualisation et d'analyse statistique des résultats académiques.

- **Backend** : Python / FastAPI  
- **Frontend** : HTML + JavaScript + Tailwind CSS

---

## Prérequis

- Python **3.10 ou supérieur**
- Node.js **18 ou supérieur** (avec npm)

---

## Mise en place

### Étape 1 — Récupérer le projet

```bash
git clone https://github.com/L3pompier/epl-projet-analyse-flo
cd GoodAdmin
```

---

### Étape 2 — Configurer le backend

```bash
cd backend

# Créer l'environnement virtuel
python -m venv .venv

# L'activer
source .venv/bin/activate        # Linux / macOS
# .venv\Scripts\activate         # Windows

# Installer les dépendances
pip install -r requirements.txt
```

#### Configuration (fichier `.env`)

Créer le fichier `backend/.env` avec le contenu suivant :

```env
SECRET_KEY=remplacer_par_une_cle_secrete_longue_et_aleatoire
ADMIN_PASSWORD=remplacer_par_un_mot_de_passe_fort
TOKEN_EXPIRE_MINUTES=1440
ALLOWED_ORIGINS=*
```

> ⚠️ Sans ce fichier, l'application utilise des valeurs par défaut *

#### Données

Les fichiers de données ne sont pas inclus dans le dépôt. Déposer le fichier source à l'emplacement suivant :

```
backend/data/donnees_generees.parquet
```

> Ou importer un fichier CSV ou Parquet directement depuis l'interface une fois l'application démarrée.

---

### Étape 3 — Configurer le frontend

```bash
cd ../frontend

# Installer les dépendances
npm install

# Compiler le CSS Tailwind (étape obligatoire)
npm run build
```

> ⚠️ Sans cette compilation, l'interface s'affichera **sans aucun style**.

---

### Étape 4 — Lancer l'application

```bash
cd ../backend
source .venv/bin/activate

uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

- Interface : [http://localhost:8000](http://localhost:8000)  
- Documentation API : [http://localhost:8000/docs](http://localhost:8000/docs)

---

## Connexion par défaut

| Champ        | Valeur                                          |
|--------------|-------------------------------------------------|
| Utilisateur  | `admin`                                         |
| Mot de passe | `admin123` (ou la valeur de `ADMIN_PASSWORD`)   |

---

## Structure du projet

```
new_architecture/
├── backend/
│   ├── app/
│   │   ├── api/          # Endpoints REST et authentification
│   │   ├── core/         # Configuration, sécurité, rate limiting
│   │   ├── models/       # Schémas Pydantic
│   │   ├── services/     # Logique métier (analytics, graphiques, PDF…)
│   │   └── main.py       # Point d'entrée
│   ├── data/             # Données sources
│   ├── reports/          # Rapports générés
│   └── requirements.txt
└── frontend/
    ├── css/
    │   ├── input.css     # Source Tailwind
    │   └── styles.css    # Styles personnalisés
    ├── js/               # Modules frontend (api, app, ui, vues)
    ├── index.html
    └── package.json
```

---

## Commandes résumées

```bash
# 1. Backend
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# → Créer backend/.env
# → Déposer backend/data/donnees_generees.parquet

# 2. Frontend
cd ../frontend
npm install && npm run build

# 3. Démarrage
cd ../backend
uvicorn app.main:app --port 8000 --reload
```
