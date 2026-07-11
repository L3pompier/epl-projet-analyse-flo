import io
import time
import json
import hashlib
import matplotlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
import diskcache
from app.core.config import FIGURES_DIR, FIGURES_TMP, CACHE_TTL, BUCKETS, BUCKET_LABELS, SEUIL_REUSSITE, FIG_DPI, DATA_PATH

THEME_BG = "#161b22"
THEME_TEXT = "#e6edf3"
THEME_GRID = "#30363d"
ACCENT_INDIGO = "#6366f1"
ACCENT_CYAN = "#22d3ee"
ACCENT_BLUE = "#3b82f6"

# Paramètres globaux Matplotlib
FONT_SIZE_TITLE = 15
FONT_SIZE_LABEL = 13
FONT_SIZE_TICK  = 12
FONT_SIZE_ANNOT = 12

plt.rcParams.update({
    "figure.facecolor": THEME_BG,
    "axes.facecolor": THEME_BG,
    "axes.edgecolor": THEME_GRID,
    "axes.labelcolor": THEME_TEXT,
    "text.color": THEME_TEXT,
    "xtick.color": THEME_TEXT,
    "ytick.color": THEME_TEXT,
    "grid.color": THEME_GRID,
    "grid.alpha": 0.5,
    "font.family": "sans-serif",
    "font.sans-serif": ["DM Sans", "DejaVu Sans", "Arial"],
    "font.size":        FONT_SIZE_TICK,
    "axes.titlesize":   FONT_SIZE_TITLE,
    "axes.labelsize":   FONT_SIZE_LABEL,
    "xtick.labelsize":  FONT_SIZE_TICK,
    "ytick.labelsize":  FONT_SIZE_TICK,
    "legend.fontsize":  FONT_SIZE_TICK,
})

PALETTE_MAIN = sns.color_palette("ch:s=-.2,r=.6", as_cmap=True) # Dégradé indigo/cyan personnalisé
COLOR_REUSSITE = "#10b981"
COLOR_ECHEC = "#ef4444"
COLOR_MEAN = "#f59e0b"
COLOR_MEDIAN = "#8b5cf6"

class PlotFunction(Protocol):
    def __call__(self, df: pd.DataFrame) -> plt.Figure: ...


_shared_cache_dir = DATA_PATH.parent / ".shared_cache"
TABLEAU_CACHE = diskcache.Cache(str(_shared_cache_dir / "tableau"))
FIGURE_CACHE = diskcache.Cache(str(_shared_cache_dir / "figure"))

def build_figure_subpath(
    dossier: Path,
    annee, semestre, cohorte, ue, vue: str, nom_fichier: str,
    filiere=None, departement=None, type_formation=None, niveau=None,
) -> Path:
    """
    Structure plate : <FIGURES_DIR>/<vue>/<hash>.<ext>
    """
    sous_dossier = dossier / vue
    sous_dossier.mkdir(parents=True, exist_ok=True)
    return sous_dossier / nom_fichier

def make_cache_key(annee, semestre, cohorte, sexe, ue, vue: str, filiere=None, departement=None, type_formation=None, niveau=None) -> str:
    return (f"a={annee or ''}|s={semestre if semestre is not None else ''}"
            f"|c={cohorte if cohorte is not None else ''}|x={sexe or ''}"
            f"|ue={ue or ''}|f={filiere or ''}|d={departement or ''}"
            f"|t={type_formation or ''}|n={niveau or ''}|v={vue}")

def get_cached(cache: dict, key: str, current_fingerprint: str) -> Optional[Any]:
    entree = cache.get(key)
    if not entree: return None
    valeur, horodatage, cached_fp = entree
    if time.time() - horodatage > CACHE_TTL:
        del cache[key]
        return None
    if cached_fp != current_fingerprint:
        del cache[key]
        return None
    return valeur

def set_cached_tableau(key: str, donnees: List[Dict[str, Any]], fingerprint: str):
    import copy
    TABLEAU_CACHE[key] = (copy.deepcopy(donnees), time.time(), fingerprint)
    
def get_cached_tableau(key: str, fp: str):
    import copy
    valeur = get_cached(TABLEAU_CACHE, key, fp)
    return copy.deepcopy(valeur) if valeur else None

def set_cached_figure(key: str, chemin_fichier: str, fingerprint: str = ""):
    FIGURE_CACHE[key] = (chemin_fichier, time.time(), fingerprint)

def get_cached_figure(key: str, current_fingerprint: str) -> Optional[str]:
    chemin = get_cached(FIGURE_CACHE, key, current_fingerprint)
    if chemin and not Path(chemin).exists():
        if key in FIGURE_CACHE: FIGURE_CACHE.pop(key, None)
        return None
    return chemin

def clear_cache(all_keys: bool = True, key: Optional[str] = None):
    import logging
    logger = logging.getLogger(__name__)
    if all_keys:
        TABLEAU_CACHE.clear()
        FIGURE_CACHE.clear()
        plt.close("all")
        for motif in ["**/*.png", "**/*.meta.json"]:
            for fichier in FIGURES_DIR.glob(motif):
                try: fichier.unlink(missing_ok=True)
                except Exception as erreur: logger.error(f"Impossible de supprimer {fichier} : {erreur}")
        for dossier in sorted(FIGURES_DIR.glob("**/"), reverse=True):
            try:
                if dossier != FIGURES_DIR and dossier.is_dir() and not any(dossier.iterdir()):
                    dossier.rmdir()
            except Exception: continue
    elif key:
        TABLEAU_CACHE.pop(key, None)
        FIGURE_CACHE.pop(key, None)

def _close_excess_figures(threshold: int = 10) -> None:
    import matplotlib._pylab_helpers as _helpers
    open_count = len(_helpers.Gcf.get_all_fig_managers())
    if open_count > threshold:
        plt.close("all")


def save_figure_atomic(fig: plt.Figure, chemin_cible: Path, meta: dict, fmt: str = "png"):
    chemin_final = chemin_cible
    nom_fichier  = chemin_cible.name
    digest       = chemin_cible.stem
    ts_ms      = int(time.time() * 1000)
    FIGURES_TMP.mkdir(parents=True, exist_ok=True)
    tmp_img    = FIGURES_TMP / f"{nom_fichier}.tmp-{ts_ms}"
    tmp_meta   = FIGURES_TMP / f"{digest}.meta.json.tmp-{ts_ms}"
    try:
        fig.savefig(tmp_img, format=fmt, bbox_inches="tight")
        tmp_img.replace(chemin_final)
        meta_complete = {
            **meta, "generated_at_ts": int(time.time()),
            "ttl_seconds": meta.get("ttl_seconds", CACHE_TTL),
            "data_fingerprint": meta.get("data_fingerprint"),
            "params": meta.get("params", {}), "source_path": chemin_final.name,
        }
        tmp_meta.write_text(json.dumps(meta_complete))
        tmp_meta.replace(chemin_final.with_suffix(".meta.json"))
    finally:
        plt.close(fig)
        tmp_img.unlink(missing_ok=True)
        tmp_meta.unlink(missing_ok=True)
        _close_excess_figures(threshold=8)

def render_insufficient_data_image(message: str = "Données insuffisantes", largeur: int = 800, hauteur: int = 400) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(largeur / FIG_DPI, hauteur / FIG_DPI))
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=13, color=THEME_TEXT, alpha=0.5, wrap=True, transform=ax.transAxes)
    ax.axis("off")
    fig.tight_layout()
    return fig

def _sem_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype(float)


def _guard(df: pd.DataFrame, msg: str, l: int = 900, h: int = 450):
    if df is None or df.empty: return render_insufficient_data_image(msg, l, h)
    return None

def plot_hist_generic(df_in: pd.DataFrame) -> plt.Figure:
    garde = _guard(df_in, "Pas assez de données pour l'histogramme")
    if garde: return garde
    notes = df_in["note"].dropna()
    moyenne = notes.mean()
    mediane = notes.median()
    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)
    sns.histplot(notes, bins=BUCKETS, kde=True, color=ACCENT_INDIGO, edgecolor="white", linewidth=0.6, ax=ax)
    if not np.isnan(moyenne): ax.axvline(moyenne, color=COLOR_MEAN, linestyle="--", linewidth=1.8, label=f"Moyenne = {moyenne:.2f}")
    if not np.isnan(mediane): ax.axvline(mediane, color=COLOR_MEDIAN, linestyle=":", linewidth=1.8, label=f"Médiane = {mediane:.2f}")
    ax.axvline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="-.", linewidth=1.4, label=f"Seuil réussite = {SEUIL_REUSSITE}")
    ax.set_title("Distribution des notes", color=THEME_TEXT)
    ax.set_xlabel("Note", color=THEME_TEXT)
    ax.set_ylabel("Nombre d'étudiants", color=THEME_TEXT)
    ax.tick_params(colors=THEME_TEXT)
    ax.xaxis.set_major_locator(mticker.MultipleLocator(2))
    leg = ax.legend(frameon=False, labelcolor=THEME_TEXT)
    ax.grid(True, linestyle=":", alpha=0.1, color=THEME_GRID)
    fig.tight_layout()
    return fig

def plot_box_generic(df_in: pd.DataFrame) -> plt.Figure:
    garde = _guard(df_in, "Pas assez de données pour le boxplot")
    if garde: return garde
    notes = df_in["note"].dropna()
    fig, ax = plt.subplots(figsize=(8, 3.5), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)
    sns.boxplot(x=notes, color=ACCENT_INDIGO, width=0.45, flierprops=dict(marker="o", markerfacecolor=COLOR_ECHEC, markersize=4, alpha=0.6, markeredgecolor="white"), ax=ax)
    ax.axvline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="--", linewidth=1.4, label=f"Seuil réussite = {SEUIL_REUSSITE}")
    ax.set_title("Distribution des notes (boxplot)", color=THEME_TEXT)
    ax.set_xlabel("Note", color=THEME_TEXT)
    ax.tick_params(colors=THEME_TEXT)
    ax.yaxis.set_visible(False)
    ax.legend(frameon=False, labelcolor=THEME_TEXT)
    fig.tight_layout()
    return fig

def plot_box_by_sex(df_in: pd.DataFrame) -> plt.Figure:
    garde = _guard(df_in, "Pas assez de données pour la comparaison par sexe")
    if garde: return garde
    df_plot = df_in.dropna(subset=["note", "sexe"]).copy()
    df_plot["sexe"] = df_plot["sexe"].astype(str).str.upper()
    sexes   = sorted(df_plot["sexe"].unique())
    palette = {"M": "#5b9bd5", "F": "#f08080"}
    palette = {s: palette.get(s, "#95a5a6") for s in sexes}
    fig, ax = plt.subplots(figsize=(7, 5), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)
    sns.violinplot(data=df_plot, x="sexe", y="note", palette=palette, inner=None, cut=0, linewidth=0.8, ax=ax)
    sns.boxplot(data=df_plot, x="sexe", y="note", palette=palette, width=0.18, linewidth=1.2, flierprops=dict(marker=".", color="grey", alpha=0.5), ax=ax)
    ax.axhline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="--", linewidth=1.4, label=f"Seuil réussite = {SEUIL_REUSSITE}")
    effectifs = df_plot.groupby("sexe")["anonymat"].nunique() if "anonymat" in df_plot.columns else df_plot.groupby("sexe")["note"].count()
    y_bas = df_plot["note"].min() - 1.2
    for i, s in enumerate(sexes): ax.text(i, y_bas, f"n = {effectifs.get(s, 0)}", ha="center", va="top", fontsize=9, color=THEME_TEXT, alpha=0.6)
    ax.set_title("Distribution des notes par sexe", color=THEME_TEXT)
    ax.set_xlabel("Sexe", color=THEME_TEXT)
    ax.set_ylabel("Note", color=THEME_TEXT)
    ax.tick_params(colors=THEME_TEXT)
    ax.legend(frameon=False, labelcolor=THEME_TEXT)
    fig.tight_layout()
    return fig

def plot_courbe_moyenne_par_sexe(df_in: pd.DataFrame) -> plt.Figure:
    garde = _guard(df_in, "Pas assez de données pour la courbe par sexe")
    if garde: return garde
    df_plot = df_in.dropna(subset=["note", "sexe", "semestre"]).copy()
    df_plot["semestre"] = _sem_int(df_plot["semestre"])
    df_plot["sexe"] = df_plot["sexe"].astype(str).str.upper()
    def moyenne_ponderee(groupe: pd.DataFrame) -> float:
        poids = groupe["credit"].fillna(1)
        return float(np.average(groupe["note"], weights=poids)) if poids.sum() > 0 else float(groupe["note"].mean())
    moyennes = df_plot.groupby(["sexe", "semestre"], observed=False).apply(moyenne_ponderee, include_groups=False).reset_index(name="moyenne")
    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)
    sns.lineplot(data=moyennes, x="semestre", y="moyenne", hue="sexe", marker="o", linewidth=2, palette={"M": ACCENT_BLUE, "F": "#f472b6"}, ax=ax)
    ax.axhline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="--", linewidth=1.2, alpha=0.7, label=f"Seuil = {SEUIL_REUSSITE}")
    semestres = sorted(moyennes["semestre"].unique())
    ax.set_xticks(semestres)
    ax.set_xlim(min(semestres) - 0.4, max(semestres) + 0.4)
    ax.set_title("Évolution de la moyenne par sexe et semestre", color=THEME_TEXT)
    ax.set_xlabel("Semestre", color=THEME_TEXT)
    ax.set_ylabel("Moyenne pondérée", color=THEME_TEXT)
    ax.tick_params(colors=THEME_TEXT)
    leg = ax.legend(title="Sexe", frameon=False, labelcolor=THEME_TEXT)
    if leg.get_title(): leg.get_title().set_color(THEME_TEXT)
    fig.tight_layout()
    return fig

def plot_heatmap_ue_semestre(df_in: pd.DataFrame) -> plt.Figure:
    garde = _guard(df_in, "Pas assez de données pour la heatmap", 1000, 800)
    if garde: return garde
    df_in = df_in.copy(); df_in["semestre"] = _sem_int(df_in["semestre"])
    pivot  = df_in.pivot_table(index="ue", columns="semestre", values="note", aggfunc="mean", observed=False)
    nb_ues = len(pivot)
    hauteur = max(6, min(nb_ues * 0.35, 24))
    fig, ax = plt.subplots(figsize=(10, hauteur), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)
    sns.heatmap(pivot, cmap="YlGnBu", annot=(nb_ues <= 40), fmt=".1f", linewidths=0.4, linecolor=THEME_GRID, cbar_kws={"label": "Moyenne", "shrink": 0.6}, ax=ax)
    ax.set_title("Heatmap des moyennes — UE × Semestre", color=THEME_TEXT)
    ax.set_xlabel("Semestre", color=THEME_TEXT)
    ax.set_ylabel("UE", color=THEME_TEXT)
    ax.tick_params(colors=THEME_TEXT)
    # Mettre à jour la couleur de l'étiquette de la barre de couleurs
    cbar = ax.collections[0].colorbar
    cbar.set_label("Moyenne", color=THEME_TEXT)
    cbar.ax.yaxis.set_tick_params(color=THEME_TEXT, labelcolor=THEME_TEXT)
    fig.tight_layout()
    return fig

def plot_courbe_cohortes(df_in: pd.DataFrame) -> plt.Figure:
    """Évolution des moyennes par cohorte + moyenne générale toutes cohortes.
    Appelé sans filtres annee/semestre/cohorte."""
    garde = _guard(df_in, "Pas assez de données pour la courbe par cohorte")
    if garde: return garde
    df_in = df_in.copy()
    df_in["semestre"] = _sem_int(df_in["semestre"])
    df_in = df_in.dropna(subset=["semestre"])
    df_in["semestre"] = df_in["semestre"].astype(int)

    # Moyenne par cohorte × semestre
    courbes = (df_in.groupby(["cohorte", "semestre"], observed=True, as_index=False)["note"]
               .mean().rename(columns={"note": "moyenne"}))
    # Moyenne générale par semestre (toutes cohortes confondues)
    moy_gen = (df_in.groupby("semestre", observed=True)["note"]
               .mean().reset_index(name="moyenne")
               .sort_values("semestre"))

    semestres = sorted(df_in["semestre"].unique())
    if not semestres:
        return render_insufficient_data_image("Aucun semestre disponible")

    fig, ax = plt.subplots(figsize=(10, 5), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)

    # Tracé par cohorte (lignes fines, couleurs palette)
    palette = sns.color_palette("tab10", n_colors=courbes["cohorte"].nunique())
    sns.lineplot(data=courbes, x="semestre", y="moyenne", hue="cohorte",
                 marker="o", linewidth=1.6, alpha=0.75, palette=palette, ax=ax)

    # Moyenne générale 
    ax.plot(moy_gen["semestre"], moy_gen["moyenne"],
            color=THEME_TEXT, linewidth=2.8, linestyle="-",
            marker="D", markersize=6, label="Moyenne générale", zorder=5)

    ax.axhline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="--",
               linewidth=1.2, alpha=0.7, label=f"Seuil {SEUIL_REUSSITE}")

    ax.set_xticks(semestres)
    ax.set_xlim(min(semestres) - 0.4, max(semestres) + 0.4)
    ax.set_title("Évolution des moyennes par cohorte + moyenne générale",
                 color=THEME_TEXT, fontsize=FONT_SIZE_TITLE)
    ax.set_xlabel("Semestre", color=THEME_TEXT, fontsize=FONT_SIZE_LABEL)
    ax.set_ylabel("Moyenne", color=THEME_TEXT, fontsize=FONT_SIZE_LABEL)
    ax.tick_params(colors=THEME_TEXT)
    ax.grid(axis="y", linestyle="--", alpha=0.12)

    leg = ax.legend(title="Légende", frameon=False, labelcolor=THEME_TEXT,
                    fontsize=FONT_SIZE_TICK)
    if leg.get_title(): leg.get_title().set_color(THEME_TEXT)
    fig.tight_layout()
    return fig

def plot_evolution_moyenne_by_annee(df_in: pd.DataFrame) -> plt.Figure:
    garde = _guard(df_in, "Pas assez de données pour l'évolution des moyennes")
    if garde: return garde

    def moyenne_ponderee(groupe: pd.DataFrame) -> float:
        poids = groupe["credit"].fillna(1)
        return float(np.average(groupe["note"], weights=poids)) if poids.sum() > 0 else float(groupe["note"].mean())

    moyennes  = df_in.groupby(["annee", "semestre"]).apply(moyenne_ponderee, include_groups=False).reset_index(name="moyenne")
    annees    = sorted(moyennes["annee"].unique())
    semestres = sorted(moyennes["semestre"].unique())

    if not annees or not semestres:
        return render_insufficient_data_image("Pas assez de données pour l'évolution annuelle")

    largeur_barre = 0.8 / len(annees)
    positions     = np.arange(len(semestres))

    fig, ax = plt.subplots(figsize=(9, 4.5), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)

    for i, annee in enumerate(annees):
        data_annee = moyennes[moyennes["annee"] == annee]
        data_indexed = (
            data_annee.set_index("semestre")
            .reindex(semestres)["moyenne"]
            .fillna(0)
            .values
        )
        pos_barres = positions + i * largeur_barre
        ax.bar(pos_barres, data_indexed, largeur_barre, label=annee, alpha=0.8)

    ax.axhline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="--", linewidth=1.2, alpha=0.7, label=f"Seuil = {SEUIL_REUSSITE}")
    ax.set_xticks(positions + largeur_barre * (len(annees) - 1) / 2)
    ax.set_xticklabels(semestres)
    ax.set_title("Évolution des moyennes par année académique", color=THEME_TEXT)
    ax.set_xlabel("Semestre", color=THEME_TEXT)
    ax.set_ylabel("Moyenne pondérée", color=THEME_TEXT)
    ax.tick_params(colors=THEME_TEXT)
    leg = ax.legend(title="Année", frameon=False, labelcolor=THEME_TEXT)
    if leg.get_title(): leg.get_title().set_color(THEME_TEXT)
    fig.tight_layout()
    return fig

def plot_evolution_taux_by_semestre(df_in: pd.DataFrame) -> plt.Figure:
    garde = _guard(df_in, "Pas assez de données pour le taux de réussite par semestre")
    if garde: return garde
    agg = df_in.groupby("semestre").agg(taux=("note", lambda x: (x >= SEUIL_REUSSITE).mean() * 100)).reset_index()
    fig, ax = plt.subplots(figsize=(9, 4), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)
    ax.plot(agg["semestre"], agg["taux"], color=ACCENT_CYAN, marker="o", linewidth=2.5, label="Taux de réussite (%)")
    ax.fill_between(agg["semestre"], agg["taux"], 0, color=ACCENT_CYAN, alpha=0.15)
    ax.set_ylim(0, 105)
    ax.set_xticks(sorted(agg["semestre"].unique()))
    ax.set(title="Taux de réussite par semestre", xlabel="Semestre", ylabel="Réussite (%)")
    ax.grid(axis="y", linestyle="--", alpha=0.2)
    fig.tight_layout()
    return fig

def plot_validation_global(df_in: pd.DataFrame) -> plt.Figure:
    """Taux de réussite global par semestre (courbe + barres)."""
    garde = _guard(df_in, "Pas assez de données pour le taux par semestre")
    if garde: return garde
    df_v = df_in.dropna(subset=["note", "semestre"]).copy()
    df_v["semestre"] = _sem_int(df_v["semestre"])
    agg = (
        df_v.groupby("semestre", observed=True)
        .agg(
            taux=("note", lambda x: (x >= SEUIL_REUSSITE).mean() * 100),
            effectif=("anonymat", "nunique" if "anonymat" in df_v.columns else "count"),
        )
        .reset_index()
        .sort_values("semestre")
    )
    if agg.empty: return render_insufficient_data_image("Aucun semestre disponible")
    fig, ax1 = plt.subplots(figsize=(9, 4.5), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax1.set_facecolor(THEME_BG)
    # Barres effectif (axe secondaire)
    ax2 = ax1.twinx()
    ax2.bar(agg["semestre"], agg["effectif"], color=ACCENT_INDIGO, alpha=0.18, label="Effectif")
    ax2.set_ylabel("Effectif", color=THEME_TEXT, fontsize=FONT_SIZE_LABEL)
    ax2.tick_params(colors=THEME_TEXT)
    ax2.set_facecolor(THEME_BG)
    # Courbe taux (axe principal)
    ax1.plot(agg["semestre"], agg["taux"], color=COLOR_REUSSITE, marker="o",
             linewidth=2.5, label="Taux de réussite (%)", zorder=5)
    ax1.fill_between(agg["semestre"], agg["taux"], 0, color=COLOR_REUSSITE, alpha=0.10)
    ax1.axhline(50, color=COLOR_ECHEC, linestyle="--", linewidth=1.4, alpha=0.8, label="Seuil 50 %")
    ax1.set_ylim(0, 105)
    ax1.set_xticks(sorted(agg["semestre"].unique()))
    ax1.set_xlabel("Semestre", color=THEME_TEXT, fontsize=FONT_SIZE_LABEL)
    ax1.set_ylabel("Taux de réussite (%)", color=THEME_TEXT, fontsize=FONT_SIZE_LABEL)
    ax1.tick_params(colors=THEME_TEXT)
    ax1.set_title("Taux de réussite global par semestre", color=THEME_TEXT, fontsize=FONT_SIZE_TITLE)
    ax1.grid(axis="y", linestyle="--", alpha=0.15)
    # Légende combinée
    lines1, labs1 = ax1.get_legend_handles_labels()
    lines2, labs2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labs1 + labs2, frameon=False, labelcolor=THEME_TEXT, fontsize=FONT_SIZE_TICK)
    plt.tight_layout()
    return fig

def donut(df_in: pd.DataFrame) -> plt.Figure:
    """
    Double anneau (donut) comme dans script.py :
        - Anneau externe : taux global réussite / échec
        - Anneau interne : déclinaison par sexe (M / F)
    """
    garde = _guard(df_in, "Pas assez de données pour le donut", 1200, 600)
    if garde: return garde

    df_plot = df_in.dropna(subset=["note"]).copy()
    if "sexe" in df_plot.columns:
        df_plot["sexe"] = df_plot["sexe"].astype(str).str.upper()
    else:
        df_plot["sexe"] = "Inconnu"
    
    reussi = df_plot["note"] >= SEUIL_REUSSITE
    nb_reussis = int(reussi.sum())
    nb_echoues = len(df_plot) - nb_reussis
    total = len(df_plot)

    def nb(cond): return int(cond.sum())

    vals_ext = [nb_reussis, nb_echoues]
    labels_ext = ["Admis", "Ajourné"]
    couleurs_ext = [COLOR_REUSSITE, COLOR_ECHEC]

    # Déclinaison par sexe pour l'anneau interne
    vals_int = [
        nb(reussi & (df_plot["sexe"] == "M")),
        nb(reussi & (df_plot["sexe"] == "F")),
        nb(~reussi & (df_plot["sexe"] == "M")),
        nb(~reussi & (df_plot["sexe"] == "F")),
    ]
    # Si des sexes sont inconnus ou autres, on peut avoir du reste, mais restons sur M/F pour la clarté
    labels_int = ["Hommes Admis", "Femmes Admises", "Hommes Ajournés", "Femmes Ajournées"]
    # Variantes plus claires/sombres des couleurs de base
    couleurs_int = ["#059669", "#34d399", "#dc2626", "#f87171"] # Verts et Rouges variés

    fig, (ax_donut, ax_legende) = plt.subplots(1, 2, figsize=(14, 7), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax_donut.set_facecolor(THEME_BG)
    ax_legende.set_facecolor(THEME_BG)

    formater = lambda pct: f"{pct:.1f}%" if pct > 3 else ""

    # Anneau externe
    wedges_ext, _, textes_ext = ax_donut.pie(
        vals_ext, labels=None, autopct=formater, colors=couleurs_ext,
        radius=1.0, startangle=90,
        wedgeprops=dict(width=0.25, edgecolor=THEME_BG, linewidth=2),
        pctdistance=0.87,
    )
    plt.setp(textes_ext, size=FONT_SIZE_LABEL + 6, weight="bold", color="white")

    # Anneau interne
    wedges_int, _, textes_int = ax_donut.pie(
        vals_int, labels=None, autopct=formater, colors=couleurs_int,
        radius=0.72, startangle=90,
        wedgeprops=dict(width=0.22, edgecolor=THEME_BG, linewidth=1.5),
        pctdistance=0.78,
    )
    plt.setp(textes_int, size=FONT_SIZE_TICK + 4, color="white", alpha=0.9)

    # Texte central
    pct_glob = nb_reussis / total * 100 if total else 0
    ax_donut.text(0, 0.05, f"{pct_glob:.1f}%", ha="center", va="center", fontsize=40, fontweight="bold", color=COLOR_REUSSITE)
    ax_donut.text(0, -0.15, "réussite", ha="center", va="center", fontsize=FONT_SIZE_LABEL + 4, color=THEME_TEXT, alpha=0.7)
    
    ax_donut.set_title("Bilan Réussite / Échec (Global & Sexe)", color=THEME_TEXT, fontsize=FONT_SIZE_TITLE + 4, pad=20)
    ax_donut.set(aspect="equal")

    mx = vals_int[0]; fx = vals_int[1]; ma = vals_int[2]; fa = vals_int[3]
    labels_ext_pct = [
        f"Admis ({nb_reussis/total*100:.1f}% — {nb_reussis})",
        f"Ajournés ({nb_echoues/total*100:.1f}% — {nb_echoues})",
    ]
    labels_int_pct = []
    wedges_int_vis  = []
    for lbl, n, w in zip(
        ["H. admis", "F. admises", "H. ajournés", "F. ajournées"],
        [mx, fx, ma, fa], wedges_int
    ):
        if n > 0:
            labels_int_pct.append(f"{lbl} ({n/total*100:.1f}%)")
            wedges_int_vis.append(w)

    ax_legende.axis("off")
    leg = ax_legende.legend(
        list(wedges_ext) + wedges_int_vis,
        labels_ext_pct + labels_int_pct,
        loc="center left",
        frameon=False,
        labelcolor=THEME_TEXT,
        fontsize=FONT_SIZE_LABEL + 4,
        title="Répartition détaillée",
        title_fontsize=FONT_SIZE_LABEL + 5,
    )
    plt.setp(leg.get_title(), color=THEME_TEXT, fontweight="bold")

    plt.tight_layout()
    return fig

def plot_heatmap_filiere_semestre(df_in: pd.DataFrame) -> plt.Figure:
    garde = _guard(df_in, "Pas assez de données pour la heatmap filière")
    if garde: return garde
    if "filiere" not in df_in.columns: return render_insufficient_data_image("Données 'filiere' introuvables")
    df_in = df_in.copy(); df_in["semestre"] = _sem_int(df_in["semestre"])
    pivot = df_in.pivot_table(index="filiere", columns="semestre", values="note", aggfunc="mean", observed=False)
    if pivot.empty: return render_insufficient_data_image("Heatmap vide")
    fig, ax = plt.subplots(figsize=(8, 5), dpi=FIG_DPI)
    sns.heatmap(pivot, cmap="YlGnBu", annot=True, fmt=".1f", linewidths=0.5, linecolor="#eee", ax=ax, cbar_kws={"label": "Moyenne"})
    ax.set(title="Moyennes Académiques : Filière × Semestre", xlabel="Semestre", ylabel="Filière")
    fig.tight_layout()
    return fig

def plot_radar_filieres(df_semestres: pd.DataFrame) -> plt.Figure:
    """Radar araignée sur TOUS les semestres disponibles, toutes filières."""
    garde = _guard(df_semestres, "Pas assez de données pour le radar")
    if garde: return garde
    df_valid = df_semestres.dropna(subset=["filiere", "semestre", "note"]).copy()
    df_valid["semestre"] = _sem_int(df_valid["semestre"])
    if df_valid.empty: return render_insufficient_data_image("Aucune donnée disponible")
    pivot = (
        df_valid.groupby(["filiere", "semestre"], observed=True)["note"]
        .mean().unstack("semestre").fillna(0)
    )
    if pivot.empty: return render_insufficient_data_image("Matrice radar vide")
    categories = sorted(pivot.columns.tolist())
    N = len(categories)
    if N < 3: return render_insufficient_data_image("Moins de 3 semestres — radar impossible")
    angles = [n / float(N) * 2 * np.pi for n in range(N)]
    angles += angles[:1]
    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw={"polar": True}, dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels([f"S{s}" for s in categories], size=FONT_SIZE_LABEL, weight="semibold", color=THEME_TEXT)
    ax.set_ylim(0, 20)
    ax.set_yticks([5, 10, 15, 20])
    ax.set_yticklabels(["5", "10", "15", "20"], color="grey", size=FONT_SIZE_TICK - 1)
    ax.yaxis.set_tick_params(labelcolor="grey")
    # Grille colorée
    ax.grid(color=THEME_GRID, linestyle="--", linewidth=0.6, alpha=0.5)
    ax.spines["polar"].set_color(THEME_GRID)
    # Ligne seuil réussite
    seuil_vals = [SEUIL_REUSSITE] * N + [SEUIL_REUSSITE]
    ax.plot(angles, seuil_vals, color=COLOR_ECHEC, linewidth=1.2, linestyle="--", alpha=0.6, label=f"Seuil {SEUIL_REUSSITE}")
    palette = sns.color_palette("Set2", n_colors=len(pivot))
    for (filiere, row), color in zip(pivot.iterrows(), palette):
        vals = row.tolist() + [row.tolist()[0]]
        ax.plot(angles, vals, linewidth=2.2, linestyle="solid", label=str(filiere), color=color)
        ax.fill(angles, vals, color=color, alpha=0.12)
        # Annoter la valeur max
        max_idx = int(np.argmax(row.values))
        ax.annotate(f"{row.values[max_idx]:.1f}",
                    xy=(angles[max_idx], row.values[max_idx]),
                    fontsize=FONT_SIZE_TICK - 1, color=color, ha="center")
    sems_label = " · ".join(f"S{s}" for s in categories)
    ax.set_title(f"Profil comparatif des filières — {sems_label}", size=FONT_SIZE_TITLE,
                 weight="bold", pad=24, color=THEME_TEXT)
    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.15),
              title="Filière", frameon=False, labelcolor=THEME_TEXT,
              title_fontsize=FONT_SIZE_TICK)
    fig.tight_layout()
    return fig


def plot_student_vs_cohorte(df_etudiant: pd.DataFrame, df_cohorte: pd.DataFrame) -> plt.Figure:
    """
    Courbe d'évolution de la moyenne de l'étudiant par semestre,
    superposée à la moyenne de sa cohorte.
    df_etudiant : DataFrame filtré sur l'anonymat de l'étudiant.
    df_cohorte  : DataFrame filtré sur la même cohorte (tous étudiants).
    """
    if df_etudiant is None or df_etudiant.empty:
        return render_insufficient_data_image("Données étudiant insuffisantes")
    df_e = df_etudiant.dropna(subset=["note", "semestre"]).copy()
    df_e["semestre"] = _sem_int(df_e["semestre"])
    df_e = df_e.dropna(subset=["semestre"])   # supprimer les semestres non-convertis
    df_e["semestre"] = df_e["semestre"].astype(int)
    moy_etud = (df_e.groupby("semestre", observed=True)["note"]
                .mean().reset_index(name="moyenne")
                .sort_values("semestre"))
    series = []
    moy_coh = pd.DataFrame()
    if df_cohorte is not None and not df_cohorte.empty:
        df_c = df_cohorte.dropna(subset=["note", "semestre"]).copy()
        df_c["semestre"] = _sem_int(df_c["semestre"])
        df_c = df_c.dropna(subset=["semestre"])
        df_c["semestre"] = df_c["semestre"].astype(int)
        moy_coh = (df_c.groupby("semestre", observed=True)["note"]
                   .mean().reset_index(name="moyenne")
                   .sort_values("semestre"))
        series.append(("Moyenne cohorte", moy_coh, ACCENT_INDIGO, "--"))
    series.append(("Étudiant", moy_etud, COLOR_REUSSITE, "-"))
    sems_all = sorted(set(moy_etud["semestre"].tolist() +
                          (moy_coh["semestre"].tolist() if not moy_coh.empty else [])))
    if not sems_all:
        return render_insufficient_data_image("Aucun semestre disponible")
    fig, ax = plt.subplots(figsize=(9, 4), dpi=FIG_DPI)
    fig.patch.set_facecolor(THEME_BG)
    ax.set_facecolor(THEME_BG)
    for label, data, color, ls in series:
        ax.plot(data["semestre"], data["moyenne"], marker="o", linewidth=2.2,
                linestyle=ls, color=color, label=label)
        for _, row in data.iterrows():
            ax.annotate(f"{row['moyenne']:.2f}",
                        xy=(row["semestre"], row["moyenne"]),
                        xytext=(0, 7), textcoords="offset points",
                        ha="center", fontsize=FONT_SIZE_TICK - 1, color=color)
    ax.axhline(SEUIL_REUSSITE, color=COLOR_ECHEC, linestyle="-.", linewidth=1.2,
               alpha=0.7, label=f"Seuil {SEUIL_REUSSITE}")
    ax.set_xticks(sems_all)
    ax.set_xlim(min(sems_all) - 0.4, max(sems_all) + 0.4)
    ax.set_ylim(0, 22)
    ax.set_xlabel("Semestre", color=THEME_TEXT, fontsize=FONT_SIZE_LABEL)
    ax.set_ylabel("Moyenne", color=THEME_TEXT, fontsize=FONT_SIZE_LABEL)
    ax.set_title("Évolution de la moyenne — étudiant vs cohorte",
                 color=THEME_TEXT, fontsize=FONT_SIZE_TITLE)
    ax.tick_params(colors=THEME_TEXT)
    ax.grid(axis="y", linestyle="--", alpha=0.15)
    ax.legend(frameon=False, labelcolor=THEME_TEXT, fontsize=FONT_SIZE_TICK)
    fig.tight_layout()
    return fig

PLOT_DISPATCH: Dict[str, PlotFunction] = {
    "heatmap_ue_semestre":        plot_heatmap_ue_semestre,
    "courbe_cohortes":            plot_courbe_cohortes,
    "histogram":                  plot_hist_generic,
    "boxplot":                    plot_box_generic,
    "boxplot_by_sex":             plot_box_by_sex,
    "evolution_taux_by_semestre": plot_evolution_taux_by_semestre,
    "courbe_moyenne_par_sexe":    plot_courbe_moyenne_par_sexe,
    "validation_global":          plot_validation_global,
    "donut":                      donut,
    "heatmap_filiere_semestre":   plot_heatmap_filiere_semestre,
    "radar_filieres":             plot_radar_filieres,
}
