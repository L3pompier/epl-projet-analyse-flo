"""
pdf_export.py — GoodAdmin
Génération de rapports PDF avec fpdf2, figures matplotlib intégrées.
"""
import logging
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fpdf import FPDF

from app.core.config import REPORTS_DIR, SEUIL_REUSSITE, FIG_DPI
from app.services.interpreter import interpreter_dashboard, interpreter_ue

_FONTS_DIR  = "/usr/share/fonts/truetype/dejavu"
_FONT_MAP   = {
    "":   "DejaVuSans.ttf",
    "B":  "DejaVuSans-Bold.ttf",
    "I":  "DejaVuSans-Oblique.ttf",
    "BI": "DejaVuSans-BoldOblique.ttf",
}
_FONT_FAMILY = "DejaVu"

logger = logging.getLogger(__name__)
PDF_DIR = REPORTS_DIR / "pdf"

for _noisy in ("fpdf", "fonttools", "fonttools.subset",
               "fonttools.ttLib", "fonttools.ttLib.ttFont",
               "fonttools.subset.timer"):
    logging.getLogger(_noisy).setLevel(logging.ERROR)

FIGURES_PDF = [
    ("histogram",                "Distribution des notes",              170),
    ("donut",                    "Répartition Réussite / Échec",        170),
    ("validation_global",        "Taux de réussite par semestre",       170),
    ("courbe_cohortes",          "Évolution des moyennes par cohorte",  170),
    ("heatmap_filiere_semestre", "Moyennes filière × semestre",         170),
    ("radar_filieres",           "Profil comparatif des filières",      130),
    ("boxplot_by_sex",           "Distribution par sexe",               130),
    ("courbe_moyenne_par_sexe",  "Évolution de la moyenne par sexe",   170),
]


def _fig_to_tmp(vue: str, df: pd.DataFrame, df_extra: pd.DataFrame = None) -> Optional[Path]:
    try:
        import matplotlib.pyplot as plt
        from app.services.plotting import PLOT_DISPATCH, plot_student_vs_cohorte
        
        if vue == "student_cohorte" and df_extra is not None:
             fig = plot_student_vs_cohorte(df, df_extra)
        else:
            fn = PLOT_DISPATCH.get(vue)
            if fn is None: return None
            fig = fn(df)
            
        if fig is None: return None
        tmp = Path(tempfile.mkdtemp()) / f"{vue}.png"
        fig.savefig(str(tmp), format="png", bbox_inches="tight", dpi=FIG_DPI)
        plt.close(fig)
        return tmp if tmp.exists() else None
    except Exception as e:
        logger.warning("Figure '%s' : %s", vue, e)
        return None


class RapportPDF(FPDF):
    def __init__(self, titre="Rapport d'Analyse Académique", filtres=""):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.titre   = titre
        self.filtres = filtres
        self.set_auto_page_break(auto=True, margin=20)
        from pathlib import Path as _P
        fonts_ok = (_P(_FONTS_DIR) / "DejaVuSans.ttf").exists()
        if fonts_ok:
            for style, fname in _FONT_MAP.items():
                self.add_font(_FONT_FAMILY, style=style, fname=f"{_FONTS_DIR}/{fname}")
        self._ff = _FONT_FAMILY if fonts_ok else "Helvetica"
        self.set_font(self._ff, size=10)

    def header(self):
        self.set_font(self._ff, "B", 10)
        self.set_text_color(100, 102, 241)
        self.cell(0, 6, self.titre, align="L")
        self.set_text_color(150, 150, 150)
        self.set_font(self._ff, "", 7)
        self.cell(0, 6, f"Page {self.page_no()}/{{nb}}", align="R")
        self.ln(8)
        self.set_draw_color(200, 200, 200)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font(self._ff, "I", 7)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Généré le {time.strftime('%d/%m/%Y à %H:%M')}", align="C")

    def section_title(self, title: str):
        self.ln(4)
        self.set_font(self._ff, "B", 12)
        self.set_text_color(30, 30, 30)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(100, 102, 241)
        self.set_line_width(0.6)
        self.line(10, self.get_y(), 60, self.get_y())
        self.set_line_width(0.2)
        self.ln(4)

    def kpi_box(self, label: str, value: str, x: float, y: float, w=42, h=20):
        self.set_fill_color(245, 245, 250)
        self.rect(x, y, w, h, "F")
        self.set_draw_color(200, 200, 220)
        self.rect(x, y, w, h, "D")
        self.set_xy(x, y + 2)
        self.set_font(self._ff, "", 7)
        self.set_text_color(120, 120, 140)
        self.cell(w, 5, label, align="C")
        self.set_xy(x, y + 8)
        self.set_font(self._ff, "B", 14)
        self.set_text_color(30, 30, 50)
        self.cell(w, 8, value, align="C")

    def add_figure(self, img_path: Path, titre: str, largeur=170):
        if not img_path or not img_path.exists():
            return
        if self.get_y() > 210:
            self.add_page()
        self.set_font(self._ff, "I", 8)
        self.set_text_color(100, 100, 120)
        self.cell(0, 5, titre, new_x="LMARGIN", new_y="NEXT", align="C")
        self.image(str(img_path), x=(210 - largeur) / 2, w=largeur)
        self.ln(4)

    def add_table(self, headers: List[str], rows: List[List[str]], col_widths=None):
        if not rows:
            self.set_font(self._ff, "I", 9)
            self.set_text_color(150, 150, 150)
            self.cell(0, 8, "Aucune donnée", new_x="LMARGIN", new_y="NEXT")
            return
        if col_widths is None:
            col_widths = [190 / len(headers)] * len(headers)
        # Entête
        self.set_font(self._ff, "B", 8)
        self.set_fill_color(100, 102, 241)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
        self.ln()
        # Lignes
        self.set_font(self._ff, "", 8)
        self.set_text_color(50, 50, 50)
        fill = False
        for row in rows:
            if self.get_y() > 265:
                self.add_page()
                self.set_font(self._ff, "B", 8)
                self.set_fill_color(100, 102, 241)
                self.set_text_color(255, 255, 255)
                for i, h in enumerate(headers):
                    self.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
                self.ln()
                self.set_font(self._ff, "", 8)
                self.set_text_color(50, 50, 50)
                fill = False
            self.set_fill_color(248, 248, 252) if fill else self.set_fill_color(255, 255, 255)
            for i, val in enumerate(row):
                self.cell(col_widths[i], 6, str(val)[:30], border=1, fill=True, align="C")
            self.ln()
            fill = not fill


def add_interpretation_block(pdf: RapportPDF, interp: dict) -> None:
    if not interp or not interp.get("corps"):
        return
    ff = pdf._ff 
    COLOR_MAP = {"success":(16,185,129),"info":(99,102,241),"warning":(245,158,11),"danger":(239,68,68)}
    r, g, b = COLOR_MAP.get(interp.get("niveau","info"), (99,102,241))
    pdf.ln(3)
    x0, y0 = 10, pdf.get_y()
    pdf.set_font(ff, "B", 10)
    pdf.set_text_color(r, g, b)
    pdf.cell(0, 6, interp.get("titre","Analyse"), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font(ff, "", 9)
    pdf.set_text_color(60, 60, 70)
    pdf.multi_cell(0, 5, interp.get("corps",""))
    for pt in interp.get("points",[]):
        pdf.set_font(ff, "I", 8)
        pdf.set_text_color(80, 80, 100)
        pdf.cell(4, 5, "")
        pdf.cell(0, 5, f"• {pt[:90]}", new_x="LMARGIN", new_y="NEXT")
    if interp.get("conseil"):
        pdf.ln(2)
        pdf.set_font(ff, "BI", 8)
        pdf.set_text_color(r, g, b)
        pdf.cell(4, 5, "")
        pdf.multi_cell(0, 5, f"→ {interp['conseil']}")
    pdf.set_fill_color(r, g, b)
    pdf.rect(x0, y0, 1.5, pdf.get_y() - y0, "F")
    pdf.ln(4)


def _format_filtres(filtres: Dict[str, Any]) -> str:
    L = {"annee":"Année","semestre":"Semestre","cohorte":"Cohorte","sexe":"Sexe",
         "ue":"UE","filiere":"Filière","departement":"Département","type_formation":"Type","niveau":"Niveau"}
    p = [f"{L.get(k,k)}: {v}" for k,v in filtres.items() if v and str(v).strip()]
    return " | ".join(p) if p else "Aucun filtre"


def _ordinal(n: int) -> str:
    return "er" if n == 1 else "e"



def generate_report_pdf(df: pd.DataFrame, filtres: Dict[str, Any], context: str = 'dashboard', label: str = 'Rapport Académique', df_extra: pd.DataFrame = None) -> Path:
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    filtres_text = _format_filtres(filtres)

    ctx_title = {
        "departement": "Rapport Département",
        "filiere":     "Rapport Filière",
        "ue":          "Rapport UE",
        "etudiant":    "Rapport Étudiant",
        "dashboard":   "Rapport Académique Global",
    }.get(context, "Rapport Académique")

    pdf = RapportPDF(titre=f"{ctx_title} — GoodAdmin", filtres=filtres_text)
    pdf.alias_nb_pages()
    pdf.add_page()

    pdf.ln(20)
    pdf.set_font(_FONT_FAMILY, "B", 22); pdf.set_text_color(30,30,50)
    pdf.cell(0,12,ctx_title,align="C",new_x="LMARGIN",new_y="NEXT")
    
    if context == "etudiant" and not df.empty:
        fil = df["filiere"].iloc[0] if "filiere" in df.columns else "--"
        coh = df["cohorte"].iloc[0] if "cohorte" in df.columns else "--"
        label = f"{label} | {fil} (Cohorte {coh})"

    pdf.set_font(_FONT_FAMILY,"B",16); pdf.set_text_color(100,102,241)
    pdf.cell(0,10,label,align="C",new_x="LMARGIN",new_y="NEXT")
    pdf.ln(6); pdf.set_font(_FONT_FAMILY,"",10); pdf.set_text_color(100,100,110)
    pdf.cell(0,6,f"Généré le {time.strftime('%d/%m/%Y à %H:%M')}",align="C",new_x="LMARGIN",new_y="NEXT")
    pdf.ln(3); pdf.set_font(_FONT_FAMILY,"I",9)
    pdf.cell(0,6,f"Filtres : {filtres_text}",align="C",new_x="LMARGIN",new_y="NEXT")

    pdf.add_page(); pdf.section_title("Indicateurs Clés de Performance")
    notes    = pd.to_numeric(df.get("note"), errors="coerce").dropna()
    moyenne  = float(notes.mean()) if not notes.empty else 0.0
    taux     = float((notes >= SEUIL_REUSSITE).mean()*100) if not notes.empty else 0.0
    effectif = int(df["anonymat"].nunique()) if "anonymat" in df.columns else 0
    mediane  = float(notes.median()) if not notes.empty else 0.0
    ecart    = float(notes.std())    if len(notes)>1 else 0.0
    variance = float(notes.var())    if len(notes)>1 else 0.0
    nb_ues   = int(df["ue"].nunique()) if "ue" in df.columns else 0
    nb_risque = 0
    if "anonymat" in df.columns and not notes.empty:
        nb_risque = int((df.groupby("anonymat")["note"].mean() < SEUIL_REUSSITE).sum())

    y0 = pdf.get_y()+2
    pdf.kpi_box("Moyenne",       f"{moyenne:.2f}/20", 10,  y0)
    pdf.kpi_box("Taux Réussite", f"{taux:.1f}%",      56,  y0)
    
    if context == "etudiant":
        cr_v = int(df[df["note"] >= SEUIL_REUSSITE]["credit"].sum())
        cr_t = int(df["credit"].sum())
        pdf.kpi_box("Crédits", f"{cr_v}/{cr_t}", 102, y0)
    else:
        pdf.kpi_box("Effectif", str(effectif), 102, y0)
        
    pdf.kpi_box("Médiane",       f"{mediane:.2f}",     148, y0)
    pdf.set_y(y0+26)
    y1 = pdf.get_y()
    pdf.kpi_box("Écart-type",    f"{ecart:.2f}",       10,  y1)
    pdf.kpi_box("Variance",      f"{variance:.2f}",    56,  y1)
    if context == "etudiant":
        if df_extra is not None and "anonymat" in df.columns:
            anonymat = df["anonymat"].iloc[0]
            moy_coh = df_extra.groupby("anonymat")["note"].mean().sort_values(ascending=False).reset_index()
            pos = moy_coh[moy_coh["anonymat"] == anonymat].index
            rang = int(pos[0]) + 1 if len(pos) > 0 else None
            nb_coh = len(moy_coh)
            val_rang = f"{rang}{_ordinal(rang)} / {nb_coh}" if rang else "--"
            pdf.kpi_box("Rang Cohorte", val_rang, 102, y1)
        else:
            pdf.kpi_box("Nb UEs", str(nb_ues), 102, y1)
    else:
        pdf.kpi_box("Nb UEs", str(nb_ues), 102, y1)
        
    pdf.kpi_box("À Risque",      str(nb_risque),       148, y1)
    pdf.set_y(y1+26)

    try:
        interp = interpreter_dashboard({
            "moyenne_global": moyenne, "taux_reussite_global": taux,
            "effectif_exact": effectif, "mediane": mediane,
            "ecart_type": ecart, "variance": variance,
            "ue_difficiles": [], "risques": [],
        })
        pdf.section_title("Analyse et Interprétation")
        add_interpretation_block(pdf, interp)
    except Exception as e:
        logger.warning("Interprétation : %s", e)

    FIGURES_BY_CONTEXT: Dict[str, List[str]] = {
        "dashboard":   [v for v, _, _ in FIGURES_PDF],
        "departement": ["radar_filieres","heatmap_filiere_semestre","courbe_cohortes","boxplot_by_sex","courbe_moyenne_par_sexe"],
        "filiere":     ["histogram","heatmap_ue_semestre","boxplot_by_sex","courbe_moyenne_par_sexe","courbe_cohortes"],
        "ue":          ["histogram","boxplot","validation_global","donut"],
        "etudiant":    ["student_cohorte"],
    }
    vues_actives = FIGURES_BY_CONTEXT.get(context, [v for v, _, _ in FIGURES_PDF])

    TITRES_SPECIAUX = {"student_cohorte": "Comparaison de l'étudiant vs cohorte"}
    
    figs_to_gen  = [(v, TITRES_SPECIAUX.get(v, t), l) for v,t,l in FIGURES_PDF if v in vues_actives]
    if context == "etudiant" and "student_cohorte" in vues_actives:
        figs_to_gen = [("student_cohorte", TITRES_SPECIAUX["student_cohorte"], 170)]

    if figs_to_gen:
        pdf.add_page(); pdf.section_title("Visualisations")
        tmp_paths: Dict[str, Optional[Path]] = {}
        for vue, _, _ in figs_to_gen:
            tmp_paths[vue] = _fig_to_tmp(vue, df, df_extra=df_extra)
        for vue, titre_fig, largeur in figs_to_gen:
            tmp = tmp_paths.get(vue)
            if tmp:
                pdf.add_figure(tmp, titre_fig, largeur)
    else:
        tmp_paths = {}

    if context == "etudiant":
        pdf.add_page(); pdf.section_title("UEs suivies")
        if not df.empty and "ue" in df.columns:
            ues_etud = df.groupby(["ue","semestre"]).agg(
                moyenne=("note","mean"), taux=("note", lambda x: (x>=SEUIL_REUSSITE).mean()*100),
                credit=("credit","first")
            ).reset_index().sort_values(["semestre","ue"])
            rows_e = [[str(r["ue"]), str(int(r["semestre"])), f"{r['moyenne']:.2f}",
                       f"{r['taux']:.1f}%", str(int(r["credit"]))]
                      for _, r in ues_etud.iterrows()]
            pdf.add_table(["UE","Sem.","Moyenne","Taux","Crédit"], rows_e, [45,18,35,35,25])
        filepath = PDF_DIR / f"rapport_etudiant_{int(time.time())}.pdf"
        pdf.output(str(filepath)); logger.info("Rapport PDF : %s", filepath)
        for tmp in tmp_paths.values():
            try:
                if tmp and tmp.exists(): tmp.unlink(); tmp.parent.rmdir()
            except Exception: pass
        return filepath

    pdf.add_page()
    from app.services.analytics import tableau_ue, ue_difficiles, etudiants_a_risque, calculer_performance_par_dimension

    agg = pd.DataFrame()
    try:
        agg = tableau_ue(df)
    except Exception as e:
        logger.warning("tableau_ue : %s", e)

    if context == "departement":
        pdf.section_title("Détail par Filière")
        try:
            fperfs = calculer_performance_par_dimension(df, "filiere", include_effectif=True)
            if fperfs:
                 pdf.add_table(["Filière", "Moyenne", "Effectif"],
                               [[r["filiere"], f"{r['score']:.2f}", str(r["effectif"])] for r in fperfs],
                               [100, 45, 45])
        except Exception as e:
            logger.warning("Filiere perf PDF : %s", e)

    H  = ["UE","Sem.","Moyenne","Taux","Effectif","Crédit"]
    CW = [40, 18, 28, 28, 28, 20]

    def _ue_row(r):
        return [str(r["ue"]), str(int(r["semestre"])), f"{r['moyenne']:.2f}",
                f"{r['taux_reussite']:.1f}%", str(int(r["effectif"])), str(int(r["credit"]))]

    pdf.section_title("Top 10 UE — Meilleur taux de réussite")
    if not agg.empty:
        pdf.add_table(H, [_ue_row(r) for _, r in agg.sort_values("taux_reussite", ascending=False).head(10).iterrows()], CW)

    pdf.section_title("Bottom 10 UE — Plus faible taux de réussite")
    if not agg.empty:
        pdf.add_table(H, [_ue_row(r) for _, r in agg.sort_values("taux_reussite").head(10).iterrows()], CW)

    pdf.add_page(); pdf.section_title("UE Difficiles — Taux < 50 % et moyenne < 10")
    try:
        diff = ue_difficiles(df, agg_df=agg)
        if not diff.empty:
            pdf.add_table(H, [_ue_row(r) for _, r in diff.iterrows()], CW)
            pdf.section_title("Analyse des UE en Difficulté")
            for _, r in diff.head(5).iterrows():
                interp_ue = interpreter_ue({
                    "ue": str(r["ue"]), "moyenne": float(r["moyenne"]),
                    "taux_reussite": float(r["taux_reussite"]),
                    "effectif": int(r["effectif"]), "isDifficile": True,
                })
                pdf.set_font(_FONT_FAMILY,"B",9); pdf.set_text_color(80,80,100)
                pdf.cell(0,6,f"UE : {r['ue']}  —  S{r['semestre']}  ·  {r['effectif']} étudiants",
                         new_x="LMARGIN", new_y="NEXT")
                add_interpretation_block(pdf, interp_ue)
        else:
            pdf.set_font(_FONT_FAMILY,"I",9); pdf.set_text_color(150,150,150)
            pdf.cell(0,8,"Aucune UE difficile détectée",new_x="LMARGIN",new_y="NEXT")
    except Exception as e:
        logger.warning("UE difficiles : %s", e)

    pdf.section_title("Étudiants à Risque — Moyenne < 10")
    try:
        risques = etudiants_a_risque(df, n=15)
        if risques:
            pdf.add_table(["Anonymat","Nom","Département","Moyenne"],
                          [[str(r.get("anonymat","")), str(r.get("nom_prenoms",""))[:28],
                            str(r.get("departement","")), f"{r.get('moyenne',0):.2f}"] for r in risques],
                          [40,65,40,30])
    except Exception as e:
        logger.warning("Risques : %s", e)

    if "departement" in df.columns:
        pdf.section_title("Performance par Département")
        try:
            depts = calculer_performance_par_dimension(df, "departement")
            if depts:
                pdf.add_table(["Département","Moyenne"],
                              [[d["departement"], f"{d['score']:.2f}"] for d in depts], [80,60])
        except Exception as e:
            logger.warning("Depts : %s", e)

    filepath = PDF_DIR / f"rapport_academique_{int(time.time())}.pdf"
    pdf.output(str(filepath))
    logger.info("Rapport PDF : %s", filepath)

    for tmp in tmp_paths.values():
        try:
            if tmp and tmp.exists():
                tmp.unlink(); tmp.parent.rmdir()
        except Exception:
            pass

    return filepath
