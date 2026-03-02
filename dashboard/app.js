// Frontend JS – Dashboard interactif

const API_URL = "http://localhost:8000";

//  État global 
let dashboardMeta   = {};
let currentAnalysis = "global";
let allUEs          = [];
let filteredUEs     = [];
let currentPage     = 1;
let currentSort     = { field: null, direction: "asc" };
let _abortCtrl      = null; 
let _searchTimer    = null;

const ITEMS_PER_PAGE = 20;


//  Initialisation 

document.addEventListener("DOMContentLoaded", async () => {
    loadThemePreference();
    await loadMetadata();
    await loadDashboard({});
    setupListeners();
});


async function loadMetadata() {
    try {
        const res = await fetch(`${API_URL}/meta/disponibilites`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        dashboardMeta = await res.json();
    } catch (err) {
        console.error("Erreur métadonnées:", err);
    }
}


function setupListeners() {
    document.getElementById("analysis-type").addEventListener("change", e => {
        currentAnalysis = e.target.value;
        renderDynamicFilters(currentAnalysis);
        updateAnalysisTypeDisplay(currentAnalysis, {});
    });

    document.getElementById("apply-filters").addEventListener("click", applyFilters);
    document.getElementById("theme-toggle-btn").addEventListener("click", toggleTheme);

    //  Sidebar mobile 
    const sidebar = document.getElementById("sidebar");
    const overlay = document.getElementById("sidebar-overlay");
    document.getElementById("menu-burger")?.addEventListener("click", () => {
        sidebar.classList.remove("-translate-x-full");
        overlay.classList.remove("hidden");
    });
    const closeSidebar = () => {
        sidebar.classList.add("-translate-x-full");
        overlay.classList.add("hidden");
    };
    document.getElementById("close-sidebar")?.addEventListener("click", closeSidebar);
    overlay?.addEventListener("click", closeSidebar);

    //  Recherche étudiant
    const searchInput = document.getElementById("search-etudiant");
    const doSearch = async () => {
        const id = searchInput.value.trim();
        if (!id) return;
        document.getElementById("etudiant-details").innerHTML = "";
        const filters = getActiveFilters(["annee", "semestre", "cohorte", "sexe"]);
        const qs      = new URLSearchParams(filters).toString();
        try {
            const res = await fetch(
                `${API_URL}/etudiants/${encodeURIComponent(id)}/parcours${qs ? `?${qs}` : ""}`
            );
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            showEtudiantDetails(await res.json());
        } catch {
            document.getElementById("etudiant-details").innerHTML =
                `<p class="text-red-600 text-sm mt-2">Étudiant introuvable</p>`;
        }
    };
    document.getElementById("btn-search-etudiant")?.addEventListener("click", doSearch);
    searchInput?.addEventListener("keydown", e => { if (e.key === "Enter") doSearch(); });

    document.addEventListener("keydown", e => {
        if (e.key !== "Escape") return;
        closeUEDetails();
        closeEtudiantModal();
    });
}


//  Utilitaires 

function getActiveFilters(keys = ["annee", "semestre", "cohorte", "sexe"]) {
    const filters = {};
    keys.forEach(k => {
        const el = document.getElementById(`filter-${k}`);
        if (el?.value) filters[k] = el.value;
    });
    return filters;
}

function figureUrl(view, qs) {
    return `${API_URL}/figures?view=${view}&${qs}`;
}

function setLoading(active) {
    const btn = document.getElementById("apply-filters");
    if (!btn) return;
    btn.disabled    = active;
    btn.textContent = active ? "Chargement…" : "Appliquer";
    btn.classList.toggle("opacity-60", active);
    btn.classList.toggle("cursor-not-allowed", active);
}

function setEl(id, val) {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
}

async function loadDashboard(filters) {
    if (_abortCtrl) _abortCtrl.abort();
    _abortCtrl = new AbortController();

    setLoading(true);
    try {
        const params = new URLSearchParams(filters).toString();
        const res    = await fetch(
            `${API_URL}/dashboard/aggregates${params ? `?${params}` : ""}`,
            { signal: _abortCtrl.signal }
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        updateKPIs(data);
        renderCharts(currentAnalysis, filters);

        allUEs = filteredUEs = data.tableau_ue || [];
        currentPage = 1;
        renderUETable();

        updateAnalysisTypeDisplay(currentAnalysis, filters);
        document.getElementById("search-etudiant").value = "";
    } catch (err) {
        if (err.name === "AbortError") return;
        console.error("Erreur chargement dashboard:", err);
        showToast("Impossible de joindre l'API. Vérifiez que le serveur est démarré.", "error");
    } finally {
        setLoading(false);
        _abortCtrl = null;
    }
}


async function applyFilters() {
    const filters = getActiveFilters(["annee", "semestre", "cohorte", "sexe"]);
    await loadDashboard(filters);
}


//  Titre analyse courante 

function updateAnalysisTypeDisplay(type, filters) {
    const el = document.getElementById("current-analysis-type");
    if (!el) return;
    const LABELS = {
        global:   "— Vue globale",
        annee:    `— Année : ${filters.annee    || "—"}`,
        semestre: `— Semestre : ${filters.semestre || "—"}`,
        cohorte:  `— Cohorte : ${filters.cohorte  || "—"}${filters.semestre ? ` — Semestre : ${filters.semestre}` : ""}`,
    };
    el.textContent = LABELS[type] ?? "";
}


//  KPIs 

function updateKPIs(data) {
    const fmt = (v, d = 2) => v != null ? Number(v).toFixed(d) : "—";

    setEl("kpi-moyenne",  fmt(data?.moyenne_global));
    setEl("kpi-reussite", data?.taux_reussite_global != null
        ? fmt(data.taux_reussite_global, 1) + "%" : "—");
    setEl("kpi-effectif", data?.effectif_exact ?? "—");
    
    const extraStats = document.getElementById("stats-extra-container");
    if (extraStats) {
        const hasExtra = data?.mediane != null || data?.ecart_type != null;
        extraStats.classList.toggle("hidden", !hasExtra);
        setEl("kpi-mediane",  fmt(data?.mediane));
        setEl("kpi-std",      fmt(data?.ecart_type));
        setEl("kpi-variance", fmt(data?.variance));
    }
}


//  Graphiques 

function renderCharts(type, filters) {
    const c = document.getElementById("additional-charts");
    if (!c) return;

    const imgCard = (src, title) =>
        `<div class="bg-white p-4 rounded-xl border shadow-sm">
            <h4 class="font-semibold mb-3 text-gray-700">${title}</h4>
            <img src="${src}" class="w-full rounded" alt="${title}" loading="lazy"
                 onerror="this.parentElement.innerHTML='<p class=\\'text-sm text-gray-400 py-8 text-center\\'>Données insuffisantes</p>'">
         </div>`;

    const qs = new URLSearchParams();
    ["annee", "semestre", "cohorte", "sexe"].forEach(k => {
        if (filters?.[k]) qs.set(k, filters[k]);
    });

    const fig = (view, title) => imgCard(figureUrl(view, qs), title);

    const CHARTS = {
        global: [
            ["heatmap_ue_semestre",        "Heatmap UE / Semestre"],
            ["courbe_cohortes",            "Évolution des moyennes par cohorte"],
            ["evolution_taux_by_semestre", "Taux de réussite par semestre"],
            ["courbe_moyenne_par_sexe",    "Moyennes par sexe et semestre"],
            ["validation_global",          "Taux de validation par cohorte"],
            ["donut",                      "Répartition réussite / échec"],
        ],
        annee: filters?.annee ? [
            ["evolution_moyenne_by_annee", `Évolution des moyennes — ${filters.annee}`],
            ["evolution_taux_by_semestre", `Taux de réussite — ${filters.annee}`],
            ["histogram",                  `Distribution des notes — ${filters.annee}`],
            ["donut",                      `Répartition réussite / échec — ${filters.annee}`],
        ] : [],
        semestre: filters?.semestre ? [
            ["histogram",      `Distribution des notes — S${filters.semestre}`],
            ["boxplot",        `Boxplot global — S${filters.semestre}`],
            ["boxplot_by_sex", `Distribution par sexe — S${filters.semestre}`],
            ["donut",          `Répartition réussite / échec — S${filters.semestre}`],
        ] : [],
        cohorte: filters?.cohorte ? [
            ["courbe_cohortes",            `Évolution de la cohorte ${filters.cohorte}`],
            ["histogram",                  `Distribution des notes — Cohorte ${filters.cohorte}`],
            ["evolution_taux_by_semestre", `Taux de réussite — Cohorte ${filters.cohorte}`],
            ["donut",                      `Répartition réussite / échec — Cohorte ${filters.cohorte}`],
        ] : [],
    };

    const charts = CHARTS[type] ?? [];

    if (!charts.length) {
        c.innerHTML = `<p class="text-gray-400 text-sm col-span-2 py-4">
            Sélectionne un filtre pour afficher les graphiques.</p>`;
        return;
    }

    c.innerHTML = charts.map(([view, title]) => fig(view, title)).join("");
}


function renderDynamicFilters(type) {
    const container = document.getElementById("dynamic-filters");
    if (!container) return;

    const FILTER_CONFIGS = {
        global:   null,
        annee:    [{ label: "Année académique", id: "filter-annee",    values: dashboardMeta.annees }],
        semestre: [{ label: "Semestre",         id: "filter-semestre", values: dashboardMeta.semestres }],
        cohorte:  [
            { label: "Cohorte",              id: "filter-cohorte",  values: dashboardMeta.cohortes },
            { label: "Année (optionnelle)",  id: "filter-annee",    values: dashboardMeta.annees,    optional: true },
            { label: "Semestre (optionnel)", id: "filter-semestre", values: dashboardMeta.semestres, optional: true },
        ],
    };

    const configs = FILTER_CONFIGS[type];
    if (!configs) {
        container.innerHTML = `<p class="text-gray-500 text-sm text-center py-4">Tous les résultats — aucun filtre.</p>`;
        return;
    }

    container.innerHTML = configs.map(c => selectHTML(c.label, c.id, c.values, c.optional)).join("");

    if (type === "cohorte") {
        setupDependency("filter-cohorte", "filter-annee",    "cohorte");
        setupDependency("filter-annee",   "filter-semestre", "annee");
    }
}


function setupDependency(parentId, childId, paramName) {
    const parentEl = document.getElementById(parentId);
    const childEl  = document.getElementById(childId);
    if (!parentEl || !childEl) return;

    parentEl.addEventListener("change", async () => {
        const val = parentEl.value;
        if (!val) {
            childEl.innerHTML = `<option value="">${childId.includes("semestre") ? "Tous" : "Choisir…"}</option>`;
            childEl.disabled  = false;
            return;
        }

        childEl.innerHTML = `<option value="">Chargement…</option>`;
        childEl.disabled  = true;

        try {
            const qs = new URLSearchParams({ [paramName]: val });
            if (paramName === "annee") {
                const coh = document.getElementById("filter-cohorte");
                if (coh?.value) qs.set("cohorte", coh.value);
            }
            const res     = await fetch(`${API_URL}/meta/disponibilites?${qs}`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data    = await res.json();
            const options = paramName === "cohorte" ? data.annees : data.semestres;
            const placeholder = childId.includes("semestre") ? "Tous" : "Choisir…";
            childEl.innerHTML = `<option value="">${placeholder}</option>` +
                (options || []).map(o => `<option value="${o}">${o}</option>`).join("");
            childEl.disabled  = !options?.length;
        } catch {
            childEl.innerHTML = `<option value="">Choisir…</option>`;
            childEl.disabled  = false;
        }
    });
}


function selectHTML(label, id, values = [], optional = false) {
    return `
        <div class="mb-4">
            <label class="block text-sm font-medium mb-1">${label}</label>
            <select id="${id}" class="w-full border rounded p-2">
                <option value="">${optional ? "Tous" : "Choisir…"}</option>
                ${(values || []).map(v => `<option value="${v}">${v}</option>`).join("")}
            </select>
        </div>`;
}


//  Tableau UE 

function getUEDifficultyColor(tauxReussite, moyenne) {
    if (tauxReussite < 50 && moyenne < 10)
        return { bg: "bg-red-100",   text: "text-red-800",   border: "border-red-300",   label: "Difficile" };
    if (tauxReussite < 75 || moyenne < 12)
        return { bg: "bg-blue-100",  text: "text-blue-800",  border: "border-blue-300",  label: "Moyen" };
    return     { bg: "bg-green-100", text: "text-green-800", border: "border-green-300", label: "Facile" };
}


function countUEsByDifficulty(ues) {
    return ues.reduce((acc, ue) => {
        const label = getUEDifficultyColor(ue.taux_reussite, ue.moyenne).label;
        acc[label] = (acc[label] || 0) + 1;
        return acc;
    }, { Difficile: 0, Moyen: 0, Facile: 0 });
}


function renderUETable() {
    const body = document.getElementById("table-ue-body");
    if (!body) return;

    const start = (currentPage - 1) * ITEMS_PER_PAGE;
    const page  = filteredUEs.slice(start, start + ITEMS_PER_PAGE);

    body.innerHTML = page.map(ue => {
        const d = getUEDifficultyColor(ue.taux_reussite, ue.moyenne);
        return `
            <tr class="${d.bg}">
                <td class="p-4 font-semibold ${d.text}">
                    <div class="flex items-center justify-between">
                        <span>${ue.ue}</span>
                        <span class="text-xs px-2 py-1 rounded-full bg-white ${d.text} font-medium">${d.label}</span>
                    </div>
                </td>
                <td class="p-4">
                    <span class="font-medium ${d.text}">${ue.moyenne != null ? ue.moyenne.toFixed(2) : "—"}</span>
                </td>
                <td class="p-4">
                    <span class="font-medium ${d.text}">${ue.taux_reussite != null ? ue.taux_reussite.toFixed(1) + "%" : "—"}</span>
                </td>
                <td class="p-4 text-gray-600 bg-white">${ue.effectif ?? "—"}</td>
                <td class="p-4">
                    <button onclick="showUEDetails('${ue.ue}')" type="button"
                        class="px-3 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 text-sm font-medium transition-colors">
                        Détails
                    </button>
                </td>
            </tr>`;
    }).join("");

    const totalPages = Math.ceil(filteredUEs.length / ITEMS_PER_PAGE) || 1;
    setEl("ue-count",     `${filteredUEs.length} UE(s)`);
    setEl("current-page", currentPage);
    setEl("total-pages",  totalPages);

    const counts   = countUEsByDifficulty(filteredUEs);
    const legendEl = document.getElementById("ue-legend");
    if (legendEl) {
        legendEl.innerHTML = `
            <div class="flex flex-wrap gap-3 text-sm">
                <span class="px-2 py-1 rounded bg-red-100 text-red-800 border border-red-300">Difficile (${counts.Difficile})</span>
                <span class="px-2 py-1 rounded bg-blue-100 text-blue-800 border border-blue-300">Moyen (${counts.Moyen})</span>
                <span class="px-2 py-1 rounded bg-green-100 text-green-800 border border-green-300">Facile (${counts.Facile})</span>
            </div>`;
    }

    const prevBtn = document.getElementById("prev-page");
    const nextBtn = document.getElementById("next-page");
    if (prevBtn) prevBtn.disabled = currentPage === 1;
    if (nextBtn) nextBtn.disabled = currentPage >= totalPages;
}


window.changePage = dir => {
    const totalPages = Math.ceil(filteredUEs.length / ITEMS_PER_PAGE) || 1;
    currentPage = Math.min(totalPages, Math.max(1, currentPage + dir));
    renderUETable();
};


window.sortUEs = field => {
    currentSort = currentSort.field === field
        ? { field, direction: currentSort.direction === "asc" ? "desc" : "asc" }
        : { field, direction: "asc" };

    filteredUEs.sort((a, b) => {
        let aVal = a[field], bVal = b[field];
        if (typeof aVal === "string") { aVal = aVal.toLowerCase(); bVal = bVal.toLowerCase(); }
        const cmp = aVal > bVal ? 1 : aVal < bVal ? -1 : 0;
        return currentSort.direction === "asc" ? cmp : -cmp;
    });

    updateSortIndicators();
    currentPage = 1;
    renderUETable();
};


function updateSortIndicators() {
    ["ue", "moyenne", "taux_reussite", "effectif"].forEach(f => {
        const el = document.getElementById(`sort-${f}`);
        if (el) el.textContent = f === currentSort.field
            ? (currentSort.direction === "asc" ? "↑" : "↓") : "";
    });
}


window.searchUEs = () => {
    clearTimeout(_searchTimer);
    _searchTimer = setTimeout(() => {
        const term = document.getElementById("ue-search").value.toLowerCase();
        filteredUEs = term
            ? allUEs.filter(ue => ue.ue.toLowerCase().includes(term))
            : [...allUEs];
        if (currentSort.field) {
            filteredUEs.sort((a, b) => {
                let aVal = a[currentSort.field], bVal = b[currentSort.field];
                if (typeof aVal === "string") { aVal = aVal.toLowerCase(); bVal = bVal.toLowerCase(); }
                const cmp = aVal > bVal ? 1 : aVal < bVal ? -1 : 0;
                return currentSort.direction === "asc" ? cmp : -cmp;
            });
        }
        currentPage = 1;
        renderUETable();
    }, 200);
};


//  Thème

function loadThemePreference() {
    const isDark = localStorage.getItem("theme") === "dark";
    document.body.classList.toggle("dark", isDark);
    document.getElementById("theme-icon-light").classList.toggle("hidden", isDark);
    document.getElementById("theme-icon-dark").classList.toggle("hidden", !isDark);
}

function toggleTheme() {
    const isDark = document.body.classList.toggle("dark");
    document.getElementById("theme-icon-light").classList.toggle("hidden");
    document.getElementById("theme-icon-dark").classList.toggle("hidden");
    localStorage.setItem("theme", isDark ? "dark" : "light");
}

function showToast(message, type = "info") {
    const colors = { error: "bg-red-600", info: "bg-blue-600", success: "bg-green-600" };
    const toast  = document.createElement("div");
    toast.className = `fixed bottom-6 right-6 z-[100] px-5 py-3 rounded-lg text-white text-sm font-medium shadow-lg transition-opacity duration-300 ${colors[type] || colors.info}`;
    toast.textContent = message;
    document.body.appendChild(toast);
    setTimeout(() => {
        toast.classList.add("opacity-0");
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}


//  Modal détails UE 

async function showUEDetails(ueName) {
    const modal   = document.getElementById("ue-details-modal");
    const content = document.getElementById("ue-details-content");
    if (!modal) return;

    modal.classList.remove("hidden");
    content.innerHTML = `
        <div class="flex justify-center items-center py-16">
            <div class="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            <span class="ml-3 text-gray-600">Chargement des détails…</span>
        </div>`;

    try {
        const filters = getActiveFilters(["annee", "semestre", "cohorte", "sexe"]);
        const qs      = new URLSearchParams(filters).toString();
        const res     = await fetch(
            `${API_URL}/ues/${encodeURIComponent(ueName)}/stats${qs ? `?${qs}` : ""}`
        );

        if (!res.ok) {
            content.innerHTML = res.status === 404
                ? `<div class="text-center py-12">
                       <p class="text-lg font-semibold text-gray-700 mb-2">Aucune donnée</p>
                       <p class="text-gray-500">Aucun résultat pour « ${ueName} » avec les filtres sélectionnés.</p>
                   </div>`
                : `<div class="text-center py-12"><p class="text-red-600">Erreur réseau ${res.status}</p></div>`;
            return;
        }

        renderUEDetails(await res.json(), ueName, filters);
    } catch {
        content.innerHTML = `<div class="text-center py-12">
            <p class="text-red-600">Impossible de charger « ${ueName} ».</p></div>`;
    }
}


function renderUEDetails(data, ueName, filters) {
    const content = document.getElementById("ue-details-content");
    if (!content) return;

    const qs        = new URLSearchParams({ ...filters, ue: ueName });
    const histUrl   = figureUrl("histogram",      qs);
    const boxSexUrl = figureUrl("boxplot_by_sex", qs);
    const donutUrl  = figureUrl("donut",           qs);
    const d         = getUEDifficultyColor(data.taux_reussite, data.moyenne);
    const fmt2      = v => v?.toFixed(2) ?? "—";
    const fmt1      = v => v?.toFixed(1) ?? "—";
    const errSlot   = `onerror="this.parentElement.innerHTML='<p class=\\'text-sm text-gray-400 py-8 text-center\\'>Données insuffisantes</p>'"`;

    const contextParts = Object.entries({
        Année: filters.annee, Semestre: filters.semestre, Cohorte: filters.cohorte,
    }).filter(([, v]) => v)
      .map(([k, v]) => `<span class="px-2 py-1 bg-white rounded text-sm">${k} : ${v}</span>`);

    content.innerHTML = `
        <div class="space-y-6">

            <div class="bg-gradient-to-r from-blue-50 to-indigo-50 p-6 rounded-lg border">
                <div class="flex justify-between items-start">
                    <div>
                        <h3 class="text-2xl font-bold text-gray-800 mb-2">${ueName}</h3>
                        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                            <div><span class="text-gray-600">Moyenne :</span>
                                <span class="font-semibold text-lg ${data.moyenne >= 10 ? "text-green-600" : "text-red-600"}">${fmt2(data.moyenne)}</span>
                            </div>
                            <div><span class="text-gray-600">Taux réussite :</span>
                                <span class="font-semibold text-lg ${data.taux_reussite >= 50 ? "text-green-600" : "text-red-600"}">${fmt1(data.taux_reussite)}%</span>
                            </div>
                            <div><span class="text-gray-600">Effectif :</span>
                                <span class="font-semibold text-lg text-blue-600">${data.effectif ?? "—"}</span>
                            </div>
                            <div><span class="text-gray-600">Crédits :</span>
                                <span class="font-semibold text-lg text-purple-600">${data.credit ?? "—"}</span>
                            </div>
                        </div>
                    </div>
                    <span class="inline-block px-3 py-1 rounded-full text-xs font-medium ${d.bg} ${d.text}">${d.label}</span>
                </div>
            </div>

            <div class="bg-gray-50 p-4 rounded-lg">
                <h4 class="font-semibold text-gray-700 mb-2">Contexte d'analyse</h4>
                <div class="flex flex-wrap gap-2 text-sm">
                    ${contextParts.length
                        ? contextParts.join(" ")
                        : '<span class="px-2 py-1 bg-white rounded text-sm">Vue globale</span>'}
                </div>
            </div>

            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div class="bg-white p-4 rounded-lg border">
                    <h4 class="font-semibold text-gray-700 mb-3">Distribution des notes</h4>
                    <img src="${histUrl}"   class="w-full rounded" alt="Histogramme"      ${errSlot}>
                </div>
                <div class="bg-white p-4 rounded-lg border">
                    <h4 class="font-semibold text-gray-700 mb-3">Boxplot par sexe</h4>
                    <img src="${boxSexUrl}" class="w-full rounded" alt="Boxplot par sexe" ${errSlot}>
                </div>
                <div class="bg-white p-4 rounded-lg border">
                    <h4 class="font-semibold text-gray-700 mb-3">Réussite / Échec</h4>
                    <img src="${donutUrl}"  class="w-full rounded" alt="Donut"            ${errSlot}>
                </div>
            </div>

            <div class="bg-white p-6 rounded-lg border">
                <h4 class="font-semibold text-gray-700 mb-4">Statistiques détaillées</h4>
                <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div>
                        <h5 class="font-medium text-gray-600 mb-2">Performance</h5>
                        <div class="space-y-1 text-sm">
                            ${[["Note minimale", fmt2(data.min_note)],
                               ["Note maximale", fmt2(data.max_note)],
                               ["Écart-type",    fmt2(data.std_note)],
                               ["Médiane",       fmt2(data.mediane_note)]]
                              .map(([l, v]) => `
                                <div class="flex justify-between py-1 border-b border-gray-100">
                                    <span>${l}</span><span class="font-medium">${v}</span>
                                </div>`).join("")}
                        </div>
                    </div>
                    <div>
                        <h5 class="font-medium text-gray-600 mb-2">Admis / Ajournés</h5>
                        <div class="space-y-1 text-sm">
                            ${[["Nombre admis",    data.nombre_admis    ?? "—"],
                               ["% admis",         `${fmt2(data.pourcentage_admis)}%`],
                               ["Nombre ajournés", data.nombre_ajournes ?? "—"],
                               ["% ajournés",      `${fmt2(data.pourcentage_ajournes)}%`]]
                              .map(([l, v]) => `
                                <div class="flex justify-between py-1 border-b border-gray-100">
                                    <span>${l}</span><span class="font-medium">${v}</span>
                                </div>`).join("")}
                        </div>
                    </div>
                </div>
            </div>
        </div>`;
}


window.closeUEDetails = function () {
    document.getElementById("ue-details-modal")?.classList.add("hidden");
};


//  Modal parcours étudiant 

function showEtudiantDetails(data) {
    const modal   = document.getElementById("etudiant-modal");
    const content = document.getElementById("etudiant-modal-content");
    if (!modal || !content) return;

    const fmt2  = v => v?.toFixed(2) ?? "—";
    const color = v => v >= 10 ? "text-green-700" : "text-red-600";

    content.innerHTML = `
        <div class="bg-white shadow rounded-lg p-5 space-y-3 mb-4">
            <h3 class="text-lg font-bold">${data.nom_prenoms || "—"}</h3>
            <div class="text-sm text-gray-600 space-y-1">
                <div class="flex gap-4 flex-wrap">
                    <span>Anonymat : <strong>${data.anonymat ?? "—"}</strong></span>
                    <span>Carte : <strong>${data.carte ?? "—"}</strong></span>
                </div>
                <div class="flex gap-4 flex-wrap">
                    <span>Sexe : <strong>${data.sexe ?? "—"}</strong></span>
                    <span>Cohorte : <strong>${data.cohorte ?? "—"}</strong></span>
                </div>
                <div>Crédits validés : <strong>${data.credits_valides} / ${data.credits_total}</strong></div>
            </div>
            <div class="flex flex-wrap gap-6 pt-2 text-sm font-semibold">
                <span>Moyenne globale :
                    <span class="${color(data.moyenne_globale)}">${fmt2(data.moyenne_globale)}</span>
                </span>
                <span>Taux validation :
                    <span class="text-blue-700">${data.taux_reussite_global ?? "—"}%</span>
                </span>
            </div>
        </div>

        <div class="space-y-2">
            ${(data.parcours || []).map(p => `
                <details class="border rounded-lg bg-gray-50">
                    <summary class="cursor-pointer p-3 font-semibold text-gray-800 flex justify-between items-center">
                        <span>Semestre ${p.semestre}</span>
                        <span class="text-sm font-normal ${color(p.moyenne)}">
                            Moy. ${fmt2(p.moyenne)} — ${p.credits_valides}/${p.credits} crédits
                        </span>
                    </summary>
                    <div class="px-4 pb-3 pt-1">
                        <p class="text-sm text-gray-500 mb-2">${p.nombre_ues} UE(s)</p>
                        <ul class="text-xs text-gray-600 space-y-1">
                            ${(p.details_ues || []).map(ue => `
                                <li class="flex justify-between border-b border-gray-100 py-1">
                                    <span>${ue.ue}</span>
                                    <span>
                                        <strong class="${color(ue.note)}">${ue.note}</strong>
                                        <span class="text-gray-400 ml-1">(${ue.credit} cr.)</span>
                                    </span>
                                </li>`).join("")}
                        </ul>
                    </div>
                </details>`).join("")}
        </div>`;

    modal.classList.remove("hidden");
}


window.closeEtudiantModal = function () {
    document.getElementById("etudiant-modal")?.classList.add("hidden");
};
