/* ============================================================
 views.js — Contrôleurs des vues secondaires
 Chaque vue gère son propre état, ses KPIs et ses figures.
 ============================================================ */

/*  Utilitaires de rendu  */
function _setText(id, val) {
    const el = document.getElementById(id);
    if (el) el.textContent = val ?? '--';
}

function _esc(s) {
    if (s == null) return '--';
    return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function _updateKPI(id, val, unit = '') {
    const el = document.getElementById(id);
    if (!el) return;
    if (val === undefined || val === null) {
        el.innerHTML = `--<span class="kpi-unit">${unit}</span>`;
        return;
    }
    const displayVal = typeof val === 'number' ? (val % 1 === 0 ? val : val.toFixed(1)) : val;
    el.innerHTML = `${displayVal}<span class="kpi-unit">${unit}</span>`;
}

/*  Utilitaire : charger une figure backend  */
function loadFig(imgId, view, filters, onDone) {
    const img = document.getElementById(imgId);
    if (!img) { onDone?.(); return; }
    const loader = document.getElementById('fl-' + imgId.replace('fig-', ''));
    
    const token = (img._loadToken = (img._loadToken || 0) + 1);
    
    img.classList.remove('loaded');
    if (loader) {
        loader.classList.remove('hidden');
        loader.innerHTML = `<div class="spin"></div>
        <span class="text-[9px] font-mono text-gray-600 uppercase">Chargement…</span>`;
    }
    
    const url = api.getFigureUrl(view, filters || window.app?.currentFilters || {});
    
    
    img.onload = () => {
        if (img._loadToken !== token) return;
        img.classList.add('loaded');
        loader?.classList.add('hidden');
        onDone?.();
    };
    img.onerror = () => {
        if (img._loadToken !== token) return;
        img.classList.remove('loaded');
        if (loader) {
            loader.classList.remove('hidden');
            loader.innerHTML = `<i data-lucide="image-off" class="w-5 h-5 text-gray-700"></i>
            <span class="text-[9px] text-gray-600 font-mono uppercase">Données insuffisantes</span>`;
            lucide.createIcons();
        }
        onDone?.();
    };
    img.src = '';
    img.src = url;
}
window.loadFig = loadFig;

const FigQueue = (() => {
    const MAX_CONCURRENT = 4;
    let running = 0;
    let _gen = 0;
    const queue = [];
    
    function next() {
        if (running >= MAX_CONCURRENT || !queue.length) return;
        running++;
        const { imgId, view, filters, resolve, gen } = queue.shift();
        
        if (gen !== _gen) { running--; next(); resolve(); return; }
        
        const img = document.getElementById(imgId);
        if (!img) { running--; next(); resolve(); return; }
        
        let settled = false;
        const myGen = gen;
        const done = () => {
            if (settled) return;
            settled = true;
            if (myGen === _gen) { running--; next(); }
            resolve();
        };
        
        const tokenAvant = img._loadToken || 0;
        loadFig(imgId, view, filters, done);
        const tokenActuel = img._loadToken;
        
        const loaderId = 'fl-' + imgId.replace('fig-', '');
        setTimeout(() => {
            if (settled) return;
            if (img._loadToken !== tokenActuel) { done(); return; }
            img.classList.remove('loaded');
            const loader = document.getElementById(loaderId);
            if (loader) {
                loader.classList.remove('hidden');
                loader.innerHTML =
                '<i data-lucide="server-crash" class="w-5 h-5 text-gray-700"></i>' +
                '<span class="text-[9px] text-gray-600 font-mono uppercase">Backend indisponible</span>';
                lucide.createIcons({ el: loader });
            }
            done();
        }, 30000);
    }
    
    return {
        push(imgId, view, filters) {
            const gen = _gen;
            const img = document.getElementById(imgId);
            const loader = document.getElementById('fl-' + imgId.replace('fig-', ''));
            if (img) {
                img.src = '';
                img.classList.remove('loaded');
            }
            if (loader) {
                loader.classList.remove('hidden');
                loader.innerHTML = '<div class="spin"></div>' +
                '<span class="text-[9px] font-mono text-gray-600 uppercase">Chargement…</span>';
            }
            return new Promise(resolve => {
                queue.push({ imgId, view, filters, resolve, gen });
                next();
            });
        },
        clear() {
            queue.length = 0;
            _gen++;
            running = 0;
        },
    };
})();
window.FigQueue = FigQueue;

function queueFig(imgId, view, filters) {
    FigQueue.push(imgId, view, filters);
}
window.queueFig = queueFig;

/* INTERPRET BOX — Composant d'interprétation pédagogique
 Appelle GET /api/interpret et injecte le résultat dans un encart HTML.*/
const InterpretBox = {
    
    _COLORS: {
        success: { border: 'rgba(16,185,129,.25)', bg: 'rgba(16,185,129,.07)', icon: '#10b981', dot: 'bg-green-500' },
        info: { border: 'rgba(99,102,241,.25)', bg: 'rgba(99,102,241,.07)', icon: '#6366f1', dot: 'bg-accent' },
        warning: { border: 'rgba(245,158,11,.25)', bg: 'rgba(245,158,11,.07)', icon: '#f59e0b', dot: 'bg-amber-400' },
        danger: { border: 'rgba(239,68,68,.25)', bg: 'rgba(239,68,68,.07)', icon: '#ef4444', dot: 'bg-red-500' },
    },
    
    _ICONS: {
        success: 'check-circle',
        info: 'info',
        warning: 'alert-triangle',
        danger: 'alert-octagon',
    },
    
    _controllers: {},
    
    /**
     * Charge et affiche l'interprétation dans l'élément #containerId.
     * @param {string} containerId  - ID du div cible dans le DOM
     * @param {string} context      - "ue"|"dashboard"|"filiere"|"departement"|"etudiant"
     * @param {object} params       - Query params à passer à /api/interpret
     */
    async load(containerId, context, params = {}) {
        const el = document.getElementById(containerId);
        if (!el) return;
        
        if (this._controllers[containerId]) {
            this._controllers[containerId].abort();
        }
        const ctrl = new AbortController();
        this._controllers[containerId] = ctrl;
        
        el.innerHTML = `
        <div class="interpret-box interpret-loading">
        <div class="flex items-center gap-2">
        <div class="w-3 h-3 rounded-full bg-white/10 animate-pulse"></div>
        <div class="h-3 rounded bg-white/10 animate-pulse w-40"></div>
        </div>
        <div class="h-2 rounded bg-white/6 animate-pulse w-full mt-2"></div>
        <div class="h-2 rounded bg-white/6 animate-pulse w-3/4 mt-1"></div>
        </div>`;
        
        try {
            const clean = { context, ...params };
            const q = new URLSearchParams(
                Object.fromEntries(Object.entries(clean).filter(([, v]) => v != null && v !== ''))
            ).toString();
            
            const res = await api._fetch(`${API_BASE_URL}/interpret?${q}`, { signal: ctrl.signal });
            if (!res.ok) { el.innerHTML = ''; return; }
            const data = await res.json();
            el.innerHTML = this._render(data);
            lucide.createIcons();
        } catch (e) {
            if (e.name === 'AbortError') return;
            console.warn('InterpretBox.load:', e);
            el.innerHTML = '';
        }
    },
    
    _render(d) {
        const lvl = d.niveau || 'info';
        const c = this._COLORS[lvl] || this._COLORS.info;
        const iconNm = this._ICONS[lvl] || 'info';
        const points = (d.points || []).map(p =>
        `<li class="flex items-start gap-1.5 text-[11px] text-gray-400">
        <span class="mt-1 w-1 h-1 rounded-full shrink-0" style="background:${c.icon}"></span>
        ${p}
        </li>`
        ).join('');
        const conseil = d.conseil
        ? `<div class="interpret-conseil">
        <i data-lucide="lightbulb" class="w-3.5 h-3.5 shrink-0" style="color:${c.icon}"></i>
        <span>${d.conseil}</span>
        </div>`
        : '';
        
        return `
        <div class="interpret-box" style="border-color:${c.border}; background:${c.bg};">
        <div class="interpret-header">
        <i data-lucide="${iconNm}" class="w-4 h-4 shrink-0" style="color:${c.icon}"></i>
        <span class="interpret-titre" style="color:${c.icon}">${d.titre || ''}</span>
        </div>
        <p class="interpret-corps">${d.corps || ''}</p>
        ${points ? `<ul class="interpret-points">${points}</ul>` : ''}
        ${conseil}
        </div>`;
    },
    
    forUE(containerId, stats) {
        if (!stats) return;
        this.load(containerId, 'ue', {
            ue: stats.ue,
            moyenne: stats.moyenne,
            taux_reussite: stats.taux_reussite,
            effectif: stats.effectif,
            min_note: stats.min_note,
            max_note: stats.max_note,
            mediane_note: stats.mediane_note,
            std_note: stats.std_note,
            nombre_admis: stats.nombre_admis,
            nombre_ajournes: stats.nombre_ajournes,
            pourcentage_admis: stats.pourcentage_admis,
            pourcentage_ajournes: stats.pourcentage_ajournes,
            isDifficile: stats.isDifficile,
            credit: stats.credit,
            semestre: stats.semestre,
        });
    },
    
    _globalFilters() {
        return api._cleanFilters(window.app?.currentFilters || {});
    },
    
    forDashboard(containerId, data, filters = {}) {
        const statsPayload = data ? {
            moyenne_global: data.moyenne_global,
            taux_reussite_global: data.taux_reussite_global,
            effectif_exact: data.effectif_exact,
            mediane: data.mediane,
            ecart_type: data.ecart_type,
            variance: data.variance,
            q1: data.q1,
            q3: data.q3,
            iqr: data.iqr,
        } : {};
        const _nb_diff = (data?.ue_difficiles || []).length;
        const _nb_risq = (data?.risques || []).length;
        this.load(containerId, 'dashboard', {
            ...this._globalFilters(),
                  ...api._cleanFilters(filters),
                  ...Object.fromEntries(Object.entries(statsPayload).filter(([, v]) => v != null)),
                  _nb_ue_difficiles: _nb_diff || undefined,
                  _nb_risques: _nb_risq || undefined,
        });
    },
    
    forFiliere(containerId, filiere, data, filters = {}) {
        const statsPayload = data ? {
            moyenne_global: data.moyenne_global,
            taux_reussite_global: data.taux_reussite_global,
            effectif_exact: data.effectif_exact,
            mediane: data.mediane,
            ecart_type: data.ecart_type,
            q1: data.q1,
            q3: data.q3,
            iqr: data.iqr,
        } : {};
        const _ues = (data?.tableau_ue || []).slice(0, 50);
        this.load(containerId, 'filiere', {
            ...this._globalFilters(),
                  ...api._cleanFilters(filters),
                  filiere,
                  ...Object.fromEntries(Object.entries(statsPayload).filter(([, v]) => v != null)),
        });
    },
    
    forDepartement(containerId, departement, data, filters = {}) {
        const statsPayload = data ? {
            moyenne_global: data.moyenne_global,
            taux_reussite_global: data.taux_reussite_global,
            effectif_exact: data.effectif_exact,
            mediane: data.mediane,
            ecart_type: data.ecart_type,
            q1: data.q1,
            q3: data.q3,
            iqr: data.iqr,
        } : {};
        this.load(containerId, 'departement', {
            ...this._globalFilters(),
                  ...api._cleanFilters(filters),
                  departement,
                  ...Object.fromEntries(Object.entries(statsPayload).filter(([, v]) => v != null)),
        });
    },
    
    forEtudiant(containerId, parcours) {
        if (!parcours) return;
        this.load(containerId, 'etudiant', {
            moyenne: parcours.moyenne_globale,
            taux_reussite: parcours.taux_reussite_global,
            credits_valides: parcours.credits_valides,
            credits_total: parcours.credits_total,
        });
    },
};
window.InterpretBox = InterpretBox;


function tauxBadge(taux) {
    const t = typeof taux === 'number' ? taux : 0;
    const cls = t >= 50 ? 'bdg-green' : t >= 35 ? 'bdg-amber' : 'bdg-red';
    return `<span class="bdg ${cls}">${t.toFixed(1)}%</span>`;
}

/*  Utilitaire : statut UE */
function ueStatut(moy, taux) {
    if (taux < 50 && moy < 10) return '<span class="bdg bdg-red">Difficile</span>';
    if (taux >= 50) return '<span class="bdg bdg-green">OK</span>';
    return '<span class="bdg bdg-amber">Attention</span>';
}

/* PAGINATION HELPER
 Gère une table paginée : tri, pages, rendu.*/
class PaginatedTable {
    constructor(tbodyId, paginationId, pageSize = 10) {
        this.tbodyId = tbodyId;
        this.paginationId = paginationId;
        this.pageSize = pageSize;
        this._rows = [];
        this._page = 1;
        this._sortCol = null;
        this._sortAsc = true;
        this._rowRenderer = null;
    }
    
    setRenderer(fn) { this._rowRenderer = fn; return this; }
    
    load(rows) {
        this._rows = rows || [];
        this._page = 1;
        this._render();
    }
    
    _sorted() {
        if (!this._sortCol) return this._rows;
        return [...this._rows].sort((a, b) => {
            const av = a[this._sortCol] ?? '';
            const bv = b[this._sortCol] ?? '';
            const cmp = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv));
            return this._sortAsc ? cmp : -cmp;
        });
    }
    
    _render() {
        const tbody = document.getElementById(this.tbodyId);
        const pgEl = document.getElementById(this.paginationId);
        if (!tbody) return;
        
        const sorted = this._sorted();
        const total = sorted.length;
        const pages = Math.max(1, Math.ceil(total / this.pageSize));
        this._page = Math.min(this._page, pages);
        const start = (this._page - 1) * this.pageSize;
        const slice = sorted.slice(start, start + this.pageSize);
        
        if (!total) {
            tbody.innerHTML = `<tr><td colspan="99" class="text-center py-8 text-gray-600 italic text-xs">Aucune donnée</td></tr>`;
            if (pgEl) pgEl.innerHTML = '';
            return;
        }
        
        tbody.innerHTML = slice.map(r => this._rowRenderer(r)).join('');
        tbody.querySelectorAll('[data-load-id]').forEach(btn => {
            btn.addEventListener('click', () => {
                const id = btn.dataset.loadId;
                if (id && window.StudentView) StudentView.loadById(id);
            });
        });
        lucide.createIcons({ el: tbody });
        
        if (pgEl) {
            const range = this._pageRange(this._page, pages);
            const btnCls = 'px-2.5 py-1 rounded text-[10px] font-mono border transition-colors';
            const active = `${btnCls} bg-accent/20 border-accent/40 text-accent`;
            const normal = `${btnCls} bg-white/5 border-white/8 text-gray-400 hover:text-white hover:bg-white/10`;
            const disabled = `${btnCls} bg-transparent border-transparent text-gray-700 cursor-default`;
            
            const pageBtn = (p) => p === '…'
            ? `<span class="${disabled}">…</span>`
            : `<button class="${p === this._page ? active : normal}"
            onclick="this.closest('[data-pg]').__pg.goTo(${p})">${p}</button>`;
            
            pgEl.setAttribute('data-pg', '');
            pgEl.innerHTML = `
            <div class="flex items-center gap-1.5 flex-wrap">
            <button class="${this._page > 1 ? normal : disabled}"
            onclick="this.closest('[data-pg]').__pg.goTo(${this._page - 1})">‹</button>
            ${range.map(pageBtn).join('')}
            <button class="${this._page < pages ? normal : disabled}"
            onclick="this.closest('[data-pg]').__pg.goTo(${this._page + 1})">›</button>
            <span class="text-[10px] text-gray-600 ml-2">${start + 1}–${Math.min(start + this.pageSize, total)} / ${total}</span>
            </div>`;
            pgEl.__pg = this;
        }
    }
    
    goTo(p) {
        const pages = Math.ceil(this._rows.length / this.pageSize);
        if (p < 1 || p > pages) return;
        this._page = p;
        this._render();
    }
    
    sort(col) {
        if (this._sortCol === col) this._sortAsc = !this._sortAsc;
        else { this._sortCol = col; this._sortAsc = true; }
        this._page = 1;
        this._render();
    }
    
    _pageRange(cur, total) {
        if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1);
        const r = [];
        if (cur <= 4) {
            for (let i = 1; i <= 5; i++) r.push(i);
            r.push('…'); r.push(total);
        } else if (cur >= total - 3) {
            r.push(1); r.push('…');
            for (let i = total - 4; i <= total; i++) r.push(i);
        } else {
            r.push(1); r.push('…');
            for (let i = cur - 1; i <= cur + 1; i++) r.push(i);
            r.push('…'); r.push(total);
        }
        return r;
    }
}

/* 
 V UE DASHBOARD — patch de renderDashboard
 */
const DashboardView = {
    /* Tables paginées Top / Bottom */
    _top10Table: null,
    _bottom10Table: null,
    /* Table paginée filière */
    _filiereTable: null,
    /* Table paginée alertes UE */
    _alertsUETable: null,
    
    init() {
        /* Top / Bottom */
        this._top10Table = new PaginatedTable('top10-tbody', 'top10-pagination', 10)
        .setRenderer(r => this._ueRow(r));
        this._bottom10Table = new PaginatedTable('bottom10-tbody', 'bottom10-pagination', 10)
        .setRenderer(r => this._ueRow(r));
    },
    
    _ueRow(r) {
        const taux = r.taux_reussite ?? 0;
        return `<tr class="hover:bg-white/3 transition-colors cursor-pointer"
        onclick="UEView.loadCode('${_esc(r.ue)}')">
        <td class="font-mono text-accent font-semibold text-xs px-3 py-2.5">${_esc(r.ue)}</td>
        <td class="text-gray-400 text-xs px-3 py-2.5">S${_esc(r.semestre)}</td>
        <td class="font-mono text-xs px-3 py-2.5">${(r.moyenne || 0).toFixed(2)}</td>
        <td class="text-xs px-3 py-2.5">${tauxBadge(taux)}</td>
        <td class="text-gray-500 text-xs px-3 py-2.5">${r.effectif || 0}</td>
        </tr>`;
    },
    
    render(data, filters) {
        /* 1. KPIs */
        _updateKPI('kpi-moyenne', data.moyenne_global, '/20');
        _updateKPI('kpi-taux', data.taux_reussite_global, '%');
        _updateKPI('kpi-mediane', data.mediane, '/20');
        _updateKPI('kpi-std', data.ecart_type, '');
        _updateKPI('kpi-variance', data.variance, '');
        _updateKPI('kpi-q1', data.q1, '/20');
        _updateKPI('kpi-q3', data.q3, '/20');
        _updateKPI('kpi-iqr', data.iqr, ' pts');
        _updateKPI('kpi-effectif', data.effectif_exact, '');
        _updateKPI('kpi-risques', (data.risques || []).length, '');
        
        _setText('kpi-moyenne-trend', data.moyenne_global >= 10 ? 'Moyenne satisfaisante' : 'Moyenne critique');
        const trendEl = document.getElementById('kpi-moyenne-trend');
        if (trendEl) trendEl.className = `kpi-trend ${data.moyenne_global >= 10 ? 'trend-up' : 'trend-down'}`;
        
        _setText('kpi-taux-trend', `${(data.taux_reussite_global || 0).toFixed(1)}% de réussite`);
        const txTE = document.getElementById('kpi-taux-trend');
        if (txTE) txTE.className = `kpi-trend ${data.taux_reussite_global >= 50 ? 'trend-up' : 'trend-down'}`;
        
        /* 2. Figures (Visibilité conditionnelle) */
        const hasCohorte = filters.cohorte && filters.cohorte.length > 0;
        const cohortContainer = document.getElementById('cohort-curve-container');
        if (cohortContainer) cohortContainer.classList.toggle('hidden', !hasCohorte);
        
        const hasAnyFilter = Object.values(filters).some(v => Array.isArray(v) && v.length > 0);
        const successContainer = document.getElementById('success-rate-container');
        if (successContainer) successContainer.classList.toggle('hidden', !hasAnyFilter || (filters.ue && filters.ue.length > 0));
        
        const active = document.querySelector('.nav-item.active')?.getAttribute('data-view');
        
        if (active === 'dashboard') {
            FigQueue.clear();
            
            /* Lancer les chargements */
            queueFig('fig-donut', 'donut', filters);
            queueFig('fig-hist', 'histogram', filters);
            queueFig('fig-box', 'boxplot', filters);
            queueFig('fig-bysex', 'boxplot_by_sex', filters);
            queueFig('fig-valid', 'validation_global', filters);
            if (hasCohorte) queueFig('cohort-curve-img', 'courbe_cohortes', filters);
        }
        
        /* 3. Tables paginées */
        this._top10Table.load(data.top10 || []);
        this._bottom10Table.load(data.bottom10 || []);
        
        if (active === 'dashboard') {
            InterpretBox.forDashboard('interpret-dashboard', data, filters);
        }
        
        /* Vues actives */
        if (active === 'dept') DeptView.render(data, filters);
        if (active === 'filiere') FiliereView.renderFigsOnly(filters);
        if (active === 'ue') UEView.onDashboardData(data, filters);
        if (active === 'dashboard' || active === 'alerts' || active === 'alertes') {
            _renderStudentsPalmares(data.risques, 'dashboard');
        }
        if (active === 'alerts' || active === 'alertes')
            AlertsView.render(data, filters);
    }
};
window.DashboardView = DashboardView;
let _dashRisquesTable = null;
let _studRisquesTable = null;

function _renderStudentsPalmares(risques, view = 'dashboard') {
    let table = null;
    if (view === 'dashboard') {
        if (!_dashRisquesTable) _dashRisquesTable = _createRisquesTable('students-tbody', 'students-pagination', false);
        table = _dashRisquesTable;
    } else {
        if (!_studRisquesTable) _studRisquesTable = _createRisquesTable('etudiant-risk-tbody', 'etudiant-risk-pagination', true);
        table = _studRisquesTable;
    }
    if (table) {
        table.load(risques || []);
    }
    lucide.createIcons();
}

function _createRisquesTable(tbodyId, paginationId, isRanking = false) {
    const t = new PaginatedTable(tbodyId, paginationId, 10);
    const getCurrent = () => {
        const el = document.getElementById('student-name');
        return el ? el.dataset.anonymat : null;
    };
    
    t.setRenderer(r => {
        const isMe = r.anonymat === getCurrent();
        const rang = r.rang || '--';
        const suffix = _ordinal(r.rang);
        
        return `
        <tr class="border-t border-white/5 hover:bg-white/3 transition-colors ${isMe ? 'bg-accent/5' : ''}">
        ${isRanking ? `<td class="px-3 py-2.5"><span class="text-[10px] font-bold ${isMe ? 'text-accent' : 'text-gray-500'}">${rang}${suffix}</span></td>` : ''}
        <td class="px-3 py-2.5">
        <div class="flex flex-col">
        <span class="font-medium ${isMe ? 'text-accent' : 'text-white'} text-xs">${_esc(r.nom_prenoms) || '--'} ${isMe ? '(Vous)' : ''}</span>
        <span class="text-[9px] text-gray-500 font-mono">${_esc(r.anonymat) || '--'}</span>
        </div>
        </td>
        <td class="px-3 py-2.5"><span class="text-gray-400 text-[10px]">${_esc(r.departement) || '--'}</span></td>
        <td class="px-3 py-2.5 font-mono text-red-400 font-bold text-xs">${(r.moyenne || 0).toFixed(2)}</td>
        <td class="px-3 py-2.5 flex items-center justify-between">
        <span class="bdg ${(r.moyenne || 0) >= 10 ? 'bdg-green' : 'bdg-red'} text-[10px]">${(r.moyenne || 0) >= 10 ? 'Admis' : 'Ajourné'}</span>
        ${!isMe ? `
            <button onclick="StudentView.loadById('${_esc(r.anonymat || '')}')" title="Voir profil"
            class="ml-2 text-gray-600 hover:text-accent transition-colors">
            <i data-lucide="user" class="w-3 h-3"></i>
            </button>
            ` : ''}
            </td>
            </tr>`;
    });
    return t;
}
window._renderStudentsPalmares = _renderStudentsPalmares;

/* 
 VUE DÉPARTEMENT           
 */
const DeptView = {
    _dept: 'GI',
    _data: null,
    _deptData: null,
    _filters: null,
    
    switchDept(dept, btn) {
        document.querySelectorAll('.dept-tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this._dept = dept;
        this._filters = window.app?.currentFilters || {};
        this._loadDeptData();
    },
    
    render(data, filters) {
        this._data = data;
        this._filters = filters;
        this._renderPerfBars(data.scores_depts);
        const active = document.querySelector('.nav-item.active')?.getAttribute('data-view');
        if (active === 'dept') this._loadDeptData();
    },
    
    /* Appelle getDashboardAggregates avec le filtre departement actif */
    async _loadDeptData() {
        ['dept-kpi-moy', 'dept-kpi-taux', 'dept-kpi-med', 'dept-kpi-std', 'dept-kpi-q1q3', 'dept-kpi-iqr', 'dept-kpi-ue', 'dept-kpi-eff']
        .forEach(id => { const el = document.getElementById(id); if (el) el.innerHTML = '<div class="skeleton w-12 h-4"></div>'; });
        
        FigQueue.clear();
        ['fig-radar', 'fig-hm-fil', 'fig-cohortes'].forEach(imgId => {
            const img = document.getElementById(imgId);
            const loader = document.getElementById('fl-' + imgId.replace('fig-', ''));
            if (img) { img.src = ''; img.classList.remove('loaded'); }
            if (loader) {
                loader.classList.remove('hidden');
                loader.innerHTML = '<div class="spin"></div>' +
                '<span class="text-[9px] font-mono text-gray-600 uppercase">Chargement…</span>';
            }
        });
        
        const filters = { ...(this._filters || {}), departement: [this._dept] };
        try {
            const data = await api.getDashboardAggregates(filters, 'dept');
            if (!data) return;
            this._deptData = data;
            this._updateKPIs(data);
            this._loadFigs();
            InterpretBox.forDepartement('interpret-dept', this._dept, data, filters);
        } catch (e) { console.warn('DeptView._loadDeptData:', e); }
    },
    
    _updateKPIs(data) {
        const f2 = v => v != null ? (+v).toFixed(2) : '--';
        const f1p = v => v != null ? (+v).toFixed(1) + '%' : '--';
        _setText('dept-kpi-moy', f2(data.moyenne_global));
        _setText('dept-kpi-taux', f1p(data.taux_reussite_global));
        _setText('dept-kpi-med', f2(data.mediane));
        _setText('dept-kpi-std', f2(data.ecart_type));
        _setText('dept-kpi-q1q3', data.q1 != null ? `${(+data.q1).toFixed(2)} / ${(+data.q3).toFixed(2)}` : '--');
        _setText('dept-kpi-iqr', f2(data.iqr));
        const nbUE = (data.tableau_ue || []).length;
        _setText('dept-kpi-ue', nbUE || '--');
        _setText('dept-kpi-eff', data.effectif_exact ?? '--');
    },
    
    _loadFigs() {
        FigQueue.clear();
        const f = { ...(this._filters || {}), departement: [this._dept] };
        queueFig('fig-radar', 'radar_filieres', f);
        queueFig('fig-hm-fil', 'heatmap_filiere_semestre', f);
        queueFig('fig-cohortes', 'courbe_cohortes', f);
    },
    
    _renderPerfBars(depts) {
        const c = document.getElementById('dept-perf-bars');
        if (!c) return;
        if (!depts?.length) { c.innerHTML = '<div class="text-xs text-gray-600 italic">Aucune donnée</div>'; return; }
        c.innerHTML = [...depts].sort((a, b) => b.score - a.score).map(d => {
            const pct = (d.score / 20) * 100;
            const color = d.score >= 12 ? '#10b981' : d.score >= 10 ? '#6366f1' : '#ef4444';
            return `<div class="perf-row">
            <span class="perf-dept-lbl">${d.departement}</span>
            <div class="perf-bar-outer"><div class="perf-bar-inner" style="width:${pct.toFixed(1)}%;background:${color}"></div></div>
            <span class="perf-score-lbl" style="color:${color}">${d.score.toFixed(2)}</span>
            </div>`;
        }).join('');
    }
};
window.DeptView = DeptView;

/* 
 V UE FILIÈRE               
 */
const FiliereView = {
    _table: null,
    _filieres: [],
    _currentFil: null,
    _currentIdx: 0,
    
    async init() {
        this._table = new PaginatedTable('filiere-ue-tbody', 'filiere-ue-pagination', 12)
        .setRenderer(u => {
            const taux = u.taux_reussite || 0;
            return `<tr class="hover:bg-white/3 transition-colors cursor-pointer"
            onclick="UEView.loadCode('${_esc(u.ue)}')">
            <td class="font-mono text-accent font-semibold text-xs px-3 py-2.5">${_esc(u.ue)}</td>
            <td class="text-gray-400 text-xs px-3 py-2.5">S${_esc(u.semestre)}</td>
            <td class="text-gray-400 text-xs px-3 py-2.5">${u.credit || '--'}</td>
            <td class="font-mono text-xs px-3 py-2.5">${(u.moyenne || 0).toFixed(2)}</td>
            <td class="text-xs px-3 py-2.5">${tauxBadge(taux)}</td>
            <td class="text-gray-500 text-xs px-3 py-2.5">${u.effectif || 0}</td>
            <td class="text-xs px-3 py-2.5">${ueStatut(u.moyenne || 0, taux)}</td>
            </tr>`;
        });
        
        /* Charger la liste des filières disponibles */
        try {
            const meta = await api.getDisponibilites({});
            this._filieres = meta?.filieres || [];
            this._renderNav();
            if (this._filieres.length) {
                this._currentFil = this._filieres[0];
                this._currentIdx = 0;
                const active = document.querySelector('.nav-item.active')?.getAttribute('data-view');
                if (active === 'filiere') await this.loadFiliere(this._currentFil);
            }
        } catch (e) { console.warn('FiliereView.init:', e); }
    },
    
    _renderNav() {
        const nav = document.getElementById('filiere-nav');
        if (!nav) return;
        nav.innerHTML = this._filieres.map((f, i) => `
        <button id="fil-pill-${i}"
        class="fil-pill ${i === this._currentIdx ? 'active' : ''}"
        onclick="FiliereView.selectByIndex(${i})">${f}</button>
        `).join('');
    },
    
    async selectByIndex(idx) {
        if (idx < 0 || idx >= this._filieres.length) return;
        this._currentIdx = idx;
        this._currentFil = this._filieres[idx];
        document.querySelectorAll('.fil-pill').forEach((p, i) =>
        p.classList.toggle('active', i === idx));
        await this.loadFiliere(this._currentFil);
    },
    
    prev() { this.selectByIndex(this._currentIdx - 1); },
    next() { this.selectByIndex(this._currentIdx + 1); },
    
    async loadFiliere(filiere) {
        if (!filiere) return;
        this._currentFil = filiere;
        _setText('fil-current-name', filiere);
        
        ['fil-kpi-moy', 'fil-kpi-taux', 'fil-kpi-eff', 'fil-kpi-med', 'fil-kpi-std',
        'fil-kpi-q1q3', 'fil-kpi-iqr', 'fil-kpi-dept']
        .forEach(id => { const el = document.getElementById(id); if (el) el.innerHTML = '<div class="skeleton w-12 h-4"></div>'; });
        
        FigQueue.clear();
        ['fig-fil-sex', 'fig-fil-sexevol'].forEach(imgId => {
            const img = document.getElementById(imgId);
            const loader = document.getElementById('fl-' + imgId.replace('fig-', ''));
            if (img) { img.src = ''; img.classList.remove('loaded'); }
            if (loader) {
                loader.classList.remove('hidden');
                loader.innerHTML = '<div class="spin"></div>' +
                '<span class="text-[9px] font-mono text-gray-600 uppercase">Chargement…</span>';
            }
        });
        
        const filters = { ...window.app?.currentFilters || {}, filiere: [filiere] };
        
        try {
            const data = await api.getDashboardAggregates(filters, 'filiere');
            if (!data) return;
            
            /* KPIs */
            const _f2 = v => v != null ? (+v).toFixed(2) : '--';
            const _f1p = v => v != null ? (+v).toFixed(1) + '%' : '--';
            _setText('fil-kpi-moy', _f2(data.moyenne_global));
            _setText('fil-kpi-taux', _f1p(data.taux_reussite_global));
            _setText('fil-kpi-eff', data.effectif_exact ?? '--');
            _setText('fil-kpi-med', _f2(data.mediane));
            _setText('fil-kpi-std', _f2(data.ecart_type));
            _setText('fil-kpi-q1q3', data.q1 != null ? `${(+data.q1).toFixed(2)} / ${(+data.q3).toFixed(2)}` : '--');
            _setText('fil-kpi-iqr', _f2(data.iqr));
            const ue0 = (data.tableau_ue || [])[0];
            _setText('fil-kpi-dept', ue0?.departement || filiere.substring(0, 2) || '--');
            
            ['fig-fil-sex', 'fig-fil-sexevol'].forEach(id => {
                const l = document.getElementById('fl-' + id.replace('fig-', ''));
                if (l) l.classList.remove('hidden');
            });
                
                InterpretBox.forFiliere('interpret-filiere', filiere, data, filters);
                
                /* Figures */
                this.renderFigsOnly(filters);
                
                /* Tableau UE */
                this._table.load(data.tableau_ue || []);
        } catch (e) { console.warn('FiliereView.loadFiliere:', e); }
    },
    
    async load() {
        await this._refreshList();
        if (this._currentFil && this._filieres.includes(this._currentFil)) {
            await this.selectByIndex(this._filieres.indexOf(this._currentFil));
        } else if (this._filieres.length) {
            await this.selectByIndex(0);
        }
    },
    
    async _refreshList() {
        try {
            const meta = await api.getDisponibilites(window.app?.currentFilters || {});
            const filieres = meta?.filieres || [];
            if (JSON.stringify(filieres) === JSON.stringify(this._filieres)) return;
            this._filieres = filieres;
            this._renderNav();
            if (!this._currentFil || !filieres.includes(this._currentFil)) {
                this._currentFil = filieres[0] || null;
                this._currentIdx = 0;
            }
        } catch (e) { console.warn('FiliereView._refreshList:', e); }
    },
    
    renderFigsOnly(filters) {
        queueFig('fig-fil-sex', 'boxplot_by_sex', filters);
        queueFig('fig-fil-sexevol', 'courbe_moyenne_par_sexe', filters);
    }
};
window.FiliereView = FiliereView;


const UEView = {
    _allCodes: [],
    _filteredCodes: [],
    _topRows: [],
    _bottomRows: [],
    _currentCode: null,
    
    async init() {
        this._allCodes = await api.getUEList();
        this._filteredCodes = [...this._allCodes];
        this._bindSearch();
    },
    
    /* Appelé par DashboardView quand les données globales arrivent */
    onDashboardData(data, filters) {
        this._topRows = data.top10 || [];
        this._bottomRows = data.bottom10 || [];
        /* Mettre à jour les codes disponibles depuis tableau_ue filtré */
        const codesFromTableau = (data.tableau_ue || []).map(u => u.ue);
        this._filteredCodes = codesFromTableau.length ? codesFromTableau : [...this._allCodes];
        this._renderTopBottom();
    },
    
    _bindSearch() {
        const input = document.getElementById('ue-search-input');
        const list = document.getElementById('ue-search-results');
        if (!input || !list) return;
        
        const _positionDropdown = () => {
            const rect = input.getBoundingClientRect();
            list.style.top = (rect.bottom + 4) + 'px';
            list.style.left = rect.left + 'px';
            list.style.width = rect.width + 'px';
        };
        
        input.addEventListener('input', () => {
            const q = input.value.trim().toUpperCase();
            if (q.length < 1) { list.classList.add('hidden'); list.innerHTML = ''; return; }
            _positionDropdown();
            
            const pool = this._filteredCodes.length ? this._filteredCodes : this._allCodes;
            const matches = pool.filter(c => c.toUpperCase().includes(q)).slice(0, 14);
            
            if (!matches.length) {
                list.innerHTML = `<div class="px-4 py-3 text-xs text-gray-500 italic flex items-center gap-2">
                <i data-lucide="search-x" class="w-4 h-4 shrink-0"></i>
                Aucune UE correspondante avec les filtres actifs
                </div>`;
                lucide.createIcons();
            } else {
                list.innerHTML = matches.map(c => {
                    /* Retrouver les stats depuis tableau_ue pour affichage enrichi */
                    const row = (window.app?._lastData?.tableau_ue || []).find(u => u.ue === c);
                    const badge = row
                    ? `<span class="ml-auto font-mono text-[10px] ${(row.taux_reussite || 0) >= 50 ? 'text-green-500' : 'text-red-400'}">${(row.taux_reussite || 0).toFixed(0)}%</span>`
                    : '';
                    return `<button class="w-full text-left px-4 py-2.5 text-xs font-mono
                    hover:bg-accent/15 text-gray-300 hover:text-white transition-colors
                    flex items-center gap-2 border-b border-white/5 last:border-0"
                    data-ue-code="${_esc(c)}">
                    <i data-lucide="book-open" class="w-3.5 h-3.5 shrink-0 text-accent/60"></i>
                    <span>${_esc(c)}</span>${badge}
                    </button>`;
                }).join('');
                list.querySelectorAll('[data-ue-code]').forEach(btn => {
                    btn.addEventListener('click', () => {
                        UEView.loadCode(btn.dataset.ueCode);
                        input.value = btn.dataset.ueCode;
                        list.classList.add('hidden');
                    });
                });
                lucide.createIcons();
            }
            list.classList.remove('hidden');
        });
        
        document.addEventListener('click', e => {
            if (!input.contains(e.target) && !list.contains(e.target))
                list.classList.add('hidden');
        });
        /* Repositionner si scroll ou resize */
        window.addEventListener('scroll', _positionDropdown, true);
        window.addEventListener('resize', _positionDropdown);
    },
    
    /* Rendu du tableau Top / Bottom dans la vue UE */
    _renderTopBottom() {
        const topBody = document.getElementById('ue-top-tbody');
        const bottomBody = document.getElementById('ue-bottom-tbody');
        
        const rowHtml = r => {
            const taux = r.taux_reussite ?? 0;
            return `<tr class="hover:bg-white/3 transition-colors cursor-pointer"
            onclick="UEView.loadCode('${_esc(r.ue)}')">
            <td class="font-mono text-accent font-semibold text-xs px-3 py-2.5">${_esc(r.ue)}</td>
            <td class="text-gray-400 text-xs px-3 py-2.5">S${_esc(r.semestre)}</td>
            <td class="font-mono text-xs px-3 py-2.5">${(r.moyenne || 0).toFixed(2)}</td>
            <td class="text-xs px-3 py-2.5">${tauxBadge(taux)}</td>
            <td class="text-gray-500 text-xs px-3 py-2.5">${r.effectif || 0}</td>
            </tr>`;
        };
        
        if (topBody) topBody.innerHTML = this._topRows.map(rowHtml).join('') || emptyRow(5);
        if (bottomBody) bottomBody.innerHTML = this._bottomRows.map(rowHtml).join('') || emptyRow(5);
    },
    
    /* Chargement d'une UE */
    async loadCode(code) {
        if (!code) return;
        this._currentCode = code;
        
        /* Switcher sur la vue UE */
        ui.switchView('ue');
        document.querySelectorAll('.nav-item').forEach(n =>
        n.classList.toggle('active', n.getAttribute('data-view') === 'ue'));
        
        /* Afficher le panneau de détail */
        const panel = document.getElementById('ue-detail-panel');
        const name = document.getElementById('ue-detail-name');
        if (panel) panel.classList.remove('hidden');
        if (name) name.textContent = code;
        
        /* Mettre à jour la barre de recherche */
        const input = document.getElementById('ue-search-input');
        if (input) input.value = code;
        
        const filters = { ...window.app?.currentFilters || {}, ue: [code] };
        
        /* Skeleton KPIs immédiat */
        ['ue-kpi-moy', 'ue-kpi-taux', 'ue-kpi-med', 'ue-kpi-std', 'ue-kpi-var',
        'ue-kpi-q1q3', 'ue-kpi-iqr', 'ue-kpi-minmax', 'ue-kpi-eff', 'ue-kpi-cred', 'ue-kpi-sem']
        .forEach(id => { const el = document.getElementById(id); if (el) el.innerHTML = '<div class="skeleton w-12 h-4"></div>'; });
        
        /* Reset figures immédiat */
        FigQueue.clear();
        ['fig-ue-donut', 'fig-ue-hist', 'fig-ue-box'].forEach(imgId => {
            const img = document.getElementById(imgId);
            const loader = document.getElementById('fl-' + imgId.replace('fig-', ''));
            if (img) { img.src = ''; img.classList.remove('loaded'); }
            if (loader) {
                loader.classList.remove('hidden');
                loader.innerHTML = '<div class="spin"></div>' +
                '<span class="text-[9px] font-mono text-gray-600 uppercase">Chargement…</span>';
            }
        });
        
        /* Stats détaillées depuis /ues/{code}/stats */
        const s = await api.getUEStats(code, window.app?.currentFilters || {});
        if (s) {
            this._renderStats(s);
            InterpretBox.forUE('interpret-ue', { ...s, ue: code });
        }
        
        /* Figures UE */
        queueFig('fig-ue-donut', 'donut', filters);
        queueFig('fig-ue-hist', 'histogram', filters);
        queueFig('fig-ue-box', 'boxplot', filters);
    },
    
    /* Remplit tous les KPIs + stats étendues d'une UE */
    _renderStats(s) {
        /* KPIs principaux */
        const _uf = v => v != null ? (+v).toFixed(2) : '--';
        _setText('ue-kpi-moy', _uf(s.moyenne));
        _setText('ue-kpi-med', _uf(s.mediane_note));
        _setText('ue-kpi-std', _uf(s.std_note));
        _setText('ue-kpi-var', _uf(s.variance_note));
        _setText('ue-kpi-q1q3', s.q1_note != null ? `${(+s.q1_note).toFixed(2)} / ${(+s.q3_note).toFixed(2)}` : '--');
        _setText('ue-kpi-iqr', _uf(s.iqr_note));
        _setText('ue-kpi-minmax', s.min_note != null ? `${(+s.min_note).toFixed(2)} / ${(+s.max_note).toFixed(2)}` : '--');
        _setText('ue-kpi-cred', s.credit ?? '--');
        _setText('ue-kpi-eff', s.effectif ?? '--');
        _setText('ue-kpi-sem', s.semestre != null ? `S${s.semestre}` : '--');
        
        const tauxEl = document.getElementById('ue-kpi-taux');
        if (tauxEl) {
            tauxEl.textContent = (s.taux_reussite ?? 0).toFixed(1) + '%';
            tauxEl.className = `mini-kpi-val ${(s.taux_reussite || 0) >= 50 ? 'text-green-500' : 'text-red-500'}`;
        }
        
        /* Stats étendues */
        _setText('ue-stat-min', (s.min_note ?? '--') !== '--' ? (+s.min_note).toFixed(2) : '--');
        _setText('ue-stat-max', (s.max_note ?? '--') !== '--' ? (+s.max_note).toFixed(2) : '--');
        _setText('ue-stat-med', (s.mediane_note ?? '--') !== '--' ? (+s.mediane_note).toFixed(2) : '--');
        _setText('ue-stat-std', (s.std_note ?? '--') !== '--' ? (+s.std_note).toFixed(2) : '--');
        _setText('ue-stat-var', (s.variance_note ?? '--') !== '--' ? (+s.variance_note).toFixed(2) : '--');
        _setText('ue-stat-admis', s.nombre_admis ?? '--');
        _setText('ue-stat-ajournes', s.nombre_ajournes ?? '--');
        _setText('ue-stat-pct-adm', s.pourcentage_admis != null ? s.pourcentage_admis.toFixed(1) + '%' : '--');
        _setText('ue-stat-pct-ajour', s.pourcentage_ajournes != null ? s.pourcentage_ajournes.toFixed(1) + '%' : '--');
        
        /* Alerte difficile */
        const alertBox = document.getElementById('ue-alert-box');
        if (alertBox) alertBox.classList.toggle('hidden', !s.isDifficile);
        
        /* Barre réussite */
        const bar = document.getElementById('ue-taux-bar');
        const lbl = document.getElementById('ue-kpi-taux-txt');
        const pct = (s.taux_reussite || 0).toFixed(1) + '%';
        if (bar) bar.style.width = pct;
        if (lbl) lbl.textContent = pct;
    }
};
window.UEView = UEView;

function emptyRow(cols) {
    return `<tr><td colspan="${cols}" class="text-center py-6 text-gray-600 italic text-xs">Aucune donnée</td></tr>`;
}

/* 
 V UE ÉTUDIANT              
 */
const StudentView = {
    async load() {
        const input = document.getElementById('student-search-input');
        const id = input?.value.trim();
        if (!id) return;
        await this.loadById(id);
    },
    
    async loadById(id) {
        const input = document.getElementById('student-search-input');
        if (input) input.value = id;
        ui.switchView('etudiant');
        document.querySelectorAll('.nav-item').forEach(n =>
        n.classList.toggle('active', n.getAttribute('data-view') === 'students'));
        
        try {
            const p = await api.getParcours(id, window.app?.currentFilters || {});
            this._render(p);
        } catch (e) {
            alert(e.message || 'Étudiant introuvable');
        }
    },
    
    _render(p) {
        const wrap = document.getElementById('student-profile-wrap');
        if (wrap) wrap.classList.remove('hidden');
        
        /* Identité */
        const init = (p.nom_prenoms || '?').charAt(0).toUpperCase();
        const avatar = document.getElementById('student-avatar');
        if (avatar) avatar.textContent = init;
        _setText('student-name', p.nom_prenoms || `Anonymat ${p.anonymat}`);
        /* Stocker l'anonymat pour que exportPDF puisse le récupérer */
        const nameEl = document.getElementById('student-name');
        if (nameEl) nameEl.dataset.anonymat = p.anonymat || '';
        _setText('student-meta', `${p.anonymat || '--'} · ${p.filiere || '--'} · Cohorte ${p.cohorte || '--'}`);
        
        /* Badges */
        const badgesEl = document.getElementById('student-badges');
        if (badgesEl) {
            const isAdmis = (p.taux_reussite_global || 0) >= 50;
            badgesEl.innerHTML = `
            <span class="bdg bdg-blue">${p.filiere || '--'}</span>
            <span class="bdg ${isAdmis ? 'bdg-green' : 'bdg-red'}">${isAdmis ? 'Admis' : 'À risque'}</span>
            ${p.sexe ? `<span class="bdg bdg-purple">${p.sexe === 'F' ? 'Féminin' : 'Masculin'}</span>` : ''}`;
        }
        
        /* KPIs */
        _setText('etud-kpi-moy', (p.moyenne_globale || 0).toFixed(2));
        /* Crédits : "validés / total" dans une seule cellule */
        _setText('etud-kpi-cred',
                 `${p.credits_valides ?? '--'} / ${p.credits_total ?? '--'}`);
        _setText('etud-kpi-taux', (p.taux_reussite_global || 0).toFixed(1) + '%');
        /* Rang dans la cohorte */
        if (p.rang != null) {
            _setText('etud-kpi-rang', `${p.rang}${_ordinal(p.rang)}`);
            _setText('etud-kpi-nb-cohorte', `/ ${p.nb_cohorte ?? '?'}`);
        } else {
            _setText('etud-kpi-rang', '--');
            _setText('etud-kpi-nb-cohorte', '');
        }
        
        /* Barre crédits */
        const pct = p.credits_total > 0
        ? Math.min(100, (p.credits_valides / p.credits_total) * 100) : 0;
        const barEl = document.getElementById('etud-cred-bar');
        const pctEl = document.getElementById('etud-cred-pct');
        if (barEl) barEl.style.width = pct.toFixed(1) + '%';
        if (pctEl) pctEl.textContent =
            `${p.credits_valides || 0} / ${p.credits_total || 0} crédits (${pct.toFixed(0)}%)`;
        
        /* Parcours semestriel */
        const cont = document.getElementById('student-parcours-container');
        if (cont && p.parcours?.length) {
            cont.innerHTML = p.parcours.map(sem => {
                const isOk = (sem.moyenne || 0) >= 10;
                const uesHtml = (sem.details_ues || []).map(u => {
                    const ok = (u.note || 0) >= 10;
                    return `<tr>
                    <td class="font-mono text-accent text-xs">${u.ue}</td>
                    <td class="font-mono text-xs">${(u.note || 0).toFixed(2)}</td>
                    <td class="text-xs text-gray-500">${u.credit || '--'}</td>
                    <td><span class="bdg ${ok ? 'bdg-green' : 'bdg-red'} text-[9px]">${ok ? 'Validé' : 'Ajourné'}</span></td>
                    </tr>`;
                }).join('');
                return `<div class="sem-block">
                <div class="sem-block-header cursor-pointer hover:bg-white/5 transition-colors" onclick="this.nextElementSibling.classList.toggle('hidden');">
                <span class="sem-num">Semestre ${sem.semestre}</span>
                <div class="flex items-center gap-3">
                <span class="text-xs text-gray-500">${sem.credits_valides}/${sem.credits} crédits</span>
                <span class="sem-avg ${isOk ? 'text-green-400' : 'text-red-400'}">${(sem.moyenne || 0).toFixed(2)}</span>
                <i data-lucide="chevron-down" class="w-4 h-4 text-gray-400" style="pointer-events: none;"></i>
                </div>
                </div>
                <table class="dash-table hidden">
                <thead><tr><th>UE</th><th>Note</th><th>Crédits</th><th>Résultat</th></tr></thead>
                <tbody>${uesHtml}</tbody>
                </table>
                </div>`;
            }).join('');
        }
        
        /* Interprétation du parcours */
        InterpretBox.forEtudiant('interpret-etudiant', p);
        
        /* Courbe évolution étudiant vs cohorte */
        const etudId = p.anonymat || '';
        if (etudId) {
            const imgCohorte = document.getElementById('fig-etud-cohorte');
            const loaderCoh = document.getElementById('fl-etud-cohorte');
            if (imgCohorte) {
                const token = (imgCohorte._loadToken = (imgCohorte._loadToken || 0) + 1);
                imgCohorte.classList.remove('loaded');
                if (loaderCoh) { loaderCoh.classList.remove('hidden'); }
                imgCohorte.onload = () => { if (imgCohorte._loadToken !== token) return; imgCohorte.classList.add('loaded'); loaderCoh?.classList.add('hidden'); };
                imgCohorte.onerror = () => { if (imgCohorte._loadToken !== token) return; if (loaderCoh) { loaderCoh.innerHTML = '<i data-lucide="image-off" class="w-5 h-5 text-gray-700"></i><span class="text-[9px] text-gray-600 font-mono uppercase">Données insuffisantes</span>'; lucide.createIcons(); } };
                imgCohorte.src = api.getFigureUrl('student_cohorte', { ...window.app?.currentFilters || {}, ue: etudId });
            }
        }
        
        
        _studRisquesTable = null;
        _renderStudentsPalmares(p.palmares_cohorte || [], 'etudiant');
        lucide.createIcons();
    }
};
function _ordinal(n) {
    if (n === 1) return 'er';
    return 'e';
}
window.StudentView = StudentView;

/* 
 VUE ALERTES — avec table paginée UE difficiles
 */
const AlertsView = {
    _ueTable: null,
    
    init() {
        this._ueTable = new PaginatedTable('alerts-ue-tbody', 'alerts-ue-pagination', 10)
        .setRenderer(u => `<tr class="hover:bg-white/3 transition-colors cursor-pointer"
        onclick="UEView.loadCode('${_esc(u.ue)}')">
        <td class="font-mono text-amber-400 font-semibold text-xs px-3 py-2.5">${_esc(u.ue)}</td>
        <td class="text-gray-400 text-xs px-3 py-2.5">S${_esc(u.semestre)}</td>
        <td class="font-mono text-red-400 text-xs px-3 py-2.5">${(u.moyenne || 0).toFixed(2)}</td>
        <td class="text-xs px-3 py-2.5">${tauxBadge(u.taux_reussite || 0)}</td>
        <td class="text-gray-500 text-xs px-3 py-2.5">${u.effectif || 0}</td>
        </tr>`);
    },
    
    render(data, filters) {
        this._renderStudents(data.risques || []);
        this._ueTable.load(data.ue_difficiles || []);
        
        /* KPI résumé */
        _setText('alert-kpi-risque', (data.risques || []).length);
        _setText('alert-kpi-difficile', (data.ue_difficiles || []).length);
        _setText('alert-kpi-taux', (data.taux_reussite_global ?? 0).toFixed(1) + '%');
        _setText('alert-kpi-moy', (data.moyenne_global ?? 0).toFixed(2));
    },
    
    _renderStudents(risques) {
        /* Réinitialiser si le tbody a été supprimé du DOM */
        if (this._alertsTable && !document.getElementById('alerts-risk-tbody')) {
            this._alertsTable = null;
        }
        if (!this._alertsTable) {
            this._alertsTable = new PaginatedTable(
                'alerts-risk-tbody', 'alerts-risk-pagination', 10
            ).setRenderer(r => {
                const init = (r.nom_prenoms || '?')[0].toUpperCase();
                return `<tr class="hover:bg-white/3 transition-colors">
                <td class="px-3 py-2.5">
                <div class="flex items-center gap-2">
                <div class="w-7 h-7 rounded-full bg-red-500/20 text-red-400 flex items-center justify-center text-xs font-bold shrink-0">${init}</div>
                <div class="min-w-0">
                <div class="text-sm font-medium text-white truncate">${r.nom_prenoms || '--'}</div>
                <div class="text-[10px] text-gray-500">${r.departement || '--'}</div>
                </div>
                </div>
                </td>
                <td class="px-3 py-2.5 font-mono text-red-400 font-bold text-sm">${(r.moyenne || 0).toFixed(2)}</td>
                <td class="px-3 py-2.5 text-right">
                <button data-load-id="${_esc(r.anonymat || '')}"
                class="text-[10px] text-gray-500 hover:text-accent transition-colors">Voir →</button>
                </td>
                </tr>`;
            });
        }
        this._alertsTable.load(risques || []);
    }
};
window.AlertsView = AlertsView;

window._setText = _setText;
window._esc = _esc;

function _initViews() {
    /* Patch ui.switchView */
    const _origSwitch = ui.switchView.bind(ui);
    ui.switchView = function (viewId) {
        if (window.api && api.abortAll) api.abortAll();
        if (window.FigQueue && FigQueue.clear) FigQueue.clear();
        _origSwitch(viewId);
        const filters = window.app?.currentFilters || {};
        const data = window.app?._lastData;
        if (viewId === 'dept' && data) DeptView.render(data, filters);
        if (viewId === 'filiere') FiliereView.load();
        if (viewId === 'ue' && data) UEView.onDashboardData(data, filters);
        if (viewId === 'alertes' || viewId === 'alerts') {
            if (data) AlertsView.render(data, filters);
        }
    };
    
    /* Initialisation des vues */
    try {
        DashboardView.init();
        AlertsView.init();
        FiliereView.init();
        UEView.init();
        CompareView.init();
    } catch (e) { console.error('Views initialization failed:', e); }
    
    /* Invalider le cache de suggestions de CompareView quand les filtres changent
     (ex: un filtre département actif réduit les filières disponibles) */
    const _origRefresh = window.app?.refresh?.bind(window.app);
    if (window.app && _origRefresh) {
        window.app.refresh = async function (force) {
            if (window.CompareView) CompareView._allItems = [];
            return _origRefresh(force);
        };
    }
    
    const _origGet = api.getDashboardAggregates.bind(api);
    api.getDashboardAggregates = async function (filters, abortKey) {
        const data = await _origGet(filters, abortKey);
        if (data && window.app) window.app._lastData = data;
        return data;
    };
}
window._initViews = _initViews;

/* 
 VUE COMPARAISON           
 Permet de comparer UEs / filières / départements côte à côte.
 Indicateurs : moyenne, médiane, taux, σ, σ², Q1, Q3, IQR, min, max.
 */
const CompareView = {
    _type: 'filiere',    // type courant
    _selected: [],           // entités sélectionnées
    _allItems: [],           // toutes les options disponibles (pour suggestions)
    
    init() {
        if (this._initialized) return;
        this._initialized = true; 
        /* Boutons de type */
        document.querySelectorAll('.cmp-type-btn').forEach(btn => {
            btn.onclick = () => {
                document.querySelectorAll('.cmp-type-btn').forEach(b => b.classList.remove('cmp-active'));
                btn.classList.add('cmp-active');
                this._type = btn.dataset.cmpType;
                this._selected = [];
                this._allItems = [];   /* forcer rechargement au prochain focus */
                this._renderChips();
            };
        });
        
        /* Recherche avec suggestions — chargées au premier focus uniquement */
        const searchEl = document.getElementById('cmp-search');
        if (searchEl) {
            searchEl.addEventListener('input', () => this._filterSuggestions(searchEl.value));
            searchEl.addEventListener('focus', async () => {
                /* Charger les suggestions à la demande (pas au démarrage) */
                if (!this._allItems.length) await this._loadSuggestions();
                this._filterSuggestions(searchEl.value);
            });
            document.addEventListener('click', e => {
                if (!e.target.closest('#cmp-search') && !e.target.closest('#cmp-suggestions')) {
                    document.getElementById('cmp-suggestions')?.classList.add('hidden');
                }
            });
        }
        
        /* Bouton comparer */
        document.getElementById('cmp-compare-btn')?.addEventListener('click', () => this.run());
        
        /* Bouton reset */
        document.getElementById('cmp-reset-btn')?.addEventListener('click', () => {
            this._selected = [];
            this._renderChips();
            document.getElementById('cmp-results').innerHTML =
            '<div class="text-xs text-gray-600 italic text-center py-8">Sélectionnez des entités à comparer ci-dessus, puis cliquez sur <strong>Comparer</strong>.</div>';
        });
        this._initialized = true;
    },
    
    async _loadSuggestions() {
        try {
            if (this._type === 'ue') {
                this._allItems = await api.getUEList();
            } else {
                const dispo = await api.getDisponibilites(window.app?.currentFilters || {});
                const map = {
                    filiere: dispo?.filieres || [],
                    departement: dispo?.departements || [],
                };
                this._allItems = (map[this._type] || []).map(v =>
                typeof v === 'object' ? (v.value || v.label) : v
                );
            }
        } catch (e) {
            if (e && e.name === 'AbortError') return;
            console.warn('_loadSuggestions:', e);
            this._allItems = [];
        }
    },
    
    _filterSuggestions(q) {
        const box = document.getElementById('cmp-suggestions');
        if (!box) return;
        const items = this._allItems.filter(v =>
        !this._selected.includes(v) &&
        (!q || v.toLowerCase().includes(q.toLowerCase()))
        );
        
        if (!items.length) { box.classList.add('hidden'); return; }
        box.classList.remove('hidden');
        box.innerHTML = items.map(v =>
        `<button class="w-full text-left px-3 py-1.5 text-xs text-gray-300 hover:bg-white/10 rounded transition-colors"
        data-cmp-item="${_esc(v)}">${_esc(v)}</button>`
        ).join('');
        box.querySelectorAll('[data-cmp-item]').forEach(btn => {
            btn.addEventListener('click', () => {
                CompareView._addItem(btn.dataset.cmpItem);
                const s = document.getElementById('cmp-search');
                if (s) s.value = '';
                box.classList.add('hidden');
            });
        });
    },
    
    _addItem(v) {
        if (!this._selected.includes(v)) {
            this._selected.push(v);
            this._renderChips();
        }
    },
    
    _removeItem(v) {
        this._selected = this._selected.filter(x => x !== v);
        this._renderChips();
    },
    
    _renderChips() {
        const el = document.getElementById('cmp-chips');
        if (!el) return;
        const COLORS = ['bg-accent/20 border-accent/40 text-accent',
        'bg-green-500/20 border-green-500/40 text-green-400',
        'bg-purple-500/20 border-purple-500/40 text-purple-400',
        'bg-amber-500/20 border-amber-500/40 text-amber-400',
        'bg-pink-500/20 border-pink-500/40 text-pink-400',
        'bg-teal-500/20 border-teal-500/40 text-teal-400'];
        el.innerHTML = this._selected.length
        ? this._selected.map((v, i) => {
            const cls = COLORS[i % COLORS.length];
            return `<span class="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-semibold rounded-full border ${cls}">
            ${_esc(v)}
            <button data-cmp-remove="${_esc(v)}"
            class="opacity-60 hover:opacity-100 ml-0.5">✕</button>
            </span>`;
        }).join('')
        : '<span class="text-xs text-gray-600 italic">Aucune entité sélectionnée</span>';
        el.querySelectorAll('[data-cmp-remove]').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                CompareView._removeItem(btn.dataset.cmpRemove);
            });
        });
    },
    
    async run() {
        const resultsEl = document.getElementById('cmp-results');
        if (!resultsEl) return;
        if (!this._selected.length) {
            resultsEl.innerHTML = '<div class="text-xs text-amber-400 italic text-center py-4">Sélectionnez au moins une entité.</div>';
            return;
        }
        
        resultsEl.innerHTML = '<div class="text-xs text-gray-500 italic text-center py-8 animate-pulse">Chargement des comparaisons…</div>';
        try {
            const data = await api.getCompare(this._type, this._selected, window.app?.currentFilters || {});
            this._render(data.entites || []);
        } catch (e) {
            resultsEl.innerHTML = `<div class="text-xs text-red-400 text-center py-4">Erreur : ${e.message}</div>`;
        }
    },
    
    _render(entites) {
        const el = document.getElementById('cmp-results');
        if (!el || !entites.length) {
            el.innerHTML = '<div class="text-xs text-gray-600 italic text-center py-8">Aucun résultat.</div>';
            return;
        }
        
        const COLORS = ['#6366f1', '#22c55e', '#a855f7', '#f59e0b', '#ec4899', '#14b8a6'];
        const METRICS = [
            { key: 'moyenne', label: 'Moyenne', unit: '/20', fmt: v => v.toFixed(2) },
            { key: 'taux_reussite', label: 'Taux réussite', unit: '%', fmt: v => v.toFixed(1) },
            { key: 'mediane', label: 'Médiane (Q2)', unit: '/20', fmt: v => v.toFixed(2) },
            { key: 'ecart_type', label: 'Écart-type σ', unit: '', fmt: v => v.toFixed(2) },
            { key: 'variance', label: 'Variance σ²', unit: '', fmt: v => v.toFixed(2) },
            { key: 'q1', label: 'Q1 (25e pct)', unit: '/20', fmt: v => v.toFixed(2) },
            { key: 'q3', label: 'Q3 (75e pct)', unit: '/20', fmt: v => v.toFixed(2) },
            { key: 'iqr', label: 'IQR (Q3−Q1)', unit: ' pts', fmt: v => v.toFixed(2) },
            { key: 'min', label: 'Min', unit: '/20', fmt: v => v.toFixed(2) },
            { key: 'max', label: 'Max', unit: '/20', fmt: v => v.toFixed(2) },
            { key: 'effectif', label: 'Effectif', unit: '', fmt: v => v },
            { key: 'nb_ues', label: 'Nb UEs', unit: '', fmt: v => v },
        ];
        
        /*  Tableau comparatif  */
        const headerCols = entites.map((e, i) =>
        `<th class="px-4 py-3 text-center text-xs font-bold" style="color:${COLORS[i % COLORS.length]}">${e.nom}</th>`
        ).join('');
        
        /* Meilleur par métrique */
        const bestFor = {};
        for (const m of METRICS) {
            const vals = entites.map(e => e[m.key] ?? -Infinity);
            const higher = ['moyenne', 'taux_reussite', 'mediane', 'q1', 'q3', 'max', 'effectif', 'nb_ues'].includes(m.key);
            bestFor[m.key] = higher ? Math.max(...vals) : Math.min(...vals.filter(v => v > -Infinity));
        }
        
        const rows = METRICS.map(m => {
            const cells = entites.map((e, i) => {
                const v = e[m.key];
                if (v === undefined || v === null) return `<td class="px-4 py-2 text-center text-gray-600 text-xs">—</td>`;
                const isBest = v === bestFor[m.key];
                const color = COLORS[i % COLORS.length];
                const bold = isBest ? 'font-bold' : 'font-normal';
                const badge = isBest ? `<span class="ml-1 text-[9px] px-1 py-0.5 rounded" style="background:${color}22;color:${color}">✓</span>` : '';
                return `<td class="px-4 py-2 text-center text-xs font-mono ${bold} text-white">${m.fmt(v)}${m.unit}${badge}</td>`;
            }).join('');
            return `<tr class="border-t border-white/5 hover:bg-white/3 transition-colors">
            <td class="px-4 py-2 text-xs text-gray-400 font-medium whitespace-nowrap">${m.label}</td>
            ${cells}
            </tr>`;
        }).join('');
        
        /*  Barres de comparaison visuelles  */
        const BAR_METRICS = [
            { key: 'moyenne', label: 'Moyenne', max: 20 },
            { key: 'taux_reussite', label: 'Taux réussite', max: 100 },
            { key: 'ecart_type', label: 'Écart-type', max: 10 },
            { key: 'iqr', label: 'IQR', max: 20 },
        ];
        const bars = BAR_METRICS.map(m => {
            const items = entites.map((e, i) => {
                const v = e[m.key] ?? 0;
                const pct = Math.min(100, (v / m.max) * 100).toFixed(1);
                const clr = COLORS[i % COLORS.length];
                return `<div class="flex items-center gap-2 text-xs">
                <div class="w-16 text-right text-gray-400 shrink-0">${e.nom}</div>
                <div class="flex-1 bg-white/5 rounded-full h-2 overflow-hidden">
                <div class="h-full rounded-full transition-all duration-700"
                style="width:${pct}%;background:${clr}"></div>
                </div>
                <div class="w-12 font-mono text-white shrink-0">${typeof m.max === 'number' && m.max === 100 ? v.toFixed(1) + '%' : v.toFixed(2)}</div>
                </div>`;
            }).join('');
            return `<div>
            <div class="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-2">${m.label}</div>
            <div class="space-y-1.5">${items}</div>
            </div>`;
        }).join('');
        
        el.innerHTML = `
        <!-- Tableau -->
        <div class="glass-card overflow-x-auto">
        <div class="p-4 border-b border-white/5 flex items-center gap-2">
        <i data-lucide="table-2" class="w-4 h-4 text-accent"></i>
        <span class="text-sm font-semibold">Tableau comparatif</span>
        <span class="fig-badge ml-auto">${entites.length} entité${entites.length > 1 ? 's' : ''}</span>
        </div>
        <table class="dash-table w-full">
        <thead><tr>
        <th class="px-4 py-3 text-left text-xs text-gray-500">Indicateur</th>
        ${headerCols}
        </tr></thead>
        <tbody>${rows}</tbody>
        </table>
        </div>
        
        <!-- Barres -->
        <div class="glass-card p-5">
        <div class="p-0 border-b border-white/5 pb-3 mb-4 flex items-center gap-2">
        <i data-lucide="bar-chart-horizontal" class="w-4 h-4 text-accent"></i>
        <span class="text-sm font-semibold">Comparaison visuelle</span>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">${bars}</div>
        </div>`;
        
        lucide.createIcons();
    },
};
window.CompareView = CompareView;

/*  AdminView ─
 Gère l'affichage de l'historique des uploads.
 */
const AdminView = {
    async loadHistory() {
        const history = await api.getUploadHistory();
        this.renderHistory(history);
    },
    
    async rollback() {
        if (!confirm("⚠️ Êtes-vous sûr de vouloir restaurer les données à l'état précédent ?\n\nCette action remplacera les données actuelles par le dernier backup (.bak) disponible.")) {
            return;
        }
        
        try {
            const res = await api.rollbackData();
            alert("✅ " + res.message);
            // Rafraîchir l'historique et les données globales
            await this.loadHistory();
            if (window.app) await app.refresh(true);
        } catch (e) {
            alert("❌ Erreur lors du rollback: " + e.message);
        }
    },
    
    renderHistory(history) {
        const tbody = document.getElementById('admin-history-tbody');
        if (!tbody) return;
        
        if (!history || history.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center py-6 text-gray-600 italic text-xs">Aucun historique disponible</td></tr>';
            return;
        }
        
        tbody.innerHTML = history.map(event => {
            const date = new Date(event.timestamp * 1000).toLocaleString();
            const statusClass = {
                'success': 'bg-green-500/10 text-green-500 border-green-500/20',
                'partial_success': 'bg-amber-500/10 text-amber-500 border-amber-500/20',
                'error': 'bg-red-500/10 text-red-500 border-red-500/20'
            }[event.status] || 'bg-gray-500/10 text-gray-500 border-gray-500/20';
            
            const statusLabel = {
                'success': 'Succès',
                'partial_success': 'Partiel',
                'error': 'Échec'
            }[event.status] || event.status;
            
            const details = event.error ? `<span class="text-red-400">${_esc(event.error)}</span>` :
            `+${event.added} ajoutés, ${event.updated} MAJ`;
            
            return `
            <tr class="hover:bg-white/5 transition-colors border-b border-white/5">
            <td class="text-[10px] font-mono text-gray-400 font-medium">${date}</td>
            <td>
            <div class="flex flex-col">
            <span class="text-xs font-bold text-white">${_esc(event.filename)}</span>
            <span class="text-[10px] text-gray-500">${_esc(event.user)}</span>
            </div>
            </td>
            <td>
            <span class="px-2 py-0.5 rounded text-[10px] font-bold border ${statusClass}">
            ${statusLabel}
            </span>
            </td>
            <td class="text-[10px] text-gray-400">${details}</td>
            <td class="text-right">
            ${event.warnings && event.warnings.length ?
                `<i data-lucide="alert-circle" class="w-3.5 h-3.5 text-amber-500 inline cursor-help" title="${_esc(event.warnings.join('\n'))}"></i>` : ''}
                </td>
                </tr>
                `;
        }).join('');
        
        lucide.createIcons({ el: tbody });
    }
};
window.AdminView = AdminView;
