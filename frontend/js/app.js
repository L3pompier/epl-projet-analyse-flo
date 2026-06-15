/* ============================================================
   app.js — Orchestrateur principal
   ============================================================ */

const app = {
    currentFilters: {
        annee: [], semestre: [], cohorte: [],
        filiere: [], departement: [], type: [], niveau: [], sexe: []
    },


    _cache: {},
    _CACHE_TTL: 30_000,
    _lastData: null,
    _cacheBust: 0,
    _initialized: false,

    /* ── Debounce & Timers ────────────────────────────────────── */
    _metaDebounce: null,
    _idleTimer: null,
    _IDLE_LIMIT: 10 * 60 * 1000,

    async init() {
        if (this._initialized) return;
        this._initialized = true;

        ui.init();
        if (api._token) ui.setLoginState(true);

        api.getHealth().then(h => {
            const ok = h.status === 'ok';
            ui.updateStatus(ok);
            ui.showOfflineBanner(!ok);
        }).catch(() => {
            ui.updateStatus(false);
            ui.showOfflineBanner(true);
        });

        setInterval(async () => {
            try {
                const h = await api.getHealth();
                const ok = h.status === 'ok';
                ui.updateStatus(ok);
                ui.showOfflineBanner(!ok);
            } catch {
                ui.updateStatus(false);
                ui.showOfflineBanner(true);
            }
        }, 300_000);

        this.setupEventListeners();
        this.setupIdleTimer();

        await this.refresh();
        this.updateFiltersMeta().catch(e => console.warn('updateFiltersMeta:', e));
    },

    setupIdleTimer() {
        const events = ['mousedown', 'mousemove', 'keypress', 'scroll', 'touchstart'];
        const reset = () => {
            clearTimeout(this._idleTimer);
            if (api._token) {
                this._idleTimer = setTimeout(() => {
                    console.warn("Session expirée pour inactivité.");
                    this.handleLogout();
                    alert("Votre session a expiré après 15 minutes d'inactivité pour votre sécurité.");
                }, this._IDLE_LIMIT);
            }
        };
        events.forEach(e => document.addEventListener(e, reset, true));

        this._resetIdleTimer = reset;
        reset();
    },

    updateFiltersMeta() {
        clearTimeout(this._metaDebounce);
        return new Promise(resolve => {
            this._metaDebounce = setTimeout(async () => {
                const meta = await api.getDisponibilites(this.currentFilters);
                if (!meta) { resolve(); return; }
                ui.populateCheckboxes(ui.elements.filters.annee, meta.annees || [], this.currentFilters.annee);
                ui.populateCheckboxes(ui.elements.filters.semestre, meta.semestres || [], this.currentFilters.semestre);
                ui.populateCheckboxes(ui.elements.filters.cohorte, meta.cohortes || [], this.currentFilters.cohorte);
                ui.populateCheckboxes(ui.elements.filters.departement, meta.departements || [], this.currentFilters.departement);
                ui.populateCheckboxes(ui.elements.filters.filiere, meta.filieres || [], this.currentFilters.filiere);
                ui.populateCheckboxes(ui.elements.filters.type, meta.types_formation || [], this.currentFilters.type);
                ui.populateCheckboxes(ui.elements.filters.niveau, meta.niveaux || [], this.currentFilters.niveau);
                ui.populateCheckboxes(ui.elements.filters.sexe, meta.sexes || [], this.currentFilters.sexe);
                resolve();
            }, 300);
        });
    },

    setupEventListeners() {
        const bindCheckboxes = (container, key) => {
            if (!container) return;
            container.addEventListener('change', () => {
                this.currentFilters[key] = Array.from(
                    container.querySelectorAll('input[type="checkbox"]:checked')
                ).map(cb => cb.value);
                this.updateFiltersMeta();
            });
        };
        bindCheckboxes(ui.elements.filters.annee, 'annee');
        bindCheckboxes(ui.elements.filters.semestre, 'semestre');
        bindCheckboxes(ui.elements.filters.cohorte, 'cohorte');
        bindCheckboxes(ui.elements.filters.departement, 'departement');
        bindCheckboxes(ui.elements.filters.filiere, 'filiere');
        bindCheckboxes(ui.elements.filters.type, 'type');
        bindCheckboxes(ui.elements.filters.niveau, 'niveau');
        bindCheckboxes(ui.elements.filters.sexe, 'sexe');

        const btnReset = document.getElementById('btn-reset-filters');
        if (btnReset) {
            btnReset.onclick = async () => {
                this.currentFilters = {
                    annee: [], semestre: [], cohorte: [],
                    filiere: [], departement: [], type: [], niveau: [], sexe: []
                };
                this._cache = {};
                await this.refresh();
                await this.updateFiltersMeta();
                ui.toggleFilterModal();
            };
        }
    },

    async bulkSelect(key, all) {
        const container = ui.elements.filters[key];
        if (!container) return;
        const inputs = Array.from(container.querySelectorAll('input[type="checkbox"]'));
        this.currentFilters[key] = all ? inputs.map(i => i.value) : [];
        inputs.forEach(i => i.checked = all);
        await this.updateFiltersMeta();
    },

    _cacheKey() {
        return JSON.stringify(this.currentFilters);
    },

    async refresh(force = false) {
        ui.updateSummary(this.currentFilters);

        if (force) {
            this._cache = {};
            this._cacheBust++;

            document.querySelectorAll('.fig-wrap img').forEach(img => {
                img.src = '';
                img.classList.remove('loaded');
            });
            document.querySelectorAll('.fig-loader').forEach(l => {
                l.classList.remove('hidden');
                l.innerHTML = '<div class="spin"></div>' +
                    '<span class="text-[9px] font-mono text-gray-600 uppercase">Chargement…</span>';
            });
        }

        const key = this._cacheKey();
        const cached = this._cache[key];

        if (!force && cached && (Date.now() - cached.ts) < this._CACHE_TTL) {
            console.debug("[cache] hit — pas d'appel API");
            DashboardView.render(cached.data, this.currentFilters);
            this._refreshDependentViews(cached.data);
            return;
        }

        /* Vider la queue et masquer les anciennes figures du dashboard immédiatement */
        if (window.FigQueue) FigQueue.clear();
        const activeView = document.querySelector('.nav-item.active')?.getAttribute('data-view');
        if (activeView === 'dashboard') {
            ['fig-donut', 'fig-hist', 'fig-box', 'fig-evol-taux', 'fig-bysex', 'fig-valid'].forEach(imgId => {
                const img = document.getElementById(imgId);
                const loader = document.getElementById('fl-' + imgId.replace('fig-', ''));
                if (img) { img.src = ''; img.classList.remove('loaded'); }
                if (loader) {
                    loader.classList.remove('hidden');
                    loader.innerHTML = '<div class="spin"></div>' +
                        '<span class="text-[9px] font-mono text-gray-600 uppercase">Chargement…</span>';
                }
            });
        }

        /* Afficher skeletons pendant le chargement */
        ui.showSkeletons();

        const data = await api.getDashboardAggregates(this.currentFilters);
        if (data) {
            this._cache[key] = { data, ts: Date.now() };
            this._lastData = data;
            DashboardView.render(data, this.currentFilters);
            this._refreshDependentViews(data);
        }
    },

    /* Met à jour les vues sensibles aux filtres globaux */
    _refreshDependentViews(data) {
        const active = document.querySelector('.nav-item.active')?.getAttribute('data-view');
        const filters = this.currentFilters;

        if (window.UEView)
            UEView.onDashboardData(data, filters);

        if (active === 'filiere' && window.FiliereView)
            FiliereView.load();

        if (window.DeptView)
            DeptView._filters = filters;

        if (window.AlertsView)
            AlertsView.render(data, filters);
    },

    async exportPDF() {
        if (!api._token) {
            alert("Veuillez vous connecter pour exporter un rapport PDF.");
            ui.toggleLoginModal();
            return;
        }

        const filters = this._buildPdfFilters();
        const url = api.getReportPdfUrl(filters);

        try {
            const res = await api._fetch(url);
            if (!res.ok) {
                if (res.status === 401) {
                    alert("Session expirée. Veuillez vous reconnecter.");
                    api.setToken(null);
                    ui.setLoginState(false);
                } else {
                    const errData = await res.json().catch(() => ({}));
                    throw new Error(errData.detail || `Erreur HTTP ${res.status}`);
                }
                return;
            }
            const ct = res.headers.get('Content-Type') || '';
            if (!ct.includes('pdf')) {
                throw new Error("La réponse du serveur n'est pas un PDF valide.");
            }
            const blob = await res.blob();
            const downloadUrl = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = downloadUrl;
            a.download = `Rapport_${filters._label || 'Academic'}.pdf`;
            document.body.appendChild(a);
            a.click();
            a.remove();
        } catch (e) {
            console.error(e);
            alert("Erreur: " + e.message);
        }
    },

    async handleLogout() {
        clearTimeout(this._idleTimer);
        this._idleTimer = null;
        api.setToken(null);
        ui.setLoginState(false);
        ui.toggleLoginModal();
        await this.refresh();
    },

    async handleLogin() {
        const u = document.getElementById('login-username')?.value;
        const p = document.getElementById('login-password')?.value;
        const err = document.getElementById('login-error');
        const btn = document.getElementById('login-submit-btn');

        if (err) err.classList.add('hidden');
        if (btn) btn.disabled = true;

        try {
            await api.login(u, p);
            ui.toggleLoginModal();
            ui.setLoginState(true);
            this._resetIdleTimer?.();
            await this.refresh();
        } catch (e) {
            if (err) err.classList.remove('hidden');
        } finally {
            if (btn) btn.disabled = false;
        }
    },

    _buildPdfFilters() {
        const base = { ...this.currentFilters };
        const active = document.querySelector('.nav-item.active')?.getAttribute('data-view') || 'dashboard';

        switch (active) {
            case 'dept':
                if (window.DeptView?._dept) {
                    base.departement = [window.DeptView._dept];
                    base._context = 'departement';
                    base._label = `Département ${window.DeptView._dept}`;
                } else {
                    base._context = 'dashboard';
                    base._label = 'Vue globale';
                }
                break;

            case 'filiere':
                if (window.FiliereView?._currentFil) {
                    base.filiere = [window.FiliereView._currentFil];
                    base._context = 'filiere';
                    base._label = `Filière ${window.FiliereView._currentFil}`;
                } else {
                    base._context = 'dashboard';
                    base._label = 'Vue globale';
                }
                break;

            case 'ue':
                if (window.UEView?._currentCode) {
                    base.ue = [window.UEView._currentCode];
                    base._context = 'ue';
                    base._label = `UE ${window.UEView._currentCode}`;
                } else {
                    base._context = 'dashboard';
                    base._label = "Vue Unités d'Enseignement";
                }
                break;

            case 'students': {
                const anon = document.getElementById('student-name')?.dataset?.anonymat;
                if (anon) {
                    base.anonymat = anon;
                    base._context = 'etudiant';
                    base._label = document.getElementById('student-name')?.textContent?.trim() || anon;
                }
                break;
            }

            default:
                base._context = 'dashboard';
                base._label = 'Vue globale';
        }

        return base;
    }
};

window.app = app;
