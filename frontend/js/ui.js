const ui = {
    elements: {},

    _initElements() {
        this.elements = {
            sidebar: document.getElementById('sidebar'),
            sidebarToggle: document.getElementById('sidebarToggle'),
            toggleIcon: document.getElementById('toggleIcon'),
            headerTitle: document.getElementById('headerTitle'),
            filterModal: document.getElementById('filter-modal'),
            activeFiltersSummary: document.getElementById('active-filters-summary'),
            statusDot: document.getElementById('status-dot'),
            statusText: document.getElementById('status-text'),
            analysisDetails: document.getElementById('current-analysis-details'),
            dbRowsCount: document.getElementById('db-rows-count'),
            views: {
                dashboard: document.getElementById('view-dashboard'),
                etudiant: document.getElementById('view-etudiant'),
                dept: document.getElementById('view-dept'),
                filiere: document.getElementById('view-filiere'),
                ue: document.getElementById('view-ue'),
                alertes: document.getElementById('view-alertes'),
                compare: document.getElementById('view-compare')
            },
            filters: {
                annee: document.getElementById('filter-annee'),
                semestre: document.getElementById('filter-semestre'),
                cohorte: document.getElementById('filter-cohorte'),
                departement: document.getElementById('filter-departement'),
                filiere: document.getElementById('filter-filiere'),
                type: document.getElementById('filter-type'),
                niveau: document.getElementById('filter-niveau'),
                sexe: document.getElementById('filter-sexe')
            },
            kpis: {
                moyenne: document.getElementById('kpi-moyenne'),
                moyenneTrend: document.getElementById('kpi-moyenne-trend'),
                taux: document.getElementById('kpi-taux'),
                tauxTrend: document.getElementById('kpi-taux-trend'),
                mediane: document.getElementById('kpi-mediane'),
                std: document.getElementById('kpi-std'),
                variance: document.getElementById('kpi-variance'),
                q1: document.getElementById('kpi-q1'),
                q3: document.getElementById('kpi-q3'),
                iqr: document.getElementById('kpi-iqr'),
                effectif: document.getElementById('kpi-effectif'),
                risques: document.getElementById('kpi-risques'),
            },
            alerts: document.getElementById('alerts-container'),
            alertsBadge: document.getElementById('alerts-count-badge'),
            tooltip: document.getElementById('hm-tooltip'),
            loginModal: document.getElementById('login-modal'),
            loginBtn: document.getElementById('login-btn'),
            sideLoginBtn: document.getElementById('sidebar-login-btn')
        };
    },

    init() {
        this._initElements();
        this.elements.sidebarToggle.onclick = () => {
            const isEx = this.elements.sidebar.classList.toggle('expanded');
            this.elements.toggleIcon.setAttribute('data-lucide', isEx ? 'panel-left-close' : 'panel-left-open');
            lucide.createIcons();
        };


        const SELF_LOADING_VIEWS = new Set(['dept', 'filiere', 'ue', 'students', 'compare']);

        document.querySelectorAll('.nav-item').forEach(item => {
            const view = item.getAttribute('data-view');
            if (!view) return;
            item.onclick = (e) => {
                document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
                item.classList.add('active');
                this.switchView(view);
                if (window.app && !SELF_LOADING_VIEWS.has(view)) window.app.refresh();
            };
        });

    },

    switchView(viewId) {
        const mapping = {
            'dashboard': 'dashboard',
            'etudiant': 'etudiant',
            'students': 'etudiant',
            'dept': 'dept',
            'filiere': 'filiere',
            'ue': 'ue',
            'alertes': 'alertes',
            'alerts': 'alertes',
            'compare': 'compare'
        };

        const target = mapping[viewId];
        Object.entries(this.elements.views).forEach(([key, el]) => {
            if (el) el.classList.toggle('hidden', key !== target);
        });

        const titles = {
            'dashboard': 'Vue <span>Globale</span>',
            'etudiant': 'Suivi <span>Étudiant</span>',
            'dept': 'Performance <span>Département</span>',
            'filiere': 'Analyse <span>Filière</span>',
            'ue': 'Détails <span>Module (UE)</span>',
            'alertes': 'Alertes <span>& Risques</span>',
            'compare': 'Comparaison <span>Multi-entités</span>',
            'alerts': 'Gestion des <span>Alertes</span>'
        };
        this.elements.headerTitle.innerHTML = titles[viewId] || 'Academic <span>Analytics</span>';
    },

    toggleFilterModal() {
        this.elements.filterModal.classList.toggle('hidden');
    },

    toggleLoginModal() {
        if (this.elements.loginModal) {
            const isHidden = this.elements.loginModal.classList.toggle('hidden');
            if (!isHidden && api._token && window.AdminView) {
                AdminView.loadHistory();
            }
        }
    },
    setLoginState(isLoggedIn) {
        const btns = [this.elements.loginBtn, this.elements.sideLoginBtn];
        btns.forEach(btn => {
            if (!btn) return;
            const isSidebar = btn === this.elements.sideLoginBtn;

            if (isLoggedIn) {
                if (isSidebar) {
                    btn.innerHTML = '<i data-lucide="unlock" class="w-4 h-4 shrink-0 transition-transform group-hover:scale-110 text-green-400"></i><span class="nav-label text-[11px] font-bold uppercase tracking-wider text-green-400">Connecté</span>';
                } else {
                    btn.innerHTML = '<i data-lucide="unlock" class="w-3.5 h-3.5 text-green-400"></i>';
                }
                btn.title = "Connecté (Admin)";
            } else {
                if (isSidebar) {
                    btn.innerHTML = '<i data-lucide="lock" class="w-4 h-4 shrink-0 transition-transform group-hover:scale-110"></i><span class="nav-label text-[11px] font-bold uppercase tracking-wider">Administration</span>';
                } else {
                    btn.innerHTML = '<i data-lucide="lock" class="w-3.5 h-3.5"></i>';
                }
                btn.title = "Connexion Admin";
            }
        });

        const form = document.getElementById('login-form');
        const logout = document.getElementById('logout-container');
        const title = document.querySelector('#login-modal h3');

        if (form) form.classList.toggle('hidden', isLoggedIn);
        if (logout) logout.classList.toggle('hidden', !isLoggedIn);
        if (title) title.textContent = isLoggedIn ? "Session Active" : "Connexion Administration";

        if (isLoggedIn && window.AdminView) {
            AdminView.loadHistory();
        }

        try { lucide.createIcons(); } catch (e) { }
    },

    updateStatus(isOk) {
        this.elements.statusDot.style.background = isOk ? 'var(--success)' : 'var(--danger)';
        this.elements.statusDot.style.boxShadow = isOk ? '0 0 8px var(--success)' : '0 0 8px var(--danger)';
        this.elements.statusText.textContent = isOk ? 'Live' : 'Hors ligne';
    },

    populateCheckboxes(container, options, selectedValues) {
        if (!container) return;

        const scrollTop = container.scrollTop;
        container.innerHTML = '';

        if (!options || options.length === 0) {
            container.innerHTML = '<div class="text-[10px] text-gray-600 px-2 py-4 text-center italic">Aucune option disponible</div>';
            return;
        }

        const values = Array.isArray(selectedValues) ? selectedValues.map(String) : [];

        options.forEach(opt => {
            const wrapper = document.createElement('label');
            wrapper.className = 'flex items-center gap-2.5 px-3 py-2 rounded-lg hover:bg-white/5 cursor-pointer transition-all group active:scale-[0.98]';

            const isChecked = values.includes(String(opt));

            wrapper.innerHTML = `
                <div class="relative flex items-center justify-center">
                    <input type="checkbox" value="${opt}" ${isChecked ? 'checked' : ''} 
                           class="peer w-4 h-4 rounded border-gray-600 bg-transparent text-accent focus:ring-accent accent-accent transition-all cursor-pointer">
                </div>
                <span class="text-xs text-gray-400 group-hover:text-gray-200 transition-colors truncate">${opt}</span>
            `;

            container.appendChild(wrapper);
        });
        container.scrollTop = scrollTop;
    },

    updateSummary(filters) {
        const LABEL_MAP = {
            annee: 'Année', semestre: 'Sem.', cohorte: 'Cohorte',
            departement: 'Dép.', filiere: 'Filière', type: 'Type', niveau: 'Grade'
        };

        const active = Object.entries(filters)
            .filter(([k, v]) => k !== 'ue' && Array.isArray(v) && v.length > 0);

        const btn = this.elements.activeFiltersSummary;
        if (btn) btn.textContent = active.length ? `${active.length} filtre${active.length > 1 ? 's' : ''}` : 'Filtres';

        const noFilter = document.getElementById('header-no-filter');
        const chipsEl = document.getElementById('header-filter-chips');
        if (!chipsEl) return;

        chipsEl.innerHTML = '';
        if (!active.length) {
            if (noFilter) noFilter.classList.remove('hidden');
            return;
        }
        if (noFilter) noFilter.classList.add('hidden');

        active.forEach(([key, values]) => {
            const label = LABEL_MAP[key] || key;
            const val = values.length > 2 ? `${values.slice(0, 2).join(', ')} +${values.length - 2}` : values.join(', ');
            const chip = document.createElement('span');
            chip.className = 'header-chip';
            chip.innerHTML = `<span class="header-chip-label">${label}</span>${val}`;
            chipsEl.appendChild(chip);
        });
    },

    //  UPLOAD MODAL
    _showUploadSummary(result) {
        const zone = document.getElementById('upload-progress');
        if (!zone) { this.toast(`Import : ${result.added} ajoutées, ${result.updated} mises à jour`, 'success', 10000); return; }

        const hasWarnings = result.warnings && result.warnings.length > 0;
        const warnHtml = hasWarnings
            ? `<div class="mt-3 space-y-1 max-h-40 overflow-y-auto">
                ${result.warnings.map(w => `<div class="text-[11px] text-amber-400 flex gap-1.5 items-start"><span class="shrink-0">⚠</span><span>${w}</span></div>`).join('')}
               </div>`
            : '';

        zone.innerHTML = `
            <div class="rounded-lg p-4 ${hasWarnings ? 'bg-amber-500/10 border border-amber-500/20' : 'bg-green-500/10 border border-green-500/20'}">
                <div class="flex items-center gap-2 mb-2">
                    <i data-lucide="${hasWarnings ? 'alert-triangle' : 'check-circle'}"
                       class="w-4 h-4 ${hasWarnings ? 'text-amber-400' : 'text-green-400'} shrink-0"></i>
                    <span class="text-sm font-semibold ${hasWarnings ? 'text-amber-300' : 'text-green-300'}">
                        Import terminé
                    </span>
                </div>
                <div class="grid grid-cols-3 gap-2 text-center mb-2">
                    <div class="bg-white/5 rounded p-2">
                        <div class="text-lg font-bold text-green-400">${result.added}</div>
                        <div class="text-[10px] text-gray-500">Ajoutées</div>
                    </div>
                    <div class="bg-white/5 rounded p-2">
                        <div class="text-lg font-bold text-accent">${result.updated}</div>
                        <div class="text-[10px] text-gray-500">Mises à jour</div>
                    </div>
                    <div class="bg-white/5 rounded p-2">
                        <div class="text-lg font-bold text-gray-300">${result.total.toLocaleString()}</div>
                        <div class="text-[10px] text-gray-500">Total lignes</div>
                    </div>
                </div>
                ${warnHtml}
            </div>
            <button onclick="ui.toggleUploadModal()"
                class="mt-3 w-full py-2 text-xs font-medium rounded-lg bg-accent/20 border border-accent/30 text-accent hover:bg-accent/30 transition-colors">
                Fermer
            </button>`;
        lucide.createIcons();
    },

    toggleUploadModal() {
        const modal = document.getElementById('upload-modal');
        modal.classList.toggle('hidden');
        if (!modal.classList.contains('hidden')) {
            this._setupUploadHandlers();
            lucide.createIcons();
        }
    },

    _setupUploadHandlers() {
        const dropzone = document.getElementById('upload-dropzone');
        const fileInput = document.getElementById('upload-file-input');

        if (!dropzone || !fileInput || dropzone._handlersSet) return;
        dropzone._handlersSet = true;

        dropzone.onclick = () => fileInput.click();
        fileInput.onchange = (e) => {
            if (e.target.files.length > 0) this._handleUpload(e.target.files[0]);
        };

        dropzone.ondragover = (e) => { e.preventDefault(); dropzone.classList.add('drag-over'); };
        dropzone.ondragleave = () => dropzone.classList.remove('drag-over');
        dropzone.ondrop = (e) => {
            e.preventDefault();
            dropzone.classList.remove('drag-over');
            if (e.dataTransfer.files.length > 0) this._handleUpload(e.dataTransfer.files[0]);
        };
    },

    async _handleUpload(file) {
        const progress = document.getElementById('upload-progress');
        const progressBar = document.getElementById('upload-progress-bar');
        const filename = document.getElementById('upload-filename');
        const statusText = document.getElementById('upload-status-text');
        const resultDiv = document.getElementById('upload-result');

        progress.classList.remove('hidden');
        resultDiv.classList.add('hidden');
        filename.textContent = file.name;
        statusText.textContent = 'Envoi en cours...';
        statusText.className = 'text-xs font-medium text-accent';
        progressBar.style.width = '30%';

        try {
            progressBar.style.width = '70%';
            const result = await api.uploadFile(file);
            progressBar.style.width = '100%';
            progressBar.style.background = 'var(--success)';
            statusText.textContent = 'Terminé !';
            statusText.className = 'text-xs font-medium text-green-400';

            this._showUploadSummary(result);
            app.refresh(true);

        } catch (error) {
            progressBar.style.width = '100%';
            progressBar.style.background = 'var(--danger)';
            statusText.textContent = 'Erreur';
            statusText.className = 'text-xs font-medium text-red-400';

            this.toast(`Erreur d'import : ${error.message}`, 'error', 8000);
        }
    },

    showOfflineBanner(show) {
        let banner = document.getElementById('offline-banner');
        if (!banner) {
            banner = document.createElement('div');
            banner.id = 'offline-banner';
            banner.className = 'offline-banner';
            banner.innerHTML = '<i data-lucide="wifi-off" class="w-4 h-4"></i> Backend indisponible — les filtres ne sont pas pris en compte';
            document.body.prepend(banner);
            lucide.createIcons();
        }
        banner.classList.toggle('hidden', !show);
    },

    toast(message, type = 'info', duration = 4000) {
        let container = document.getElementById('toast-container');
        if (!container) {
            container = document.createElement('div');
            container.id = 'toast-container';
            container.className = 'toast-container';
            document.body.appendChild(container);
        }
        const ICONS = { success: 'check-circle', error: 'x-circle', warning: 'alert-triangle', info: 'info' };
        const t = document.createElement('div');
        t.className = `toast toast-${type}`;
        t.innerHTML = `<i data-lucide="${ICONS[type] || 'info'}" class="w-4 h-4 shrink-0"></i>
                       <span class="text-xs">${message}</span>
                       <button onclick="this.parentElement.remove()" class="ml-auto text-current opacity-50 hover:opacity-100">
                           <i data-lucide="x" class="w-3 h-3"></i>
                       </button>`;
        container.appendChild(t);
        lucide.createIcons();
        requestAnimationFrame(() => t.classList.add('toast-visible'));
        setTimeout(() => {
            t.classList.remove('toast-visible');
            setTimeout(() => t.remove(), 300);
        }, duration);
    },

    showSkeletons() {
        Object.values(this.elements.kpis).forEach(el => {
            if (el) el.innerHTML = '<div class="skeleton skeleton-kpi"></div>';
        });
        ['top10-tbody', 'bottom10-tbody', 'students-tbody'].forEach(id => {
            const tbody = document.getElementById(id);
            if (!tbody) return;
            tbody.innerHTML = Array.from({ length: 5 }, () =>
                `<tr>${Array.from({ length: 5 }, () =>
                    '<td class="px-3 py-3"><div class="skeleton skeleton-line"></div></td>'
                ).join('')}</tr>`
            ).join('');
        });
        const grid = document.getElementById('modules-grid');
        if (grid) {
            grid.innerHTML = Array.from({ length: 6 }, () =>
                `<div class="glass-card p-4 space-y-3">
                    <div class="skeleton skeleton-line w-1/2"></div>
                    <div class="skeleton skeleton-line w-3/4"></div>
                    <div class="skeleton skeleton-line w-full" style="height:8px"></div>
                </div>`
            ).join('');
        }
    }
};

document.documentElement.setAttribute('data-theme', 'dark');

window.ui = ui;
