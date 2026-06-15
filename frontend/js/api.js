/* ============================================================
   api.js — Couche de communication avec le backend FastAPI
   ============================================================ */

const API_BASE_URL = (['localhost', '127.0.0.1'].includes(window.location.hostname) || window.location.protocol === 'file:')
    ? 'http://localhost:8000/api'
    : '/api';

const api = {

    /* Contrôleurs d'annulation — une requête en cours par type.*/
    _controllers: {},
    _token: sessionStorage.getItem('admin_token') || null,

    setToken(t) {
        this._token = t;
        if (t) sessionStorage.setItem('admin_token', t);
        else sessionStorage.removeItem('admin_token');
    },

    async _fetch(url, options = {}) {
        const headers = { ...options.headers };
        if (this._token) {
            headers['Authorization'] = `Bearer ${this._token}`;
        }
        return fetch(url, { ...options, headers });
    },

    _abort(key) {
        if (this._controllers[key]) {
            this._controllers[key].abort();
        }
        this._controllers[key] = new AbortController();
        return this._controllers[key].signal;
    },

    /** Annule toutes les requêtes API en cours */
    abortAll() {
        Object.keys(this._controllers).forEach(k => {
            if (this._controllers[k]) this._controllers[k].abort();
        });
        this._controllers = {};
    },

    async getHealth() {
        try {
            const res = await this._fetch(`${API_BASE_URL}/health`);
            if (!res.ok) throw new Error('API non disponible');
            return await res.json();
        } catch (e) {
            console.error('getHealth:', e);
            return { status: 'error' };
        }
    },

    async login(username, password) {
        try {
            const fd = new URLSearchParams();
            fd.append('username', username);
            fd.append('password', password);
            const res = await fetch(`${API_BASE_URL}/auth/token`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: fd
            });
            if (!res.ok) throw new Error('Identifiants incorrects');
            const data = await res.json();
            this.setToken(data.access_token);
            return data;
        } catch (e) {
            console.error('login:', e);
            throw e;
        }
    },

    async getDisponibilites(filters = {}) {
        const signal = this._abort('disponibilites');
        const q = new URLSearchParams(this._cleanFilters(filters)).toString();
        const url = `${API_BASE_URL}/meta/disponibilites${q ? '?' + q : ''}`;
        try {
            const res = await this._fetch(url, { signal });
            if (!res.ok) throw new Error('Erreur dispo');
            return await res.json();
        } catch (e) {
            if (e.name === 'AbortError') return null;
            console.error(e); return null;
        }
    },

    async getUEList() {
        const signal = this._abort('ue_list');
        try {
            const res = await this._fetch(`${API_BASE_URL}/meta/ues?limit=5000`, { signal });
            if (!res.ok) throw new Error('Erreur liste UE');
            const j = await res.json();
            return j.ues || [];
        } catch (e) {
            if (e.name === 'AbortError') return [];
            console.error(e); return [];
        }
    },

    async getUEStats(code, filters = {}) {
        const signal = this._abort('ue');
        try {
            const clean = this._cleanFilters(filters);
            delete clean.ue;
            const q = new URLSearchParams(clean).toString();
            const url = `${API_BASE_URL}/ues/${encodeURIComponent(code)}/stats${q ? '?' + q : ''}`;
            const res = await this._fetch(url, { signal });
            if (!res.ok) return null;
            return await res.json();
        } catch (e) {
            if (e.name === 'AbortError') return null;
            console.error(e); return null;
        }
    },

    async getDashboardAggregates(filters = {}, abortKey = 'dashboard') {
        const signal = this._abort(abortKey);
        const q = new URLSearchParams(this._cleanFilters(filters)).toString();
        const url = `${API_BASE_URL}/dashboard/aggregates${q ? '?' + q : ''}`;
        try {
            const res = await this._fetch(url, { signal });
            if (!res.ok) throw new Error('Erreur agrégats');
            return await res.json();
        } catch (e) {
            if (e.name === 'AbortError') return null;
            console.error(e); return null;
        }
    },

    async getUploadHistory() {
        const url = `${API_BASE_URL}/admin/upload-history`;
        try {
            const res = await this._fetch(url);
            if (!res.ok) throw new Error('Erreur historique');
            return await res.json();
        } catch (e) {
            console.error(e);
            return [];
        }
    },

    async rollbackData() {
        const url = `${API_BASE_URL}/admin/rollback`;
        try {
            const res = await this._fetch(url, { method: 'POST' });
            if (!res.ok) {
                const err = await res.json();
                throw new Error(err.detail || 'Erreur rollback');
            }
            return await res.json();
        } catch (e) {
            console.error(e);
            throw e;
        }
    },

    async getParcours(id, filters = {}) {
        const signal = this._abort('parcours');
        try {
            const clean = this._cleanFilters(filters);
            const q = new URLSearchParams(clean).toString();
            const url = `${API_BASE_URL}/etudiants/${encodeURIComponent(id)}/parcours${q ? '?' + q : ''}`;
            const res = await this._fetch(url, { signal }); // Bug F2 fix — envoyer le token JWT
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                throw new Error(err.detail || `HTTP ${res.status}`);
            }
            return await res.json();
        } catch (e) {
            if (e.name === 'AbortError') return null;
            throw e;
        }
    },

    getFigureUrl(view, filters = {}, fmt = 'png') {
        const clean = this._cleanFilters(filters);
        clean.view = view;
        clean.fmt = fmt;
        const bust = window.app?._cacheBust;
        if (bust) clean._v = bust;
        if (this._token) clean._t = this._token;
        return `${API_BASE_URL}/figures?${new URLSearchParams(clean)}`;
    },

    async uploadFile(file) {
        const fd = new FormData();
        fd.append('file', file);
        try {
            const res = await this._fetch(`${API_BASE_URL}/data/upload`, { method: 'POST', body: fd });
            const data = await res.json();
            if (!res.ok) throw new Error(data.detail || 'Erreur upload');
            return data;
        } catch (e) { console.error(e); throw e; }
    },

    getReportPdfUrl(filters = {}) {
        const clean = this._cleanFilters(filters);
        /* Passer _context et _label en plus des filtres standards */
        if (filters._context) clean._context = filters._context;
        if (filters._label) clean._label = filters._label;
        if (filters.anonymat) clean.anonymat = filters.anonymat;
        if (this._token) clean._t = this._token;
        const q = new URLSearchParams(clean).toString();
        return `${API_BASE_URL}/reports/pdf${q ? '?' + q : ''}`;
    },

    async getCompare(type, entites, filters = {}) {
        const signal = this._abort('compare');
        const params = new URLSearchParams();
        params.set('type', type);
        if (entites.length) params.set('entites', entites.join(','));
        if (filters.annee) params.set('annee', Array.isArray(filters.annee) ? filters.annee.join(',') : filters.annee);
        if (filters.cohorte) params.set('cohorte', Array.isArray(filters.cohorte) ? filters.cohorte.join(',') : filters.cohorte);
        if (filters.semestre) params.set('semestre', Array.isArray(filters.semestre) ? filters.semestre.join(',') : filters.semestre);
        const res = await this._fetch(`${API_BASE_URL}/compare?${params}`, { signal });
        if (!res.ok) throw new Error(`compare error ${res.status}`);
        return res.json();
    },

    _cleanFilters(filters) {
        const clean = {};
        const ALLOWED = new Set([
            'annee', 'semestre', 'cohorte', 'sexe', 'ue', 'filiere',
            'departement', 'type_formation', 'niveau', 'type'
        ]);
        for (const [k, v] of Object.entries(filters)) {
            if (!ALLOWED.has(k)) continue;
            const key = k === 'type' ? 'type_formation' : k;
            const has = Array.isArray(v) ? v.length > 0
                : (v !== '' && v !== null && v !== undefined);
            if (!has) continue;
            const raw = Array.isArray(v) ? v.join(',') : String(v);
            const safe = raw.replace(/[^a-zA-Z0-9,\s\-_\.éèêàùûîôç]/g, '').replace(/[\r\n\t]/g, '').slice(0, 500);
            if (safe) clean[key] = safe;
        }
        return clean;
    }
};

window.api = api;