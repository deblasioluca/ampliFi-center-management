/**
 * Centralised API client for the ampliFi cleanup backend.
 */

const API_BASE = import.meta.env.PUBLIC_API_URL || '/api';

function getToken(): string | null {
  if (typeof window === 'undefined') return null;
  return localStorage.getItem('amplifi_token');
}

function authHeaders(): Record<string, string> {
  const token = getToken();
  if (!token) return {};
  return { Authorization: `Bearer ${token}` };
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  if (typeof window === 'undefined') return {} as T;

  const headers: Record<string, string> = {
    ...authHeaders(),
    ...(options.headers as Record<string, string> || {}),
  };

  if (!(options.body instanceof FormData)) {
    headers['Content-Type'] = 'application/json';
  }

  const url = `${API_BASE}${path}`;
  let res: Response;

  try {
    res = await fetch(url, { ...options, headers, credentials: 'include' });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    throw new Error(`Network error: ${url} — ${msg}`);
  }

  if (res.status === 401) {
    // Try to refresh the token before giving up
    try {
      const refreshRes = await fetch(`${API_BASE}/auth/refresh`, {
        method: 'POST',
        credentials: 'include',
      });
      if (refreshRes.ok) {
        const data = await refreshRes.json();
        if (data.access_token && typeof window !== 'undefined') {
          localStorage.setItem('amplifi_token', data.access_token);
          // Retry the original request with the new token
          const retryHeaders: Record<string, string> = {
            ...headers,
            Authorization: `Bearer ${data.access_token}`,
          };
          const retryRes = await fetch(url, {
            ...options,
            headers: retryHeaders,
            credentials: 'include',
          });
          if (retryRes.ok) return retryRes.json();
        }
      }
    } catch { /* refresh failed, fall through to login redirect */ }
    if (typeof window !== 'undefined') {
      localStorage.removeItem('amplifi_token');
      localStorage.removeItem('amplifi_user');
      window.location.href = '/login';
    }
    throw new Error('Unauthorized');
  }

  if (!res.ok) {
    let detail = '';
    try {
      const body = await res.json();
      const d = body.detail;
      detail = typeof d === 'string' ? d
        : Array.isArray(d) ? d.map((e: Record<string, unknown>) => e.msg || JSON.stringify(e)).join('; ')
        : d ? JSON.stringify(d) : '';
    } catch { /* ignore */ }
    throw new Error(detail || `HTTP ${res.status}`);
  }

  return res.json();
}

// Auth
export const auth = {
  login: (username: string, password: string) =>
    request<{ access_token: string }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    }),
  me: () => request<{ id: number; email: string; display_name: string; role: string }>('/auth/me'),
  logout: () => request('/auth/logout', { method: 'POST' }),
};

// Health
export const health = {
  healthz: () => request<{ status: string }>('/healthz'),
  readyz: () => request<{ status: string; checks: Record<string, string> }>('/readyz'),
};

// Stats
export const stats = {
  global: () => request<Record<string, unknown>>('/stats/global'),
  wave: (waveId: number) => request<Record<string, unknown>>(`/stats/wave/${waveId}`),
};

// Waves
export const waves = {
  list: (page = 1, size = 100) =>
    request<{ total: number; page: number; size: number; items: unknown[] }>(`/waves?page=${page}&size=${size}`),
  create: (data: unknown) =>
    request('/waves', { method: 'POST', body: JSON.stringify(data) }),
  get: (id: number) => request(`/waves/${id}`),
  cancel: (id: number) => request(`/waves/${id}/cancel`, { method: 'POST' }),
  progress: (id: number) => request(`/waves/${id}/progress`),
};

// Admin - Users
export const adminUsers = {
  list: (page = 1, size = 100) =>
    request<{ total: number; page: number; size: number; items: unknown[] }>(`/admin/users?page=${page}&size=${size}`),
  create: (data: { email: string; display_name: string; password: string; role?: string }) =>
    request('/admin/users', { method: 'POST', body: JSON.stringify(data) }),
  get: (id: number) => request(`/admin/users/${id}`),
  update: (id: number, data: { display_name?: string; role?: string; is_active?: boolean }) =>
    request(`/admin/users/${id}`, { method: 'PATCH', body: JSON.stringify(data) }),
  delete: (id: number) => request(`/admin/users/${id}`, { method: 'DELETE' }),
};

// Admin - SAP Connections
export const adminSAP = {
  list: () => request<unknown[]>('/admin/sap'),
  create: (data: unknown) => request('/admin/sap', { method: 'POST', body: JSON.stringify(data) }),
  get: (id: number) => request(`/admin/sap/${id}`),
  test: (id: number) => request(`/admin/sap/${id}/test`, { method: 'POST' }),
};

// Admin - Uploads
export const adminUploads = {
  list: (page = 1, size = 100) =>
    request<{ total: number; page: number; size: number; items: unknown[] }>(`/admin/uploads?page=${page}&size=${size}`),
  upload: (kind: string, formData: FormData) =>
    request('/admin/uploads?kind=' + encodeURIComponent(kind), { method: 'POST', body: formData }),
};

// Admin - Config
export const adminConfig = {
  get: (key: string) => request<{ key: string; value: Record<string, unknown> }>(`/admin/config/${key}`),
  set: (key: string, value: Record<string, unknown>) =>
    request(`/admin/config/${key}`, { method: 'PUT', body: JSON.stringify({ value }) }),
};

// Admin - Routines
export const adminRoutines = {
  list: () => request<unknown[]>('/admin/routines'),
  update: (code: string, data: { enabled: boolean }) =>
    request(`/admin/routines/${code}?enabled=${data.enabled}`, { method: 'PATCH' }),
};

// Admin - Audit
export const adminAudit = {
  list: (page = 1, size = 50) =>
    request<{ total: number; page: number; size: number; items: unknown[] }>(`/admin/audit?page=${page}&size=${size}`),
};

// Admin - Jobs
export const adminJobs = {
  list: (page = 1) =>
    request<{ total: number; page: number; size: number; items: unknown[] }>(`/admin/jobs?page=${page}`),
};

// Admin - Sample Data
export const adminSampleData = {
  generate: () => request('/admin/sample-data', { method: 'POST' }),
  delete: () => request('/admin/sample-data', { method: 'DELETE' }),
  status: () => request<Record<string, unknown>>('/admin/sample-data'),
};

// Data Management
export const dataManagement = {
  counts: () => request<Record<string, number>>('/data/counts'),
  purgeAll: () => request('/data/purge-all', { method: 'DELETE' }),
  deleteEntities: (ids: number[]) =>
    request('/data/entities', { method: 'DELETE', body: JSON.stringify({ ids }) }),
  deleteAllEntities: () => request('/data/entities/all', { method: 'DELETE' }),
  deleteCostCenters: (ids: number[]) =>
    request('/data/legacy/cost-centers', { method: 'DELETE', body: JSON.stringify({ ids }) }),
  deleteAllCostCenters: () => request('/data/legacy/cost-centers/all', { method: 'DELETE' }),
  deleteProfitCenters: (ids: number[]) =>
    request('/data/legacy/profit-centers', { method: 'DELETE', body: JSON.stringify({ ids }) }),
  deleteAllProfitCenters: () => request('/data/legacy/profit-centers/all', { method: 'DELETE' }),
  deleteBalances: (filters: Record<string, unknown>) =>
    request('/data/balances', { method: 'DELETE', body: JSON.stringify(filters) }),
  deleteAllBalances: () => request('/data/balances/all', { method: 'DELETE' }),
};

// Reference Data
export const reference = {
  entities: (page = 1, size = 100) =>
    request<{ total: number; page: number; size: number; items: unknown[] }>(`/entities?page=${page}&size=${size}`),
  costCenters: (page = 1, size = 100, ccode?: string) => {
    let url = `/legacy/cost-centers?page=${page}&size=${size}`;
    if (ccode) url += `&ccode=${encodeURIComponent(ccode)}`;
    return request<{ total: number; page: number; size: number; items: unknown[] }>(url);
  },
  profitCenters: (page = 1, size = 100) =>
    request<{ total: number; page: number; size: number; items: unknown[] }>(`/legacy/profit-centers?page=${page}&size=${size}`),
  hierarchies: (page = 1, size = 100) =>
    request<{ total: number; page: number; size: number; items: unknown[] }>(`/legacy/hierarchies?page=${page}&size=${size}`),
};

// Configs (analysis routines)
export const configs = {
  list: () => request<unknown[]>('/configs'),
  get: (code: string) => request(`/configs/${code}`),
};

// Review (end-user)
export const review = {
  getScope: (token: string) => request(`/review/${token}`),
  getItems: (token: string, page = 1, size = 50) =>
    request<{ total: number; page: number; size: number; items: unknown[] }>(`/review/${token}/items?page=${page}&size=${size}`),
  decide: (token: string, itemId: number, data: { decision: string; comment?: string }) =>
    request(`/review/${token}/items/${itemId}/decide`, { method: 'POST', body: JSON.stringify(data) }),
  bulkDecide: (token: string, data: { item_ids: number[]; decision: string; comment?: string }) =>
    request(`/review/${token}/items/bulk-decide`, { method: 'POST', body: JSON.stringify(data) }),
  complete: (token: string) =>
    request(`/review/${token}/complete`, { method: 'POST' }),
};
