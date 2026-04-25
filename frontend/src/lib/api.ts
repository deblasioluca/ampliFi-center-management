/**
 * Centralised API client for the ampliFi cleanup backend.
 * All JSON keys use camelCase per spec convention.
 */

const API_BASE = import.meta.env.PUBLIC_API_URL || '/api';

interface RequestOptions {
  method?: string;
  body?: unknown;
  headers?: Record<string, string>;
}

interface PaginatedResponse<T> {
  total: number;
  page: number;
  size: number;
  items: T[];
}

async function request<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const { method = 'GET', body, headers = {} } = opts;
  const url = `${API_BASE}${path}`;

  const token = typeof localStorage !== 'undefined'
    ? localStorage.getItem('access_token')
    : null;

  const finalHeaders: Record<string, string> = {
    ...headers,
    'X-Requested-With': 'XMLHttpRequest',
  };
  if (token) {
    finalHeaders['Authorization'] = `Bearer ${token}`;
  }
  if (body && !finalHeaders['Content-Type']) {
    finalHeaders['Content-Type'] = 'application/json';
  }

  const resp = await fetch(url, {
    method,
    headers: finalHeaders,
    body: body ? JSON.stringify(body) : undefined,
    credentials: 'include',
  });

  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(`API ${method} ${path} failed (${resp.status}): ${detail}`);
  }

  return resp.json() as Promise<T>;
}

export const api = {
  // Auth
  login: (email: string, password: string) =>
    request<{ access_token: string }>('/auth/login', {
      method: 'POST', body: { email, password },
    }),
  me: () => request<{ id?: number; email?: string; role?: string; authenticated?: boolean }>('/auth/me'),
  logout: () => request('/auth/logout', { method: 'POST' }),

  // Health
  healthz: () => request<{ status: string }>('/healthz'),
  readyz: () => request<{ status: string; checks: Record<string, string> }>('/readyz'),

  // Stats
  globalStats: () => request('/stats/global'),

  // Waves
  listWaves: (page = 1, size = 100) =>
    request<PaginatedResponse<unknown>>(`/waves?page=${page}&size=${size}`),
  createWave: (data: unknown) =>
    request('/waves', { method: 'POST', body: data }),
  getWave: (id: number) => request(`/waves/${id}`),

  // SAP
  listSAPConnections: () => request<unknown[]>('/admin/sap'),
  createSAPConnection: (data: unknown) =>
    request('/admin/sap', { method: 'POST', body: data }),
  testSAPConnection: (id: number) =>
    request(`/admin/sap/${id}/test`, { method: 'POST' }),

  // Users
  listUsers: (page = 1) => request(`/admin/users?page=${page}`),

  // Uploads
  listUploads: () => request('/admin/uploads'),

  // Configs
  listConfigs: () => request('/configs'),

  // Entities
  listEntities: (page = 1) => request(`/entities?page=${page}`),
};
