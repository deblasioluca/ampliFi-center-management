import type { APIRoute } from 'astro';

const BACKEND_URL = import.meta.env.BACKEND_URL || 'http://127.0.0.1:8180';

export const ALL: APIRoute = async ({ request, params }) => {
  const path = params.path ?? '';
  const url = new URL(request.url);
  const backendUrl = `${BACKEND_URL}/api/${path}${url.search}`;

  const headers = new Headers(request.headers);
  headers.delete('host');

  const init: RequestInit = {
    method: request.method,
    headers,
    redirect: 'manual',
    signal: AbortSignal.timeout(300_000), // 5 min timeout for large uploads
  };

  if (request.method !== 'GET' && request.method !== 'HEAD') {
    init.body = request.body;
    // @ts-expect-error Node fetch supports duplex
    init.duplex = 'half';
  }

  const response = await fetch(backendUrl, init);

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
};

export const prerender = false;
