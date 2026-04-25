import type { MiddlewareHandler } from 'astro';

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:8180';

export const onRequest: MiddlewareHandler = async (context, next) => {
  const url = new URL(context.request.url);

  if (url.pathname.startsWith('/api/') || url.pathname === '/api') {
    const backendUrl = BACKEND_URL + url.pathname + url.search;
    const headers = new Headers(context.request.headers);
    headers.delete('host');

    const init: RequestInit = {
      method: context.request.method,
      headers,
      redirect: 'manual',
    };

    if (
      context.request.method !== 'GET' &&
      context.request.method !== 'HEAD'
    ) {
      init.body = context.request.body;
      // @ts-expect-error Node fetch supports duplex
      init.duplex = 'half';
    }

    const response = await fetch(backendUrl, init);

    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  }

  return next();
};
