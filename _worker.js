export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === '/') {
      return new Response('Node is running', { status: 200 });
    }
    return new Response('Not found', { status: 404 });
  }
};
