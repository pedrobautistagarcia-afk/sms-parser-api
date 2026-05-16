// Expense Tracker — Service Worker
const CACHE = "expense-tracker-v1";

// Assets to cache on install (app shell)
const SHELL = [
  "/",
  "/index.html",
  "https://fonts.googleapis.com/css2?family=Inter:ital,opsz,wght@0,14..32,300;0,14..32,400;0,14..32,500;0,14..32,600;0,14..32,700;1,14..32,400&family=JetBrains+Mono:wght@400;500;600&display=swap"
];

// ── Install: cache the app shell ──────────────────────────────────
self.addEventListener("install", e => {
  e.waitUntil(
    caches.open(CACHE).then(cache => {
      // Cache shell assets, ignore failures for external resources
      return Promise.allSettled(SHELL.map(url => cache.add(url).catch(() => {})));
    }).then(() => self.skipWaiting())
  );
});

// ── Activate: clean old caches ────────────────────────────────────
self.addEventListener("activate", e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

// ── Fetch: network-first for API, cache-first for assets ──────────
self.addEventListener("fetch", e => {
  const url = new URL(e.request.url);

  // Always go network for API calls
  if (url.hostname === "sms-parser-api.onrender.com") {
    e.respondWith(
      fetch(e.request)
        .catch(() => new Response(
          JSON.stringify({ error: "Sin conexión", expenses: [], rules: [] }),
          { headers: { "Content-Type": "application/json" } }
        ))
    );
    return;
  }

  // For FX rates and external APIs — network with cache fallback
  if (url.hostname.includes("fawazahmed0") || url.hostname.includes("frankfurter")) {
    e.respondWith(
      fetch(e.request)
        .then(res => {
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
          return res;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }

  // For everything else — cache first, then network
  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;
      return fetch(e.request).then(res => {
        // Cache successful GET responses
        if (e.request.method === "GET" && res.status === 200) {
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
        }
        return res;
      });
    })
  );
});

// ── Background sync placeholder ───────────────────────────────────
self.addEventListener("message", e => {
  if (e.data === "skipWaiting") self.skipWaiting();
});
