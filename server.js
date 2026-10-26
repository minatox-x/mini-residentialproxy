// Lightweight rotating proxy gateway. Node 20+, one dependency (undici).
// Scrape -> test (CONNECT-capable) -> classify (residential first) -> TTL pool -> auto refresh.
import http from 'node:http';
import net from 'node:net';
import { Readable } from 'node:stream';
import { fetch, ProxyAgent } from 'undici';
import { WebSocketServer } from 'ws';

const PORT = process.env.PORT || 8000;
const KEY = process.env.ACCESS_KEY || '';                 // optional; empty = open
const TTL_MS = (+process.env.TTL_MIN || 10) * 60_000;     // assumed life of a proxy
const WANT = +process.env.WANT || 30;                     // working proxies to collect per refresh
const CONCURRENCY = 300;
const CHECK_TIMEOUT = 3500;

const SOURCES = [
  'https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=IN&ssl=all&anonymity=all',
  'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt',
  'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
  'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-india.txt',
];
const PROXY_RE = /\b(?:\d{1,3}\.){3}\d{1,3}:\d{2,5}\b/g;

let pool = [];          // { host, port, url, at, fails }
let mode = 'none';      // 'residential' | 'datacenter'
let refreshing = null;
let rr = 0;
const agents = new Map();

/* ---------- pool building ---------- */

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = (Math.random() * (i + 1)) | 0;
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

async function scrape() {
  const set = new Set();
  await Promise.all(SOURCES.map(async (u) => {
    try {
      const r = await fetch(u, { signal: AbortSignal.timeout(6000), headers: { 'user-agent': 'Mozilla/5.0' } });
      if (r.ok) (await r.text()).match(PROXY_RE)?.forEach((x) => set.add(x));
    } catch {}
  }));
  return shuffle([...set]);
}

// A proxy is "working" if it answers CONNECT with 200 (needed for HTTPS).
const check = (addr) => new Promise((resolve) => {
  const [host, port] = addr.split(':');
  const s = net.connect(+port, host);
  let done = false;
  const end = (ok) => { if (!done) { done = true; s.destroy(); resolve(ok); } };
  s.setTimeout(CHECK_TIMEOUT, () => end(false));
  s.on('error', () => end(false));
  s.on('connect', () => s.write('CONNECT www.google.com:443 HTTP/1.1\r\nHost: www.google.com:443\r\n\r\n'));
  s.on('data', (d) => end(/^HTTP\/1\.[01] 200/.test(d.toString('latin1', 0, 20))));
});

async function findWorking(cands) {
  const ok = [];
  let i = 0;
  const worker = async () => {
    while (ok.length < WANT && i < cands.length) {
      const a = cands[i++];
      if (await check(a)) ok.push(a);
    }
  };
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  return ok;
}

// ip-api batch (free, HTTP only, 100 IPs per call). residential = not hosting and not known proxy.
async function classify(addrs) {
  const ips = addrs.map((a) => a.split(':')[0]);
  const out = {};
  for (let i = 0; i < ips.length; i += 100) {
    try {
      const r = await fetch('http://ip-api.com/batch?fields=query,status,proxy,hosting', {
        method: 'POST',
        body: JSON.stringify(ips.slice(i, i + 100)),
        signal: AbortSignal.timeout(5000),
      });
      for (const x of await r.json()) out[x.query] = x.status === 'success' && !x.hosting && !x.proxy;
    } catch {}
  }
  return out;
}

function refresh() {
  return (refreshing ??= (async () => {
    try {
      const ok = await findWorking(await scrape());
      if (!ok.length) return console.log('refresh: no working proxies, keeping old pool');
      const kinds = await classify(ok);
      const resi = ok.filter((a) => kinds[a.split(':')[0]]);
      const use = resi.length ? resi : ok;               // fallback: datacenter
      mode = resi.length ? 'residential' : 'datacenter';
      pool = use.map((a) => {
        const [host, port] = a.split(':');
        return { host, port: +port, url: `http://${a}`, at: Date.now(), fails: 0 };
      });
      console.log(`refresh: ${pool.length} ${mode} proxies`);
    } catch (e) {
      console.error('refresh failed:', e.message);
    } finally {
      refreshing = null;
    }
  })());
}

/* ---------- rotation ---------- */

const alive = () => pool.filter((p) => Date.now() - p.at < TTL_MS && p.fails < 2);

function pick(skip = []) {
  const a = alive();
  if (a.length <= 1) refresh();                          // refetch before the last one dies
  const c = a.filter((p) => !skip.includes(p));
  return c.length ? c[rr++ % c.length] : null;
}

const getAgent = (p) =>
  agents.get(p.url) ?? agents.set(p.url, new ProxyAgent({ uri: p.url, connectTimeout: 5000 })).get(p.url);

setInterval(() => {
  if (alive().length <= 1) refresh();
  const live = new Set(pool.map((p) => p.url));
  for (const [u, ag] of agents) if (!live.has(u)) { ag.close().catch(() => {}); agents.delete(u); }
}, 20_000);

/* ---------- helpers ---------- */

const json = (res, code, obj) => {
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
};

const proxyAuthOk = (req) =>
  !KEY ||
  Buffer.from((req.headers['proxy-authorization'] || '').split(' ')[1] || '', 'base64')
    .toString().split(':')[1] === KEY;

/* ---------- mode 1: /fetch?url=... (works on Render) ---------- */

async function handleFetch(target, res) {
  let u;
  try {
    u = new URL(target);
    if (!/^https?:$/.test(u.protocol)) throw 0;
  } catch {
    return json(res, 400, { error: 'bad url' });
  }
  if (!alive().length) await refresh();
  const tried = [];
  for (let n = 0; n < 3; n++) {
    const p = pick(tried);
    if (!p) break;
    tried.push(p);
    try {
      const r = await fetch(u, {
        dispatcher: getAgent(p),
        signal: AbortSignal.timeout(10_000),
        headers: { 'user-agent': 'Mozilla/5.0' },
      });
      res.writeHead(r.status, {
        'content-type': r.headers.get('content-type') || 'application/octet-stream',
        'x-proxy-used': p.host,
        'x-proxy-mode': mode,
      });
      return r.body ? Readable.fromWeb(r.body).pipe(res) : res.end();
    } catch {
      p.fails++;
    }
  }
  json(res, 502, { error: 'all upstream attempts failed' });
}

/* ---------- mode 2: real forward proxy (needs a VPS / raw TCP, not Render) ---------- */

function forward(req, res, tried = []) {
  if (!proxyAuthOk(req)) {
    res.writeHead(407, { 'proxy-authenticate': 'Basic realm="proxy"' });
    return res.end();
  }
  const p = pick(tried);
  if (!p) return json(res, 503, { error: 'no upstream proxies' });
  const up = http.request(
    { host: p.host, port: p.port, method: req.method, path: req.url, headers: req.headers, timeout: 8000 },
    (r) => { res.writeHead(r.statusCode, r.headers); r.pipe(res); }
  );
  const retry = () => {
    p.fails++;
    if (!res.headersSent && ['GET', 'HEAD'].includes(req.method) && tried.length < 2) forward(req, res, [...tried, p]);
    else if (!res.headersSent) json(res, 502, { error: 'upstream failed' });
  };
  up.on('timeout', () => up.destroy());
  up.on('error', retry);
  req.pipe(up);
}

// Opens a CONNECT tunnel to `target` through a pool proxy (retries up to 3 proxies).
function openUpstream(target, cb, tried = []) {
  const p = pick(tried);
  if (!p) return cb(new Error('no upstream proxies'));
  const up = net.connect(p.port, p.host);
  let done = false;
  const fail = () => {
    if (done) return;
    done = true;
    p.fails++;
    up.destroy();
    if (tried.length < 2) openUpstream(target, cb, [...tried, p]);
    else cb(new Error('upstream failed'));
  };
  up.setTimeout(8000, fail);
  up.on('error', fail);
  up.once('connect', () => up.write(`CONNECT ${target} HTTP/1.1\r\nHost: ${target}\r\n\r\n`));
  up.once('data', (d) => {
    if (done) return;
    if (!/^HTTP\/1\.[01] 200/.test(d.toString('latin1', 0, 20))) return fail();
    done = true;
    up.setTimeout(0);
    up.removeListener('error', fail);
    cb(null, up, p);
  });
}

function tunnel(target, client, head) {
  openUpstream(target, (err, up) => {
    if (err) return client.end('HTTP/1.1 502 Bad Gateway\r\n\r\n');
    client.write('HTTP/1.1 200 Connection Established\r\n\r\n');
    if (head?.length) up.write(head);
    up.pipe(client);
    client.pipe(up);
    up.on('error', () => client.destroy());
    client.on('error', () => up.destroy());
  });
}

// WebSocket tunnel: lets a local bridge (local-proxy.js) act as a real proxy through Render.
const wss = new WebSocketServer({ noServer: true });
function onUpgrade(req, socket, head) {
  const u = new URL(req.url, 'http://x');
  const target = u.searchParams.get('target') || '';
  if (u.pathname !== '/tunnel' || (KEY && u.searchParams.get('key') !== KEY) || !/^[\w.-]+:\d{1,5}$/.test(target))
    return socket.destroy();
  wss.handleUpgrade(req, socket, head, (ws) => {
    let up = null;
    const q = [];
    ws.on('message', (m) => (up ? up.write(m) : q.push(m)));
    ws.on('close', () => up?.destroy());
    ws.on('error', () => up?.destroy());
    openUpstream(target, (err, s) => {
      if (err) return ws.close(1011, 'upstream failed');
      up = s;
      q.forEach((m) => up.write(m));
      ws.send('ok');
      up.on('data', (d) => ws.send(d));
      up.on('close', () => ws.close());
      up.on('error', () => ws.close());
    });
  });
}

/* ---------- server ---------- */

const server = http.createServer((req, res) => {
  if (/^https?:\/\//i.test(req.url)) return forward(req, res);   // proxy-style request
  const u = new URL(req.url, 'http://x');

  if (u.pathname === '/health') return json(res, 200, { ok: true, mode, alive: alive().length });

  if (KEY && u.searchParams.get('key') !== KEY && req.headers['x-key'] !== KEY)
    return json(res, 401, { error: 'bad key' });

  if (u.pathname === '/fetch') return handleFetch(u.searchParams.get('url'), res);
  if (u.pathname === '/proxies')
    return json(res, 200, { mode, count: alive().length, proxies: alive().map((p) => p.url) });

  json(res, 200, { usage: '/fetch?url=https://example.com', also: ['/proxies', '/health'], mode, alive: alive().length });
});

server.on('upgrade', onUpgrade);
server.on('connect', (req, client, head) => {
  if (!proxyAuthOk(req)) return client.end('HTTP/1.1 407 Proxy Authentication Required\r\n\r\n');
  client.on('error', () => {});
  tunnel(req.url, client, head);
});

server.keepAliveTimeout = 65_000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`listening on :${PORT}`);
  refresh();
});
