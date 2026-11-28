'use strict';
/* SKINSENTIALS22 backend: zero npm dependencies. Requires Node 22.5+ (built-in SQLite). */
const http = require('node:http'), fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto');
const { DatabaseSync } = require('node:sqlite');
const { pipeline } = require('node:stream/promises');
const { Transform } = require('node:stream');

/* ---------- config ---------- */
const ROOT = __dirname;
try { for (const l of fs.readFileSync(path.join(ROOT, '.env'), 'utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/); if (m && !(m[1] in process.env)) process.env[m[1]] = m[2].replace(/^["']|["']$/g, ''); } } catch {}
const ENV = process.env, PORT = +ENV.PORT || 3000, DATA = ENV.DATA_DIR || path.join(ROOT, 'data'), PUB = path.join(ROOT, 'public');
fs.mkdirSync(DATA, { recursive: true });

/* ---------- data safety: your store survives updates, pushes and redeploys ----------
   Updating server.js or index.html never touches the data folder. But some hosts rebuild the WHOLE app folder on every push, which also
   wipes a data folder that lives inside it. So a second copy of the database, the uploaded pictures and the login secret is kept OUTSIDE the
   app folder (BACKUP_DIR; default: a "skinsentials22-backups" folder next to it), and if the database is ever missing at start-up, the newest
   copy is put back automatically. On a hosted app, point DATA_DIR and BACKUP_DIR at a persistent disk or volume. */
const BACKUP_OUT = path.resolve(ENV.BACKUP_DIR || path.join(ROOT, '..', 'skinsentials22-backups'));
const SNAP_RE = /^store-\d{8}-\d{4,6}(-(manual|update|restore))?\.db$/;
(() => {
  try {
    let recovered = false;
    if (!fs.existsSync(path.join(DATA, 'store.db'))) {
      let best = null;
      for (const dir of [BACKUP_OUT, path.join(DATA, 'backups')]) { try { for (const f of fs.readdirSync(dir)) { if (!SNAP_RE.test(f)) continue; const full = path.join(dir, f), m = fs.statSync(full).mtimeMs; if (!best || m > best.m) best = { full, m }; } } catch {} }
      if (best) { fs.copyFileSync(best.full, path.join(DATA, 'store.db')); recovered = true; console.log('DATA RECOVERY: the database was missing, so it was restored from ' + best.full + ' (saved ' + new Date(best.m).toISOString() + ').'); }
    }
    const sec = path.join(DATA, '.secret'), secBk = path.join(BACKUP_OUT, '.secret');
    if (!fs.existsSync(sec) && fs.existsSync(secBk)) { fs.copyFileSync(secBk, sec); console.log('DATA RECOVERY: restored the login secret, so customers stay signed in.'); }
    const up = path.join(BACKUP_OUT, 'uploads'), dn = path.join(DATA, 'uploads');
    let emptyUp = true; try { emptyUp = !fs.readdirSync(dn).length; } catch {}
    if (fs.existsSync(up) && (recovered || emptyUp)) { fs.mkdirSync(dn, { recursive: true }); let n = 0; for (const f of fs.readdirSync(up)) { const to = path.join(dn, f); if (!fs.existsSync(to)) { try { fs.copyFileSync(path.join(up, f), to); n++; } catch {} } } if (n) console.log('DATA RECOVERY: restored ' + n + ' uploaded picture(s).'); }
  } catch (e) { console.error('Recovery check failed:', e.message); }
  if (!ENV.DATA_DIR && path.resolve(DATA).startsWith(path.resolve(ROOT) + path.sep)) console.log('NOTE: your data folder is inside the app folder. That is fine on your own server. On a host that rebuilds the app on every push, set DATA_DIR to a persistent disk.');
})();
const UPLOADS = path.join(DATA, 'uploads'); fs.mkdirSync(UPLOADS, { recursive: true });
const MAX_UPLOAD = (+ENV.MAX_UPLOAD_MB || 0) * 1048576;   // 0 = no limit set by this app (your host / disk may still have one)
const SECRET = ENV.JWT_SECRET || (() => { const f = path.join(DATA, '.secret'); try { return fs.readFileSync(f, 'utf8'); } catch { const s = crypto.randomBytes(48).toString('hex'); fs.writeFileSync(f, s, { mode: 0o600 }); return s; } })();
const PS_API = ENV.PAYSTACK_API || 'https://api.paystack.co', PAYSTACK = ENV.PAYSTACK_SECRET_KEY || '';
let SITE = (ENV.SITE_URL || `http://localhost:${PORT}`).replace(/\/$/, '');   // can be changed from Admin > General (see applySite)

/* ---------- database ---------- */
const db = new DatabaseSync(path.join(DATA, 'store.db'));
db.exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, phone TEXT DEFAULT '', pw TEXT NOT NULL, created INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS likes(user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, product_id TEXT NOT NULL, created INTEGER NOT NULL, PRIMARY KEY(user_id, product_id));
CREATE TABLE IF NOT EXISTS subscribers(id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE, name TEXT DEFAULT '', token TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, created INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS products(id TEXT PRIMARY KEY, pos INTEGER NOT NULL, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY AUTOINCREMENT, ref TEXT NOT NULL UNIQUE, user_id INTEGER, customer TEXT, email TEXT, phone TEXT, address TEXT, items TEXT NOT NULL, item_count INTEGER, total INTEGER NOT NULL, method TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'Pending', paid INTEGER NOT NULL DEFAULT 0, paid_at INTEGER, claimed INTEGER NOT NULL DEFAULT 0, note TEXT DEFAULT '', created INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS brands(name TEXT PRIMARY KEY, logo TEXT NOT NULL DEFAULT '');`);
try { db.exec("ALTER TABLE users ADD COLUMN address TEXT DEFAULT ''"); } catch {}   // saved delivery address (added for the profile page)
for (const a of ["ALTER TABLE orders ADD COLUMN shipping INTEGER NOT NULL DEFAULT 0", "ALTER TABLE orders ADD COLUMN restocked INTEGER NOT NULL DEFAULT 0", "ALTER TABLE orders ADD COLUMN state TEXT NOT NULL DEFAULT ''", "ALTER TABLE orders ADD COLUMN zone TEXT NOT NULL DEFAULT ''"]) try { db.exec(a); } catch {}
db.exec(`CREATE TABLE IF NOT EXISTS payments(id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER NOT NULL UNIQUE REFERENCES orders(id), ref TEXT NOT NULL UNIQUE, method TEXT NOT NULL, amount INTEGER NOT NULL, received INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending', channel TEXT DEFAULT '', detail TEXT DEFAULT '', gateway_id TEXT DEFAULT '', fees INTEGER NOT NULL DEFAULT 0, paid_at INTEGER, confirmed_by TEXT DEFAULT '', note TEXT DEFAULT '', created INTEGER NOT NULL, updated INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS pay_status ON payments(status, created);
CREATE TABLE IF NOT EXISTS payment_events(id INTEGER PRIMARY KEY AUTOINCREMENT, payment_id INTEGER NOT NULL, at INTEGER NOT NULL, type TEXT NOT NULL, text TEXT DEFAULT '', alert INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS resets(hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL, exp INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS stock(product_id TEXT PRIMARY KEY, qty INTEGER NOT NULL);`);
/* older databases: give every existing order a payment record so the admin ledger is complete */
for (const o of db.prepare('SELECT * FROM orders WHERE id NOT IN (SELECT order_id FROM payments)').all()) {
  const st = o.paid ? 'paid' : o.status === 'Cancelled' ? 'cancelled' : o.claimed ? 'review' : 'pending';
  const id = db.prepare('INSERT INTO payments(order_id,ref,method,amount,received,status,paid_at,confirmed_by,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)').run(o.id, o.ref, o.method, o.total, o.paid ? o.total : 0, st, o.paid_at || null, o.paid ? 'imported' : '', o.created, o.created).lastInsertRowid;
  db.prepare('INSERT INTO payment_events(payment_id,at,type,text) VALUES(?,?,?,?)').run(id, o.created, 'order', 'Order imported from the earlier version of the store');
}
if (!db.prepare("SELECT 1 FROM settings WHERE k='seeded'").get()) {   // the starter products are added once, on a brand-new store only: deleting products never brings them back
  if (!db.prepare('SELECT 1 FROM products LIMIT 1').get()) {
  try { const seed = JSON.parse(fs.readFileSync(path.join(ROOT, 'products.seed.json'), 'utf8')); const ins = db.prepare('INSERT INTO products VALUES(?,?,?)'); seed.forEach((p, i) => ins.run(p.id, i, JSON.stringify(p))); } catch {}
  }
  try { db.prepare("INSERT OR IGNORE INTO settings VALUES('seeded','1')").run(); } catch {}
}
const DEFAULTS = { bank_name: '', account_number: '', account_name: '', extra_accounts: '[]', pay_off: '', cod_note: '', ship_lagos: '4000', ship_other: '6500', days_lagos: '1-3', days_other: '3-7', process_time: '24 hours', free_over: '0', intl_note: '', whatsapp: '2349032078594', instagram: 'skinsentials22', announcement: '' };
const LIMITS = { extra_accounts: 1500, pay_off: 120, cod_note: 300, ship_lagos: 9, ship_other: 9, free_over: 9, days_lagos: 6, days_other: 6, process_time: 40, intl_note: 300 };
const getSettings = () => { const s = { ...DEFAULTS }; for (const r of db.prepare('SELECT k,v FROM settings').all()) if (r.k in DEFAULTS) s[r.k] = r.v; return s; };
/* ---------- General settings (Admin > General): website address (domain), logo, favicon, share image, app download page ---------- */
const SITE_DEFAULT = SITE;
const GEN_TEXT = { store_name: 60, tagline: 120, legal_name: 80, address: 200, hours: 120, support_phone: 30, meta_title: 70, meta_desc: 160, cutoff_text: 160, mail_name: 60, mail_footer: 200, maintenance_title: 80, maintenance_msg: 400, maintenance_until: 60, legal_effective: 40, app_headline: 120, app_text: 300 };
const GEN_EMAIL = ['support_email', 'privacy_email', 'mail_reply_to'], GEN_SOCIAL = ['facebook', 'tiktok', 'youtube', 'twitter'], GEN_NUM = ['return_days', 'refund_days', 'damage_hours', 'ack_days', 'resolve_days'];
const LEGAL_IDS = ['terms', 'privacy', 'shipping', 'returns', 'cancellation', 'payments', 'cookies', 'safety', 'promos', 'referral', 'contact'];
const GEN_DEF = Object.fromEntries(['site_url', 'logo', 'favicon', 'og_image', 'app_ios', 'app_android', 'app_apk', 'app_image', 'app_qr', 'ga_id', 'fb_pixel', 'gsc_file', 'gsc_meta', 'maintenance_on', ...Object.keys(GEN_TEXT), ...GEN_EMAIL, ...GEN_SOCIAL, ...GEN_NUM, ...LEGAL_IDS.map(i => 'legal_' + i)].map(k => [k, '']));
const GEN_IMG = /^(\/uploads\/[\w-]+\.(jpe?g|png|webp|gif)|https:\/\/[^\s"'<>`\\]+)$/i, GEN_FILE = /^(\/uploads\/[\w.-]+|https:\/\/[^\s"'<>`\\]+)$/, GEN_LINK = /^https:\/\/[^\s"'<>`\\]+$/;
const getGeneral = () => { let g = {}; try { g = JSON.parse((db.prepare("SELECT v FROM settings WHERE k='general'").get() || {}).v || '{}') || {}; } catch {} const o = { ...GEN_DEF }; for (const k of Object.keys(GEN_DEF)) if (typeof g[k] === 'string') o[k] = g[k]; return o; };
const cleanSite = v => { v = String(v || '').trim().replace(/\/+$/, ''); if (!v) return ''; if (!/^https?:\/\/[a-z0-9.-]+(:\d{2,5})?$/i.test(v) || (/^http:/i.test(v) && !/^http:\/\/(localhost|127\.0\.0\.1)(:|$)/i.test(v))) return null; return v; };
const applySite = () => { SITE = cleanSite(getGeneral().site_url) || SITE_DEFAULT; };   // the address saved in Admin > General wins over SITE_URL in .env; blank = use .env again
applySite();
const PRIVATE_GEN = new Set(['gsc_file', 'gsc_meta', 'mail_name', 'mail_reply_to', 'mail_footer']);
const publicGeneral = () => { const g = getGeneral(), s = getSettings(), o = {}; for (const k of Object.keys(g)) if ((!k.startsWith('legal_') || k === 'legal_effective') && !PRIVATE_GEN.has(k)) o[k] = g[k]; o.site_url = SITE; o.whatsapp = s.whatsapp; o.instagram = s.instagram; o.legal_custom = LEGAL_IDS.filter(i => g['legal_' + i]); return o; };
const maintOn = () => getGeneral().maintenance_on === '1';
/* things added to every page the server sends: the live domain, tracking tags, Search Console tag, home page title and description */
const OLD_DOMAIN = 'https://skinsentials22.com.ng';
const absUrl = u => !u ? '' : /^https?:/i.test(u) ? u : /^data:/i.test(u) ? '' : SITE + (u[0] === '/' ? '' : '/') + u;
const escA = v => String(v).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
const phoneIntl = v => { const x = String(v || '').replace(/[^\d+]/g, ''); return x.startsWith('+') ? x : x.startsWith('0') ? '+234' + x.slice(1) : '+' + x; };
function injectHtml(h, home) {
  const g = getGeneral(), end = h.indexOf('</head>'); if (end < 0) return h;
  let head = h.slice(0, end); const rest = h.slice(end);
  if (SITE !== OLD_DOMAIN) head = head.split(OLD_DOMAIN).join(SITE);
  head = head.replace(/(<script type="application\/ld\+json" id="sk-ld-base">)([\s\S]*?)(<\/script>)/, (m, a, j, c) => { try {
    const o = JSON.parse(j), org = (o['@graph'] || []).find(x => x && x['@type'] === 'Organization'); if (!org) return m; const s = getSettings();
    if (g.store_name) org.name = g.store_name; if (g.support_email) org.email = g.support_email; if (g.support_phone) org.telephone = phoneIntl(g.support_phone); if (g.logo) org.logo = absUrl(g.logo);
    const same = [s.instagram ? 'https://www.instagram.com/' + s.instagram : '', ...GEN_SOCIAL.map(k => g[k])].filter(Boolean); if (GEN_SOCIAL.some(k => g[k])) org.sameAs = same;
    return a + JSON.stringify(o).replace(/</g, '\\u003c') + c; } catch { return m; } });
  if (home) {
    const swap = (re, v) => { head = head.replace(re, (m, a, b) => a + escA(v) + b); };
    if (g.meta_title) { head = head.replace(/<title>[\s\S]*?<\/title>/, () => '<title>' + escA(g.meta_title) + '</title>'); swap(/(<meta property="og:title" content=")[^"]*(")/, g.meta_title); swap(/(<meta name="twitter:title" content=")[^"]*(")/, g.meta_title); }
    if (g.meta_desc) { swap(/(<meta name="description" content=")[^"]*(")/, g.meta_desc); swap(/(<meta property="og:description" content=")[^"]*(")/, g.meta_desc); swap(/(<meta name="twitter:description" content=")[^"]*(")/, g.meta_desc); }
    if (g.og_image) { swap(/(<meta property="og:image" content=")[^"]*(")/, absUrl(g.og_image)); swap(/(<meta name="twitter:image" content=")[^"]*(")/, absUrl(g.og_image)); }
  }
  let add = '';
  if (g.gsc_meta) add += `<meta name="google-site-verification" content="${escA(g.gsc_meta)}">`;
  if (g.ga_id) add += `<script async src="https://www.googletagmanager.com/gtag/js?id=${g.ga_id}"></script><script>window.dataLayer=window.dataLayer||[];function gtag(){dataLayer.push(arguments)}gtag('js',new Date());gtag('config','${g.ga_id}');</script>`;
  if (g.fb_pixel) add += `<script>!function(f,b,e,v,n,t,s){if(f.fbq)return;n=f.fbq=function(){n.callMethod?n.callMethod.apply(n,arguments):n.queue.push(arguments)};if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}(window,document,'script','https://connect.facebook.net/en_US/fbevents.js');fbq('init','${g.fb_pixel}');fbq('track','PageView');</script>`;
  return head + add + rest;
}
/* Paystack secret: the .env value wins; otherwise the key saved from Admin > Settings (never sent back to the browser) */
const psKey = () => PAYSTACK || (db.prepare("SELECT v FROM settings WHERE k='paystack_secret'").get() || {}).v || '';
const psInfo = () => { const k = psKey(); return { configured: !!k, source: PAYSTACK ? 'env' : k ? 'admin' : 'none', mode: k.startsWith('sk_live') ? 'live' : k ? 'test' : '', hint: k ? k.slice(0, 7) + '...' + k.slice(-4) : '' }; };
const accountsOf = s => { const out = []; if (s.bank_name && s.account_number) out.push({ bank_name: s.bank_name, account_number: s.account_number, account_name: s.account_name }); try { for (const a of JSON.parse(s.extra_accounts || '[]')) if (a && a.bank_name && a.account_number) out.push({ bank_name: String(a.bank_name).slice(0, 60), account_number: String(a.account_number).slice(0, 30), account_name: String(a.account_name || '').slice(0, 80) }); } catch {} return out.slice(0, 4); };

/* every way a customer can pay. 'ps' = Paystack channels to offer (null = all of them) */
const PM = {
  card:       { label: 'Debit / credit card',     desc: 'Visa, Mastercard or Verve card',                 ps: ['card'] },
  bank:       { label: 'Pay with bank',           desc: 'Approve the payment from your bank account',     ps: ['bank'] },
  ussd:       { label: 'USSD',                    desc: 'Pay with a code from your phone, no internet',   ps: ['ussd'] },
  pstransfer: { label: 'Instant bank transfer',   desc: 'Get a one-time account number and transfer',     ps: ['bank_transfer'] },
  paystack:   { label: 'Pay online (all options)', desc: 'Card, bank, USSD or transfer via Paystack',     ps: null, hidden: true },
  transfer:   { label: 'Direct bank transfer',    desc: 'Send to our account and we confirm',             manual: true },
  cod:        { label: 'Pay on delivery',         desc: 'Pay the rider when your order arrives',          manual: true },
  dm:         { label: 'Order via DM',            desc: 'Arrange payment with us on WhatsApp or Instagram', manual: true }
};
const methodState = (id, s = getSettings()) => {
  const m = PM[id]; if (!m) return { ok: false, reason: 'Unknown payment method' };
  if (s.pay_off.split(',').includes(id)) return { ok: false, reason: 'Switched off by the store' };
  if (!m.manual && !psKey()) return { ok: false, reason: 'Not set up yet' };
  if (id === 'transfer' && !accountsOf(s).length) return { ok: false, reason: 'Not set up yet' };
  return { ok: true };
};

/* ---------- helpers ---------- */
class HttpError extends Error { constructor(code, msg) { super(msg); this.code = code; } }
const now = () => Date.now(), b64 = b => Buffer.from(b).toString('base64url');
const sign = p => { const d = b64(JSON.stringify(p)); return d + '.' + crypto.createHmac('sha256', SECRET).update(d).digest('base64url'); };
function verify(tok) {
  const [d, s] = String(tok || '').split('.'); if (!d || !s) return null;
  const e = crypto.createHmac('sha256', SECRET).update(d).digest('base64url');
  if (e.length !== s.length || !crypto.timingSafeEqual(Buffer.from(e), Buffer.from(s))) return null;
  try { const p = JSON.parse(Buffer.from(d, 'base64url')); return p.exp > now() ? p : null; } catch { return null; }
}
const hashPw = pw => { const salt = crypto.randomBytes(16).toString('hex'); return `s1$${salt}$${crypto.scryptSync(pw, salt, 64).toString('hex')}`; };
const checkPw = (pw, stored) => { const [, salt, h] = stored.split('$'); const a = crypto.scryptSync(pw, salt, 64), b = Buffer.from(h, 'hex'); return a.length === b.length && crypto.timingSafeEqual(a, b); };
const str = (v, max) => String(v == null ? '' : v).trim().slice(0, max);
const plainTxt = (v, n) => str(v, n).replace(/[<>]/g, '').replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '');   // no HTML in product text
const plainShort = (v, n) => plainTxt(v, n).replace(/"/g, '\u2033');   // short fields also sit inside HTML attributes, so straight quotes become the inch mark
const PIMG_OK = /^(\/uploads\/[\w.-]+|\/(?!\/)[\w.\/-]+|https?:\/\/[^\s"'<>`\\]+|data:image\/(jpeg|png|webp|gif);base64,[A-Za-z0-9+\/=]+)$/;
const PVID_OK = /^(\/uploads\/[\w.-]+|https:\/\/[^\s"'<>`\\]+)$/;
const emailOk = e => /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(e) && e.length <= 254;
const naira = n => '₦' + n.toLocaleString('en-NG');
const hits = new Map();
function limit(ip, bucket, max, win = 60000) {
  const k = bucket + ip, t = now(), h = (hits.get(k) || []).filter(x => t - x < win); h.push(t); hits.set(k, h);
  if (h.length > max) throw new HttpError(429, 'Too many attempts. Please wait a minute and try again.');
}
setInterval(() => { const t = now(); for (const [k, v] of hits) if (!v.some(x => t - x < 900000)) hits.delete(k); }, 60000).unref();
const userOut = u => ({ id: u.id, name: u.name, email: u.email, phone: u.phone, address: u.address || '', created: u.created, newsletter: !!db.prepare('SELECT active FROM subscribers WHERE email=? AND active=1').get(u.email) });
const orderOut = o => ({ id: '#' + (1000 + o.id), ref: o.ref, customer: o.customer, email: o.email, phone: o.phone, address: o.address, items: o.item_count, lines: JSON.parse(o.items), total: o.total, discount: (o.discount || 0) + (o.glow_disc || 0), glow: o.glow_disc || 0, credit: o.credit_used || 0, shipping: o.shipping || 0, state: o.state || '', zone: o.zone || '', method: o.method, status: o.status, paid: !!o.paid, claimed: !!o.claimed, note: o.note, date: o.created, payment: o.pstatus ? { id: o.pid, status: o.pstatus, channel: o.pchannel, detail: o.pdetail, fees: o.pfees, received: o.preceived } : null });

function subscribe(email, name) {
  const ex = db.prepare('SELECT * FROM subscribers WHERE email=?').get(email);
  if (ex) { db.prepare('UPDATE subscribers SET active=1, name=COALESCE(NULLIF(?,\'\'),name) WHERE id=?').run(name, ex.id); return false; }
  db.prepare('INSERT INTO subscribers(email,name,token,created) VALUES(?,?,?,?)').run(email, name, crypto.randomBytes(18).toString('hex'), now()); return true;
}

/* ---------- in-app notifications: the bell and the Notifications tab in the customer profile ---------- */
db.exec(`CREATE TABLE IF NOT EXISTS notifications(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, type TEXT NOT NULL DEFAULT 'info', title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '', link TEXT NOT NULL DEFAULT '', created INTEGER NOT NULL, seen INTEGER NOT NULL DEFAULT 0, dedupe TEXT);
CREATE INDEX IF NOT EXISTS notif_user ON notifications(user_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS notif_dedupe ON notifications(user_id, dedupe) WHERE dedupe IS NOT NULL;`);
function inbox(uid, type, title, body, link, dedupe) {   // never throws: a failed notification must never break an order or a payment
  try {
    if (!uid) return;   // guest orders have no account, so there is nobody to notify
    db.prepare('INSERT OR IGNORE INTO notifications(user_id,type,title,body,link,created,seen,dedupe) VALUES(?,?,?,?,?,?,0,?)').run(uid, String(type || 'info').slice(0, 20), String(title || '').slice(0, 140), String(body || '').slice(0, 400), String(link || '').slice(0, 30), now(), dedupe ? String(dedupe).slice(0, 80) : null);
    db.prepare('DELETE FROM notifications WHERE user_id=? AND id NOT IN (SELECT id FROM notifications WHERE user_id=? ORDER BY id DESC LIMIT 100)').run(uid, uid);   // each customer keeps their newest 100
  } catch (e) { console.error('Notification not saved:', e.message); }
}
/* the search-engine pinger (further down) plugs in here, so saving a product or post can tell it straight away */
const SEOQ = { ping() {} };

/* ---------- notifications (all optional; configured in .env) ---------- */
const TG = { token: ENV.TELEGRAM_BOT_TOKEN || '', chat: ENV.TELEGRAM_CHAT_ID || '', api: ENV.TELEGRAM_API || 'https://api.telegram.org' };
const MAILCFG = { key: ENV.RESEND_API_KEY || '', from: ENV.MAIL_FROM || '', admin: ENV.ADMIN_EMAIL || '', api: ENV.RESEND_API || 'https://api.resend.com' };
let HOOK = ENV.NOTIFY_WEBHOOK_URL || '';
const mailOn = () => !!(MAILCFG.key && MAILCFG.from);
const notifyStatus = () => ({ telegram: !!(TG.token && TG.chat), webhook: !!HOOK, adminEmail: mailOn() && !!MAILCFG.admin, customerEmail: mailOn() });
/* email + alert settings saved in Admin > General (settings key 'comms'); a value saved there overrides .env, empty falls back to .env */
const commsRaw = () => { try { return JSON.parse((db.prepare("SELECT v FROM settings WHERE k='comms'").get() || {}).v || '{}') || {}; } catch { return {}; } };
const applyComms = () => { const o = commsRaw(), pick = (k, e) => (typeof o[k] === 'string' && o[k]) || e || ''; TG.token = pick('tg_token', ENV.TELEGRAM_BOT_TOKEN); TG.chat = pick('tg_chat', ENV.TELEGRAM_CHAT_ID); MAILCFG.key = pick('mail_key', ENV.RESEND_API_KEY); MAILCFG.from = pick('mail_from', ENV.MAIL_FROM); MAILCFG.admin = pick('admin_email', ENV.ADMIN_EMAIL); HOOK = pick('webhook', ENV.NOTIFY_WEBHOOK_URL); };
applyComms();
const hx = v => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
async function postJson(url, body, headers = {}) {
  let r; try { r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body), signal: AbortSignal.timeout(10000) }); } catch (e) { throw new Error('could not reach ' + new URL(url).host); }
  if (!r.ok) throw new Error(new URL(url).host + ' answered HTTP ' + r.status); return r;
}
const mailFrom = () => { const nm = String(getGeneral().mail_name || '').replace(/[<>"\r\n]/g, '').trim(); return nm && MAILCFG.from && !/</.test(MAILCFG.from) ? `${nm} <${MAILCFG.from}>` : MAILCFG.from; };   // sender name from Admin > General
const sendMail = (to, subject, html) => mailOn() && to ? postJson(MAILCFG.api + '/emails', { from: mailFrom(), to: [to], subject, html, ...(getGeneral().mail_reply_to ? { reply_to: getGeneral().mail_reply_to } : {}) }, { Authorization: 'Bearer ' + MAILCFG.key }).then(() => true) : Promise.resolve(false);
async function notifyAdmin(title, lines) {
  const text = [title, ...lines].join('\n'), jobs = [];
  if (TG.token && TG.chat) jobs.push(postJson(`${TG.api}/bot${TG.token}/sendMessage`, { chat_id: TG.chat, text }));
  if (HOOK) jobs.push(postJson(HOOK, { text, content: text, title, lines }));
  if (mailOn() && MAILCFG.admin) jobs.push(sendMail(MAILCFG.admin, title, `<p>${lines.map(hx).join('<br>')}</p>`));
  const rs = await Promise.allSettled(jobs); rs.forEach(r => r.status === 'rejected' && console.error('Admin notification failed:', r.reason.message)); return rs.every(r => r.status === 'fulfilled') && jobs.length > 0;
}
const mailShell = (h, body) => { const g = getGeneral(); return `<div style="font-family:Arial,sans-serif;max-width:520px;margin:auto;color:#1b2a3a"><h2 style="font-family:Georgia,serif">${hx(g.store_name || 'SKINSENTIALS22')}</h2><h3>${h}</h3>${body}<p style="color:#778;font-size:12px;margin-top:24px">${hx(g.mail_footer || 'Questions? Reply to this email or message us on WhatsApp.')}</p></div>`; };
const linesHtml = o => `<table style="width:100%;border-collapse:collapse;font-size:14px">${JSON.parse(o.items).map(i => `<tr><td style="padding:4px 0">${i.qty} × ${hx(i.name)}</td><td style="text-align:right">${naira(i.price * i.qty)}</td></tr>`).join('')}${o.glow_disc ? `<tr><td style="padding:4px 0">Glow Friend offer</td><td style="text-align:right">-${naira(o.glow_disc)}</td></tr>` : ''}${o.discount ? `<tr><td style="padding:4px 0">Referral offer</td><td style="text-align:right">-${naira(o.discount)}</td></tr>` : ''}${o.credit_used ? `<tr><td style="padding:4px 0">Store credit</td><td style="text-align:right">-${naira(o.credit_used)}</td></tr>` : ''}${o.shipping ? `<tr><td style="padding:4px 0">Delivery</td><td style="text-align:right">${naira(o.shipping)}</td></tr>` : ''}<tr><td style="padding-top:8px;border-top:1px solid #ddd"><b>Total</b></td><td style="text-align:right;border-top:1px solid #ddd"><b>${naira(o.total)}</b></td></tr></table>`;
const quiet = p => { Promise.resolve(p).catch(e => console.error('Background task failed:', e.message)); };
/* referral programme (see referral.js). Hooks are fenced so a referral problem can never break an order or a payment. */
let REF = null;
const refHook = (fn, ...a) => { try { if (REF) REF[fn](...a); } catch (e) { console.error('Referral hook failed (' + fn + '):', e.message); } };

/* ---------- payments ---------- */
async function paystack(method, url, body) {
  const key = psKey(); if (!key) throw new HttpError(503, 'Online payment is not available right now. Please choose bank transfer, pay on delivery or DM.');
  let r; try { r = await fetch(PS_API + url, { method, headers: { Authorization: 'Bearer ' + key, 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(15000) }); } catch (e) { console.error('Paystack unreachable:', e.cause ? e.cause.code : e.message); throw new HttpError(502, 'We could not reach the payment provider. Please try again or choose another payment method.'); }
  const j = await r.json().catch(() => ({})); if (!r.ok || !j.status) throw new HttpError(502, j.message || 'Payment provider error');
  return j.data;
}
const payOf = oid => db.prepare('SELECT * FROM payments WHERE order_id=?').get(oid);
const orderBy = ref => db.prepare('SELECT * FROM orders WHERE ref=?').get(ref);
const ev = (pid, type, text, alert = 0) => db.prepare('INSERT INTO payment_events(payment_id,at,type,text,alert) VALUES(?,?,?,?,?)').run(pid, now(), type, text, alert ? 1 : 0);
const tx = fn => { db.exec('BEGIN'); try { const r = fn(); db.exec('COMMIT'); return r; } catch (e) { try { db.exec('ROLLBACK'); } catch {} throw e; } };
const label = m => (PM[m] || {}).label || m;
const oid = o => '#' + (1000 + o.id);

/* the one place an order becomes "paid": used by Paystack verify, the webhook, and the admin's manual confirmation */
function settle(o, i = {}) {
  const p = payOf(o.id); if (!p || p.status === 'paid') return false;
  const got = i.received == null ? o.total : i.received, t = now();
  tx(() => {
    db.prepare("UPDATE payments SET status='paid', received=?, channel=COALESCE(NULLIF(?,''),channel), detail=?, gateway_id=?, fees=?, paid_at=?, confirmed_by=?, note=COALESCE(NULLIF(?,''),note), updated=? WHERE id=?").run(got, i.channel || '', i.detail || '', i.gateway_id || '', i.fees || 0, t, i.by || '', i.note || '', t, p.id);
    db.prepare('UPDATE orders SET paid=1, paid_at=? WHERE id=?').run(t, o.id);
    ev(p.id, 'paid', `${naira(got)} received via ${label(o.method)}${i.channel ? ' (' + i.channel + ')' : ''}${i.by && i.by !== 'paystack' ? ', confirmed by ' + i.by : ''}`, 1);
  });
  inbox(o.user_id, 'payment', `Payment received for ${oid(o)}`, `Thank you! We received ${naira(got)} and we are getting your order ready.`, 'orders', 'paid:' + o.id);
  refHook('onPaid', o);
  quiet(notifyAdmin(`💰 Payment received: ${naira(got)}`, [`Order ${oid(o)} · ${o.customer}`, `Method: ${label(o.method)}${i.channel ? ' (' + i.channel + ')' : ''}`, `Reference: ${o.ref}`]));
  quiet(sendMail(o.email, `Payment received for order ${oid(o)}`, mailShell('Thank you! We have received your payment.', `<p>Hi ${hx(o.customer)}, your order <b>${oid(o)}</b> is confirmed and we are getting it ready.</p>${linesHtml(o)}${GLOW.note(o)}<p>Delivery to: ${hx(o.address)}${o.state ? ', ' + hx(o.state) : ''}</p><p style="color:#778">Reference ${hx(o.ref)}</p>`)));
  return true;
}
function unsettle(o, note) {
  const p = payOf(o.id); if (!p || p.status !== 'paid') return false;
  tx(() => { db.prepare("UPDATE payments SET status='pending', received=0, paid_at=NULL, confirmed_by='', updated=? WHERE id=?").run(now(), p.id); db.prepare('UPDATE orders SET paid=0, paid_at=NULL WHERE id=?').run(o.id); ev(p.id, 'unpaid', 'Marked as not paid by admin' + (note ? ': ' + note : '')); }); refHook('onReverse', o, 'Payment marked as not received', 'unpaid'); return true;
}
function applyPaystack(o, d) {
  const p = payOf(o.id); if (!p) return false;
  if (d.status === 'success') {
    if (d.amount === o.total * 100 && d.currency === 'NGN') { const a = d.authorization || {}; return settle(o, { channel: d.channel, detail: a.bank || (a.brand ? a.brand + (a.last4 ? ' ••' + a.last4 : '') : ''), gateway_id: String(d.id || ''), fees: Math.round((d.fees || 0) / 100), by: 'paystack' }) || p.status === 'paid'; }
    if (p.status !== 'mismatch' && p.status !== 'paid') tx(() => { db.prepare("UPDATE payments SET status='mismatch', updated=? WHERE id=?").run(now(), p.id); ev(p.id, 'mismatch', `Paystack reports ${naira((d.amount || 0) / 100)} ${d.currency || ''} but the order is ${naira(o.total)}. Check it in your Paystack dashboard before shipping.`, 1); });
    return false;
  }
  if (d.status === 'failed' && !db.prepare("SELECT 1 FROM payment_events WHERE payment_id=? AND type='attempt'").get(p.id)) ev(p.id, 'attempt', 'A payment attempt failed on Paystack. The customer can try again.');
  return false;
}
async function verifyOnline(o) {
  const p = payOf(o.id); if (p && p.status === 'paid') return true;
  return applyPaystack(o, await paystack('GET', '/transaction/verify/' + encodeURIComponent(o.ref)));
}
const lineNeeds = lines => { const m = new Map(); for (const l of lines) if (l.id !== 'giftcard') m.set(l.id, (m.get(l.id) || 0) + l.qty); return m; };
function restock(o) {
  if (o.restocked) return;
  tx(() => { for (const [id, n] of lineNeeds(JSON.parse(o.items))) db.prepare('UPDATE stock SET qty=qty+? WHERE product_id=?').run(n, id); db.prepare('UPDATE orders SET restocked=1 WHERE id=?').run(o.id); });
}
function cancelOrder(o, why) {
  const p = payOf(o.id);
  tx(() => { db.prepare("UPDATE orders SET status='Cancelled' WHERE id=?").run(o.id); if (p && ['pending', 'review', 'mismatch'].includes(p.status)) { db.prepare("UPDATE payments SET status='cancelled', updated=? WHERE id=?").run(now(), p.id); ev(p.id, 'cancelled', why || 'Order cancelled'); } });
  if (!p || p.status !== 'paid') restock(db.prepare('SELECT * FROM orders WHERE id=?').get(o.id));
  refHook('onReverse', o, why || 'Order cancelled', 'void');
  inbox(o.user_id, 'order', `Order ${oid(o)} was cancelled`, 'The items were released. If you already paid, please contact us about your refund.', 'orders', 'cancel:' + o.id);
}
function dmLinks(o, s) {
  const lines = JSON.parse(o.items).map(i => `${i.qty}x ${i.name}`).join(', ');
  const msg = `Hello Skinsentials22, I'd like to place an order.\nRef: ${o.ref}\nItems: ${lines}\nTotal: ${naira(o.total)}\nName: ${o.customer}\nAddress: ${o.address}`;
  return { whatsapp: `https://wa.me/${s.whatsapp.replace(/\D/g, '')}?text=${encodeURIComponent(msg)}`, instagram: s.instagram ? `https://ig.me/m/${encodeURIComponent(s.instagram)}` : '' };
}
/* background safety net: catches customers who paid but closed the tab before the webhook arrived, and releases stock held by abandoned online checkouts */
let reconciling = false;
async function reconcile() {
  if (reconciling || !psKey()) return; reconciling = true;
  try {
    for (const p of db.prepare("SELECT * FROM payments WHERE status IN ('pending','mismatch') AND method NOT IN ('transfer','cod','dm') AND created > ? AND created < ? ORDER BY created LIMIT 25").all(now() - 72 * 36e5, now() - 120000)) {
      const o = db.prepare('SELECT * FROM orders WHERE id=?').get(p.order_id);
      try { if (await verifyOnline(o)) continue; } catch (e) { continue; }
      if (p.status === 'pending' && now() - p.created > 24 * 36e5) cancelOrder(o, 'Not paid within 24 hours, cancelled automatically');
    }
  } finally { reconciling = false; }
}

/* ---------- routes ---------- */
const routes = [];
const route = (method, p, opts, fn) => { if (typeof opts === 'function') { fn = opts; opts = {}; } routes.push({ method, re: new RegExp('^' + p.replace(/:(\w+)/g, '(?<$1>[^/]+)') + '$'), opts, fn }); };
const stockMap = () => new Map(db.prepare('SELECT product_id, qty FROM stock').all().map(r => [r.product_id, r.qty]));
const productsAll = () => { const st = stockMap(); return db.prepare('SELECT data FROM products ORDER BY pos').all().map(r => { const p = JSON.parse(r.data); delete p.stock; if (st.has(p.id)) p.stock = st.get(p.id); return p; }); };
/* ---------- discounts: manual sale prices and automatic rules (Admin > Discounts & Points) ----------
   The product's own price is never changed. A sale price lives in settings key 'pricing'; the server works out the price customers pay
   (manual sale first, then the best automatic rule) and uses it for the shop, orders, Glow points, referral offers, search data and the Google feed. */
let GLOWPTS = null;   // filled in by the Glow Rewards section below
const PRC_DATE = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2})?$/;
const prcClean = c => {
  c = c && typeof c === 'object' ? c : {}; const o = { on: c.on !== false, round_to: [10, 50, 100, 500].includes(+c.round_to) ? +c.round_to : 0, rules: [], manual: {} };
  const dt = v => { v = String(v || '').trim(); return PRC_DATE.test(v) ? v : ''; };
  if (Array.isArray(c.rules)) o.rules = c.rules.slice(0, 50).map((r, i) => { r = r || {}; const type = r.type === 'amt' ? 'amt' : 'pct', v = Math.round(+r.value) || 0; return { id: str(r.id, 30) || 'r' + (i + 1), name: str(r.name, 60) || 'Discount', on: r.on !== false, scope: ['category', 'brand'].includes(r.scope) ? r.scope : 'all', match: str(r.match, 300), type, value: type === 'pct' ? Math.min(95, Math.max(0, v)) : Math.min(1e7, Math.max(0, v)), start: dt(r.start), end: dt(r.end), badge: str(r.badge, 30) }; }).filter(r => r.value > 0);
  if (c.manual && typeof c.manual === 'object') for (const [k, m] of Object.entries(c.manual).slice(0, 5000)) { if (k.length > 64 || !m || typeof m !== 'object') continue; const sale = Math.round(+m.sale) || 0, e = { sale: sale > 0 && sale <= 1e8 ? sale : 0, start: dt(m.start), end: dt(m.end), off: !!m.off, label: str(m.label, 30) }; if (e.sale || e.off) o.manual[k] = e; }
  return o;
};
const pricingCfg = () => { try { return prcClean(JSON.parse((db.prepare("SELECT v FROM settings WHERE k='pricing'").get() || {}).v || 'null')); } catch { return prcClean(null); } };
const watMs = (s, end) => { s = String(s || ''); if (!PRC_DATE.test(s)) return null; const t = Date.parse(s.slice(0, 10) + 'T' + (s.length > 10 ? s.slice(11) + ':00' : end ? '23:59:59' : '00:00:00') + '+01:00'); return Number.isNaN(t) ? null : t; };   // dates are Lagos time (UTC+1)
const inWin = (m, t) => { const a = watMs(m.start, false), b = watMs(m.end, true); return (a === null || t >= a) && (b === null || t <= b); };
const ruleHit = (r, p) => { if (r.scope === 'all') return true; const set = String(r.match || '').toLowerCase().split(',').map(x => x.trim()).filter(Boolean); if (!set.length) return false; const vals = r.scope === 'brand' ? [p.brand] : [p.category, p.categoryLabel, p.dept, p.deptLabel, p.tag]; return vals.some(v => v && set.includes(String(v).toLowerCase())); };
const ruleStatus = (r, t) => !r.on ? 'off' : (watMs(r.start, false) !== null && t < watMs(r.start, false)) ? 'scheduled' : (watMs(r.end, true) !== null && t > watMs(r.end, true)) ? 'ended' : 'active';
function saleFor(p, pc, t) {
  pc = pc || pricingCfg(); t = t || Date.now(); if (!pc.on) return null;
  const reg = Math.round(+p.price) || 0; if (reg <= 0) return null; const m = pc.manual[p.id];
  if (m && m.sale > 0 && m.sale < reg && inWin(m, t)) return { price: m.sale, was: reg, pct: Math.max(1, Math.round((1 - m.sale / reg) * 100)), src: 'manual', label: m.label || '', endsAt: watMs(m.end, true) };
  if (m && m.off) return null;   // "no automatic discounts for this product"
  let best = null;
  for (const r of pc.rules) {
    if (!r.on || !inWin(r, t) || !ruleHit(r, p)) continue;
    let np = r.type === 'amt' ? reg - r.value : reg * (1 - r.value / 100); np = pc.round_to > 1 ? Math.round(np / pc.round_to) * pc.round_to : Math.round(np);
    if (np <= 0 || np >= reg) continue; if (!best || np < best.price) best = { price: np, was: reg, pct: Math.max(1, Math.round((1 - np / reg) * 100)), src: 'auto', label: r.badge || r.name, endsAt: watMs(r.end, true) };
  }
  return best;
}
const pricedAll = () => { const pc = pricingCfg(), t = Date.now(); return productsAll().map(p => { const s = saleFor(p, pc, t); return s ? { ...p, price: s.price, was: s.was, salePct: s.pct } : p; }); };
const getRev = k => +((db.prepare('SELECT v FROM settings WHERE k=?').get(k) || {}).v || 0);
const bumpRev = k => { const n = getRev(k) + 1; db.prepare('INSERT INTO settings VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v').run(k, String(n)); return n; };
const sha = x => crypto.createHash('sha256').update(String(x)).digest('hex');
/* ---- Nigerian states, delivery zones and address detection (the same logic runs in the browser to preview the fee) ---- */
const NG_STATES = ['Abia', 'Adamawa', 'Akwa Ibom', 'Anambra', 'Bauchi', 'Bayelsa', 'Benue', 'Borno', 'Cross River', 'Delta', 'Ebonyi', 'Edo', 'Ekiti', 'Enugu', 'FCT (Abuja)', 'Gombe', 'Imo', 'Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 'Kogi', 'Kwara', 'Lagos', 'Nasarawa', 'Niger', 'Ogun', 'Ondo', 'Osun', 'Oyo', 'Plateau', 'Rivers', 'Sokoto', 'Taraba', 'Yobe', 'Zamfara'];
const INTL = 'International (outside Nigeria)';
const PLACES = { 'Lagos': 'lagos|lekki|ikeja|victoria island|ikoyi|yaba|surulere|ajah|ikorodu|badagry|epe|festac|apapa|oshodi|maryland|gbagada|magodo|ojodu|ogba|agege|alimosho|ipaja|egbeda|mushin|isolo|ajegunle|oniru|sangotedo|banana island|ketu|ojota|berger|ilupeju|lagos island|iyana ipaja|igando|abule egba|oko oba|satellite town|amuwo odofin|ogudu|anthony|ikota|chevron|osapa|jakande|idimu|akowonjo|ejigbo|ikotun|lasu|ojo|aja|eti osa',
  'FCT (Abuja)': 'abuja|fct|gwarinpa|wuse|garki|maitama|asokoro|jabi|kubwa|lugbe|gwagwalada|bwari|utako|lokogoma|apo|katampe|karu|nyanya', 'Rivers': 'port harcourt|rivers|obio akpor|rumuola|gra phase', 'Oyo': 'oyo|ibadan|ogbomosho|bodija|ring road ibadan', 'Ogun': 'ogun|abeokuta|sagamu|ijebu|mowe|ibafo|ota|sango ota|ifo|ilaro|redemption camp|magboro|arepo', 'Edo': 'edo|benin city|ekpoma|auchi', 'Kano': 'kano', 'Kaduna': 'kaduna|zaria', 'Enugu': 'enugu|nsukka', 'Imo': 'imo|owerri', 'Delta': 'delta|warri|asaba|sapele|effurun', 'Akwa Ibom': 'akwa ibom|uyo|eket', 'Cross River': 'cross river|calabar', 'Plateau': 'plateau|jos', 'Kwara': 'kwara|ilorin', 'Ondo': 'ondo|akure|owo', 'Anambra': 'anambra|onitsha|awka|nnewi', 'Abia': 'abia|aba|umuahia', 'Borno': 'borno|maiduguri', 'Sokoto': 'sokoto', 'Kogi': 'kogi|lokoja', 'Benue': 'benue|makurdi', 'Niger': 'niger state|minna|suleja', 'Osun': 'osun|osogbo|ile ife|ilesa', 'Ekiti': 'ekiti|ado ekiti', 'Bayelsa': 'bayelsa|yenagoa', 'Ebonyi': 'ebonyi|abakaliki', 'Bauchi': 'bauchi', 'Gombe': 'gombe', 'Adamawa': 'adamawa|yola', 'Taraba': 'taraba|jalingo', 'Yobe': 'yobe|damaturu', 'Jigawa': 'jigawa|dutse', 'Katsina': 'katsina', 'Kebbi': 'kebbi|birnin kebbi', 'Zamfara': 'zamfara|gusau', 'Nasarawa': 'nasarawa|lafia|keffi' };
const PLACE_RX = Object.entries(PLACES).map(([st, w]) => [st, new RegExp('(^|[^a-z])(' + w + ')(?![a-z])', 'i')]);
const INTL_RX = /(^|[^a-z])(united kingdom|uk|england|london|usa|u\.s\.a|united states|canada|toronto|ghana|accra|south africa|germany|france|paris|dubai|uae|china|india|kenya|nairobi|ireland|australia|italy|spain|netherlands|cameroon|benin republic|togo|senegal|outside nigeria|international)(?![a-z])/i;
function detectPlace(text) {   // -> a state name, INTL, or '' when nothing in the address gives it away
  const t = String(text || ''); if (!t.trim()) return '';
  if (/nigeria/i.test(t) === false && INTL_RX.test(t) && !PLACE_RX.some(([, rx]) => rx.test(t))) return INTL;
  if (/(^|[^a-z])lagos(?![a-z])/i.test(t)) return 'Lagos';
  const hits = PLACE_RX.filter(([, rx]) => rx.test(t)).map(([st]) => st); return hits.length ? hits[0] : '';
}
const zoneOf = st => st === 'Lagos' ? 'lagos' : st === INTL ? 'intl' : NG_STATES.includes(st) ? 'other' : '';

const money0 = (v, d) => { const n = parseInt(v, 10); return Number.isFinite(n) && n >= 0 ? n : d; };
const daysOk = (v, d) => /^\d{1,2}(-\d{1,2})?$/.test(String(v || '').trim()) ? String(v).trim() : d;
const deliveryCfg = s => ({ lagos: { fee: money0(s.ship_lagos, 4000), days: daysOk(s.days_lagos, '1-3') }, other: { fee: money0(s.ship_other, 6500), days: daysOk(s.days_other, '3-7') }, processing: s.process_time || '24 hours', freeOver: money0(s.free_over, 0), intlNote: s.intl_note || 'International orders: please contact us on WhatsApp for shipping rates.', cutoff: getGeneral().cutoff_text || '' });
const shipFor = (s, zone, sub, physical) => { const d = deliveryCfg(s); return physical && (zone === 'lagos' || zone === 'other') && !(d.freeOver && sub >= d.freeOver) ? d[zone].fee : 0; };
const etaText = (s, zone) => { const d = deliveryCfg(s); return `Orders are processed within ${d.processing}. Delivery takes ${zone === 'lagos' ? d.lagos.days + ' working days within Lagos' : d.other.days + ' working days outside Lagos'}.${d.cutoff ? ' ' + d.cutoff : ''}`; };
const csvCell = v => { let t = String(v == null ? '' : v); if (/^[=+\-@\t\r]/.test(t)) t = "'" + t; return '"' + t.replace(/"/g, '""') + '"'; };   // the leading quote stops spreadsheet formula injection
const contentRaw = () => { try { return JSON.parse((db.prepare("SELECT v FROM settings WHERE k='content'").get() || {}).v || 'null'); } catch { return null; } };

route('GET', '/api/health', () => ({ ok: true }));
route('GET', '/api/config', () => {
  const s = getSettings(), methods = Object.entries(PM).filter(([, m]) => !m.hidden).map(([id, m]) => { const st = methodState(id, s); return { id, label: m.label, desc: m.desc, group: m.manual ? 'direct' : 'online', available: st.ok, reason: st.reason || '' }; });
  return { paystack: !!psKey(), transfer: methodState('transfer', s).ok, dm: methodState('dm', s).ok, methods, whatsapp: s.whatsapp, instagram: s.instagram, announcement: s.announcement, codNote: s.cod_note, email: mailOn(), delivery: { ...deliveryCfg(s), states: NG_STATES, intl: INTL }, general: publicGeneral() };
});
route('GET', '/api/products', () => {
  const products = productsAll(), pc = pricingCfg(), t = Date.now(), sales = {};   // product.price stays the regular price; sales[id] is what customers pay now
  for (const p of products) { const s = saleFor(p, pc, t); if (s) sales[p.id] = { price: s.price, was: s.was, pct: s.pct, endsAt: s.endsAt, label: s.label }; }
  let pts = {}; try { pts = GLOWPTS ? GLOWPTS.all(products, sales) : {}; } catch {}
  return { products, rev: getRev('products_rev'), featured: FEATURED.ids(products), sales, pts };
});
/* admin: one call gives everything the Discounts & Points page needs */
route('GET', '/api/admin/pricing', { auth: 'admin' }, () => {
  const pc = pricingCfg(), t = Date.now(), prods = productsAll();
  const rows = prods.map(p => { const s = saleFor(p, pc, t), price = Math.round(+p.price) || 0; return { id: p.id, name: p.name, price, category: p.categoryLabel || p.category || '', brand: p.brand || '', keys: [p.category, p.categoryLabel, p.dept, p.deptLabel, p.tag].filter(Boolean).map(x => String(x).toLowerCase()), hidden: !!(p.hidden || p.draft), sale: s ? { price: s.price, pct: s.pct, src: s.src, label: s.label, endsAt: s.endsAt } : null, pts: GLOWPTS ? GLOWPTS.detail(p, s ? s.price : price) : null }; });
  const rules = pc.rules.map(r => ({ ...r, status: ruleStatus(r, t), count: prods.filter(p => ruleHit(r, p)).length }));
  return { cfg: pc, rules, rows, glowOn: GLOWPTS ? GLOWPTS.on() : false, brands: [...new Set(prods.map(p => p.brand).filter(Boolean))].sort(), categories: [...new Set(prods.map(p => p.categoryLabel || p.category).filter(Boolean))].sort(), now: new Date(t).toISOString() };
});
route('PUT', '/api/admin/pricing', { auth: 'admin', big: true }, ({ body }) => {
  const c = prcClean(body), regs = new Map(productsAll().map(p => [p.id, p]));
  for (const k of Object.keys(c.manual)) { const p = regs.get(k); if (!p) { delete c.manual[k]; continue; } const m = c.manual[k]; if (m.sale && m.sale >= Math.round(+p.price)) throw new HttpError(400, `The sale price of ${p.name} must be lower than its regular price (${naira(Math.round(+p.price))}).`); if (m.start && m.end && watMs(m.start, false) > watMs(m.end, true)) throw new HttpError(400, `The sale of ${p.name} ends before it starts.`); }
  for (const r of c.rules) if (r.start && r.end && watMs(r.start, false) > watMs(r.end, true)) throw new HttpError(400, `The rule "${r.name}" ends before it starts.`);
  db.prepare("INSERT INTO settings VALUES('pricing',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify(c)); try { SEOQ.ping(); } catch {}
  return { ok: true, manual: Object.keys(c.manual).length, rules: c.rules.length };
});
route('GET', '/api/brands', () => ({ brands: Object.fromEntries(db.prepare('SELECT name, logo FROM brands').all().map(r => [r.name, r.logo])) }));
route('GET', '/api/content', () => { const c = contentRaw(); return c ? { has: true, slides: c.slides || null, posts: (c.posts || []).filter(p => p.pub), bar: c.bar || '', interval: c.interval, overlay: c.overlay, guides: Array.isArray(c.guides) ? c.guides : null, guideOn: c.guideOn !== false, guideSecs: c.guideSecs || 6 } : { has: false }; });

/* accounts */
const pvOf = u => sha(u.pw).slice(0, 10);   // changes whenever the password changes, which signs every older login out
const userToken = u => sign({ sub: u.id, pv: pvOf(u), exp: now() + 30 * 864e5 });
route('POST', '/api/auth/register', { limit: ['reg', 8] }, ({ body }) => {
  const name = str(body.name, 80), email = str(body.email, 254).toLowerCase(), phone = str(body.phone, 30), address = str(body.address, 400), pw = String(body.password || '');
  if (!name) throw new HttpError(400, 'Enter your name.'); if (!emailOk(email)) throw new HttpError(400, 'Enter a valid email address.');
  if (pw.length < 8 || pw.length > 128) throw new HttpError(400, 'Password must be 8 to 128 characters.');
  if (db.prepare('SELECT 1 FROM users WHERE email=?').get(email)) throw new HttpError(409, 'An account with this email already exists. Sign in instead.');
  const id = db.prepare('INSERT INTO users(name,email,phone,address,pw,created) VALUES(?,?,?,?,?,?)').run(name, email, phone, address, hashPw(pw), now()).lastInsertRowid;
  if (body.newsletter) subscribe(email, name);
  const u = db.prepare('SELECT * FROM users WHERE id=?').get(id);
  inbox(id, 'welcome', 'Welcome to Skinsentials22', 'Your account is ready. Order updates, payment confirmations and rewards will show up here.', '', 'welcome');
  return { token: userToken(u), user: userOut(u) };
});
route('POST', '/api/auth/login', { limit: ['login', 10] }, ({ body }) => {
  const em = str(body.email, 254).toLowerCase(), failKey = 'loginF' + em, fails = (hits.get(failKey) || []).filter(x => now() - x < 900000);
  if (fails.length >= 8) throw new HttpError(429, 'Too many wrong passwords for this account. Please wait 15 minutes, or use "Forgot password".');
  const u = db.prepare('SELECT * FROM users WHERE email=?').get(em);
  const ok = u ? checkPw(String(body.password || ''), u.pw) : (crypto.scryptSync('x', 'y', 64), false);
  if (!ok) { fails.push(now()); hits.set(failKey, fails); throw new HttpError(401, 'Incorrect email or password.'); }
  hits.delete(failKey);
  return { token: userToken(u), user: userOut(u) };
});
route('GET', '/api/auth/me', { auth: 'user' }, ({ user }) => ({ user: userOut(user) }));
route('PATCH', '/api/auth/me', { auth: 'user' }, ({ user, body }) => {
  const name = str(body.name, 80) || user.name, phone = body.phone == null ? user.phone : str(body.phone, 30), address = body.address == null ? (user.address || '') : str(body.address, 400);
  db.prepare('UPDATE users SET name=?, phone=?, address=? WHERE id=?').run(name, phone, address, user.id);
  if (body.newsletter === true) subscribe(user.email, name); else if (body.newsletter === false) db.prepare('UPDATE subscribers SET active=0 WHERE email=?').run(user.email);
  return { user: userOut(db.prepare('SELECT * FROM users WHERE id=?').get(user.id)) };
});
route('POST', '/api/auth/password', { auth: 'user', limit: ['pw', 6] }, ({ user, body }) => {
  if (!checkPw(String(body.current || ''), user.pw)) throw new HttpError(401, 'Current password is incorrect.');
  const n = String(body.password || ''); if (n.length < 8 || n.length > 128) throw new HttpError(400, 'New password must be 8 to 128 characters.');
  db.prepare('UPDATE users SET pw=? WHERE id=?').run(hashPw(n), user.id); inbox(user.id, 'security', 'Your password was changed', 'If this was not you, reset your password straight away and contact us.', ''); return { ok: true, token: userToken(db.prepare('SELECT * FROM users WHERE id=?').get(user.id)) };
});
/* forgot password: emails a one-hour link when email sending is set up (RESEND_API_KEY); always answers the same so it never reveals who has an account */
route('POST', '/api/auth/forgot', { limit: ['forgot', 5] }, ({ body }) => {
  const email = str(body.email, 254).toLowerCase(); if (!emailOk(email)) throw new HttpError(400, 'Enter a valid email address.');
  const u = db.prepare('SELECT * FROM users WHERE email=?').get(email);
  if (u && mailOn()) {
    const t = crypto.randomBytes(32).toString('base64url'); db.prepare('DELETE FROM resets WHERE user_id=? OR exp<?').run(u.id, now()); db.prepare('INSERT INTO resets VALUES(?,?,?)').run(sha(t), u.id, now() + 36e5);
    quiet(sendMail(u.email, 'Reset your Skinsentials22 password', mailShell('Reset your password', `<p>Hi ${hx(u.name)}, use the button below to choose a new password. The link works for one hour.</p><p><a href="${SITE}/?reset=${t}" style="background:#1b2a3a;color:#fff;padding:12px 22px;border-radius:24px;text-decoration:none;display:inline-block">Choose a new password</a></p><p style="color:#778;font-size:13px">If you did not ask for this, you can ignore this email.</p>`)));
  }
  return { ok: true, email: mailOn() };
});
route('POST', '/api/auth/reset', { limit: ['reset', 8] }, ({ body }) => {
  const pw = String(body.password || ''); if (pw.length < 8 || pw.length > 128) throw new HttpError(400, 'Password must be 8 to 128 characters.');
  const r = db.prepare('SELECT * FROM resets WHERE hash=?').get(sha(String(body.token || ''))); if (!r || r.exp < now()) throw new HttpError(400, 'This reset link has expired or was already used. Please request a new one.');
  db.prepare('UPDATE users SET pw=? WHERE id=?').run(hashPw(pw), r.user_id); db.prepare('DELETE FROM resets WHERE user_id=?').run(r.user_id);
  inbox(r.user_id, 'security', 'Your password was reset', 'If this was not you, contact us straight away.', '');
  const u = db.prepare('SELECT * FROM users WHERE id=?').get(r.user_id); return { token: userToken(u), user: userOut(u) };
});

/* favourites */
route('GET', '/api/likes', { auth: 'user' }, ({ user }) => ({ likes: db.prepare('SELECT product_id FROM likes WHERE user_id=? ORDER BY created').all(user.id).map(r => r.product_id) }));
route('PUT', '/api/likes/:pid', { auth: 'user' }, ({ user, params }) => {
  if (!db.prepare('SELECT 1 FROM products WHERE id=?').get(params.pid)) throw new HttpError(404, 'Product not found');
  db.prepare('INSERT OR IGNORE INTO likes VALUES(?,?,?)').run(user.id, params.pid, now()); return { liked: true };
});
route('DELETE', '/api/likes/:pid', { auth: 'user' }, ({ user, params }) => { db.prepare('DELETE FROM likes WHERE user_id=? AND product_id=?').run(user.id, params.pid); return { liked: false }; });
route('POST', '/api/likes/merge', { auth: 'user' }, ({ user, body }) => {
  const ins = db.prepare('INSERT OR IGNORE INTO likes SELECT ?, id, ? FROM products WHERE id=?');
  (Array.isArray(body.ids) ? body.ids.slice(0, 200) : []).forEach(id => ins.run(user.id, now(), str(id, 64)));
  return { likes: db.prepare('SELECT product_id FROM likes WHERE user_id=? ORDER BY created').all(user.id).map(r => r.product_id) };
});

/* newsletter */
route('POST', '/api/subscribe', { limit: ['sub', 6] }, ({ body }) => {
  const email = str(body.email, 254).toLowerCase(); if (!emailOk(email)) throw new HttpError(400, 'Enter a valid email address.');
  return { ok: true, created: subscribe(email, str(body.name, 80)) };
});
route('GET', '/api/unsubscribe', ({ query }) => {
  const r = db.prepare('UPDATE subscribers SET active=0 WHERE token=?').run(str(query.get('token'), 80));
  return { html: `<!doctype html><meta name=viewport content="width=device-width"><body style="font-family:sans-serif;text-align:center;padding:4rem"><h2>${r.changes ? 'You have been unsubscribed.' : 'Link not recognised.'}</h2><a href="/">Back to Skinsentials22</a>` };
});

/* orders */
const ORDER_SQL = 'SELECT o.*, p.status AS pstatus, p.channel AS pchannel, p.detail AS pdetail, p.fees AS pfees, p.received AS preceived, p.id AS pid FROM orders o LEFT JOIN payments p ON p.order_id=o.id';
route('POST', '/api/orders', { auth: 'optional', limit: ['order', 10] }, async ({ user, body }) => {
  const s = getSettings(), method = str(body.method, 12), c = body.customer || {};
  if (!PM[method]) throw new HttpError(400, 'Choose a payment method.');
  const ms = methodState(method, s); if (!ms.ok) throw new HttpError(503, `${PM[method].label} is not available right now (${ms.reason.toLowerCase()}). Please choose another payment method.`);
  const customer = str(c.name, 80), email = str(c.email, 254).toLowerCase(), phone = str(c.phone, 30), address = str(c.address, 400);
  if (!customer || !emailOk(email) || !phone || !address) throw new HttpError(400, 'Please fill in all delivery details.');
  if (!Array.isArray(body.items) || !body.items.length || body.items.length > 50) throw new HttpError(400, 'Your bag is empty.');
  const prods = new Map(pricedAll().map(p => [p.id, p])), lines = []; let total = 0, count = 0;
  for (const it of body.items) {
    const qty = Math.floor(+it.qty); if (!(qty >= 1 && qty <= 20)) throw new HttpError(400, 'Invalid quantity.');
    let line;
    if (it.giftCard) { const a = Math.floor(+it.price); if (!(a >= 2000 && a <= 500000)) throw new HttpError(400, 'Gift card amount must be ₦2,000 to ₦500,000.'); line = { id: 'giftcard', name: str(it.name, 80) || 'Gift Card', price: a, qty, note: str(it.note, 200) }; }
    else { const p = prods.get(str(it.id, 64)); if (!p) throw new HttpError(400, 'A product in your bag is no longer available. Please refresh the page.'); line = { id: p.id, name: p.name, price: Math.round(p.price), qty }; }
    lines.push(line); total += line.price * qty; count += qty;   // price always comes from the database, never the browser
  }
  const needs = lineNeeds(lines), st0 = stockMap();
  for (const [id, n] of needs) if (st0.has(id) && st0.get(id) < n) throw new HttpError(409, st0.get(id) <= 0 ? `${prods.get(id).name} is sold out. Please remove it from your bag.` : `Only ${st0.get(id)} left of ${prods.get(id).name}. Please lower the quantity.`);
  const physical = lines.some(l => l.id !== 'giftcard'); let state = str(c.state, 40), zone = '';
  if (physical) {
    if (!state) state = detectPlace(address); if (!state) throw new HttpError(400, 'Please choose your state so we can work out delivery.');
    zone = zoneOf(state); if (!zone) throw new HttpError(400, 'Please choose a valid state.');
    if (zone === 'intl') throw new HttpError(422, deliveryCfg(s).intlNote);
    const seen = detectPlace(address); if (zone === 'lagos' && seen && seen !== 'Lagos' && seen !== INTL && !/lagos/i.test(address)) throw new HttpError(400, `Your address looks like it is in ${seen}, but Lagos is selected. Please check your state so the delivery fee is right.`);
  } else { state = state || 'Lagos'; zone = zoneOf(state) || 'lagos'; }
  const sub = total, shipping = shipFor(s, zone, sub, physical);
  const gcode = str(body.glow, 40).toUpperCase().replace(/\s/g, ''), gp0 = gcode ? GLOW.price({ code: gcode, user, email, phone, lines }) : { discount: 0 };
  if (gcode && !gp0.discount) throw new HttpError(400, gp0.message || 'This Glow Friend code cannot be used on this order.');   // a Glow Friend code replaces any other referral code on the order
  const rp = REF ? REF.price({ code: gcode ? '' : str(body.referral, 20).toUpperCase(), useCredit: !!body.useCredit && !!user, strict: true, user, email, phone, address, lines, sub, ship: shipping, state, method }) : { discount: 0, credit: 0, flags: [] };   // discount and credit are decided here, never by the browser
  const gp = { ...gp0, discount: Math.min(gp0.discount, Math.max(0, sub - rp.discount - rp.credit)) };
  total = sub - rp.discount - rp.credit - gp.discount + shipping;
  const ref = 'SK22-' + crypto.randomBytes(5).toString('hex').toUpperCase();
  let authUrl = '';
  if (!PM[method].manual) { const b2 = { email, amount: total * 100, currency: 'NGN', reference: ref, callback_url: `${SITE}/?pay=${ref}`, metadata: { customer, phone, method } }; if (PM[method].ps) b2.channels = PM[method].ps; authUrl = (await paystack('POST', '/transaction/initialize', b2)).authorization_url; }
  /* order + payment record + stock hold happen together or not at all */
  const o = tx(() => {
    for (const [id, n] of needs) if (db.prepare('SELECT 1 FROM stock WHERE product_id=?').get(id) && !db.prepare('UPDATE stock SET qty=qty-? WHERE product_id=? AND qty>=?').run(n, id, n).changes) throw new HttpError(409, `${prods.get(id).name} has just sold out. Please update your bag.`);
    const t = now(), info = db.prepare('INSERT INTO orders(ref,user_id,customer,email,phone,address,items,item_count,total,shipping,method,created,state,zone) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run(ref, user ? user.id : null, customer, email, phone, address, JSON.stringify(lines), count, total, shipping, method, t, state, zone);
    const pid = db.prepare('INSERT INTO payments(order_id,ref,method,amount,status,created,updated) VALUES(?,?,?,?,?,?,?)').run(info.lastInsertRowid, ref, method, total, 'pending', t, t).lastInsertRowid;
    ev(pid, 'order', `New order: ${naira(total)} via ${label(method)}${PM[method].manual ? '' : ', waiting for the customer to pay'}`, 1);
    if (REF) REF.attach(info.lastInsertRowid, rp, user);
    if (gp.discount) { GLOW.attach(info.lastInsertRowid, gp); db.prepare('UPDATE orders SET glow_disc=? WHERE id=?').run(gp.discount, info.lastInsertRowid); }
    return db.prepare('SELECT * FROM orders WHERE id=?').get(info.lastInsertRowid);
  });
  const out = { ref, glowPoints: GLOW.estimate(lines), id: oid(o), total, discount: rp.discount + gp.discount, credit: rp.credit, shipping, method, state, zone, eta: physical ? etaText(s, zone) : '' };
  if (!PM[method].manual) out.authorization_url = authUrl;
  else if (method === 'transfer') { const accs = accountsOf(s); out.bank = { ...accs[0], narration: ref }; out.accounts = accs; }
  else if (method === 'cod') out.cod = { note: s.cod_note || '' };
  else out.dm = dmLinks(o, s);
  inbox(o.user_id, 'order', `Order ${oid(o)} received`, `We have your order of ${naira(total)}. ` + (method === 'transfer' ? `Transfer exactly ${naira(total)} using reference ${ref}. We confirm it as soon as the money arrives.` : method === 'cod' ? 'You will pay on delivery. We will confirm your order shortly.' : method === 'dm' ? 'We will confirm your order with you on chat shortly.' : 'It is confirmed as soon as your payment goes through.'), 'orders', 'new:' + o.id);
  quiet(notifyAdmin(`🛒 New order ${oid(o)}: ${naira(total)}`, [`${customer} · ${phone}`, `Pay by: ${label(method)}`, `Items: ${lines.map(l => l.qty + 'x ' + l.name).join(', ')}`, `Deliver to: ${address}`, `Ref: ${ref}`]));
  if (PM[method].manual) quiet(sendMail(email, `We received your order ${oid(o)}`, mailShell('Thank you for your order!', `<p>Hi ${hx(customer)}, we have your order <b>${oid(o)}</b>.</p>${linesHtml(o)}${GLOW.note(o)}${physical ? `<p style="color:#556">${hx(etaText(s, zone))}</p>` : ''}${method === 'transfer' ? `<p><b>Please transfer ${naira(total)}</b> to one of these accounts using <b>${hx(ref)}</b> as the narration:</p>${out.accounts.map(a => `<p style="background:#f4f1ea;padding:10px;border-radius:8px">${hx(a.bank_name)}<br><b>${hx(a.account_number)}</b><br>${hx(a.account_name)}</p>`).join('')}<p>Then tap "I have made the transfer" on the website and we will confirm.</p>` : method === 'cod' ? `<p>You will pay <b>${naira(total)}</b> when it arrives. We will call to confirm delivery.</p>` : '<p>Please continue the chat with us on WhatsApp or Instagram to agree payment.</p>'}`)));
  return out;
});
route('GET', '/api/notifications', { auth: 'user' }, ({ user }) => ({ notifications: db.prepare('SELECT id,type,title,body,link,created,seen FROM notifications WHERE user_id=? ORDER BY id DESC LIMIT 50').all(user.id).map(n => ({ ...n, seen: !!n.seen })), unread: db.prepare('SELECT COUNT(*) n FROM notifications WHERE user_id=? AND seen=0').get(user.id).n }));
route('POST', '/api/notifications/read', { auth: 'user', limit: ['nread', 60] }, ({ user, body }) => { if (body && body.id) db.prepare('UPDATE notifications SET seen=1 WHERE user_id=? AND id=?').run(user.id, (+body.id) | 0); else db.prepare('UPDATE notifications SET seen=1 WHERE user_id=?').run(user.id); return { ok: true }; });
route('POST', '/api/notifications/clear', { auth: 'user', limit: ['nclear', 20] }, ({ user }) => { db.prepare('DELETE FROM notifications WHERE user_id=?').run(user.id); return { ok: true }; });
route('GET', '/api/orders/mine', { auth: 'user' }, ({ user }) => ({ orders: db.prepare(ORDER_SQL + ' WHERE o.user_id=? ORDER BY o.id DESC LIMIT 100').all(user.id).map(orderOut) }));   // by account only: an email address alone never unlocks anyone's orders
route('GET', '/api/orders/:ref/verify', { limit: ['verify', 20] }, async ({ params }) => {
  const o = orderBy(params.ref); if (!o) throw new HttpError(404, 'Order not found');
  const paid = !PM[o.method].manual ? await verifyOnline(o) : !!o.paid; return { id: oid(o), ref: o.ref, paid, total: o.total };
});
route('POST', '/api/orders/:ref/transfer-sent', { limit: ['ts', 10] }, ({ params, body }) => {
  const o = orderBy(params.ref); if (!o || o.method !== 'transfer') throw new HttpError(404, 'Order not found');
  const p = payOf(o.id); if (p.status === 'paid') return { ok: true }; if (o.status === 'Cancelled' || p.status === 'cancelled' || p.status === 'refunded') throw new HttpError(409, 'This order was cancelled. Please place a new order.');
  const note = str(body.note, 300);
  tx(() => { db.prepare('UPDATE orders SET claimed=1, note=? WHERE id=?').run(note, o.id); db.prepare("UPDATE payments SET status='review', updated=? WHERE id=?").run(now(), p.id); ev(p.id, 'review', `Customer says they sent ${naira(o.total)}. Please check your bank and confirm.${note ? ' Note: ' + note : ''}`, 1); });
  quiet(notifyAdmin(`🔔 Confirm a transfer: ${naira(o.total)}`, [`Order ${oid(o)} · ${o.customer} · ${o.phone}`, `Ref/narration: ${o.ref}`, 'Check your bank app, then press Confirm in Admin > Payments.']));
  return { ok: true };
});
route('POST', '/api/paystack/webhook', { raw: true }, async ({ raw, req }) => {
  const key = psKey(); if (!key) throw new HttpError(401, 'Bad signature');
  const sig = crypto.createHmac('sha512', key).update(raw).digest('hex'), given = String(req.headers['x-paystack-signature'] || '');
  if (sig.length !== given.length || !crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(given))) throw new HttpError(401, 'Bad signature');
  const evt = JSON.parse(raw.toString() || '{}');
  if (evt.event === 'charge.dispute.create') { const o = orderBy(str(evt.data && evt.data.transaction && evt.data.transaction.reference, 40)); if (o) refHook('onReverse', o, 'Chargeback opened with the payment provider', 'hold'); }
  if (evt.event === 'charge.success') { const o = orderBy(str(evt.data && evt.data.reference, 40)); if (o) await verifyOnline(o).catch(e => console.error('Webhook verify failed:', e.message)); }
  return { ok: true };
});

/* admin */
const adminHash = () => (db.prepare("SELECT v FROM settings WHERE k='admin_pw'").get() || {}).v || '';
const adminPwOk = pw => { const h = adminHash(); if (h) return checkPw(pw, h); if (!ENV.ADMIN_PASSWORD) return false; const x = v => crypto.createHash('sha256').update(String(v)).digest(); return crypto.timingSafeEqual(x(pw), x(ENV.ADMIN_PASSWORD)); };
const adminToken = () => sign({ role: 'admin', v: getRev('admin_pwv'), exp: now() + 12 * 36e5 });
if (ENV.RESET_ADMIN_PASSWORD === '1') { db.prepare("DELETE FROM settings WHERE k='admin_pw'").run(); bumpRev('admin_pwv'); console.log('Admin password reset: the ADMIN_PASSWORD from .env works again. Remove RESET_ADMIN_PASSWORD now.'); }
route('POST', '/api/admin/login', { limit: ['adm', 5] }, ({ body }) => {
  if (!adminPwOk(String(body.password || ''))) throw new HttpError(401, 'Incorrect password.');
  return { token: adminToken() };
});
route('POST', '/api/admin/password', { auth: 'admin', limit: ['admpw', 5] }, ({ body }) => {
  if (!adminPwOk(String(body.current || ''))) throw new HttpError(401, 'Current password is incorrect.');
  const n = String(body.password || ''); if (n.length < 10 || n.length > 128) throw new HttpError(400, 'Use at least 10 characters for the admin password.');
  db.prepare("INSERT INTO settings VALUES('admin_pw',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(hashPw(n)); bumpRev('admin_pwv'); return { ok: true, token: adminToken() };
});
const adminOrder = id => { const n = parseInt(String(id).replace('#', ''), 10) - 1000; return db.prepare(ORDER_SQL + ' WHERE o.id=?').get(n); };
route('GET', '/api/admin/orders', { auth: 'admin' }, () => ({ orders: db.prepare(ORDER_SQL + ' ORDER BY o.id').all().map(orderOut) }));
route('PATCH', '/api/admin/orders/:id', { auth: 'admin' }, ({ params, body }) => {
  let o = adminOrder(params.id); if (!o) throw new HttpError(404, 'Order not found');
  if (body.status != null) {
    if (!['Pending', 'Shipped', 'Delivered', 'Cancelled'].includes(body.status)) throw new HttpError(400, 'Invalid status');
    if (o.status === 'Cancelled' && body.status !== 'Cancelled') throw new HttpError(409, 'A cancelled order cannot be reopened (its stock was released). Ask the customer to place a new order.');
    if (body.status === 'Cancelled') { if (o.status !== 'Cancelled') cancelOrder(o, 'Order cancelled by admin'); } else db.prepare('UPDATE orders SET status=? WHERE id=?').run(body.status, o.id);
    if (['Shipped', 'Delivered'].includes(body.status) && o.status !== body.status) { const fo = db.prepare('SELECT * FROM orders WHERE id=?').get(o.id); inbox(fo.user_id, 'order', body.status === 'Shipped' ? `Order ${oid(fo)} is on its way` : `Order ${oid(fo)} was delivered`, body.status === 'Shipped' ? 'Your order has been handed over for delivery. Please keep your phone close.' : 'Your order is marked as delivered. We hope you love it. A review helps other shoppers.', 'orders', 'st:' + fo.id + ':' + body.status); }
  }
  if (typeof body.paid === 'boolean') { const fresh = db.prepare('SELECT * FROM orders WHERE id=?').get(o.id); if (body.paid) { if (fresh.status === 'Cancelled') throw new HttpError(409, 'This order is cancelled.'); settle(fresh, { by: 'admin' }); } else unsettle(fresh); }
  return { order: orderOut(adminOrder(params.id)) };
});
route('GET', '/api/admin/backup/db', { auth: 'admin', limit: ['bkdb', 5] }, () => {   // a complete, restorable copy of the database (products, posts, orders, customers, settings)
  const dir = path.join(DATA, 'tmp'); fs.mkdirSync(dir, { recursive: true }); const f = path.join(dir, 'dl-' + crypto.randomBytes(6).toString('hex') + '.db');
  try { db.exec(`VACUUM INTO '${f.replace(/'/g, "''")}'`); const data = fs.readFileSync(f), d = new Date(), z = n => String(n).padStart(2, '0'); return { bin: { data, name: `skinsentials22-store-${d.getFullYear()}${z(d.getMonth() + 1)}${z(d.getDate())}-${z(d.getHours())}${z(d.getMinutes())}.db` } }; }
  finally { try { fs.unlinkSync(f); } catch {} }
});
route('PUT', '/api/admin/products', { auth: 'admin', big: true }, ({ body }) => {
  const list = body.products; if (!Array.isArray(list) || list.length > 500) throw new HttpError(400, 'Invalid product list');
  if (+body.rev !== getRev('products_rev')) throw new HttpError(409, 'The catalogue was changed somewhere else (another tab or device). Reload this page, then repeat your change.');
  if (!list.length && db.prepare('SELECT 1 FROM products').get() && !body.allowEmpty) throw new HttpError(400, 'Refusing to delete every product. Reload the page and try again.');
  const seen = new Set(); for (const p of list) { if (!p || typeof p.id !== 'string' || !p.id || p.id.length > 64 || !/^[\w.-]+$/.test(p.id) || seen.has(p.id) || typeof p.name !== 'string' || !(+p.price >= 0)) throw new HttpError(400, 'Each product needs a unique id, a name and a valid price.'); seen.add(p.id); p.price = Math.round(+p.price); delete p.stock;
    p.name = plainShort(p.name, 160); if (!p.name) throw new HttpError(400, 'Each product needs a name.');
    if (p.tag != null) p.tag = plainShort(p.tag, 60); p.rating = Math.min(5, Math.max(0, +p.rating || 0));
    if (p.image != null) { p.image = typeof p.image === 'string' ? str(p.image, 600000) : ''; if (p.image && !PIMG_OK.test(p.image)) p.image = ''; }
    if (p.images != null) p.images = (Array.isArray(p.images) ? p.images : []).map(u => str(u, 600000)).filter(u => PIMG_OK.test(u)).slice(0, 12);
    if (p.videos != null) p.videos = (Array.isArray(p.videos) ? p.videos : []).map(u => str(u, 2000)).filter(u => PVID_OK.test(u)).slice(0, 10);
    for (const k of ['brand', 'dept', 'deptLabel', 'category', 'categoryLabel', 'usageWhen']) if (p[k] != null) p[k] = plainShort(p[k], 80);
    for (const [k, n] of [['usage', 3000], ['usageTips', 800], ['usageWarn', 600], ['ingredients', 3000], ['description', 4000]]) if (p[k] != null) p[k] = plainTxt(p[k], n);
    if (p.usageSteps != null) p.usageSteps = (Array.isArray(p.usageSteps) ? p.usageSteps : []).map(x => plainTxt(x, 500)).filter(Boolean).slice(0, 20);
    if (Array.isArray(p.videos)) p.hasVideo = p.hasVideo || p.videos.length > 0; }
  const rev = tx(() => { db.exec('DELETE FROM products'); const ins = db.prepare('INSERT INTO products VALUES(?,?,?)'); list.forEach((p, i) => ins.run(p.id, i, JSON.stringify(p))); for (const r of db.prepare('SELECT product_id FROM stock').all()) if (!seen.has(r.product_id)) db.prepare('DELETE FROM stock WHERE product_id=?').run(r.product_id); return bumpRev('products_rev'); });
  SEOQ.ping(); return { ok: true, count: list.length, rev };
});
/* stock lives in its own table so a busy shop and an open admin page never overwrite each other's numbers; blank = unlimited */
route('PUT', '/api/admin/stock', { auth: 'admin' }, ({ body }) => {
  const id = str(body.id, 64); if (!db.prepare('SELECT 1 FROM products WHERE id=?').get(id)) throw new HttpError(404, 'Product not found');
  if (body.qty === null || body.qty === '' || body.qty === undefined) { db.prepare('DELETE FROM stock WHERE product_id=?').run(id); return { ok: true, stock: null }; }
  const q = Math.floor(+body.qty); if (!(q >= 0 && q <= 1e6)) throw new HttpError(400, 'Stock must be a whole number from 0 up.');
  db.prepare('INSERT INTO stock VALUES(?,?) ON CONFLICT(product_id) DO UPDATE SET qty=excluded.qty').run(id, q); return { ok: true, stock: q };
});
/* hero slides, blog posts and the announcement bar: saved on the server so every visitor sees them */
route('GET', '/api/admin/content', { auth: 'admin' }, () => { const c = contentRaw(); return { has: !!c, ...(c || {}), rev: getRev('content_rev') }; });
route('PUT', '/api/admin/content', { auth: 'admin', big: true }, ({ body }) => {
  if (+body.rev !== getRev('content_rev')) throw new HttpError(409, 'The website content was changed somewhere else. Reload this page, then repeat your change.');
  const urlOk = u => typeof u === 'string' && u.length <= 600000 && /^(\/uploads\/[\w.-]+|https?:\/\/[^\s]+|data:image\/(jpeg|png|webp|gif);base64,[A-Za-z0-9+\/=]+)$/.test(u);
  const slides = body.slides == null ? null : (Array.isArray(body.slides) ? body.slides.filter(urlOk).slice(0, 20) : null);
  /* Featured Guide on the homepage: null = automatic (plays the videos uploaded to products), [] = custom but empty (section hidden) */
  const vidOk = u => typeof u === 'string' && u.length <= 2000 && /^(\/uploads\/[\w.-]+|https:\/\/[^\s]+)$/.test(u);
  const guides = body.guides == null ? null : (Array.isArray(body.guides) ? body.guides.slice(0, 30).map(g => ({ id: str(g && g.id, 40) || 'g' + now(), title: str(g && g.title, 120), kicker: str(g && g.kicker, 60), video: vidOk(g && g.video) ? g.video : '', poster: urlOk(g && g.poster) ? g.poster : '', pid: str(g && g.pid, 60) })).filter(g => g.video || g.poster) : null);
  const posts = (Array.isArray(body.posts) ? body.posts : []).slice(0, 200).map(p => ({ id: str(p && p.id, 40) || 'b' + now(), title: str(p && p.title, 160), cat: str(p && p.cat, 60), date: +(p && p.date) || now(), pub: !!(p && p.pub), cover: urlOk(p && p.cover) ? p.cover : '', excerpt: str(p && p.excerpt, 400), body: str(p && p.body, 30000) })).filter(p => p.title && p.body);
  const c = { slides: slides && slides.length ? slides : null, guides, guideOn: body.guideOn !== false, guideSecs: Math.min(30, Math.max(3, Math.round(+body.guideSecs) || 6)), posts, bar: str(body.bar, 300), interval: Math.min(60, Math.max(2, Math.round(+body.interval) || 5)), overlay: Math.min(0.95, Math.max(0, +body.overlay >= 0 ? +body.overlay : 0.78)) };
  db.prepare("INSERT INTO settings VALUES('content',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify(c)); SEOQ.ping(); return { ok: true, rev: bumpRev('content_rev') };
});

/* ---- payments: everything customers pay is recorded here for the admin ---- */
const PAY_SQL = 'SELECT p.*, o.customer, o.email, o.phone, o.item_count, o.status AS ostatus FROM payments p JOIN orders o ON o.id=p.order_id';
const payOut = p => ({ id: p.id, order: '#' + (1000 + p.order_id), ref: p.ref, method: p.method, methodLabel: label(p.method), amount: p.amount, received: p.received, fees: p.fees, status: p.status, channel: p.channel, detail: p.detail, paidAt: p.paid_at, confirmedBy: p.confirmed_by, note: p.note, created: p.created, customer: p.customer, email: p.email, phone: p.phone, items: p.item_count, orderStatus: p.ostatus });
function payFilter(q) {
  const w = [], a = [], st = str(q.get('status'), 20), me = str(q.get('method'), 12), text = str(q.get('q'), 80), from = +q.get('from') || 0, to = +q.get('to') || 0;
  if (st === 'attention') w.push("p.status IN ('review','mismatch')"); else if (st) { w.push('p.status=?'); a.push(st); }
  if (me) { w.push('p.method=?'); a.push(me); }
  if (text) { w.push('(p.ref LIKE ? OR o.customer LIKE ? OR o.email LIKE ? OR o.phone LIKE ? OR (? GLOB \'#[0-9]*\' AND p.order_id = ? - 1000))'); const l = '%' + text.replace(/[%_]/g, '') + '%'; a.push(l, l, l, l, text, parseInt(text.slice(1), 10) || 0); }
  if (from) { w.push('p.created>=?'); a.push(from); } if (to) { w.push('p.created<?'); a.push(to); }
  return { where: w.length ? ' WHERE ' + w.join(' AND ') : '', args: a };
}
function paySummary(tz) {
  const t = now(), local = t - tz * 6e4, d0 = Math.floor(local / 864e5) * 864e5 + tz * 6e4, sum = (sql, ...a) => db.prepare(sql).get(...a);
  const paidSum = from => (sum("SELECT COALESCE(SUM(received),0) v, COUNT(*) n FROM payments WHERE status='paid' AND paid_at>=?", from));
  const days = [...Array(14)].map((_, i) => ({ start: d0 - (13 - i) * 864e5, v: 0 }));
  for (const r of db.prepare("SELECT received, paid_at FROM payments WHERE status='paid' AND paid_at>=?").all(days[0].start)) { const d = days.find(x => r.paid_at >= x.start && r.paid_at < x.start + 864e5); if (d) d.v += r.received; }
  const wait = sum("SELECT COUNT(*) n, COALESCE(SUM(amount),0) v FROM payments WHERE status IN ('pending','review') AND order_id IN (SELECT id FROM orders WHERE status!='Cancelled')");
  return {
    received: sum("SELECT COALESCE(SUM(received),0) v, COUNT(*) n, COALESCE(SUM(fees),0) f FROM payments WHERE status='paid'"), today: paidSum(d0), week: paidSum(d0 - 6 * 864e5), month: paidSum(d0 - 29 * 864e5),
    refunded: sum("SELECT COALESCE(SUM(amount),0) v, COUNT(*) n FROM payments WHERE status='refunded'"), waiting: wait, attention: sum("SELECT COUNT(*) n FROM payments WHERE status IN ('review','mismatch')").n,
    byMethod: db.prepare("SELECT method, COUNT(*) n, COALESCE(SUM(received),0) v FROM payments WHERE status='paid' GROUP BY method ORDER BY v DESC").all().map(r => ({ ...r, label: label(r.method) })),
    daily: days.map(d => ({ day: d.start, v: d.v }))
  };
}
route('GET', '/api/admin/payments', { auth: 'admin' }, ({ query }) => {
  const f = payFilter(query), lim = Math.min(200, Math.max(1, +query.get('limit') || 50)), off = Math.max(0, +query.get('offset') || 0);
  const total = db.prepare('SELECT COUNT(*) n, COALESCE(SUM(p.amount),0) v FROM payments p JOIN orders o ON o.id=p.order_id' + f.where).get(...f.args);
  return { payments: db.prepare(PAY_SQL + f.where + ' ORDER BY p.created DESC, p.id DESC LIMIT ? OFFSET ?').all(...f.args, lim, off).map(payOut), count: total.n, sum: total.v, summary: paySummary(+query.get('tz') || 0) };
});
route('GET', '/api/admin/payments.csv', { auth: 'admin' }, ({ query }) => {
  const f = payFilter(query), d = t => t ? new Date(t).toISOString().replace('T', ' ').slice(0, 19) : '';
  const rows = db.prepare(PAY_SQL + f.where + ' ORDER BY p.created DESC, p.id DESC').all(...f.args);
  return { csv: ['Date', 'Order', 'Reference', 'Customer', 'Email', 'Phone', 'Method', 'Channel', 'Bank / card', 'Amount (NGN)', 'Received (NGN)', 'Fees (NGN)', 'Status', 'Paid at', 'Confirmed by', 'Note'].map(csvCell).join(',') + '\n' + rows.map(r => [d(r.created), '#' + (1000 + r.order_id), r.ref, r.customer, r.email, r.phone, label(r.method), r.channel, r.detail, r.amount, r.received, r.fees, r.status, d(r.paid_at), r.confirmed_by, r.note].map(csvCell).join(',')).join('\n'), name: 'payments.csv' };
});
const payRow = id => { const r = db.prepare(PAY_SQL + ' WHERE p.id=?').get(+id); if (!r) throw new HttpError(404, 'Payment not found'); return r; };
const payDetail = id => { const p = payRow(id), o = db.prepare('SELECT * FROM orders WHERE id=?').get(p.order_id); return { payment: payOut(p), events: db.prepare('SELECT at,type,text FROM payment_events WHERE payment_id=? ORDER BY id').all(p.id), lines: JSON.parse(o.items), shipping: o.shipping || 0, state: o.state || '', zone: o.zone || '', address: o.address, canRefund: p.status === 'paid' };
};
route('GET', '/api/admin/payments/:id', { auth: 'admin' }, ({ params }) => payDetail(params.id));
route('POST', '/api/admin/payments/:id/confirm', { auth: 'admin' }, ({ params, body }) => {
  const p = payRow(params.id), o = db.prepare('SELECT * FROM orders WHERE id=?').get(p.order_id);
  if (p.status === 'paid') return payDetail(p.id); if (o.status === 'Cancelled' || ['refunded', 'cancelled'].includes(p.status)) throw new HttpError(409, 'This order was cancelled or refunded.');
  const got = body.received == null || body.received === '' ? p.amount : Math.floor(+body.received); if (!(got > 0 && got <= 1e8)) throw new HttpError(400, 'Enter the amount you received.');
  settle(o, { by: 'admin', received: got, note: str(body.note, 300) });
  if (got !== p.amount) ev(p.id, 'note', `Amount received (${naira(got)}) differs from the order total (${naira(p.amount)}). Confirmed by admin.`, 0);
  return payDetail(p.id);
});
route('POST', '/api/admin/payments/:id/reject', { auth: 'admin' }, ({ params, body }) => {
  const p = payRow(params.id); if (!['pending', 'review', 'mismatch'].includes(p.status)) throw new HttpError(409, 'Only unpaid payments can be marked as not received.');
  const note = str(body.note, 300);
  tx(() => { db.prepare("UPDATE payments SET status='failed', note=?, updated=? WHERE id=?").run(note, now(), p.id); db.prepare('UPDATE orders SET claimed=0 WHERE id=?').run(p.order_id); ev(p.id, 'failed', 'Marked as not received by admin' + (note ? ': ' + note : '')); });
  { const fo = db.prepare('SELECT * FROM orders WHERE id=?').get(p.order_id); if (fo) inbox(fo.user_id, 'payment', `We could not find your payment for ${oid(fo)}`, 'If you have paid, please send your receipt to us on WhatsApp and we will check again.', 'orders'); }
  return payDetail(p.id);
});
route('POST', '/api/admin/payments/:id/recheck', { auth: 'admin', limit: ['recheck', 30] }, async ({ params }) => {
  const p = payRow(params.id); if (PM[p.method].manual) throw new HttpError(400, 'Only online payments can be re-checked with Paystack.');
  const o = db.prepare('SELECT * FROM orders WHERE id=?').get(p.order_id); const paid = await verifyOnline(o); const d = payDetail(p.id); d.result = paid ? 'Paystack confirms this payment.' : 'Paystack has not received this payment yet.'; return d;
});
route('POST', '/api/admin/payments/:id/refund', { auth: 'admin', limit: ['refund', 10] }, async ({ params, body }) => {
  const p = payRow(params.id), o = db.prepare('SELECT * FROM orders WHERE id=?').get(p.order_id); if (p.status !== 'paid') throw new HttpError(409, 'Only paid payments can be refunded.');
  const note = str(body.note, 300); let how = 'Refund recorded (send the money back yourself)';
  if (!PM[p.method].manual) { await paystack('POST', '/refund', { transaction: p.ref }); how = 'Refund sent through Paystack (the customer is credited within a few days)'; }
  tx(() => { db.prepare("UPDATE payments SET status='refunded', note=COALESCE(NULLIF(?,''),note), updated=? WHERE id=?").run(note, now(), p.id); db.prepare("UPDATE orders SET status='Cancelled', paid=0 WHERE id=?").run(o.id); ev(p.id, 'refunded', `${naira(p.amount)} refunded. ${how}${note ? '. ' + note : ''}`, 1); });
  restock(db.prepare('SELECT * FROM orders WHERE id=?').get(o.id)); refHook('onReverse', o, 'Order refunded', 'void'); inbox(o.user_id, 'payment', `Refund for ${oid(o)}`, PM[p.method].manual ? `We are sending ${naira(p.amount)} back to you. Please contact us if it has not arrived in 2 working days.` : `Your refund of ${naira(p.amount)} was sent. It reaches your account within a few days.`, 'orders', 'refund:' + o.id); return payDetail(p.id);
});
/* the dashboard polls this for live alerts: new orders, payments received, transfers to confirm, mismatches */
route('GET', '/api/admin/events', { auth: 'admin' }, ({ query }) => {
  const after = +query.get('after'), last = (db.prepare('SELECT COALESCE(MAX(id),0) m FROM payment_events').get()).m; if (!(after >= 0)) return { events: [], last };
  const events = db.prepare('SELECT e.id,e.at,e.type,e.text,p.order_id,p.id AS payment_id,p.amount FROM payment_events e JOIN payments p ON p.id=e.payment_id WHERE e.alert=1 AND e.id>? ORDER BY e.id LIMIT 50').all(after).map(e => ({ ...e, order: '#' + (1000 + e.order_id) }));
  return { events, last: events.length ? events[events.length - 1].id : after, attention: db.prepare("SELECT COUNT(*) n FROM payments WHERE status IN ('review','mismatch')").get().n };
});
route('GET', '/api/admin/paystack/balance', { auth: 'admin', limit: ['psbal', 20] }, async () => { const d = await paystack('GET', '/balance'); return { balances: (Array.isArray(d) ? d : []).map(b => ({ currency: b.currency, amount: Math.round((b.balance || 0)) / 100 })) }; });
route('POST', '/api/admin/notify/test', { auth: 'admin', limit: ['ntest', 5] }, async () => {
  const st = notifyStatus(); if (!st.telegram && !st.webhook && !st.adminEmail) throw new HttpError(400, 'No alert channel is set up yet. Add Telegram, an email key or a webhook in the server .env (see README).');
  const ok = await notifyAdmin('✅ Test alert from Skinsentials22', ['If you can read this, order and payment alerts will reach you here.']); if (!ok) throw new HttpError(502, 'At least one alert channel failed. Check the server log for the reason.'); return { ok: true };
});
route('GET', '/api/admin/backup', { auth: 'admin' }, () => {
  const strip = r => { const { pw, ...x } = r; return x; };
  return { file: { name: 'skinsentials22-backup-' + new Date().toISOString().slice(0, 10) + '.json', type: 'application/json', data: JSON.stringify({ exported: new Date().toISOString(), products: productsAll(), stock: [...stockMap()], orders: db.prepare('SELECT * FROM orders ORDER BY id').all(), payments: db.prepare('SELECT * FROM payments ORDER BY id').all(), payment_events: db.prepare('SELECT * FROM payment_events ORDER BY id').all(), users: db.prepare('SELECT * FROM users').all().map(strip), subscribers: db.prepare('SELECT * FROM subscribers').all(), brands: db.prepare('SELECT * FROM brands').all(), settings: getSettings(), general: getGeneral(), pricing: pricingCfg(), content: contentRaw(), glow: Object.fromEntries(['glow_ledger', 'glow_claims', 'glow_codes', 'glow_refs'].map(t => [t, db.prepare('SELECT * FROM ' + t).all()])), referral: Object.fromEntries(['ref_versions', 'ref_codes', 'ref_people', 'ref_orders', 'ref_ledger', 'ref_events', 'ref_flags', 'ref_audit'].map(t => [t, db.prepare('SELECT * FROM ' + t).all()])) }, null, 1) } };
});

/* brands (name -> logo) */
route('PUT', '/api/admin/brands', { auth: 'admin' }, ({ body }) => {
  const name = str(body.name, 80), logo = str(body.logo, 2000);
  if (!name) throw new HttpError(400, 'Enter a brand name.');
  if (logo && !/^(\/uploads\/[\w.-]+|https:\/\/[^\s]+)$/.test(logo)) throw new HttpError(400, 'The logo must be an uploaded image or an https link.');
  if (logo) db.prepare('INSERT INTO brands VALUES(?,?) ON CONFLICT(name) DO UPDATE SET logo=excluded.logo').run(name, logo); else db.prepare('DELETE FROM brands WHERE name=?').run(name);
  return { ok: true };
});
/* file upload: streamed straight to disk, so large videos never sit in memory */
const UP_EXT = { '.mp4': 'video', '.m4v': 'video', '.mov': 'video', '.webm': 'video', '.ogv': 'video', '.jpg': 'image', '.jpeg': 'image', '.png': 'image', '.webp': 'image', '.gif': 'image' };
route('POST', '/api/admin/upload', { auth: 'admin', stream: true }, async ({ req, query }) => {
  const ext = path.extname(str(query.get('name'), 200)).toLowerCase(), kind = UP_EXT[ext];
  if (!kind) throw new HttpError(400, 'Unsupported file type. Use MP4, WebM or MOV for video, and JPG, PNG, WebP or GIF for images.');
  if (MAX_UPLOAD && +req.headers['content-length'] > MAX_UPLOAD) throw new HttpError(413, `File is larger than the ${ENV.MAX_UPLOAD_MB} MB limit set on the server.`);
  const file = crypto.randomBytes(12).toString('hex') + ext, tmp = path.join(UPLOADS, file + '.part'); let n = 0;
  const count = new Transform({ transform(c, _, cb) { n += c.length; MAX_UPLOAD && n > MAX_UPLOAD ? cb(new HttpError(413, `File is larger than the ${ENV.MAX_UPLOAD_MB} MB limit set on the server.`)) : cb(null, c); } });
  try { await pipeline(req, count, fs.createWriteStream(tmp)); if (!n) throw new HttpError(400, 'The file is empty.'); fs.renameSync(tmp, path.join(UPLOADS, file)); }
  catch (e) { fs.rm(tmp, { force: true }, () => {}); throw e; }
  return { url: '/uploads/' + file, size: n, kind };
});
/* documents (app file, PDF, spreadsheet) go through their own upload route so the picture/video route keeps its strict list */
const DOC_EXT = new Set(['.apk', '.pdf', '.zip', '.xlsx', '.csv', '.txt']);
route('POST', '/api/admin/upload-file', { auth: 'admin', stream: true }, async ({ req, query }) => {
  const ext = path.extname(str(query.get('name'), 200)).toLowerCase();
  if (!DOC_EXT.has(ext)) throw new HttpError(400, 'Unsupported file type. Use APK, PDF, ZIP, XLSX, CSV or TXT.');
  if (MAX_UPLOAD && +req.headers['content-length'] > MAX_UPLOAD) throw new HttpError(413, `File is larger than the ${ENV.MAX_UPLOAD_MB} MB limit set on the server.`);
  const file = crypto.randomBytes(12).toString('hex') + ext, tmp = path.join(UPLOADS, file + '.part'); let n = 0;
  const count = new Transform({ transform(c, _, cb) { n += c.length; MAX_UPLOAD && n > MAX_UPLOAD ? cb(new HttpError(413, `File is larger than the ${ENV.MAX_UPLOAD_MB} MB limit set on the server.`)) : cb(null, c); } });
  try { await pipeline(req, count, fs.createWriteStream(tmp)); if (!n) throw new HttpError(400, 'The file is empty.'); fs.renameSync(tmp, path.join(UPLOADS, file)); }
  catch (e) { fs.rm(tmp, { force: true }, () => {}); throw e; }
  return { url: '/uploads/' + file, size: n, kind: 'file' };
});
const generalOut = () => { const s = getSettings(); return { general: { ...getGeneral(), whatsapp: s.whatsapp, instagram: s.instagram }, site: SITE, siteDefault: SITE_DEFAULT, siteFromEnv: !!ENV.SITE_URL }; };
route('GET', '/api/admin/general', { auth: 'admin' }, () => generalOut());
route('PUT', '/api/admin/general', { auth: 'admin', big: true }, ({ body }) => {
  const cur = getGeneral(), next = { ...cur }, bad = m => { throw new HttpError(400, m); }; let wa = null, ig = null;
  if (body.site_url != null) { const u = cleanSite(body.site_url); if (u === null) bad('The website address must look like https://www.yourdomain.com (no spaces, no page path). Plain http is only allowed for localhost.'); next.site_url = u; }
  for (const k of ['logo', 'favicon', 'og_image', 'app_image', 'app_qr']) if (body[k] != null) { const v = String(body[k]).trim().slice(0, 600); if (v && !GEN_IMG.test(v)) bad('Pictures must be an uploaded JPG, PNG, WebP or GIF, or an https link.'); next[k] = v; }
  if (body.app_apk != null) { const v = String(body.app_apk).trim().slice(0, 600); if (v && !GEN_FILE.test(v)) bad('The app file must be an uploaded file or an https link.'); next.app_apk = v; }
  for (const k of ['app_ios', 'app_android']) if (body[k] != null) { const v = String(body[k]).trim().slice(0, 600); if (v && !GEN_LINK.test(v)) bad('Store links must start with https://'); next[k] = v; }
  for (const [k, n] of Object.entries(GEN_TEXT)) if (body[k] != null) next[k] = str(body[k], n);
  for (const k of GEN_EMAIL) if (body[k] != null) { const v = str(body[k], 254).toLowerCase(); if (v && !emailOk(v)) bad('That email address does not look right.'); next[k] = v; }
  for (const k of GEN_SOCIAL) if (body[k] != null) { const v = String(body[k]).trim().slice(0, 300); if (v && !GEN_LINK.test(v)) bad('Social links must start with https://'); next[k] = v; }
  for (const k of GEN_NUM) if (body[k] != null) { const v = String(body[k]).trim(); if (v && !/^\d{1,3}$/.test(v)) bad('Day and hour counts must be whole numbers.'); next[k] = v; }
  if (body.ga_id != null) { const v = String(body.ga_id).trim().toUpperCase(); if (v && !/^G-[A-Z0-9]{4,14}$/.test(v)) bad('A Google Analytics ID looks like G-XXXXXXXXXX.'); next.ga_id = v; }
  if (body.fb_pixel != null) { const v = String(body.fb_pixel).trim(); if (v && !/^\d{6,20}$/.test(v)) bad('A Meta Pixel ID is a number of 6 to 20 digits.'); next.fb_pixel = v; }
  if (body.gsc_file != null) { let v = String(body.gsc_file).trim().replace(/^\//, '').toLowerCase(); if (v && !v.endsWith('.html')) v += '.html'; if (v && !/^google[a-f0-9]{8,40}\.html$/.test(v)) bad('The Search Console file name looks like google0123456789abcdef.html'); next.gsc_file = v; }
  if (body.gsc_meta != null) { const v = String(body.gsc_meta).trim(); if (v && !/^[\w-]{20,100}$/.test(v)) bad('Paste only the content value of the Search Console verification tag (letters, numbers, - and _).'); next.gsc_meta = v; }
  if (body.maintenance_on != null) next.maintenance_on = (body.maintenance_on === true || body.maintenance_on === 1 || body.maintenance_on === '1') ? '1' : '';
  for (const id of LEGAL_IDS) if (body['legal_' + id] != null) next['legal_' + id] = String(body['legal_' + id]).replace(/\r\n?/g, '\n').replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '').trim().slice(0, 30000);
  if (body.whatsapp != null) { wa = String(body.whatsapp).replace(/\D/g, ''); if (wa && !/^\d{8,15}$/.test(wa)) bad('WhatsApp number: digits with country code, for example 2349012345678.'); }
  if (body.instagram != null) { ig = String(body.instagram).trim().replace(/^@/, '').replace(/^https?:\/\/(www\.)?instagram\.com\//i, '').replace(/[/?].*$/, ''); if (ig && !/^[\w.]{1,30}$/.test(ig)) bad('Instagram: enter just the username.'); }
  const up = db.prepare('INSERT INTO settings VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v');
  tx(() => { up.run('general', JSON.stringify(next)); if (wa !== null) up.run('whatsapp', wa); if (ig !== null) up.run('instagram', ig); });
  applySite(); try { SEOQ.ping(); } catch {}
  return { ...generalOut(), siteChanged: next.site_url !== cur.site_url };
});
/* text of policies the owner rewrote in Admin > General (the website falls back to the built-in wording for any left empty) */
route('GET', '/api/legal', { limit: ['legal', 60] }, () => { const g = getGeneral(), custom = {}; for (const id of LEGAL_IDS) if (g['legal_' + id]) custom[id] = g['legal_' + id]; return { custom, effective: g.legal_effective }; });
/* email sender and alert channels. Secrets are never sent back to the browser. */
const COMMS_SECRET = { tg_token: /^\d{5,12}:[\w-]{30,}$/, mail_key: /^[\w-]{10,200}$/, webhook: /^https:\/\/[^\s"'<>`\\]{8,500}$/ };
const COMMS_PLAIN = { tg_chat: /^(-?\d{3,20}|@\w{4,64})$/, mail_from: /^([^<>@\r\n"]{1,60}<)?[^\s@<>]+@[^\s@<>]+\.[^\s@<>]{2,}>?$/, admin_email: null };
const commsOut = () => { const o = commsRaw(), envv = { tg_token: ENV.TELEGRAM_BOT_TOKEN, tg_chat: ENV.TELEGRAM_CHAT_ID, mail_key: ENV.RESEND_API_KEY, mail_from: ENV.MAIL_FROM, admin_email: ENV.ADMIN_EMAIL, webhook: ENV.NOTIFY_WEBHOOK_URL }, out = { status: notifyStatus(), fields: {} };
  for (const k of [...Object.keys(COMMS_SECRET), ...Object.keys(COMMS_PLAIN)]) { const saved = typeof o[k] === 'string' && o[k], live = saved || envv[k] || '', secret = k in COMMS_SECRET; out.fields[k] = { source: saved ? 'admin' : envv[k] ? 'env' : 'none', set: !!live, ...(secret ? { hint: live ? live.slice(0, 4) + '...' + live.slice(-3) : '' } : { value: live }) }; } return out; };
route('GET', '/api/admin/comms', { auth: 'admin' }, () => commsOut());
route('PUT', '/api/admin/comms', { auth: 'admin' }, ({ body }) => {
  const o = commsRaw(), clear = Array.isArray(body.clear) ? body.clear : [], bad = m => { throw new HttpError(400, m); };
  for (const k of [...Object.keys(COMMS_SECRET), ...Object.keys(COMMS_PLAIN)]) {
    if (clear.includes(k)) { delete o[k]; continue; }
    if (body[k] == null) continue; const v = String(body[k]).trim().slice(0, 600); if (!v) continue;   // empty = keep what is saved
    if (k in COMMS_SECRET) { if (!COMMS_SECRET[k].test(v)) bad({ tg_token: 'That does not look like a Telegram bot token (it looks like 123456789:AA...).', mail_key: 'That does not look like an email API key.', webhook: 'The webhook must be an https link.' }[k]); }
    else if (k === 'admin_email') { if (!emailOk(v.toLowerCase())) bad('The alert email address does not look right.'); }
    else if (!COMMS_PLAIN[k].test(v)) bad({ tg_chat: 'The Telegram chat id is a number (it can start with -) or @channelname.', mail_from: 'The sender must be an address like orders@yourdomain.com, or Name <orders@yourdomain.com>.' }[k]);
    o[k] = k === 'admin_email' ? v.toLowerCase() : v;
  }
  db.prepare("INSERT INTO settings VALUES('comms',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify(o)); applyComms(); return commsOut();
});
/* newsletter list: the upload that goes with Download CSV. People who unsubscribed are never added back. */
route('POST', '/api/admin/subscribers/import', { auth: 'admin', big: true, limit: ['subimp', 10] }, ({ body }) => {
  const text = typeof body.csv === 'string' ? body.csv.replace(/^\ufeff/, '') : ''; if (!text.trim()) throw new HttpError(400, 'The file is empty.');
  const split = line => { const out = []; let cur = '', q = false; for (let i = 0; i < line.length; i++) { const ch = line[i]; if (q) { if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; } else if (ch === '"') q = false; else cur += ch; } else if (ch === '"') q = true; else if (ch === ',' || ch === ';' || ch === '\t') { out.push(cur.trim()); cur = ''; } else cur += ch; } out.push(cur.trim()); return out; };
  const out = { added: 0, existing: 0, unsubscribed: 0, invalid: 0 }, seen = new Set(), sel = db.prepare('SELECT active FROM subscribers WHERE email=?'), ins = db.prepare('INSERT INTO subscribers(email,name,token,created) VALUES(?,?,?,?)');
  const lines = text.split(/\r?\n/).filter(l => l.trim()).slice(0, 50001);
  tx(() => { lines.forEach((line, i) => {
    const cells = split(line), at = cells.findIndex(c => emailOk(c.toLowerCase()));
    if (at < 0) { if (!(i === 0 && cells.some(c => /^e-?mail/i.test(c)))) out.invalid++; return; }
    const email = cells[at].toLowerCase(); if (seen.has(email)) return; seen.add(email);
    const ex = sel.get(email); if (ex) { out[ex.active ? 'existing' : 'unsubscribed']++; return; }
    const nm = cells.slice(at + 1).find(c => c && c.length <= 80 && !emailOk(c.toLowerCase()) && !/^(\d+|true|false|\d{4}-\d\d-\d\d.*)$/i.test(c)) || '';
    ins.run(email, str(nm, 80), crypto.randomBytes(18).toString('hex'), now()); out.added++;
  }); });
  return out;
});
route('GET', '/api/admin/subscribers', { auth: 'admin' }, () => ({ subscribers: db.prepare('SELECT id,email,name,active,created FROM subscribers ORDER BY id DESC').all() }));
route('GET', '/api/admin/subscribers.csv', { auth: 'admin' }, () => ({ csv: 'email,name,active,subscribed\n' + db.prepare('SELECT * FROM subscribers WHERE active=1 ORDER BY id').all().map(r => [r.email, r.name, r.active, new Date(r.created).toISOString()].map(csvCell).join(',')).join('\n'), name: 'subscribers.csv' }));
route('GET', '/api/admin/users', { auth: 'admin' }, () => ({ users: db.prepare('SELECT id,name,email,phone,created,(SELECT COUNT(*) FROM likes WHERE user_id=users.id) likes FROM users ORDER BY id DESC').all() }));
route('POST', '/api/admin/users/:id/reset', { auth: 'admin' }, ({ params }) => {
  const u = db.prepare('SELECT id FROM users WHERE id=?').get(+params.id); if (!u) throw new HttpError(404, 'User not found');
  const tmp = crypto.randomBytes(6).toString('base64url'); db.prepare('UPDATE users SET pw=? WHERE id=?').run(hashPw(tmp), u.id); return { temporaryPassword: tmp };
});
const adminSettings = () => { const s = getSettings(); return { notify: notifyStatus(), settings: s, paystackConfigured: !!psKey(), paystack: psInfo(), accounts: accountsOf(s), methods: Object.entries(PM).filter(([, m]) => !m.hidden).map(([id, m]) => ({ id, label: m.label, manual: !!m.manual, enabled: !s.pay_off.split(',').includes(id), ...methodState(id, s) })) }; };
route('GET', '/api/admin/settings', { auth: 'admin' }, () => adminSettings());
route('PUT', '/api/admin/settings', { auth: 'admin' }, ({ body }) => {
  const up = db.prepare('INSERT INTO settings VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v');
  if (body.extra_accounts != null) { let a; try { a = typeof body.extra_accounts === 'string' ? JSON.parse(body.extra_accounts) : body.extra_accounts; } catch { throw new HttpError(400, 'Invalid bank account list'); } if (!Array.isArray(a)) throw new HttpError(400, 'Invalid bank account list'); body.extra_accounts = JSON.stringify(a.slice(0, 3).map(x => ({ bank_name: str(x && x.bank_name, 60), account_number: str(x && x.account_number, 30), account_name: str(x && x.account_name, 80) })).filter(x => x.bank_name && x.account_number)); }
  for (const k of ['ship_lagos', 'ship_other', 'free_over']) if (body[k] != null) body[k] = String(Math.max(0, Math.min(1e7, parseInt(body[k], 10) || 0)));
  for (const k of ['days_lagos', 'days_other']) if (body[k] != null) { if (!/^\d{1,2}(-\d{1,2})?$/.test(String(body[k]).trim())) throw new HttpError(400, 'Delivery days must look like 1-3 or 2.'); body[k] = String(body[k]).trim(); }
  if (body.pay_off != null) body.pay_off = (Array.isArray(body.pay_off) ? body.pay_off : String(body.pay_off).split(',')).map(x => String(x).trim()).filter(x => PM[x]).join(',');
  for (const k of Object.keys(DEFAULTS)) if (body[k] != null) up.run(k, str(body[k], LIMITS[k] || 300));
  return adminSettings();
});
/* Paystack secret key can be pasted in the dashboard instead of editing .env (a key in .env always wins) */
route('PUT', '/api/admin/paystack', { auth: 'admin', limit: ['pskey', 10] }, ({ body }) => {
  if (body.clear) { db.prepare("DELETE FROM settings WHERE k='paystack_secret'").run(); return adminSettings(); }
  const k = str(body.key, 200); if (!/^sk_(test|live)_[A-Za-z0-9]{10,}$/.test(k)) throw new HttpError(400, 'That does not look like a Paystack SECRET key. It starts with sk_test_ or sk_live_ (not pk_).');
  db.prepare("INSERT INTO settings VALUES('paystack_secret',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(k); return adminSettings();
});
route('POST', '/api/admin/paystack/test', { auth: 'admin', limit: ['pstest', 10] }, async () => {
  if (!psKey()) throw new HttpError(400, 'No Paystack key is set yet.');
  await paystack('GET', '/balance'); return { ok: true, ...psInfo() };
});

/* ---------- referral programme (disabled until an admin activates it) ---------- */
/* SKINSENTIALS22 referral program (server). Mounted by server.js (REF = referralInit({...}))
   Disabled by default. Every discount and reward is computed here; the browser never supplies amounts. */
const referralInit = function (X) {
const { db, route, tx, now, str, HttpError, naira, csvCell, hx, sendMail, mailShell, quiet, productsAll, shipFor, zoneOf, getSettings, PM, crypto } = X;
const DAY = 864e5, sha = v => crypto.createHmac('sha256', X.SECRET).update(String(v)).digest('hex');   // keyed hash: a leaked database cannot be matched against guessed email addresses

/* ---------- migrations (additive and idempotent) ---------- */
db.exec(`CREATE TABLE IF NOT EXISTS ref_versions(id INTEGER PRIMARY KEY AUTOINCREMENT, cfg TEXT NOT NULL, reason TEXT DEFAULT '', actor TEXT DEFAULT '', created INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS ref_codes(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES users(id), code TEXT NOT NULL UNIQUE, status TEXT NOT NULL DEFAULT 'active', reason TEXT DEFAULT '', created INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS ref_people(user_id INTEGER PRIMARY KEY, status TEXT NOT NULL, reason TEXT DEFAULT '', updated INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS ref_orders(id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER NOT NULL UNIQUE REFERENCES orders(id), code_id INTEGER NOT NULL, referrer_id INTEGER NOT NULL, friend_key TEXT NOT NULL, version INTEGER NOT NULL, subtotal INTEGER NOT NULL, discount INTEGER NOT NULL, reward INTEGER NOT NULL, status TEXT NOT NULL, unlock_at INTEGER, created INTEGER NOT NULL, updated INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS ref_orders_ref ON ref_orders(referrer_id, status);
CREATE TABLE IF NOT EXISTS ref_ledger(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, ref_order_id INTEGER, kind TEXT NOT NULL, amount INTEGER NOT NULL, available_at INTEGER NOT NULL, expires_at INTEGER, idem TEXT NOT NULL UNIQUE, reason TEXT DEFAULT '', actor TEXT DEFAULT '', created INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS ref_ledger_user ON ref_ledger(user_id);
CREATE TABLE IF NOT EXISTS ref_events(id INTEGER PRIMARY KEY AUTOINCREMENT, ref_order_id INTEGER NOT NULL, at INTEGER NOT NULL, type TEXT NOT NULL, text TEXT DEFAULT '', actor TEXT DEFAULT 'system');
CREATE TABLE IF NOT EXISTS ref_flags(id INTEGER PRIMARY KEY AUTOINCREMENT, ref_order_id INTEGER NOT NULL, reason_code TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open', decided_by TEXT DEFAULT '', decision TEXT DEFAULT '', created INTEGER NOT NULL, decided_at INTEGER);
CREATE TABLE IF NOT EXISTS ref_clicks(day TEXT NOT NULL, code_id INTEGER NOT NULL, n INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(day, code_id));
CREATE TABLE IF NOT EXISTS ref_audit(id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL, target TEXT DEFAULT '', reason TEXT DEFAULT '', detail TEXT DEFAULT '');`);
for (const a of ['ALTER TABLE orders ADD COLUMN discount INTEGER NOT NULL DEFAULT 0', 'ALTER TABLE orders ADD COLUMN credit_used INTEGER NOT NULL DEFAULT 0']) try { db.exec(a); } catch {}

/* ---------- configuration (versioned) and program state ---------- */
const TPL = {
  earned: ['Your referral reward is ready', 'Hi {{name}}, a referral reward of {{amount}} store credit is now available. You can use it at checkout on your next order. Questions? Contact {{support}}.'],
  reversed: ['A referral reward was reversed', 'Hi {{name}}, a referral reward of {{amount}} was reversed because the related order was cancelled, refunded or disputed. If you think this is a mistake, contact {{support}}.'],
  expiring: ['Your store credit expires soon', 'Hi {{name}}, {{amount}} of your referral store credit expires within 7 days. Use it at checkout on your next order. Questions? Contact {{support}}.'],
  review: ['A referral is being reviewed', 'Hi {{name}}, a referral reward of {{amount}} is on hold while we complete a routine check. We will update you. You can contact {{support}} at any time.']
};
const DEF = { name: '', headline: '', terms: '', support: '', image: '', tz: 'Africa/Lagos', start: '', end: '', friend: { type: 'fixed', value: 0, max: 0 }, referrer: { value: 0, expiry_days: 0 }, min_spend: 0, newOnly: true, who: 'any', regions: '', excl_products: '', excl_categories: '', onlineOnly: false, attr: 'last', window_days: 30, buffer_days: 7, codeRequired: false, cap_total: 0, max_per_referrer: 0, max_per_day: 5, credit_with_discount: true, notify: { earned: { on: true, subject: '', body: '' }, reversed: { on: true, subject: '', body: '' }, review: { on: true, subject: '', body: '' }, expiring: { on: true, subject: '', body: '' } } };
const cfgRow = () => db.prepare('SELECT * FROM ref_versions ORDER BY id DESC LIMIT 1').get();
const cfg = () => { const r = cfgRow(); let j = {}; try { j = JSON.parse(r ? r.cfg : '{}'); } catch {} return { ...DEF, ...j, friend: { ...DEF.friend, ...j.friend }, referrer: { ...DEF.referrer, ...j.referrer }, notify: { ...DEF.notify, ...j.notify } }; };
const version = () => (cfgRow() || { id: 0 }).id;
const getState = () => { try { return JSON.parse((db.prepare("SELECT v FROM settings WHERE k='ref_state'").get() || {}).v || '{}'); } catch { return {}; } };
const setState = s => db.prepare("INSERT INTO settings VALUES('ref_state',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify(s));
function zonedMs(local, tz) {   // 'YYYY-MM-DDTHH:mm' in an IANA timezone -> epoch ms
  if (!local) return 0; const g = Date.parse(local + ':00Z'); if (!g) return 0;
  const p = Object.fromEntries(new Intl.DateTimeFormat('en-US', { timeZone: tz, hourCycle: 'h23', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }).formatToParts(g).map(x => [x.type, x.value]));
  return g - (Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second) - g);
}
const stateName = c => { const s = getState().status || 'off'; if (s !== 'live') return s; const t = now(); if (c.start && t < zonedMs(c.start, c.tz)) return 'scheduled'; if (c.end && t >= zonedMs(c.end, c.tz)) return 'ended'; return 'live'; };
const isLive = c => stateName(c || cfg()) === 'live';
const audit = (action, target, reason, detail, actor = 'admin') => db.prepare('INSERT INTO ref_audit(at,actor,action,target,reason,detail) VALUES(?,?,?,?,?,?)').run(now(), actor, action, String(target || ''), str(reason, 300), typeof detail === 'string' ? detail.slice(0, 2000) : JSON.stringify(detail || '').slice(0, 2000));
const int = (v, max = 1e8) => Math.max(0, Math.min(max, parseInt(v, 10) || 0));
const list = (v, n) => str(Array.isArray(v) ? v.join(',') : v, n).split(',').map(x => x.trim()).filter(Boolean).join(',');
const validTz = z => { try { new Intl.DateTimeFormat('en', { timeZone: z }); return true; } catch { return false; } };
function clean(b) {
  const f = b.friend || {}, r = b.referrer || {}, n = b.notify || {}, ldt = /^\d{4}-\d\d-\d\dT\d\d:\d\d$/, notify = {};
  for (const e of Object.keys(TPL)) notify[e] = { on: !(n[e] && n[e].on === false), subject: str(n[e] && n[e].subject, 120), body: str(n[e] && n[e].body, 1000) };
  return { name: str(b.name, 80), headline: str(b.headline, 160), terms: str(b.terms, 6000), support: str(b.support, 200), image: /^(\/uploads\/[\w.-]+|https:\/\/\S+)$/.test(b.image || '') ? b.image : '', tz: validTz(b.tz) ? b.tz : 'Africa/Lagos', start: ldt.test(b.start) ? b.start : '', end: ldt.test(b.end) ? b.end : '',
    friend: { type: f.type === 'percent' ? 'percent' : 'fixed', value: int(f.value, f.type === 'percent' ? 100 : 1e7), max: int(f.max, 1e7) }, referrer: { value: int(r.value, 1e7), expiry_days: int(r.expiry_days, 3650) },
    min_spend: int(b.min_spend), newOnly: b.newOnly !== false, who: b.who === 'paid' ? 'paid' : 'any', regions: list(b.regions, 600), excl_products: list(b.excl_products, 2000), excl_categories: list(b.excl_categories, 600), onlineOnly: !!b.onlineOnly,
    attr: b.attr === 'first' ? 'first' : 'last', window_days: Math.max(1, int(b.window_days, 365)), buffer_days: int(b.buffer_days, 180), codeRequired: !!b.codeRequired, cap_total: int(b.cap_total), max_per_referrer: int(b.max_per_referrer, 10000), max_per_day: Math.max(1, int(b.max_per_day, 1000)), credit_with_discount: !!b.credit_with_discount, notify };
}
const fd = c => c.friend.type === 'percent' ? `${c.friend.value}% off${c.friend.max ? ' (up to ' + naira(c.friend.max) + ')' : ''}` : naira(c.friend.value) + ' off';
/* plain-language summary generated from the settings, so published terms always match what the system does */
function summary(c) {
  const L = [], t = x => L.push(x);
  if (c.friend.value) t(`Your friend gets ${fd(c)} their first order with your code.`);
  if (c.referrer.value) t(`You earn ${naira(c.referrer.value)} store credit when your friend's order is paid and stays completed${c.buffer_days ? ` for ${c.buffer_days} day${c.buffer_days > 1 ? 's' : ''} (the return period)` : ''}. Store credit is not cash, cannot be withdrawn or transferred, is used at checkout on products (not delivery), and ${c.referrer.expiry_days ? 'expires ' + c.referrer.expiry_days + ' days after it becomes available' : 'does not expire'}.`);
  t(c.newOnly ? 'The friend offer is for customers who have not ordered before, one use per friend. You cannot use your own code.' : 'One referral per friend. You cannot use your own code.');
  if (c.min_spend) t(`Minimum spend on qualifying products: ${naira(c.min_spend)}.`);
  t('Gift cards, delivery fees and excluded products do not count towards the offer or the minimum spend.' + (c.excl_products || c.excl_categories ? ` Excluded: ${[c.excl_products, c.excl_categories].filter(Boolean).join(', ')}.` : ''));
  if (c.regions) t(`Available for delivery to: ${c.regions}.`);
  if (c.onlineOnly) t('The friend offer applies to online payments only (not bank transfer, pay on delivery or DM orders).');
  t(c.codeRequired ? 'Your friend must enter your code at checkout.' : `Opening your link applies your code for ${c.window_days} days (${c.attr === 'first' ? 'the first link they open counts' : 'the most recent link they open counts'}); entering a code at checkout always works. Only one referral code applies per order.`);
  if (c.max_per_referrer) t(`Each person can earn rewards for up to ${c.max_per_referrer} successful referrals.`);
  if (c.cap_total) t('The programme has a limited budget and may close early when it is used up.');
  t('Rewards are reversed if the order is cancelled, refunded, returned or charged back. Store credit used on a cancelled order is returned. Suspected misuse is held for a human review, and you can contact us to appeal a decision.');
  t('If the programme is paused or ended, new referrals stop. Orders already placed and rewards already earned are still honoured under these terms.');
  t('We never show a friend\'s name, contact details or order contents to the referrer. A click on a link does not guarantee a purchase or a reward.');
  return L;
}

/* ---------- codes, participants, balances ---------- */
const ALPHA = '23456789ABCDEFGHJKMNPQRSTUVWXYZ', mkCode = () => 'SK-' + Array.from(crypto.randomBytes(8), b => ALPHA[b % ALPHA.length]).join('');
const normCode = v => str(v, 20).toUpperCase().replace(/\s/g, '');
const canon = e => { const [l, d] = String(e || '').toLowerCase().split('@'); return (l || '').split('+')[0].replace(/\./g, '') + '@' + (d || ''); };
const last10 = p => String(p || '').replace(/\D/g, '').slice(-10);
const normAddr = a => String(a || '').toLowerCase().replace(/[^a-z0-9]/g, '');
const person = uid => db.prepare('SELECT status FROM ref_people WHERE user_id=?').get(uid);
const lookup = code => db.prepare("SELECT c.id, c.user_id, u.name, u.email, u.phone, u.address FROM ref_codes c JOIN users u ON u.id=c.user_id WHERE c.code=? AND c.status='active'").get(normCode(code));
function canRefer(u, c) {
  const p = person(u.id); if (p && p.status !== 'active') return false;
  return c.who === 'paid' ? !!db.prepare("SELECT 1 FROM orders WHERE paid=1 AND status!='Cancelled' AND (user_id=? OR email=?)").get(u.id, u.email) : true;
}
const lg = (uid, roId, kind, amount, avail, exp, idem, reason, actor = 'system') => db.prepare('INSERT OR IGNORE INTO ref_ledger(user_id,ref_order_id,kind,amount,available_at,expires_at,idem,reason,actor,created) VALUES(?,?,?,?,?,?,?,?,?,?)').run(uid, roId, kind, amount, avail, exp, idem, reason, actor, now()).changes;
function balance(uid) {   // store credit: spent credit is taken from the soonest-expiring credit first, so a lapsed reward can never eat a newer one
  const t = now(), rows = db.prepare('SELECT amount, available_at, expires_at FROM ref_ledger WHERE user_id=?').all(uid);
  let pend = 0, used = 0; const pos = [];
  for (const r of rows) { if (r.available_at > t) pend += r.amount; else if (r.amount < 0) used -= r.amount; else if (r.amount > 0) pos.push({ left: r.amount, exp: r.expires_at }); }
  pos.sort((a, b) => (a.exp == null ? Infinity : a.exp) - (b.exp == null ? Infinity : b.exp));
  for (const p of pos) { const take = Math.min(p.left, used); p.left -= take; used -= take; }
  return { available: pos.reduce((n, p) => n + (p.exp == null || p.exp > t ? p.left : 0), 0), pending: Math.max(0, pend) };
}
const orderRow = id => db.prepare('SELECT o.*, p.status AS pstatus FROM orders o LEFT JOIN payments p ON p.order_id=o.id WHERE o.id=?').get(id);

/* ---------- referral states: allowed transitions ---------- */
const T = { pending: ['review', 'qualifying', 'void'], review: ['pending', 'qualifying', 'rejected', 'void'], qualifying: ['qualified', 'reversed', 'review', 'rejected', 'pending', 'void'], qualified: ['reversed', 'review', 'rejected'], reversed: ['qualifying'], rejected: ['qualifying'], void: [] };
const evt = (roId, type, text, actor = 'system') => db.prepare('INSERT INTO ref_events(ref_order_id,at,type,text,actor) VALUES(?,?,?,?,?)').run(roId, now(), type, str(text, 400), actor);
function move(ro, to, text, actor) {
  if (ro.status === to) return; if (!T[ro.status].includes(to)) throw new HttpError(409, `A referral cannot move from "${ro.status}" to "${to}".`);
  if (!db.prepare('UPDATE ref_orders SET status=?, updated=? WHERE id=? AND status=?').run(to, now(), ro.id, ro.status).changes) throw new HttpError(409, 'This referral was just changed. Reload and try again.');
  evt(ro.id, to, text, actor); ro.status = to;
}
function reverse(ro, reason, actor) {   // appends an offsetting ledger entry; history is never deleted. Safe to repeat.
  const n = db.prepare("SELECT COALESCE(SUM(amount),0) s, MAX(available_at) av, MAX(expires_at) ex FROM ref_ledger WHERE ref_order_id=? AND kind IN('reward','reversal','adjust')").get(ro.id); if (n.s <= 0) return 0;
  const k = db.prepare("SELECT COUNT(*) c FROM ref_ledger WHERE ref_order_id=? AND kind='reversal'").get(ro.id).c;
  lg(ro.referrer_id, ro.id, 'reversal', -n.s, Math.max(now(), n.av || 0), n.ex, `rev:${ro.id}:${k}`, reason, actor); return n.s;
}
function begin(ro, actor) {   // order is paid: hold the reward until the return period ends
  const c = cfg(), o = orderRow(ro.order_id), unlock = Math.max(now(), (o.paid_at || now()) + c.buffer_days * DAY);
  move(ro, 'qualifying', `Order paid. Reward held until ${new Date(unlock).toISOString().slice(0, 10)}`, actor); db.prepare('UPDATE ref_orders SET unlock_at=? WHERE id=?').run(unlock, ro.id);
  if (ro.reward > 0) { const k = db.prepare("SELECT COUNT(*) c FROM ref_ledger WHERE ref_order_id=? AND kind='reversal'").get(ro.id).c; lg(ro.referrer_id, ro.id, 'reward', ro.reward, unlock, c.referrer.expiry_days ? unlock + c.referrer.expiry_days * DAY : null, `reward:${ro.id}:${k}`, 'Referral reward', actor || 'system'); }
}
const roBy = oid => db.prepare('SELECT * FROM ref_orders WHERE order_id=?').get(oid);
function notify(uid, ev, amount) {
  const c = cfg(), u = db.prepare('SELECT name,email FROM users WHERE id=?').get(uid), n = c.notify[ev]; if (!u || !n || !n.on) return;
  const fill = s => s.replace(/\{\{name\}\}/g, u.name.split(' ')[0]).replace(/\{\{amount\}\}/g, naira(amount || 0)).replace(/\{\{support\}\}/g, c.support || 'our support team');
  inbox(uid, 'referral', fill(n.subject || TPL[ev][0]), fill(n.body || TPL[ev][1]), 'referral');
  quiet(sendMail(u.email, fill(n.subject || TPL[ev][0]), mailShell(fill(n.subject || TPL[ev][0]), `<p>${hx(fill(n.body || TPL[ev][1]))}</p>`)));   // transactional only, sent to the account holder
}
/* ---- hooks called by server.js ---- */
function onPaid(o) {
  const ro = roBy(o.id); if (!ro) return;
  tx(() => { if (ro.status === 'pending') begin(ro); else evt(ro.id, 'paid', ro.status === 'review' ? 'Order paid; reward stays on hold while the review is open' : 'Duplicate payment signal ignored'); });
}
function onReverse(o, why, mode) {   // mode: void (cancelled/refunded) | unpaid | hold (chargeback)
  const ro = roBy(o.id), fresh = db.prepare('SELECT credit_used FROM orders WHERE id=?').get(o.id);
  tx(() => {
    if (fresh && fresh.credit_used > 0 && mode !== 'hold' && mode !== 'unpaid') { const c = cfg(); lg(o.user_id, ro ? ro.id : null, 'restore', fresh.credit_used, now(), c.referrer.expiry_days ? now() + c.referrer.expiry_days * DAY : null, `restore:${o.id}`, 'Credit returned: ' + why, 'system'); }
    if (!ro || ['void', 'reversed', 'rejected'].includes(ro.status)) return;
    const amt = reverse(ro, why, 'system');
    if (mode === 'hold') { db.prepare("INSERT INTO ref_flags(ref_order_id,reason_code,created) VALUES(?,?,?)").run(ro.id, 'CHARGEBACK', now()); move(ro, 'review', why); }
    else if (mode === 'unpaid') move(ro, ['qualifying'].includes(ro.status) ? 'pending' : ro.status === 'qualified' ? 'reversed' : ro.status, why);
    else move(ro, ['pending', 'review'].includes(ro.status) ? 'void' : 'reversed', why);
    if (amt) notify(ro.referrer_id, mode === 'hold' ? 'review' : 'reversed', amt);
  });
}
function release() {
  const rows = db.prepare("SELECT * FROM ref_orders WHERE status='qualifying' AND unlock_at<=?").all(now());
  for (const ro of rows) {
    const o = orderRow(ro.order_id), ok = o && o.paid && o.status !== 'Cancelled' && o.pstatus === 'paid';
    try { tx(() => { if (!ok) { reverse(ro, 'Order no longer paid at release', 'system'); move(ro, 'reversed', 'Order was not paid when the return period ended'); } else move(ro, 'qualified', 'Return period ended. Reward is now available'); }); if (ok) notify(ro.referrer_id, 'earned', ro.reward); } catch (e) { console.error('Referral release failed:', e.message); }
  }
}
function expiring() {
  const t = now(), rows = db.prepare("SELECT l.id, l.user_id, l.amount, l.ref_order_id FROM ref_ledger l WHERE l.kind IN('reward','restore','adjust') AND l.amount>0 AND l.available_at<=? AND l.expires_at>? AND l.expires_at<=? AND NOT EXISTS(SELECT 1 FROM ref_ledger n WHERE n.idem='expn:'||l.id)").all(t, t, t + 7 * DAY);
  for (const l of rows) {
    if (l.ref_order_id && db.prepare("SELECT COALESCE(SUM(amount),0) s FROM ref_ledger WHERE ref_order_id=? AND kind IN('reward','reversal','adjust')").get(l.ref_order_id).s <= 0) continue;   // already reversed
    const b = balance(l.user_id).available; if (b > 0 && lg(l.user_id, null, 'notice', 0, t, null, 'expn:' + l.id, 'Expiry reminder sent', 'system')) notify(l.user_id, 'expiring', Math.min(l.amount, b));
  }
}
setInterval(() => { try { release(); expiring(); db.prepare("DELETE FROM ref_clicks WHERE day<?").run(new Date(now() - 90 * DAY).toISOString().slice(0, 10)); } catch (e) { console.error('Referral job failed:', e.message); } }, 6e4).unref();

/* ---------- pricing: the single source of truth for checkout, quote and order creation ---------- */
const BAD = 'This referral code cannot be used on this order.';
function price(a) {
  const c = cfg(), out = { discount: 0, credit: 0, ref: null, message: '', flags: [] }, sub = a.sub, ship = a.ship || 0, lines = a.lines;
  if (a.code) {
    const fail = m => { out.message = m; if (a.strict) throw new HttpError(422, m); return out; };
    if (!isLive(c)) return fail('The referral programme is not running right now.');
    const rc = lookup(a.code), pp = rc && person(rc.user_id); if (!rc || (pp && pp.status !== 'active')) return fail(BAD);
    if ((a.user && a.user.id === rc.user_id) || (a.email && canon(a.email) === canon(rc.email))) return fail(BAD);   // self-referral: same answer as an unknown code
    const prods = new Map(productsAll().map(p => [p.id, p])), xp = c.excl_products.toLowerCase().split(',').filter(Boolean), xc = c.excl_categories.toLowerCase().split(',').filter(Boolean);
    const elig = lines.filter(l => l.id !== 'giftcard' && !xp.includes(String(l.id).toLowerCase()) && !xc.includes(String((prods.get(l.id) || {}).category || '').toLowerCase())).reduce((s, l) => s + l.price * l.qty, 0);
    if (elig <= 0) return fail('No items in your bag qualify for this offer.');
    if (c.min_spend && elig < c.min_spend) return fail(`Spend at least ${naira(c.min_spend)} on qualifying products to use this offer.`);
    if (c.regions && a.state && !c.regions.toLowerCase().split(',').includes(String(a.state).toLowerCase())) return fail('This offer is not available for your delivery state.');
    if (c.onlineOnly && a.method && PM[a.method] && PM[a.method].manual) return fail('This offer applies to online payments only. Choose card, bank, USSD or instant transfer.');
    const fk = sha('fk|' + canon(a.email || ''));
    if (c.newOnly && (db.prepare("SELECT 1 FROM orders WHERE status!='Cancelled' AND (paid=1 OR claimed=1 OR method IN('cod','dm')) AND (lower(email)=? OR (?>0 AND user_id=?))").get(String(a.email || '').toLowerCase(), a.user ? 1 : 0, a.user ? a.user.id : 0))) return fail('This offer is for first-time customers.');
    const prior = db.prepare("SELECT ro.status, o.paid, o.method FROM ref_orders ro JOIN orders o ON o.id=ro.order_id WHERE ro.friend_key=? AND ro.status NOT IN('void','rejected','reversed')").get(fk);
    if (prior) return fail(prior.status === 'pending' && !prior.paid && !PM[prior.method].manual ? 'You have an unfinished order using this offer. Finish paying for it, or try again once it expires (within 24 hours).' : 'This offer is for first-time customers.');
    const disc = c.friend.type === 'percent' ? Math.min(Math.floor(elig * c.friend.value / 100), c.friend.max || Infinity) : Math.min(c.friend.value, elig), reward = c.referrer.value;
    if (c.cap_total && db.prepare("SELECT COALESCE(SUM(discount+reward),0) s FROM ref_orders WHERE status NOT IN('void','rejected','reversed')").get().s + disc + reward > c.cap_total) return fail('This offer has reached its limit.');
    if (c.max_per_referrer && db.prepare("SELECT COUNT(*) n FROM ref_orders WHERE referrer_id=? AND status NOT IN('void','rejected','reversed')").get(rc.user_id).n >= c.max_per_referrer) return fail(BAD);
    if (rc.phone && a.phone && last10(rc.phone) === last10(a.phone)) out.flags.push('SAME_PHONE');
    if (rc.address && a.address && normAddr(rc.address) === normAddr(a.address)) out.flags.push('SAME_ADDRESS');
    if (db.prepare('SELECT COUNT(*) n FROM ref_orders WHERE referrer_id=? AND created>?').get(rc.user_id, now() - DAY).n >= c.max_per_day) out.flags.push('HIGH_VOLUME');
    out.discount = Math.max(0, Math.min(disc, sub + ship - 100)); out.ref = { rc, fk, reward, subtotal: elig };   // keep at least ₦100 payable (card processors reject zero)
  }
  if (a.useCredit && a.user) {
    if (out.discount && !c.credit_with_discount) out.message = out.message || 'Store credit cannot be combined with the referral offer on the same order.';
    else { const goods = lines.filter(l => l.id !== 'giftcard').reduce((t, l) => t + l.price * l.qty, 0); out.credit = Math.max(0, Math.min(balance(a.user.id).available, goods - out.discount, sub + ship - 100 - out.discount)); }   // credit buys products only: never gift cards or delivery
  }
  return out;
}
function attach(orderId, p, user) {   // runs inside the order transaction
  if (p.credit) { if (balance(user.id).available < p.credit) throw new HttpError(409, 'Your store credit changed. Please review your order and try again.'); lg(user.id, null, 'redeem', -p.credit, now(), null, `redeem:${orderId}`, 'Used at checkout', 'customer'); }
  db.prepare('UPDATE orders SET discount=?, credit_used=? WHERE id=?').run(p.discount, p.credit, orderId);
  if (!p.ref) return;
  const st = p.flags.length ? 'review' : 'pending', id = db.prepare('INSERT INTO ref_orders(order_id,code_id,referrer_id,friend_key,version,subtotal,discount,reward,status,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(orderId, p.ref.rc.id, p.ref.rc.user_id, p.ref.fk, version(), p.ref.subtotal, p.discount, p.ref.reward, st, now(), now()).lastInsertRowid;
  evt(id, 'created', `Order placed with a referral code. Offer ${naira(p.discount)}${p.flags.length ? '; held for review: ' + p.flags.join(', ') : ''}`);
  for (const f of p.flags) db.prepare('INSERT INTO ref_flags(ref_order_id,reason_code,created) VALUES(?,?,?)').run(id, f, now());
}
function buildLines(items) {
  if (!Array.isArray(items) || !items.length || items.length > 50) throw new HttpError(400, 'Your bag is empty.');
  const prods = new Map(productsAll().map(p => [p.id, p]));
  return items.map(it => { const qty = Math.floor(+it.qty); if (!(qty >= 1 && qty <= 20)) throw new HttpError(400, 'Invalid quantity.'); if (it.giftCard) return { id: 'giftcard', price: Math.max(0, Math.floor(+it.price) || 0), qty }; const p = prods.get(str(it.id, 64)); if (!p) throw new HttpError(400, 'A product in your bag is no longer available.'); return { id: p.id, price: Math.round(p.price), qty }; });
}

/* ---------- customer routes ---------- */
const LBL = { pending: 'Order placed, waiting for payment', review: 'Being reviewed', qualifying: 'Order paid, in return period', qualified: 'Reward earned', reversed: 'Reward reversed', rejected: 'Not eligible', void: 'Order not completed' };
route('GET', '/api/referral/public', { limit: ['rpub', 60] }, () => {
  const c = cfg(), st = stateName(c); if (st !== 'live') return { active: false, status: st === 'off' ? 'off' : st };
  return { active: true, name: c.name, headline: c.headline, image: c.image, support: c.support, terms: c.terms, summary: summary(c), window_days: c.window_days, attr: c.attr, codeRequired: c.codeRequired, friend: c.friend.value ? fd(c) : '', reward: c.referrer.value };
});
route('POST', '/api/referral/visit', { limit: ['rvis', 30] }, ({ body }) => {   // aggregate counter only: no IP, no device data, no per-visitor record
  const rc = isLive() && lookup(body.code); if (rc) db.prepare('INSERT INTO ref_clicks(day,code_id,n) VALUES(?,?,1) ON CONFLICT(day,code_id) DO UPDATE SET n=n+1').run(new Date().toISOString().slice(0, 10), rc.id);
  return { ok: true };
});
route('POST', '/api/referral/quote', { auth: 'optional', limit: ['rq', 25] }, ({ user, body }) => {
  const s = getSettings(), lines = buildLines(body.items), sub = lines.reduce((t, l) => t + l.price * l.qty, 0), state = str(body.state, 40), physical = lines.some(l => l.id !== 'giftcard'), zone = zoneOf(state);
  const ship = physical && zone && zone !== 'intl' ? shipFor(s, zone, sub, true) : 0, email = str(body.email, 254).toLowerCase();
  const p = price({ code: normCode(body.code), useCredit: !!body.useCredit, user, email, phone: str(body.phone, 30), address: str(body.address, 400), lines, sub, ship, state, method: str(body.method, 12) });
  const bal = user ? balance(user.id) : { available: 0, pending: 0 };
  return { valid: !!p.ref || (!body.code), message: p.message, subtotal: sub, discount: p.discount, credit: p.credit, shipping: ship, shippingKnown: !physical || !!zone, total: sub - p.discount - p.credit + ship, balance: bal.available };
});
route('GET', '/api/referral/me', { auth: 'user' }, ({ user }) => {
  release(); const c = cfg(), live = isLive(c), code = db.prepare("SELECT id,code FROM ref_codes WHERE user_id=? AND status='active'").get(user.id), pp = person(user.id), bal = balance(user.id);
  const off = !code && !!db.prepare("SELECT 1 FROM ref_codes WHERE user_id=? AND status='disabled'").get(user.id), rows = db.prepare('SELECT id,status,reward,created FROM ref_orders WHERE referrer_id=? ORDER BY id DESC LIMIT 50').all(user.id).map(r => ({ id: r.id, date: r.created, status: r.status, label: LBL[r.status], reward: ['qualifying', 'qualified', 'review'].includes(r.status) ? r.reward : 0 }));
  const clicks = code ? db.prepare("SELECT COALESCE(SUM(n),0) n FROM ref_clicks WHERE code_id=? AND day>=?").get(code.id, new Date(now() - 30 * DAY).toISOString().slice(0, 10)).n : 0;
  const hist = db.prepare("SELECT kind,amount,available_at,expires_at,created FROM ref_ledger WHERE user_id=? AND kind!='notice' ORDER BY id DESC LIMIT 30").all(user.id), nx = bal.available > 0 ? db.prepare("SELECT MIN(expires_at) e FROM ref_ledger WHERE user_id=? AND kind IN('reward','restore','adjust') AND amount>0 AND available_at<=? AND expires_at>?").get(user.id, now(), now()).e : null;
  return { live, status: stateName(c), eligible: live && !off && canRefer(user, c), disabled: off, nextExpiry: nx, suspended: !!(pp && pp.status !== 'active'), code: code && code.code, link: code ? `${X.SITE}/?ref=${code.code}` : '', balance: bal, referrals: rows, clicks30: clicks, history: hist, support: c.support, summary: live ? summary(c) : [] };
});
route('POST', '/api/referral/code', { auth: 'user', limit: ['rcode', 5] }, ({ user }) => {
  const c = cfg(); if (!isLive(c)) throw new HttpError(409, 'The referral programme is not running right now.'); if (!canRefer(user, c)) throw new HttpError(403, c.who === 'paid' ? 'Referral codes are available after your first paid order.' : 'You are not able to refer friends right now. Please contact support.');
  let r = db.prepare("SELECT code FROM ref_codes WHERE user_id=? AND status='active'").get(user.id);
  if (!r && db.prepare("SELECT 1 FROM ref_codes WHERE user_id=? AND status='disabled'").get(user.id)) throw new HttpError(403, 'Your referral code has been turned off. Please contact support.');
  if (!r) { for (let i = 0; i < 5 && !r; i++) { const code = mkCode(); try { db.prepare('INSERT INTO ref_codes(user_id,code,created) VALUES(?,?,?)').run(user.id, code, now()); r = { code }; } catch {} } audit('code.create', 'user:' + user.id, 'Customer requested code', '', 'customer'); }
  return { code: r.code, link: `${X.SITE}/?ref=${r.code}` };
});

/* ---------- admin routes (the store has one admin role; each route is behind the server-side admin check) ---------- */
const A = { auth: 'admin' }, reason = b => { const r = str(b.reason, 300); if (r.length < 3) throw new HttpError(400, 'Please enter a reason (at least 3 characters).'); return r; };
const iso = t => t ? new Date(t).toISOString().replace('T', ' ').slice(0, 19) : '';
const example = c => { const s = getSettings(), sub = 20000, ship = +s.ship_lagos || 0, d = c.friend.type === 'percent' ? Math.min(Math.floor(sub * c.friend.value / 100), c.friend.max || Infinity) : Math.min(c.friend.value, sub); return { items: sub, discount: d, shipping: ship, total: sub - d + ship, reward: c.referrer.value, note: 'Example: a first-time Lagos customer, one qualifying basket of ' + naira(sub) }; };
route('GET', '/api/admin/referral', A, () => {
  const c = cfg(), counts = Object.fromEntries(db.prepare('SELECT status, COUNT(*) n FROM ref_orders GROUP BY status').all().map(r => [r.status, r.n]));
  return { state: stateName(c), raw: getState(), cfg: c, clicks: db.prepare('SELECT COALESCE(SUM(n),0) n FROM ref_clicks').get().n, versions: db.prepare('SELECT id,created,actor,reason FROM ref_versions ORDER BY id DESC LIMIT 20').all(), version: version(), summary: summary(c), example: example(c), counts, openFlags: db.prepare("SELECT COUNT(*) n FROM ref_flags WHERE status='open'").get().n, mail: X.mailOn(), tpl: TPL };
});
route('PUT', '/api/admin/referral/config', A, ({ body }) => {
  const r = reason(body), c = clean(body.cfg || {});
  if (c.start && c.end && zonedMs(c.end, c.tz) <= zonedMs(c.start, c.tz)) throw new HttpError(400, 'The end date must be after the start date.');
  if (!c.friend.value && !c.referrer.value) throw new HttpError(400, 'Set a friend discount, a referrer reward, or both.');
  db.prepare('INSERT INTO ref_versions(cfg,reason,actor,created) VALUES(?,?,?,?)').run(JSON.stringify(c), r, 'admin', now());
  audit('config.save', 'version:' + version(), r, c); return { ok: true, version: version(), state: stateName(c), summary: summary(c), example: example(c) };
});
route('POST', '/api/admin/referral/state', A, ({ body }) => {
  const r = reason(body), c = cfg(), a = body.action, cur = getState().status || 'off';
  if (a === 'activate') {
    const miss = [], cf = body.confirm || {}; if (!c.name) miss.push('campaign name'); if (c.terms.length < 40) miss.push('published terms'); if (!c.support) miss.push('support contact'); if (!c.friend.value && !c.referrer.value) miss.push('at least one reward'); if (!cf.checkout) miss.push('checkout preview review'); if (!cf.terms) miss.push('terms review'); if (c.referrer.value && !cf.tax) miss.push('tax/accounting confirmation for store credit');
    if (miss.length) throw new HttpError(400, 'Before activating, complete: ' + miss.join(', ') + '.'); setState({ status: 'live', version: version() });
  } else if (a === 'pause') { if (cur !== 'live') throw new HttpError(409, 'Only a running programme can be paused.'); setState({ ...getState(), status: 'paused' }); }
  else if (a === 'resume') { if (cur !== 'paused') throw new HttpError(409, 'Only a paused programme can be resumed.'); setState({ ...getState(), status: 'live' }); }
  else if (a === 'end') { setState({ ...getState(), status: 'ended' }); } else throw new HttpError(400, 'Unknown action.');
  audit('state.' + a, 'program', r, { from: cur, version: version(), confirm: body.confirm || null }); return { state: stateName(c) };
});
route('GET', '/api/admin/referral/referrals', A, ({ query }) => {
  const st = str(query.get('status'), 20), q = str(query.get('q'), 60).replace(/[%_]/g, ''), w = [], a = [];
  if (st) { w.push('ro.status=?'); a.push(st); }
  if (q) { const n = /^#?\d+$/.test(q) ? parseInt(q.replace('#', ''), 10) - 1000 : -1, l = '%' + q + '%'; w.push('(u.name LIKE ? OR c.code LIKE ? OR o.id=?)'); a.push(l, l, n); }
  const rows = db.prepare(`SELECT ro.*, u.name rname, c.code, o.status ostatus, o.paid, (SELECT GROUP_CONCAT(reason_code) FROM ref_flags f WHERE f.ref_order_id=ro.id AND f.status='open') flags FROM ref_orders ro JOIN users u ON u.id=ro.referrer_id JOIN ref_codes c ON c.id=ro.code_id JOIN orders o ON o.id=ro.order_id ${w.length ? 'WHERE ' + w.join(' AND ') : ''} ORDER BY ro.id DESC LIMIT 200`).all(...a);
  return { rows: rows.map(r => ({ id: r.id, order: '#' + (1000 + r.order_id), referrer: r.rname, referrerId: r.referrer_id, code: r.code, status: r.status, subtotal: r.subtotal, discount: r.discount, reward: r.reward, flags: r.flags || '', orderStatus: r.ostatus, paid: !!r.paid, unlock: r.unlock_at, created: r.created })) };
});
route('GET', '/api/admin/referral/referrals/:id', A, ({ params }) => ({ events: db.prepare('SELECT at,type,text,actor FROM ref_events WHERE ref_order_id=? ORDER BY id').all(+params.id), flags: db.prepare('SELECT reason_code,status,decision,decided_by,decided_at FROM ref_flags WHERE ref_order_id=?').all(+params.id), ledger: db.prepare('SELECT kind,amount,available_at,reason,actor,created FROM ref_ledger WHERE ref_order_id=? ORDER BY id').all(+params.id) }));
route('POST', '/api/admin/referral/referrals/:id/decision', A, ({ params, body }) => {
  const r = reason(body), a = body.action;
  tx(() => {
    const ro = db.prepare('SELECT * FROM ref_orders WHERE id=?').get(+params.id); if (!ro) throw new HttpError(404, 'Referral not found'); const o = orderRow(ro.order_id), paid = o.paid && o.status !== 'Cancelled' && o.pstatus === 'paid', closeFlags = s => db.prepare("UPDATE ref_flags SET status=?, decided_by='admin', decision=?, decided_at=? WHERE ref_order_id=? AND status='open'").run(s, r, now(), ro.id);
    if (a === 'approve') { if (ro.status !== 'review') throw new HttpError(409, 'Only referrals under review can be approved.'); closeFlags('approved'); if (paid) begin(ro, 'admin'); else move(ro, 'pending', 'Approved by admin: ' + r, 'admin'); }
    else if (a === 'reject') { if (!['review', 'qualifying', 'qualified'].includes(ro.status)) throw new HttpError(409, 'This referral cannot be rejected in its current state.'); closeFlags('rejected'); reverse(ro, r, 'admin'); move(ro, 'rejected', 'Rejected by admin: ' + r, 'admin'); }
    else if (a === 'reverse') { if (!['qualifying', 'qualified'].includes(ro.status)) throw new HttpError(409, 'Only earned or pending rewards can be reversed.'); reverse(ro, r, 'admin'); move(ro, 'reversed', 'Reversed by admin: ' + r, 'admin'); }
    else if (a === 'reinstate') { if (!['reversed', 'rejected'].includes(ro.status)) throw new HttpError(409, 'Only reversed or rejected referrals can be reinstated.'); if (!paid) throw new HttpError(409, 'The order is no longer paid, so the reward cannot be reinstated.'); begin(ro, 'admin'); }
    else throw new HttpError(400, 'Unknown action.');
    audit('referral.' + a, 'referral:' + ro.id, r, { from: ro.status, order: '#' + (1000 + ro.order_id) });
  });
  release(); return { ok: true };
});
route('GET', '/api/admin/referral/people', A, () => ({ rows: db.prepare("SELECT u.id, u.name, u.email, c.id cid, c.code, c.status cstatus, COALESCE(p.status,'active') pstatus, p.reason, (SELECT COUNT(*) FROM ref_orders WHERE referrer_id=u.id) n FROM ref_codes c JOIN users u ON u.id=c.user_id LEFT JOIN ref_people p ON p.user_id=u.id ORDER BY c.id DESC LIMIT 300").all().map(r => ({ ...r, ...(() => { const b = balance(r.id); return { available: b.available, pending: b.pending }; })() })) }));
route('POST', '/api/admin/referral/people/:id', A, ({ params, body }) => {
  const r = reason(body), uid = +params.id, a = body.action; if (!db.prepare('SELECT 1 FROM users WHERE id=?').get(uid)) throw new HttpError(404, 'User not found');
  if (['suspend', 'ineligible', 'reinstate'].includes(a)) db.prepare('INSERT INTO ref_people VALUES(?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET status=excluded.status, reason=excluded.reason, updated=excluded.updated').run(uid, a === 'reinstate' ? 'active' : a === 'suspend' ? 'suspended' : 'ineligible', r, now());
  else if (['disable', 'enable'].includes(a)) db.prepare('UPDATE ref_codes SET status=?, reason=? WHERE user_id=? AND id=?').run(a === 'disable' ? 'disabled' : 'active', r, uid, +body.codeId);
  else if (a === 'regenerate') { db.prepare("UPDATE ref_codes SET status='disabled', reason=? WHERE user_id=? AND status='active'").run('Regenerated: ' + r, uid); db.prepare('INSERT INTO ref_codes(user_id,code,created) VALUES(?,?,?)').run(uid, mkCode(), now()); }
  else throw new HttpError(400, 'Unknown action.'); audit('person.' + a, 'user:' + uid, r, ''); return { ok: true };
});
function report(from, to) {
  const rows = db.prepare("SELECT ro.status, ro.subtotal, ro.discount, ro.reward, o.total, o.credit_used, o.paid, p.status ps FROM ref_orders ro JOIN orders o ON o.id=ro.order_id LEFT JOIN payments p ON p.order_id=o.id WHERE ro.created>=? AND ro.created<?").all(from, to), z = { referrals: rows.length, paidOrders: 0, sales: 0, discounts: 0, refundedSales: 0, rewardsCost: 0, creditRedeemed: 0, flagged: 0 };
  for (const r of rows) { if (r.ps === 'refunded') z.refundedSales += r.subtotal; else if (r.paid) { z.paidOrders++; z.sales += r.subtotal; z.discounts += r.discount; z.creditRedeemed += r.credit_used; } if (r.status === 'review') z.flagged++; }
  z.rewardsCost = db.prepare("SELECT COALESCE(SUM(l.amount),0) s FROM ref_ledger l JOIN ref_orders ro ON ro.id=l.ref_order_id WHERE l.kind IN('reward','reversal','adjust') AND ro.created>=? AND ro.created<?").get(from, to).s;
  z.netContribution = z.sales - z.discounts - z.rewardsCost; z.note = 'Net contribution = sales on referred orders (after refunds) minus discounts and net rewards. It is NOT profit: product cost, payment fees, delivery and tax are not included.'; return z;
}
route('GET', '/api/admin/referral/report', A, ({ query }) => ({ report: report(+query.get('from') || 0, +query.get('to') || now() + DAY), clicks: db.prepare('SELECT COALESCE(SUM(n),0) n FROM ref_clicks').get().n }));
route('GET', '/api/admin/referral/export.csv', A, ({ query }) => {
  const kind = query.get('kind') === 'ledger' ? 'ledger' : 'referrals'; audit('export', kind, 'CSV export', 'by admin');
  const head = h => h.map(csvCell).join(',') + '\n', cell = v => typeof v === 'number' ? String(v) : csvCell(v);
  if (kind === 'ledger') return { csv: head(['Date (UTC)', 'User ID', 'Referral', 'Kind', 'Amount (NGN)', 'Available from (UTC)', 'Expires (UTC)', 'Reason', 'Actor']) + db.prepare('SELECT * FROM ref_ledger ORDER BY id').all().map(r => [iso(r.created), r.user_id, r.ref_order_id, r.kind, r.amount, iso(r.available_at), iso(r.expires_at), r.reason, r.actor].map(cell).join(',')).join('\n'), name: 'referral-ledger.csv' };
  return { csv: head(['Date (UTC)', 'Referral', 'Order', 'Referrer ID', 'Status', 'Qualifying subtotal (NGN)', 'Friend discount (NGN)', 'Reward (NGN)', 'Campaign version']) + db.prepare('SELECT * FROM ref_orders ORDER BY id').all().map(r => [iso(r.created), r.id, '#' + (1000 + r.order_id), r.referrer_id, r.status, r.subtotal, r.discount, r.reward, r.version].map(cell).join(',')).join('\n'), name: 'referrals.csv' };
});
route('GET', '/api/admin/referral/audit', A, () => ({ rows: db.prepare('SELECT at,actor,action,target,reason,detail FROM ref_audit ORDER BY id DESC LIMIT 200').all() }));
return { price, attach, onPaid, onReverse, release, isLive, buildLines, balance };
};
REF = referralInit({ db, route, tx, now, str, HttpError, naira, csvCell, hx, sendMail, mailShell, quiet, productsAll: pricedAll, shipFor, zoneOf, getSettings, PM, crypto, get SITE() { return SITE; }, mailOn, SECRET });

/* ---------- automatic safety snapshots: your products, posts, orders and customers are copied before anything else runs ----------
   A copy of the whole database is saved in data/backups every 6 hours at most (the 14 newest are kept). Updating server.js or index.html never touches data/. */
(() => {
  try {
    const dir = path.join(DATA, 'backups'); fs.mkdirSync(dir, { recursive: true });
    const old = fs.readdirSync(dir).filter(f => /^store-\d{8}-\d{4}\.db$/.test(f)).sort(), last = old.length ? fs.statSync(path.join(dir, old[old.length - 1])).mtimeMs : 0;
    const snap = () => { try { const d = new Date(), z = n => String(n).padStart(2, '0'), name = `store-${d.getUTCFullYear()}${z(d.getUTCMonth() + 1)}${z(d.getUTCDate())}-${z(d.getUTCHours())}${z(d.getUTCMinutes())}.db`; const f = path.join(dir, name); if (!fs.existsSync(f)) db.exec(`VACUUM INTO '${f.replace(/'/g, "''")}'`); const all = fs.readdirSync(dir).filter(x => /^store-\d{8}-\d{4}\.db$/.test(x)).sort(); all.slice(0, Math.max(0, all.length - 14)).forEach(x => { try { fs.unlinkSync(path.join(dir, x)); } catch {} }); } catch (e) { console.error('Snapshot failed:', e.message); } };
    if (Date.now() - last > 6 * 36e5) snap(); setInterval(() => { try { snap(); } catch {} }, 6 * 36e5).unref();
    console.log(`Safety copy: ${db.prepare('SELECT COUNT(*) n FROM products').get().n} products are in the store.`);
  } catch (e) { console.error('Snapshot setup failed:', e.message); }
})();

/* ---------- second copy outside the app folder (see "data safety" near the top) ----------
   Every 3 hours, and again when the app is told to stop (which is what happens just before an update or redeploy): the database, any new
   uploaded pictures and the login secret are copied to BACKUP_DIR. Nothing is ever deleted from there except the oldest database copies (12 are kept). */
(() => {
  const run = force => {
    try {
      const up = path.join(BACKUP_OUT, 'uploads'); fs.mkdirSync(up, { recursive: true });
      const have = fs.readdirSync(BACKUP_OUT).filter(f => SNAP_RE.test(f)).map(f => ({ f, m: fs.statSync(path.join(BACKUP_OUT, f)).mtimeMs })).sort((a, b) => a.m - b.m);
      if (force || !have.length || Date.now() - have[have.length - 1].m > 3 * 36e5) {
        const d = new Date(), z = n => String(n).padStart(2, '0'), f = path.join(BACKUP_OUT, `store-${d.getUTCFullYear()}${z(d.getUTCMonth() + 1)}${z(d.getUTCDate())}-${z(d.getUTCHours())}${z(d.getUTCMinutes())}.db`);
        if (!fs.existsSync(f)) { db.exec(`VACUUM INTO '${f.replace(/'/g, "''")}'`); have.push({ f: path.basename(f), m: Date.now() }); }
        have.slice(0, Math.max(0, have.length - 12)).forEach(x => { try { fs.unlinkSync(path.join(BACKUP_OUT, x.f)); } catch {} });
      }
      for (const f of fs.readdirSync(UPLOADS)) { if (f.endsWith('.part')) continue; const a = path.join(UPLOADS, f), b = path.join(up, f); try { const s = fs.statSync(a); if (s.isFile() && (!fs.existsSync(b) || fs.statSync(b).size !== s.size)) fs.copyFileSync(a, b); } catch {} }
      try { fs.copyFileSync(path.join(DATA, '.secret'), path.join(BACKUP_OUT, '.secret')); } catch {}
    } catch (e) { if (!run.warned) { run.warned = true; console.error('Could not keep a copy outside the app folder (' + BACKUP_OUT + '): ' + e.message + '. Set BACKUP_DIR to a folder outside the app, ideally on another disk.'); } }
  };
  setTimeout(run, 15e3).unref(); setInterval(run, 3 * 36e5).unref();
  const bye = () => { try { run(true); } catch {} process.exit(0); };
  process.on('SIGTERM', bye); process.on('SIGINT', bye);
  console.log('Second safety copy (database, pictures, login secret): ' + BACKUP_OUT);
})();

/* ---------- Glow Rewards: Glow Points + Glow Friend referrals ----------
   Points live in an append-only ledger (glow_ledger). Hooks into orders are minimal and fenced:
   a Glow problem can never break an order or a payment. Delete this block to remove the feature. */
const GLOW = (() => {
  const DAY = 864e5, GA = { auth: 'admin' };
  db.exec(`CREATE TABLE IF NOT EXISTS glow_ledger(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, kind TEXT NOT NULL, points INTEGER NOT NULL, order_id INTEGER, claim_id INTEGER, idem TEXT NOT NULL UNIQUE, note TEXT DEFAULT '', expires_at INTEGER, created INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS glow_ledger_user ON glow_ledger(user_id);
CREATE TABLE IF NOT EXISTS glow_claims(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, tier TEXT NOT NULL, tier_name TEXT NOT NULL, reward TEXT DEFAULT '', points INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'requested', note TEXT DEFAULT '', created INTEGER NOT NULL, decided INTEGER);
CREATE TABLE IF NOT EXISTS glow_links(order_id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, created INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS glow_codes(user_id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE, created INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS glow_refs(id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER NOT NULL UNIQUE, referrer_id INTEGER NOT NULL, friend_key TEXT NOT NULL, discount INTEGER NOT NULL, bonus INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS glow_refs_friend ON glow_refs(friend_key) WHERE status!='void';`);
  try { db.exec('ALTER TABLE orders ADD COLUMN glow_disc INTEGER NOT NULL DEFAULT 0'); } catch {}

  /* ---- settings (editable in Admin > Glow Rewards; saved in the settings table) ---- */
  const DEF = {
    on: true, mode: 'category', per_naira: 400, overrides: {}, expiry_months: 12, friend_off: 1000, friend_bonus: 30, terms: '',
    cats: [
      { key: 'bundle', label: 'Bundles and kits', pts: 50, kw: 'bundle, kit, set, duo, trio, routine' },
      { key: 'sunscreen', label: 'Sunscreen', pts: 20, kw: 'sunscreen, spf, sunblock, sun cream' },
      { key: 'mask', label: 'Face mask and selected extras', pts: 10, kw: 'mask, patch' },
      { key: 'toner', label: 'Toner or micellar water', pts: 15, kw: 'toner, micellar, essence, mist' },
      { key: 'serum', label: 'Serum, including Vitamin C or Niacinamide', pts: 15, kw: 'serum, vitamin c, niacinamide, concentrate, ampoule, solution' },
      { key: 'cleanser', label: 'Cleanser', pts: 10, kw: 'cleanser, cleansing, face wash, foaming wash' },
      { key: 'moisturizer', label: 'Moisturizer', pts: 20, kw: 'moisturi, cream, lotion, gel cream' }
    ],
    tiers: [
      { id: 'starter', name: 'Glow Starter', pts: 100, reward: 'A complimentary sheet mask and free delivery on your next order' },
      { id: 'queen', name: 'Glow Queen', pts: 250, reward: 'A complimentary 50 ml cleanser and a ₦2,000 voucher' },
      { id: 'vip', name: 'Glow VIP', pts: 500, reward: '₦5,000 off, a complimentary full-size gift, early access to new products, and a birthday gift' }
    ]
  };
  const int = (v, lo, hi, d) => { v = Math.floor(+v); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d; };
  const clean = c => {
    c = c || {}; const o = { ...DEF };
    o.on = c.on !== false; o.mode = c.mode === 'spend' ? 'spend' : 'category';
    o.per_naira = int(c.per_naira, 1, 1e6, DEF.per_naira); o.expiry_months = int(c.expiry_months, 1, 120, DEF.expiry_months);
    o.friend_off = int(c.friend_off, 0, 1e6, DEF.friend_off); o.friend_bonus = int(c.friend_bonus, 0, 1e5, DEF.friend_bonus); o.terms = str(c.terms, 6000);
    o.overrides = {}; if (c.overrides && typeof c.overrides === 'object') for (const [k, v] of Object.entries(c.overrides).slice(0, 5000)) { const n = Math.floor(+v); if (k.length <= 64 && v !== '' && v !== null && Number.isFinite(n) && n >= 0 && n <= 1e5) o.overrides[k] = n; }   // per-product points set by the admin: they live here, never inside the product
    if (Array.isArray(c.cats)) o.cats = c.cats.slice(0, 30).map((x, i) => ({ key: str(x.key, 30) || 'c' + i, label: str(x.label, 80), pts: int(x.pts, 0, 1e5, 0), kw: str(x.kw, 300) })).filter(x => x.label);
    if (Array.isArray(c.tiers)) o.tiers = c.tiers.slice(0, 12).map((x, i) => ({ id: str(x.id, 30) || 't' + i, name: str(x.name, 60), pts: int(x.pts, 1, 1e7, 100), reward: str(x.reward, 400) })).filter(x => x.name).sort((a, b) => a.pts - b.pts);
    return o;
  };
  const cfg = () => { try { return clean(JSON.parse((db.prepare("SELECT v FROM settings WHERE k='glow_cfg'").get() || {}).v || 'null')); } catch { return clean(null); } };

  /* ---- points for an order ---- */
  const esc = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const matcher = c => c.cats.map(x => ({ ...x, re: x.kw.split(',').map(k => k.trim().toLowerCase()).filter(Boolean).map(k => new RegExp('(^|[^a-z])' + esc(k))) }));
  function pointsFor(lines, c) {
    c = c || cfg(); const prods = new Map(productsAll().map(p => [p.id, p])), ms = matcher(c); let total = 0; const detail = [];
    for (const l of lines) {
      if (l.id === 'giftcard') continue;   // gift cards never earn points
      const p = prods.get(l.id) || {}, text = [l.name, p.name, p.tag, p.dept, p.deptLabel].filter(Boolean).join(' ').toLowerCase();
      const hit = c.mode === 'category' ? ms.find(m => m.re.some(r => r.test(text))) : null;
      const ov = c.overrides && Object.prototype.hasOwnProperty.call(c.overrides, l.id) ? c.overrides[l.id] : null;   // the admin's own number for this product wins
      const pts = ov !== null ? ov * l.qty : hit ? hit.pts * l.qty : Math.floor((l.price * l.qty) / c.per_naira);   // anything no category matches falls back to the spend rate
      if (pts > 0) { total += pts; detail.push({ name: l.name, qty: l.qty, pts, cat: hit ? hit.label : 'Spend' }); }
    }
    return { points: total, detail };
  }
  /* points one unit of a product earns: used to show "Earn N Glow Points" on every product and in Admin > Discounts & Points */
  const unitPts = (p, price, c, ms, noOv) => {
    const text = [p.name, p.tag, p.dept, p.deptLabel].filter(Boolean).join(' ').toLowerCase(), hit = c.mode === 'category' ? ms.find(m => m.re.some(r => r.test(text))) : null;
    const ov = !noOv && c.overrides && Object.prototype.hasOwnProperty.call(c.overrides, p.id) ? c.overrides[p.id] : null;
    return ov !== null ? ov : hit ? hit.pts : Math.floor(price / c.per_naira);
  };
  GLOWPTS = {
    on: () => cfg().on,
    all: (products, sales) => { const c = cfg(); if (!c.on) return {}; const ms = matcher(c), out = {}; for (const p of products) { if (!p || !p.id || p.hidden || p.draft) continue; const n = unitPts(p, (sales[p.id] || {}).price || Math.round(+p.price) || 0, c, ms); if (n > 0) out[p.id] = n; } return out; },
    detail: (p, price) => { const c = cfg(), ms = matcher(c), manual = c.overrides && Object.prototype.hasOwnProperty.call(c.overrides, p.id) ? c.overrides[p.id] : null; return { auto: unitPts(p, price, c, ms, true), manual, now: c.on ? unitPts(p, price, c, ms) : 0 }; }
  };
  const lineOf = items => {
    if (!Array.isArray(items) || items.length > 50) throw new HttpError(400, 'Your bag is empty.');
    const prods = new Map(pricedAll().map(p => [p.id, p])), out = [];
    for (const it of items) {
      const qty = Math.floor(+it.qty); if (!(qty >= 1 && qty <= 20)) continue;
      if (it.giftCard) { out.push({ id: 'giftcard', name: 'Gift Card', price: Math.floor(+it.price) || 0, qty }); continue; }
      const p = prods.get(str(it.id, 64)); if (p) out.push({ id: p.id, name: p.name, price: Math.round(p.price), qty });
    }
    return out;
  };

  /* ---- balance: spending uses the oldest points first, so expiry never double-counts what was spent ---- */
  function balance(uid, c) {
    c = c || cfg(); const t = now(), rows = db.prepare('SELECT points, expires_at, created FROM glow_ledger WHERE user_id=? AND points!=0 ORDER BY created, id').all(uid);
    let neg = -rows.filter(r => r.points < 0).reduce((s, r) => s + r.points, 0), avail = 0, soon = null;
    for (const r of rows) {
      if (r.points <= 0) continue; const use = Math.min(r.points, neg); neg -= use; const left = r.points - use; if (!left) continue;
      if (r.expires_at && r.expires_at <= t) continue; avail += left;
      if (r.expires_at && (!soon || r.expires_at < soon.at)) soon = { at: r.expires_at, pts: left };
    }
    const prods = db.prepare("SELECT o.* FROM orders o LEFT JOIN glow_links l ON l.order_id=o.id WHERE COALESCE(o.user_id, l.user_id)=? AND o.status!='Cancelled' AND NOT EXISTS(SELECT 1 FROM glow_ledger g WHERE g.idem='earn:'||o.id)").all(uid);
    let pending = 0; for (const o of prods) { try { pending += pointsFor(JSON.parse(o.items), c).points; } catch {} }
    return { available: avail, pending, soon };
  }
  const tierInfo = (pts, c) => { const ts = c.tiers; let cur = null, next = null; for (const x of ts) { if (pts >= x.pts) cur = x; else if (!next) next = x; } return { current: cur, next, need: next ? next.pts - pts : 0 }; };
  const expiryAt = (c, from) => { const d = new Date(from || now()); d.setMonth(d.getMonth() + c.expiry_months); return d.getTime(); };
  const ins = (uid, kind, points, oid, claim, idem, note, exp) => db.prepare('INSERT OR IGNORE INTO glow_ledger(user_id,kind,points,order_id,claim_id,idem,note,expires_at,created) VALUES(?,?,?,?,?,?,?,?,?)').run(uid, kind, points, oid || null, claim || null, idem, str(note, 200), exp || null, now()).changes;

  /* ---- earn / reverse / friend bonus: idempotent, safe to run as often as we like ---- */
  let busy = false;
  function sync() {
    if (busy) return; busy = true;
    try {
      const c = cfg(); if (!c.on) return;
      for (const u of db.prepare('SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM glow_codes) LIMIT 200').all()) { try { codeFor(u, c); } catch (e) { console.error('Glow code failed for user', u.id, e.message); } }   // every account has its Glow Friend code ready
      for (const o of db.prepare("SELECT o.*, COALESCE(o.user_id, l.user_id) AS uid FROM orders o LEFT JOIN glow_links l ON l.order_id=o.id WHERE COALESCE(o.user_id, l.user_id) IS NOT NULL AND o.paid=1 AND o.status='Delivered' AND NOT EXISTS(SELECT 1 FROM glow_ledger g WHERE g.idem='earn:'||o.id)").all()) {
        try { const r = pointsFor(JSON.parse(o.items), c); ins(o.uid, 'earn', r.points, o.id, 0, 'earn:' + o.id, 'Order ' + oid(o), expiryAt(c)); } catch (e) { console.error('Glow earn failed for order', o.id, e.message); }
      }
      for (const e of db.prepare("SELECT l.*, o.paid, o.status FROM glow_ledger l JOIN orders o ON o.id=l.order_id WHERE l.kind='earn' AND l.points>0").all()) {   // order cancelled or marked unpaid after points were given: take them back; reinstated: give them back
        const net = db.prepare("SELECT COALESCE(SUM(points),0) s, COUNT(*) n FROM glow_ledger WHERE order_id=? AND user_id=? AND kind IN('earn','reverse','restore')").get(e.order_id, e.user_id), bad = !e.paid || e.status === 'Cancelled';
        if (bad && net.s > 0) ins(e.user_id, 'reverse', -e.points, e.order_id, 0, `rev:${e.order_id}:${net.n}`, 'Order cancelled or not paid');
        else if (!bad && net.s <= 0) ins(e.user_id, 'restore', e.points, e.order_id, 0, `res:${e.order_id}:${net.n}`, 'Order reinstated', expiryAt(c));
      }
      for (const r of db.prepare("SELECT r.*, o.paid, o.status ost FROM glow_refs r JOIN orders o ON o.id=r.order_id WHERE r.status IN('pending','awarded')").all()) {
        if (r.ost === 'Cancelled') {
          if (r.status === 'awarded' && r.bonus > 0) ins(r.referrer_id, 'reverse', -r.bonus, r.order_id, 0, 'fbrev:' + r.id, 'Friend order cancelled');
          db.prepare("UPDATE glow_refs SET status='void' WHERE id=?").run(r.id);   // frees the friend's one use
        } else if (r.status === 'pending' && r.paid && r.ost === 'Delivered') {
          if (r.bonus > 0) ins(r.referrer_id, 'bonus', r.bonus, r.order_id, 0, 'fb:' + r.id, 'Glow Friend order completed', expiryAt(c));
          db.prepare("UPDATE glow_refs SET status='awarded' WHERE id=? AND status='pending'").run(r.id);
        }
      }
    } catch (e) { console.error('Glow sync failed:', e.message); } finally { busy = false; }
  }
  setInterval(sync, 6e4).unref();

  /* ---- Glow Friend codes: GLOW-[FIRST NAME]1000 ---- */
  const canon = e => { const [l, d] = String(e || '').toLowerCase().split('@'); if (!d) return String(e || '').toLowerCase(); return (/^(gmail|googlemail)\./.test(d) ? l.split('+')[0].replace(/\./g, '') : l.split('+')[0]) + '@' + d; };
  const tail = p => String(p || '').replace(/\D/g, '').slice(-10);
  const fkey = e => crypto.createHmac('sha256', SECRET).update('glow:' + canon(e)).digest('hex');
  function codeFor(user, c) {
    let r = db.prepare('SELECT code FROM glow_codes WHERE user_id=?').get(user.id); if (r) return r.code;
    const first = (String(user.name || '').trim().split(/\s+/)[0] || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12) || 'FRIEND';
    for (let k = 1; k < 500; k++) {
      const code = k === 1 ? `GLOW-${first}${c.friend_off}` : `GLOW-${first}${k}-${c.friend_off}`;
      if (db.prepare('INSERT OR IGNORE INTO glow_codes(user_id,code,created) VALUES(?,?,?)').run(user.id, code, now()).changes) return code;
      if ((r = db.prepare('SELECT code FROM glow_codes WHERE user_id=?').get(user.id))) return r.code;   // created by a parallel request
    }
    throw new HttpError(500, 'Could not create your code. Please try again.');
  }
  const BAD = 'This Glow Friend code cannot be used on this order.';
  function price(a) {   // a = { code, user, email, phone, lines } -> { discount, referrer?, message }
    const c = cfg(), fail = m => ({ discount: 0, message: m });
    if (!c.on || !c.friend_off) return fail('Glow Friend offers are not running right now.');
    const rc = /^GLOW-[A-Z0-9-]{3,30}$/.test(a.code) && db.prepare('SELECT g.user_id, u.email, u.phone FROM glow_codes g JOIN users u ON u.id=g.user_id WHERE g.code=?').get(a.code);
    if (!rc) return fail(BAD);
    const email = String(a.email || '').toLowerCase(); if (!emailOk(email)) return fail('Enter your email address first, then apply the code.');
    if ((a.user && a.user.id === rc.user_id) || canon(email) === canon(rc.email) || (tail(a.phone) && tail(a.phone) === tail(rc.phone))) return fail(BAD);   // own code: same answer as an unknown code
    const elig = a.lines.filter(l => l.id !== 'giftcard').reduce((t, l) => t + l.price * l.qty, 0); if (!elig) return fail('The Glow Friend offer applies to skincare products, not gift cards.');
    const prev = db.prepare("SELECT id, user_id, phone FROM orders WHERE status!='Cancelled' AND (lower(email)=? OR (? IS NOT NULL AND user_id=?))").all(email, a.user ? a.user.id : null, a.user ? a.user.id : null);
    if (prev.length || (tail(a.phone) && db.prepare("SELECT phone FROM orders WHERE status!='Cancelled'").all().some(o => tail(o.phone) === tail(a.phone)))) return fail('The Glow Friend offer is for first orders only.');
    if (db.prepare("SELECT 1 FROM glow_refs WHERE friend_key=? AND status!='void'").get(fkey(email))) return fail('This offer has already been used with this email address.');
    return { discount: Math.min(c.friend_off, elig), referrer: rc.user_id, key: fkey(email), bonus: c.friend_bonus };
  }
  function attach(orderId, gp) {   // inside the order transaction
    if (!gp || !gp.discount) return;
    try { db.prepare('INSERT INTO glow_refs(order_id,referrer_id,friend_key,discount,bonus,created) VALUES(?,?,?,?,?,?)').run(orderId, gp.referrer, gp.key, gp.discount, gp.bonus, now()); }
    catch { throw new HttpError(409, 'This Glow Friend offer was just used. Please remove the code and try again.'); }
  }

  /* ---- public + customer routes ---- */
  const pub = c => ({ active: c.on, mode: c.mode, per_naira: c.per_naira, expiry_months: c.expiry_months, friend_off: c.friend_off, friend_bonus: c.friend_bonus, cats: c.cats.map(x => ({ label: x.label, pts: x.pts })), tiers: c.tiers, terms: c.terms, whatsapp: getSettings().whatsapp || '' });
  route('GET', '/api/glow/public', { limit: ['gpub', 60] }, () => pub(cfg()));
  route('POST', '/api/glow/quote', { auth: 'optional', limit: ['gq', 30] }, ({ user, body }) => {
    const c = cfg(), lines = lineOf(body.items), sub = lines.reduce((t, l) => t + l.price * l.qty, 0), code = str(body.code, 40).toUpperCase().replace(/\s/g, '');
    const pts = c.on && user ? pointsFor(lines, c).points : 0, out = { signedIn: !!user, earn: pts, on: c.on, discount: 0, message: '', subtotal: sub };
    if (code) { const g = price({ code, user, email: str(body.email, 254).toLowerCase(), phone: str(body.phone, 30), lines }); out.discount = g.discount; out.message = g.message || ''; }
    const physical = lines.some(l => l.id !== 'giftcard'), zone = zoneOf(str(body.state, 40)); out.shippingKnown = !physical || (!!zone && zone !== 'intl'); out.shipping = physical && out.shippingKnown ? shipFor(getSettings(), zone, sub, true) : 0; out.total = sub - out.discount + out.shipping;
    return out;
  });
  route('GET', '/api/glow/me', { auth: 'user' }, ({ user }) => {
    sync(); const c = cfg(), b = balance(user.id, c), t = tierInfo(b.available, c), code = c.on ? codeFor(user, c) : '';
    const hist = db.prepare("SELECT kind, points, note, created, expires_at FROM glow_ledger WHERE user_id=? AND points!=0 ORDER BY id DESC LIMIT 40").all(user.id);
    const claims = db.prepare('SELECT id, tier_name, reward, points, status, note, created FROM glow_claims WHERE user_id=? ORDER BY id DESC LIMIT 20').all(user.id);
    const fr = db.prepare("SELECT status, COUNT(*) n FROM glow_refs WHERE referrer_id=? AND status!='void' GROUP BY status").all(user.id), n = s => (fr.find(x => x.status === s) || {}).n || 0;
    return { ...pub(c), balance: b.available, pending: b.pending, soon: b.soon, tier: t.current, next: t.next, need: t.need, code, link: code ? `${SITE}/?glow=${code}` : '', hist, claims, friends: { done: n('awarded'), waiting: n('pending') }, name: user.name };
  });
  route('POST', '/api/glow/claim', { auth: 'user', limit: ['gclaim', 6] }, ({ user, body }) => {
    sync(); const c = cfg(); if (!c.on) throw new HttpError(409, 'Glow Rewards are not running right now.');
    const t = c.tiers.find(x => x.id === str(body.tier, 30)); if (!t) throw new HttpError(400, 'Choose a reward.');
    const out = tx(() => {
      const b = balance(user.id, c); if (b.available < t.pts) throw new HttpError(409, `You need ${t.pts} Glow Points for ${t.name}. You have ${b.available}.`);
      const id = db.prepare('INSERT INTO glow_claims(user_id,tier,tier_name,reward,points,created) VALUES(?,?,?,?,?,?)').run(user.id, t.id, t.name, t.reward, t.pts, now()).lastInsertRowid;
      ins(user.id, 'redeem', -t.pts, 0, id, 'claim:' + id, t.name + ' reward requested');
      return id;
    });
    const s = getSettings(), msg = `Hello Skinsentials22, I would like to claim my ${t.name} reward (${t.pts} Glow Points). Claim #${out}. Name: ${user.name}. Email: ${user.email}`;
    quiet(notifyAdmin(`✨ Glow reward claim #${out}: ${t.name}`, [`${user.name} · ${user.phone || user.email}`, `${t.pts} points · ${t.reward}`, 'Confirm it in Admin > Glow Rewards.']));
    return { id: out, whatsapp: s.whatsapp ? `https://wa.me/${s.whatsapp.replace(/\D/g, '')}?text=${encodeURIComponent(msg)}` : '' };
  });

  /* ---- earlier guest orders: a signed-in customer proves an order is theirs with its reference + the email or phone used ---- */
  route('POST', '/api/glow/link-order', { auth: 'user', limit: ['glink', 8] }, ({ user, body }) => {
    const c = cfg(); if (!c.on) throw new HttpError(409, 'Glow Rewards are not running right now.');
    const ref = str(body.ref, 40).toUpperCase().replace(/\s/g, ''), contact = str(body.contact, 254).trim(), NO = 'We could not match that order. Check the order reference and the email or phone you used when you ordered.';
    const o = /^SK22-[0-9A-F]{10}$/.test(ref) && db.prepare('SELECT * FROM orders WHERE ref=?').get(ref); if (!o || !contact) throw new HttpError(404, NO);
    const same = contact.includes('@') ? canon(contact) === canon(o.email) : (tail(contact).length >= 7 && tail(contact) === tail(o.phone));
    if (!same || o.status === 'Cancelled') throw new HttpError(404, NO);
    if (o.user_id && o.user_id !== user.id) throw new HttpError(404, NO);
    const had = db.prepare('SELECT user_id FROM glow_links WHERE order_id=?').get(o.id); if (had && had.user_id !== user.id) throw new HttpError(404, NO);
    if (o.user_id === user.id || had) return { ok: true, already: true, message: 'That order is already on your account.' };
    db.prepare('INSERT OR IGNORE INTO glow_links(order_id,user_id,created) VALUES(?,?,?)').run(o.id, user.id, now()); sync();
    const pts = pointsFor(JSON.parse(o.items), c).points; return { ok: true, points: pts, message: pts ? `Order ${oid(o)} added. It will earn about ${pts} Glow Points once it is paid and delivered.` : `Order ${oid(o)} added.` };
  });
  const estimate = lines => { try { const c = cfg(); return c.on ? pointsFor(lines, c).points : 0; } catch { return 0; } };
  const note = o => {   // one paragraph for order emails
    try { const c = cfg(), pts = c.on ? pointsFor(JSON.parse(o.items), c).points : 0; if (!pts) return '';
      return o.user_id ? `<p style="background:#f4f7fb;padding:10px 12px;border-radius:8px">✨ You will earn about <b>${pts} Glow Points</b> once this order is delivered. See your balance in your account under Glow Points.</p>`
        : `<p style="background:#f4f7fb;padding:10px 12px;border-radius:8px">✨ This order can earn <b>${pts} Glow Points</b>. Create a free account, open <b>Account → Glow Points</b> and add this order with its reference <b>${hx(o.ref)}</b>.</p>`; } catch { return ''; }
  };

  /* ---- admin routes ---- */
  route('GET', '/api/admin/glow', GA, () => {
    sync(); const c = cfg(), prods = productsAll;
    const members = db.prepare('SELECT u.id, u.name, u.email, u.phone FROM users u WHERE u.id IN (SELECT user_id FROM glow_ledger) OR u.id IN (SELECT user_id FROM glow_codes) ORDER BY u.id DESC LIMIT 300').all().map(u => { const b = balance(u.id, c); return { ...u, balance: b.available, pending: b.pending, code: (db.prepare('SELECT code FROM glow_codes WHERE user_id=?').get(u.id) || {}).code || '' }; }).sort((a, b) => b.balance - a.balance);
    const claims = db.prepare('SELECT g.*, u.name, u.email, u.phone FROM glow_claims g JOIN users u ON u.id=g.user_id ORDER BY (g.status IN (\'requested\',\'approved\')) DESC, g.id DESC LIMIT 200').all();
    const refs = db.prepare('SELECT r.id, r.status, r.discount, r.bonus, r.created, o.id oid, o.customer, (SELECT name FROM users WHERE id=r.referrer_id) referrer FROM glow_refs r JOIN orders o ON o.id=r.order_id ORDER BY r.id DESC LIMIT 100').all().map(r => ({ ...r, order: '#' + (1000 + r.oid) }));
    const owed = members.reduce((s, m) => s + m.balance, 0);
    const ms = matcher(c), products = prods().map(p => { const text = [p.name, p.tag, p.dept, p.deptLabel].filter(Boolean).join(' ').toLowerCase(), hit = c.mode === 'category' ? ms.find(m => m.re.some(r => r.test(text))) : null; return { id: p.id, name: p.name, brand: p.brand || '', category: p.category || '', dept: p.deptLabel || p.dept || '', price: p.price, auto: hit ? hit.pts : Math.floor(Math.round(p.price) / c.per_naira), autoCat: hit ? hit.label : 'Spend rate', override: Object.prototype.hasOwnProperty.call(c.overrides, p.id) ? c.overrides[p.id] : null }; });
    return { cfg: c, products, stats: { members: members.length, outstanding: owed, open: claims.filter(x => ['requested', 'approved'].includes(x.status)).length, friendOrders: refs.filter(r => r.status !== 'void').length }, members, claims, refs };
  });
  route('PUT', '/api/admin/glow/config', GA, ({ body }) => { const c = clean({ ...body, overrides: cfg().overrides }); db.prepare("INSERT INTO settings VALUES('glow_cfg',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify(c)); return { cfg: c }; });
  route('PUT', '/api/admin/glow/overrides', GA, ({ body }) => {   // body: { ids: [...], points: number | null }  (null = back to automatic)
    const ids = Array.isArray(body.ids) ? body.ids.map(x => str(x, 64)).filter(Boolean).slice(0, 5000) : [], c = cfg(), ov = { ...c.overrides };
    if (!ids.length) throw new HttpError(400, 'Choose at least one product.');
    const clear = body.points === null || body.points === '' || body.points === undefined; const n = Math.floor(+body.points);
    if (!clear && !(Number.isFinite(n) && n >= 0 && n <= 1e5)) throw new HttpError(400, 'Enter points between 0 and 100000.');
    for (const id of ids) { if (clear) delete ov[id]; else ov[id] = n; }
    const out = clean({ ...c, overrides: ov }); db.prepare("INSERT INTO settings VALUES('glow_cfg',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify(out)); return { ok: true, changed: ids.length, overrides: Object.keys(out.overrides).length };
  });
  route('POST', '/api/admin/glow/claims/:id', GA, ({ params, body }) => {
    const st = str(body.status, 12), cl = db.prepare('SELECT * FROM glow_claims WHERE id=?').get(+params.id); if (!cl) throw new HttpError(404, 'Claim not found');
    if (!['approved', 'fulfilled', 'rejected'].includes(st)) throw new HttpError(400, 'Invalid status');
    if (['fulfilled', 'rejected'].includes(cl.status)) throw new HttpError(409, 'This claim is already closed.');
    tx(() => { db.prepare('UPDATE glow_claims SET status=?, note=?, decided=? WHERE id=?').run(st, str(body.note, 300), now(), cl.id); if (st === 'rejected') ins(cl.user_id, 'refund', cl.points, 0, cl.id, 'claimrej:' + cl.id, 'Reward request declined, points returned', expiryAt(cfg())); });
    inbox(cl.user_id, 'glow', st === 'approved' ? 'Your Glow reward was approved' : st === 'fulfilled' ? 'Your Glow reward is ready' : 'Your Glow reward request was declined', st === 'approved' ? `Your request for ${cl.tier_name} has been approved.` : st === 'fulfilled' ? `Your ${cl.tier_name} reward is complete. Enjoy!` : `We could not approve ${cl.tier_name}. Your ${cl.points} points are back in your balance.`, 'glow', 'claim:' + cl.id + ':' + st);
    return { ok: true };
  });
  route('POST', '/api/admin/glow/adjust', GA, ({ body }) => {
    const u = db.prepare('SELECT * FROM users WHERE lower(email)=?').get(str(body.email, 254).toLowerCase()), p = Math.trunc(+body.points);
    if (!u) throw new HttpError(404, 'No account with that email.'); if (!p || Math.abs(p) > 1e6) throw new HttpError(400, 'Enter the points to add (or a negative number to remove).');
    ins(u.id, 'adjust', p, 0, 0, 'adj:' + crypto.randomBytes(6).toString('hex'), str(body.note, 150) || 'Adjusted by Skinsentials22', p > 0 ? expiryAt(cfg()) : 0);
    return { balance: balance(u.id).available };
  });
  sync();
  return { price, attach, sync, estimate, note };
})();

/* ---------- Backups and Safe Update (Admin > Settings > Backups and safe update) ----------
   Manual backups are kept next to the automatic ones in data/backups and are never rotated out by them (newest 40 kept).
   "Restore products and posts" copies ONLY products, brands, blog/hero/announcement content and promo banners from a backup into
   the live store (after taking a fresh backup first). Orders, customers, payments and Glow points are never touched. */
const BK = (() => {
  const dir = path.join(DATA, 'backups'), started = Date.now(), NAME = /^store-\d{8}-\d{4,6}(-(manual|update|restore))?\.db$/;
  fs.mkdirSync(dir, { recursive: true });
  const z = n => String(n).padStart(2, '0'), q = s => String(s).replace(/'/g, "''");
  const kindOf = n => (/-(manual|update|restore)\.db$/.exec(n) || [])[1] || 'auto';
  const pathOf = name => { if (!NAME.test(String(name || ''))) throw new HttpError(404, 'Backup not found'); const f = path.join(dir, name); if (!fs.existsSync(f)) throw new HttpError(404, 'Backup not found'); return f; };
  const count = (d, sql) => { try { return d.prepare(sql).get().n; } catch { return 0; } };
  const postsOf = d => { try { const c = JSON.parse((d.prepare("SELECT v FROM settings WHERE k='content'").get() || {}).v || 'null'); return c && Array.isArray(c.posts) ? c.posts.length : 0; } catch { return 0; } };
  const nums = d => ({ products: count(d, 'SELECT COUNT(*) n FROM products'), posts: postsOf(d), orders: count(d, 'SELECT COUNT(*) n FROM orders'), customers: count(d, 'SELECT COUNT(*) n FROM users') });
  const cache = new Map();
  const info = name => {
    const f = path.join(dir, name), st = fs.statSync(f), key = name + st.mtimeMs + st.size; if (cache.has(key)) return cache.get(key);
    let n = { products: 0, posts: 0, orders: 0, customers: 0 }; try { const d = new DatabaseSync(f); try { n = nums(d); } finally { d.close(); } } catch {}
    const r = { name, kind: kindOf(name), at: st.mtimeMs, size: st.size, ...n }; cache.set(key, r); return r;
  };
  function make(kind) {
    const d = new Date(), name = `store-${d.getUTCFullYear()}${z(d.getUTCMonth() + 1)}${z(d.getUTCDate())}-${z(d.getUTCHours())}${z(d.getUTCMinutes())}${z(d.getUTCSeconds())}-${kind}.db`, f = path.join(dir, name);
    if (fs.existsSync(f)) return name; db.exec(`VACUUM INTO '${q(f)}'`);
    const man = fs.readdirSync(dir).filter(x => NAME.test(x) && kindOf(x) !== 'auto').sort(); man.slice(0, Math.max(0, man.length - 40)).forEach(x => { try { fs.unlinkSync(path.join(dir, x)); } catch {} });
    return name;
  }
  const list = () => fs.readdirSync(dir).filter(x => NAME.test(x)).map(x => { try { return info(x); } catch { return null; } }).filter(Boolean).sort((a, b) => b.at - a.at).slice(0, 60);
  const now2 = () => nums(db);
  route('GET', '/api/admin/backups', { auth: 'admin' }, () => ({ backups: list(), current: now2(), started, prepared: (() => { try { return JSON.parse((db.prepare("SELECT v FROM settings WHERE k='update_prepared'").get() || {}).v || 'null'); } catch { return null; } })() }));
  route('POST', '/api/admin/backups', { auth: 'admin', limit: ['bkmake', 10] }, ({ body }) => { const kind = body.kind === 'update' ? 'update' : 'manual', name = make(kind), i = info(name);
    if (kind === 'update') db.prepare("INSERT INTO settings VALUES('update_prepared',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify({ name, at: Date.now(), products: i.products, posts: i.posts, orders: i.orders, customers: i.customers }));
    return { backup: i, backups: list(), current: now2(), started }; });
  /* the upload that goes with Download: a .db backup file lands in the list above, then "Restore products and posts" works on it like any other */
  route('POST', '/api/admin/backups/upload', { auth: 'admin', stream: true, limit: ['bkup', 6] }, async ({ req }) => {
    const cap = MAX_UPLOAD || 500 * 1048576; if (+req.headers['content-length'] > cap) throw new HttpError(413, 'That file is too large to be a backup of this store.');
    let at = Date.now(), name = '', f = ''; const pick = () => { const t = new Date(at); name = `store-${t.getUTCFullYear()}${z(t.getUTCMonth() + 1)}${z(t.getUTCDate())}-${z(t.getUTCHours())}${z(t.getUTCMinutes())}${z(t.getUTCSeconds())}-manual.db`; f = path.join(dir, name); }; pick(); while (fs.existsSync(f)) { at += 1000; pick(); }   // two uploads in the same second get different names
    const tmp = f + '.part';
    let n = 0; const count = new Transform({ transform(c, _, cb) { n += c.length; n > cap ? cb(new HttpError(413, 'That file is too large to be a backup of this store.')) : cb(null, c); } });
    try {
      await pipeline(req, count, fs.createWriteStream(tmp)); if (!n) throw new HttpError(400, 'The file is empty.');
      const fd = fs.openSync(tmp, 'r'), b = Buffer.alloc(16); try { fs.readSync(fd, b, 0, 16, 0); } finally { fs.closeSync(fd); }
      if (b.toString('latin1') !== 'SQLite format 3\0') throw new HttpError(400, 'That is not a Skinsentials22 database backup. Choose a .db file made with "Download database backup".');
      let ok = false; try { const x = new DatabaseSync(tmp); try { ok = !!x.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='products'").get() && x.prepare('PRAGMA quick_check').get().quick_check === 'ok'; } finally { x.close(); } } catch {}
      if (!ok) throw new HttpError(400, 'That file is not a complete Skinsentials22 backup (no products table, or the file is damaged).');
      fs.renameSync(tmp, f);
    } catch (e) { fs.rm(tmp, { force: true }, () => {}); throw e; }
    return { ok: true, backup: info(name), backups: list(), current: now2(), started };
  });
  route('GET', '/api/admin/backups/:name/download', { auth: 'admin', limit: ['bkdl', 10] }, ({ params }) => ({ bin: { data: fs.readFileSync(pathOf(params.name)), name: params.name } }));
  route('POST', '/api/admin/backups/:name/restore-content', { auth: 'admin', limit: ['bkrest', 4] }, ({ params, body }) => {
    const f = pathOf(params.name); if (body.confirm !== true) throw new HttpError(400, 'Please confirm the restore.');
    const safety = make('restore');   // a fresh copy of what is live right now, so this restore can itself be undone
    db.exec(`ATTACH DATABASE '${q(f)}' AS bk`);
    try {
      const has = t => !!db.prepare("SELECT 1 FROM bk.sqlite_master WHERE type='table' AND name=?").get(t); if (!has('products')) throw new HttpError(400, 'That backup has no products to restore.');
      tx(() => {
        db.exec('DELETE FROM products'); db.exec('INSERT INTO products SELECT * FROM bk.products');
        if (has('brands')) { db.exec('DELETE FROM brands'); db.exec('INSERT INTO brands SELECT * FROM bk.brands'); }
        if (has('settings')) db.exec("INSERT INTO settings(k,v) SELECT k,v FROM bk.settings WHERE k IN ('content','promos') ON CONFLICT(k) DO UPDATE SET v=excluded.v");
        bumpRev('products_rev'); bumpRev('content_rev');
      });
    } finally { try { db.exec('DETACH DATABASE bk'); } catch {} }
    return { ok: true, safety, current: now2(), backups: list() };
  });
  return { make, started };
})();

/* ---------- Promo banners (Admin > Promo Banners) ----------
   Banners live in the settings table under 'promos'. They never touch products or posts. */
const PROMO = (() => {
  const KW = new Set(['shop', 'glow', 'quiz', 'videos', 'brands', 'blog', 'giftcards', 'consult', 'ingredients', 'referral']);
  const hex = (v, d) => /^#[0-9a-f]{6}$/i.test(String(v || '')) ? String(v).toLowerCase() : d;
  const ts = v => { const n = Math.floor(+v); return Number.isFinite(n) && n > 1e11 && n < 4.2e12 ? n : null; };
  const link = v => { v = String(v || '').replace(/[\u0000-\u001f\s]+/g, ' ').trim().slice(0, 300); return (/^https?:\/\/[^\s]+$/i.test(v) || /^\/[^\s/][^\s]*$/.test(v) || /^#[A-Za-z0-9_-]{1,60}$/.test(v) || /^p:[A-Za-z0-9_-]{1,64}$/.test(v) || KW.has(v)) ? v : ''; };
  const img = v => { v = String(v || '').trim().slice(0, 300); return (/^https:\/\/[^\s]+$/i.test(v) || /^\/uploads\/[A-Za-z0-9_.-]+$/.test(v)) ? v : ''; };
  const clean = list => (Array.isArray(list) ? list : []).slice(0, 20).map((b, i) => ({
    id: str(b && b.id, 30).replace(/[^A-Za-z0-9_-]/g, '') || 'b' + Date.now().toString(36) + i,
    on: !b || b.on !== false, place: b && b.place === 'strip' ? 'strip' : 'banner',
    title: str(b && b.title, 120), text: str(b && b.text, 300), btn: str(b && b.btn, 40), link: link(b && b.link), image: img(b && b.image),
    bg: hex(b && b.bg, '#0b2a4a'), fg: hex(b && b.fg, '#ffffff'), start: ts(b && b.start), end: ts(b && b.end),
    countdown: !!(b && b.countdown), dismiss: !b || b.dismiss !== false, updated: now()
  })).filter(b => b.title || b.text);
  const all = () => { try { const a = JSON.parse((db.prepare("SELECT v FROM settings WHERE k='promos'").get() || {}).v || '[]'); return Array.isArray(a) ? a : []; } catch { return []; } };
  const live = b => b.on && (!b.start || b.start <= now()) && (!b.end || b.end > now());
  route('GET', '/api/promos', { limit: ['promos', 120] }, () => ({ promos: all().filter(live), now: now() }));
  route('GET', '/api/admin/promos', { auth: 'admin' }, () => ({ promos: all(), now: now() }));
  route('PUT', '/api/admin/promos', { auth: 'admin' }, ({ body }) => {
    if (!Array.isArray(body.promos) || body.promos.length > 20) throw new HttpError(400, 'Invalid banner list');
    const prev = new Map(all().map(b => [b.id, b])), out = clean(body.promos).map(b => { const p = prev.get(b.id); return p && JSON.stringify({ ...p, updated: 0 }) === JSON.stringify({ ...b, updated: 0 }) ? p : b; });   // untouched banners keep their dismissed state on visitors' devices
    db.prepare("INSERT INTO settings VALUES('promos',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify(out)); return { promos: out, now: now() };
  });
  return { all };
})();

/* ---------- customer reviews: shown on the homepage, set and controlled in Admin > Reviews ----------
   Saved in the settings table under 'reviews' as { cfg, items }. They never touch products, orders or posts.
   The public endpoint returns only reviews the admin has left visible, and only when the section is switched on. */
const REVIEWS = (() => {
  const DEF = { on: true, kicker: 'Customer love', title: 'What our customers say', subtitle: '', showSummary: true, initial: 6, submit: false };
  const ts = v => { const n = Math.floor(+v); return Number.isFinite(n) && n > 1e11 && n < 4.2e12 ? n : null; };
  const img = v => { v = String(v || '').trim().slice(0, 300); return (/^https:\/\/[^\s]+$/i.test(v) || /^\/uploads\/[A-Za-z0-9_.-]+$/.test(v)) ? v : ''; };
  const cfgClean = c => {
    c = c && typeof c === 'object' ? c : {};
    return { on: c.on !== false, kicker: c.kicker === undefined ? DEF.kicker : str(c.kicker, 60), title: str(c.title, 100) || DEF.title, subtitle: str(c.subtitle, 200), showSummary: c.showSummary !== false, submit: c.submit === true, initial: Math.min(12, Math.max(3, Math.round(+c.initial) || DEF.initial)) };
  };
  const itemsClean = list => (Array.isArray(list) ? list : []).slice(0, 100).map((r, i) => {
    r = r && typeof r === 'object' ? r : {};
    return {
      id: str(r.id, 30).replace(/[^A-Za-z0-9_-]/g, '') || 'r' + Date.now().toString(36) + i,
      on: r.on !== false, name: str(r.name, 60), location: str(r.location, 60),
      rating: Math.min(5, Math.max(1, Math.round(+r.rating) || 5)), title: str(r.title, 100), text: str(r.text, 600),
      date: ts(r.date) || now(), verified: !!r.verified, pid: str(r.pid, 64), photo: img(r.photo)
    };
  }).filter(r => r.name && r.text);
  const read = () => {
    try { const d = JSON.parse((db.prepare("SELECT v FROM settings WHERE k='reviews'").get() || {}).v || 'null'); if (d) return { cfg: cfgClean(d.cfg), items: itemsClean(d.items) }; } catch {}
    return { cfg: { ...DEF }, items: [] };
  };
  const stats = list => {
    const dist = [0, 0, 0, 0, 0]; let sum = 0; for (const r of list) { dist[r.rating - 1]++; sum += r.rating; }
    return { count: list.length, avg: list.length ? Math.round(sum / list.length * 10) / 10 : 0, dist };
  };
  /* customer submissions wait in their own list ('reviews_pending') until the admin approves them, so nothing a visitor sends can reach the site by itself */
  const PK = 'reviews_pending';
  const pendRead = () => { try { const a = JSON.parse((db.prepare('SELECT v FROM settings WHERE k=?').get(PK) || {}).v || '[]'); return Array.isArray(a) ? a : []; } catch { return []; } };
  const pendWrite = a => db.prepare('INSERT INTO settings VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v').run(PK, JSON.stringify(a));
  const put = d => db.prepare("INSERT INTO settings VALUES('reviews',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify(d));
  const out = () => { const d = read(); return { ...d, pending: pendRead(), stats: stats(d.items.filter(r => r.on)) }; };
  route('GET', '/api/reviews', { limit: ['reviews', 120] }, () => {
    const d = read(), items = d.items.filter(r => r.on);
    return !d.cfg.on || (!items.length && !d.cfg.submit) ? { on: false } : { on: true, cfg: d.cfg, items, stats: stats(items) };
  });
  route('POST', '/api/reviews/submit', { auth: 'optional', limit: ['revsub', 6] }, ({ user, body }) => {
    const d = read(); if (!d.cfg.on || !d.cfg.submit) throw new HttpError(403, 'Reviews are not open right now.');
    if (str(body.website, 100)) return { ok: true };   // hidden field that only bots fill in: pretend it worked, store nothing
    const name = str(body.name, 60), email = str(body.email, 254).toLowerCase(), text = str(body.text, 600);
    if (!name) throw new HttpError(400, 'Please tell us your name.');
    if (!emailOk(email)) throw new HttpError(400, 'Please enter a valid email address. It is never shown on the website.');
    if (text.length < 10) throw new HttpError(400, 'Please write a sentence or two about your experience.');
    const list = pendRead(); if (list.length >= 200) throw new HttpError(429, 'We have a lot of reviews waiting right now. Please try again later.');
    if (list.some(p => p.email === email && p.text === text)) return { ok: true };
    const buyer = !!(user ? db.prepare('SELECT 1 FROM orders WHERE paid=1 AND (LOWER(email)=? OR user_id=?) LIMIT 1').get(email, user.id) : db.prepare('SELECT 1 FROM orders WHERE paid=1 AND LOWER(email)=? LIMIT 1').get(email));
    const p = { id: 'p' + now().toString(36) + crypto.randomBytes(3).toString('hex'), name, email, location: str(body.location, 60), rating: Math.min(5, Math.max(1, Math.round(+body.rating) || 5)), title: str(body.title, 100), text, pid: str(body.pid, 64), buyer, created: now() };
    list.push(p); pendWrite(list);
    quiet(notifyAdmin('New customer review waiting for approval', [`${p.name} gave ${p.rating}/5`, p.text, buyer ? 'Matched a paid order.' : 'No paid order found for this email.', 'Approve or reject it in Admin > Reviews.']));
    return { ok: true };
  });
  route('GET', '/api/admin/reviews', { auth: 'admin' }, () => out());
  route('PUT', '/api/admin/reviews', { auth: 'admin', big: true }, ({ body }) => {
    if (!Array.isArray(body.items) || body.items.length > 100) throw new HttpError(400, 'Invalid review list (100 reviews at most)');
    put({ cfg: cfgClean(body.cfg), items: itemsClean(body.items) }); return out();
  });
  route('POST', '/api/admin/reviews/pending/:id', { auth: 'admin' }, ({ params, body }) => {
    const list = pendRead(), i = list.findIndex(p => p.id === params.id); if (i < 0) throw new HttpError(404, 'That review was already handled. Reload the page.');
    const p = list[i];
    if (body.action === 'approve') {
      const d = read(); if (d.items.length >= 100) throw new HttpError(400, 'You already have 100 reviews. Delete one first.');
      put({ cfg: d.cfg, items: itemsClean([{ on: true, name: p.name, location: p.location, rating: p.rating, title: p.title, text: p.text, date: now(), verified: !!p.buyer, pid: p.pid, photo: '' }, ...d.items]) });
    } else if (body.action !== 'reject') throw new HttpError(400, 'Unknown action');
    list.splice(i, 1); pendWrite(list); return out();
  });
  return { read };
})();

/* ---------- server ---------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.webp': 'image/webp', '.gif': 'image/gif', '.txt': 'text/plain', '.mp4': 'video/mp4', '.m4v': 'video/mp4', '.mov': 'video/quicktime', '.webm': 'video/webm', '.ogv': 'video/ogg', '.jpeg': 'image/jpeg', '.apk': 'application/vnd.android.package-archive', '.pdf': 'application/pdf', '.zip': 'application/zip', '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.csv': 'text/csv' };
function readBody(req, max) {
  return new Promise((ok, no) => {
    const c = []; let n = 0, done = false; const fin = (f, v) => { if (done) return; done = true; clearTimeout(timer); f(v); };
    const timer = setTimeout(() => { fin(no, new HttpError(408, 'The request took too long. Please try again.')); req.destroy(); }, max > 1e6 ? 600000 : 30000);   // a slow trickle can no longer hold a connection open forever
    req.on('data', d => { n += d.length; if (n > max) { fin(no, new HttpError(413, 'Request too large')); req.destroy(); } else c.push(d); });
    req.on('end', () => fin(ok, Buffer.concat(c))); req.on('error', e => fin(no, e));
  });
}
const send = (res, code, data, type = 'application/json; charset=utf-8', extra = {}) => { res.writeHead(code, { 'Content-Type': type, 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY', 'Referrer-Policy': 'strict-origin-when-cross-origin', 'Permissions-Policy': 'camera=(), microphone=(), geolocation=(), interest-cohort=()', ...(String(type).startsWith('text/html') ? { 'Content-Security-Policy': "base-uri 'self'; object-src 'none'; frame-ancestors 'none'" } : {}), ...(SITE.startsWith('https') ? { 'Strict-Transport-Security': 'max-age=15552000' } : {}), ...extra }); res.end(data); };

/* serves uploaded files with HTTP Range support, which videos need to start quickly, seek and play on iPhone/Safari */
function serveUpload(req, res, name) {
  const f = path.join(UPLOADS, name);
  if (!/^[\w.-]+$/.test(name) || name.endsWith('.part') || !fs.existsSync(f)) throw new HttpError(404, 'Not found');
  const size = fs.statSync(f).size, type = MIME[path.extname(name).toLowerCase()] || 'application/octet-stream';
  const h = { 'Content-Type': type, 'Accept-Ranges': 'bytes', 'Cache-Control': 'public, max-age=31536000, immutable', 'X-Content-Type-Options': 'nosniff' };
  if (!/^(image|video)\//.test(type)) h['Content-Disposition'] = 'attachment; filename="' + name + '"';   // documents always download, they are never shown inside the site
  const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
  let start = 0, end = size - 1, code = 200;
  if (m && (m[1] || m[2])) {
    if (m[1]) { start = +m[1]; if (m[2]) end = Math.min(+m[2], size - 1); } else { start = Math.max(0, size - +m[2]); }
    if (start > end || start >= size) { res.writeHead(416, { ...h, 'Content-Range': `bytes */${size}` }); return res.end(); }
    code = 206; h['Content-Range'] = `bytes ${start}-${end}/${size}`;
  }
  h['Content-Length'] = end - start + 1; res.writeHead(code, h);
  if (req.method === 'HEAD') return res.end();
  const st = fs.createReadStream(f, { start, end }); st.on('error', () => res.destroy()); res.on('close', () => st.destroy()); st.pipe(res);
}

/* ---------- SEO: crawlable URLs, sitemap, robots, real titles/descriptions/Product data in the HTML ----------
   Google and WhatsApp/Facebook previews read the HTML the server sends. This gives every product, blog post and
   main page its own address (/p/ID/name, /blog/ID/title, /shop ...) with its own title, description, canonical link,
   social preview and Product structured data, plus /sitemap.xml and /robots.txt. It only reads; it never changes products. */
const SEO = (() => {
  const PAGES = { '/shop': ['Shop Skincare, Sun Care & Body Care in Nigeria | Skinsentials22', 'Browse all skincare essentials for every skin type. Authentic products, prices in naira, delivery across Nigeria.', 'catalog'],
    '/videos': ['Skincare Video Guides — Watch & Shop | Skinsentials22', 'Watch expert skincare video guides and shop the products featured.'],
    '/skin-quiz': ['Skin Finder Quiz — Find Your Routine | Skinsentials22', 'Answer a few questions and get a personalised skincare routine.'],
    '/skin-consultation': ['Free Skin Consultation in Nigeria | Skinsentials22', 'Talk to our skincare team and get routine advice that fits your skin and budget.'],
    '/ingredients': ['Shop Skincare by Ingredient | Skinsentials22', 'Find products by the ingredient your skin needs.'],
    '/gift-cards': ['Skincare Gift Cards | Skinsentials22', 'Send a Skinsentials22 gift card with a personal message.'],
    '/brands': ['Skincare Brands We Stock | Skinsentials22', 'Explore the skincare and beauty brands available at Skinsentials22.'],
    '/journal': ['Skincare Journal — Routines, Ingredients & Tips | Skinsentials22', 'Practical skincare advice and ingredient guides from the Skinsentials22 team.'],
    '/glow-rewards': ['Glow Rewards — Earn Points & Skincare Rewards | Skinsentials22', 'Earn Glow Points on every order, unlock skincare rewards and give a friend money off their first order.'] };
  const e = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const slug = s => String(s || '').toLowerCase().replace(/&/g, ' and ').replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 80) || 'item';
  const trim = (s, n) => { s = String(s || '').replace(/\s+/g, ' ').trim(); return s.length > n ? s.slice(0, n - 1).replace(/\s\S*$/, '') + '…' : s; };
  const abs = u => !u ? '' : /^https?:/i.test(u) ? u : /^data:/.test(u) ? '' : SITE + (u[0] === '/' ? '' : '/') + u;
  const jsn = o => JSON.stringify(o).replace(/</g, '\\u003c');
  const prods = () => { try { return pricedAll().filter(p => p && p.id && !p.hidden && !p.draft); } catch { return []; } };
  const posts = () => { try { return ((contentRaw() || {}).posts || []).filter(p => p && p.pub && p.id); } catch { return []; } };
  const pUrl = p => `${SITE}/p/${encodeURIComponent(p.id)}/${slug(p.name)}`, bUrl = p => `${SITE}/blog/${encodeURIComponent(p.id)}/${slug(p.title)}`;
  const day = () => new Date().toISOString().slice(0, 10);
  function page(m) {
    const oi = m.image || abs(getGeneral().og_image);   // Admin > General > Share picture
    let h; try { h = fs.readFileSync(path.join(PUB, 'index.html'), 'utf8'); } catch { return null; }
    for (const re of [/<title>[\s\S]*?<\/title>/, /<meta name="description"[^>]*>/, /<link rel="canonical"[^>]*>/, /<meta property="og:(title|description|url|type|image)"[^>]*>/g, /<meta name="twitter:(title|description|image)"[^>]*>/g]) h = h.replace(re, '');
    const tags = `<title>${e(m.title)}</title><meta name="description" content="${e(m.desc)}"><link rel="canonical" id="sk-canon" href="${e(m.url)}"><meta property="og:title" content="${e(m.title)}"><meta property="og:description" content="${e(m.desc)}"><meta property="og:url" content="${e(m.url)}"><meta property="og:type" content="${m.type || 'website'}">` +
      (oi ? `<meta property="og:image" content="${e(oi)}"><meta name="twitter:image" content="${e(oi)}">` : '') + `<meta name="twitter:title" content="${e(m.title)}"><meta name="twitter:description" content="${e(m.desc)}"><script>window.__SK_SEO__=${jsn(m.boot || { route: 'page' })};</script>`;
    h = h.replace('</head>', () => tags + '</head>');
    if (m.ld) h = h.replace('<script type="application/ld+json" id="sk-ld-page"></script>', () => `<script type="application/ld+json" id="sk-ld-page">${jsn(m.ld)}</script>`);
    if (m.body) h = h.replace(/(<body[^>]*>)/, (x, b) => b + '<noscript>' + m.body + '</noscript>');
    return injectHtml(h);
  }
  const links = (n = 200) => '<ul>' + prods().slice(0, n).map(p => `<li><a href="/p/${encodeURIComponent(p.id)}/${slug(p.name)}">${e(p.name)}</a> ₦${Math.round(+p.price || 0).toLocaleString('en-NG')}</li>`).join('') + '</ul>';
  const plinks = (n = 100) => '<ul>' + posts().slice(0, n).map(x => `<li><a href="/blog/${encodeURIComponent(x.id)}/${slug(x.title)}">${e(x.title)}</a></li>`).join('') + '</ul>';
  /* when each page last REALLY changed, so <lastmod> in the sitemap is honest: Google trusts that and re-reads changed pages sooner */
  const lastmods = () => {
    const cur = handle.urls(); let st = {}; try { st = JSON.parse(setting('seo_mod') || '{}'); } catch {}
    const t = new Date().toISOString(), out = {}; let dirty = false;
    for (const u of Object.keys(cur)) { if (!st[u] || st[u][0] !== cur[u]) { st[u] = [cur[u], t]; dirty = true; } out[u] = st[u][1]; }
    for (const u of Object.keys(st)) if (!(u in cur)) { delete st[u]; dirty = true; }
    if (dirty) keySet('seo_mod', JSON.stringify(st));
    return out;
  };
  const crumbs = l => ({ '@type': 'BreadcrumbList', itemListElement: l.map((c, i) => ({ '@type': 'ListItem', position: i + 1, name: c[0], item: c[1] })) });
  const nf = (res) => false;
  /* returns true when it handled the request */
  const setting = k => { try { return (db.prepare('SELECT v FROM settings WHERE k=?').get(k) || {}).v || ''; } catch { return ''; } };
  const keySet = (k, v) => { try { db.prepare('INSERT INTO settings VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v').run(k, v); } catch {} };
  const ixKey = () => { let k = setting('indexnow_key'); if (!/^[a-f0-9]{32}$/.test(k)) { k = crypto.randomBytes(16).toString('hex'); keySet('indexnow_key', k); } return k; };
  const handle = function (req, res, url) {
    const p = url.pathname, html = (code, body) => { send(res, code, req.method === 'HEAD' ? '' : body, 'text/html; charset=utf-8', { 'Cache-Control': 'no-cache' }); return true; };
    if (/^\/[a-f0-9]{32}\.txt$/.test(p) && p.slice(1, 33) === ixKey()) { send(res, 200, p.slice(1, 33), 'text/plain; charset=utf-8'); return true; }   // proves to search engines that this site owns the IndexNow key
    { const gf = getGeneral().gsc_file; if (gf && p === '/' + gf) { send(res, 200, req.method === 'HEAD' ? '' : 'google-site-verification: ' + gf, 'text/html; charset=utf-8', { 'Cache-Control': 'no-cache' }); return true; } }   // Search Console file chosen in Admin > General
    if (p === '/googlee63d671ac0925060.html') { send(res, 200, req.method === 'HEAD' ? '' : 'google-site-verification: googlee63d671ac0925060.html', 'text/html; charset=utf-8', { 'Cache-Control': 'no-cache' }); return true; }   // Google Search Console ownership check: served directly so it works wherever the site is deployed
    if (p === '/robots.txt') { send(res, 200, `User-agent: *\nAllow: /\nDisallow: /api/\nDisallow: /uploads/private/\n\nSitemap: ${SITE}/sitemap.xml\n`, 'text/plain; charset=utf-8', { 'Cache-Control': 'public, max-age=3600' }); return true; }
    if (p === '/feed.xml') {   // submit this in Search Console too (Sitemaps): it lists the 50 newest or changed pages, so Google sees a new post fast
      const lm = lastmods(), it = [];
      prods().forEach(x => it.push([pUrl(x), x.name, trim(x.description, 200), lm[pUrl(x)] || new Date().toISOString()]));
      posts().forEach(x => it.push([bUrl(x), x.title, trim(String(x.excerpt || x.body || ''), 200), lm[bUrl(x)] || new Date().toISOString()]));
      it.sort((a, b) => String(b[3]).localeCompare(String(a[3])));
      send(res, 200, '<?xml version="1.0" encoding="UTF-8"?><feed xmlns="http://www.w3.org/2005/Atom"><title>Skinsentials22</title><link href="' + e(SITE) + '/"/><link rel="self" href="' + e(SITE) + '/feed.xml"/><id>' + e(SITE) + '/</id><updated>' + (it.length ? it[0][3] : new Date().toISOString()) + '</updated><author><name>Skinsentials22</name></author>' + it.slice(0, 50).map(i => `<entry><title>${e(i[1])}</title><link href="${e(i[0])}"/><id>${e(i[0])}</id><updated>${i[3]}</updated><summary>${e(i[2])}</summary></entry>`).join('') + '</feed>', 'application/atom+xml; charset=utf-8', { 'Cache-Control': 'public, max-age=300' }); return true;
    }
    if (p === '/google-products.xml') {   // Google Merchant Center product feed (free Shopping listings): Google re-reads it on the schedule you set there
      const items = prods().filter(x => abs(x.image) && +x.price > 0).map(x => {
        const out = typeof x.stock === 'number' && x.stock <= 0;
        return `<item><g:id>${e(x.id)}</g:id><title>${e(trim(x.name, 150))}</title><description>${e(trim(x.description || x.name, 1000))}</description><link>${e(pUrl(x))}</link><g:image_link>${e(abs(x.image))}</g:image_link><g:availability>${out ? 'out_of_stock' : 'in_stock'}</g:availability><g:price>${Math.round(+(x.was || x.price))}.00 NGN</g:price>${x.was ? `<g:sale_price>${Math.round(+x.price)}.00 NGN</g:sale_price>` : ''}<g:condition>new</g:condition><g:brand>${e(x.brand || 'Skinsentials22')}</g:brand>${x.category ? `<g:product_type>${e(x.category)}</g:product_type>` : ''}<g:identifier_exists>no</g:identifier_exists></item>`;
      }).join('');
      send(res, 200, '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0" xmlns:g="http://base.google.com/ns/1.0"><channel><title>Skinsentials22</title><link>' + e(SITE) + '/</link><description>Skinsentials22 skincare products</description>' + items + '</channel></rss>', 'application/xml; charset=utf-8', { 'Cache-Control': 'public, max-age=300' }); return true;
    }
    if (p === '/') {   // homepage: real links to the shop, journal, newest products and posts for search engines that read the raw HTML
      let h; try { h = fs.readFileSync(path.join(PUB, 'index.html'), 'utf8'); } catch { return false; }
      h = h.replace(/(<body[^>]*>)/, (x, b) => b + '<noscript><nav><a href="/shop">Shop all skincare</a> <a href="/journal">Skincare journal</a> <a href="/brands">Brands</a> <a href="/skin-quiz">Skin finder quiz</a> <a href="/glow-rewards">Glow rewards</a>' + links(24) + plinks(12) + '</nav></noscript>');
      return html(200, injectHtml(h, true));
    }
    if (p === '/sitemap.xml') {
      const lm = lastmods(), top = Object.values(lm).sort().pop() || '';
      const u = (l, pr, img, d) => `<url><loc>${e(l)}</loc>${d ? `<lastmod>${d}</lastmod>` : ''}<priority>${pr}</priority>${img ? `<image:image><image:loc>${e(img)}</image:loc></image:image>` : ''}</url>`;
      send(res, 200, '<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">' + u(SITE + '/', '1.0', '', top) + Object.keys(PAGES).map(k => u(SITE + k, '0.8', '', k === '/shop' || k === '/journal' ? top : '')).join('') + prods().map(x => u(pUrl(x), '0.9', abs(x.image), lm[pUrl(x)])).join('') + posts().map(x => u(bUrl(x), '0.6', abs(x.cover), lm[bUrl(x)])).join('') + '</urlset>', 'application/xml; charset=utf-8', { 'Cache-Control': 'public, max-age=300' }); return true;
    }
    let m;
    if ((m = /^\/p\/([^/]+)(?:\/([^/]*))?\/?$/.exec(p))) {
      let id; try { id = decodeURIComponent(m[1]); } catch { return false; }
      const x = prods().find(q => String(q.id) === id); if (!x) return false;
      const url2 = pUrl(x); if (m[2] !== slug(x.name)) { res.writeHead(301, { Location: url2.replace(SITE, '') }); res.end(); return true; }
      const price = Math.round(+x.price || 0), img = abs(x.image), out = typeof x.stock === 'number' && x.stock <= 0;
      const ld = { '@context': 'https://schema.org', '@graph': [{ '@type': 'Product', name: x.name, description: trim(x.description, 300), sku: String(x.id), category: x.category, image: img ? [img] : undefined, brand: { '@type': 'Brand', name: x.brand || 'Skinsentials22' }, url: url2, offers: { '@type': 'Offer', url: url2, priceCurrency: 'NGN', price: String(price), itemCondition: 'https://schema.org/NewCondition', availability: out ? 'https://schema.org/OutOfStock' : 'https://schema.org/InStock' } }, crumbs([['Home', SITE + '/'], ['Shop', SITE + '/shop'], [x.name, url2]])] };
      const body = page({ title: `${x.name} — Buy in Nigeria | Skinsentials22`, desc: `${trim(x.description, 150)} ₦${price.toLocaleString('en-NG')}.`, url: url2, type: 'product', image: img, boot: { route: 'product', id: String(x.id) }, ld, body: `<article><h1>${e(x.name)}</h1><p>${e(x.description)}</p><p>₦${price.toLocaleString('en-NG')}</p><p>${e(x.ingredients || '')}</p><a href="/shop">Shop all skincare</a></article>` });
      return body ? html(200, body) : false;
    }
    if ((m = /^\/blog\/([^/]+)(?:\/([^/]*))?\/?$/.exec(p))) {
      let id; try { id = decodeURIComponent(m[1]); } catch { return false; }
      const x = posts().find(q => String(q.id) === id); if (!x) return false;
      const url2 = bUrl(x); if (m[2] !== slug(x.title)) { res.writeHead(301, { Location: url2.replace(SITE, '') }); res.end(); return true; }
      const ld = { '@context': 'https://schema.org', '@type': 'BlogPosting', headline: x.title, image: abs(x.cover) || undefined, datePublished: x.date ? new Date(x.date).toISOString() : undefined, author: { '@type': 'Organization', name: 'Skinsentials22' }, mainEntityOfPage: url2 };
      const body = page({ title: `${x.title} | Skinsentials22 Journal`, desc: trim(String(x.excerpt || x.body || '').replace(/^## /gm, ''), 155), url: url2, type: 'article', image: abs(x.cover), boot: { route: 'post', id: String(x.id) }, ld, body: `<article><h1>${e(x.title)}</h1><p>${e(trim(x.body, 1500))}</p></article>` });
      return body ? html(200, body) : false;
    }
    const key = p.length > 1 ? p.replace(/\/$/, '') : p;
    if (PAGES[key]) {
      const [title, desc] = PAGES[key], ld = key === '/shop' ? { '@context': 'https://schema.org', '@graph': [crumbs([['Home', SITE + '/'], ['Shop', SITE + '/shop']]), { '@type': 'ItemList', itemListElement: prods().slice(0, 50).map((x, i) => ({ '@type': 'ListItem', position: i + 1, url: pUrl(x), name: x.name })) }] } : undefined;
      const body = page({ title, desc, url: SITE + key, boot: { route: 'page' }, ld, body: `<h1>${e(title.split(' | ')[0])}</h1><p>${e(desc)}</p>${key === '/shop' ? links() : key === '/journal' ? plinks() : ''}<a href="/">Home</a>` });
      return body ? html(200, body) : false;
    }
    return false;
  };
  handle.mods = lastmods; handle.key = ixKey; handle.setting = setting; handle.save = keySet;
  handle.urls = () => { const o = {}, hs = v => crypto.createHash('sha1').update(JSON.stringify(v)).digest('hex').slice(0, 12); prods().forEach(x => { o[pUrl(x)] = hs([x.name, x.price, x.description, x.image, x.stock, x.brand, x.category]); }); posts().forEach(x => { o[bUrl(x)] = hs([x.title, x.body, x.cover]); }); return o; };
  return handle;
})();

/* ---------- anything you post reaches search engines on its own ----------
   Every product and blog post already has its own page and is listed in /sitemap.xml the moment you save it. On top of that, within
   2 minutes of adding, changing or deleting one, Bing, Yandex and other IndexNow search engines are told directly. Google reads the
   sitemap by itself (submit it once in Google Search Console). Skipped while SITE_URL is localhost. */
(() => {
  if (/localhost|127\.0\.0\.1/.test(SITE) || ENV.INDEXNOW === '0') return;
  const run = async () => {
    try {
      SEO.mods(); const cur = SEO.urls(); let known = {}; try { known = JSON.parse(SEO.setting('seo_known') || '{}'); } catch {}
      const changed = Object.keys(cur).filter(u => known[u] !== cur[u]).concat(Object.keys(known).filter(u => !(u in cur))); if (!changed.length) return;
      const key = SEO.key(), r = await fetch('https://api.indexnow.org/indexnow', { method: 'POST', headers: { 'Content-Type': 'application/json; charset=utf-8' }, body: JSON.stringify({ host: new URL(SITE).host, key, keyLocation: `${SITE}/${key}.txt`, urlList: [...new Set([SITE + '/', SITE + '/shop', ...changed.slice(0, 9000)])] }), signal: AbortSignal.timeout(15000) });
      if (r.status === 200 || r.status === 202) { SEO.save('seo_known', JSON.stringify(cur)); console.log(`Search engines told about ${changed.length} new or changed page(s).`); } else console.error('IndexNow answered', r.status, '(will retry)');
    } catch (e) { console.error('IndexNow failed (will retry):', e.message); }
  };
  let tm; SEOQ.ping = () => { clearTimeout(tm); tm = setTimeout(run, 5e3); tm.unref(); };
  setTimeout(run, 30e3).unref(); setInterval(run, 120e3).unref();
})();

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x'), ip = (ENV.TRUST_PROXY === '1' && String(req.headers['x-forwarded-for'] || '').split(',').pop().trim()) || req.socket.remoteAddress || '';   // last entry = the one our own proxy added; the first can be forged by the visitor
  const cors = ENV.CORS_ORIGIN ? { 'Access-Control-Allow-Origin': ENV.CORS_ORIGIN, 'Access-Control-Allow-Headers': 'Content-Type, Authorization', 'Access-Control-Allow-Methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS', Vary: 'Origin' } : {};
  if (!url.pathname.startsWith('/api/') && !url.pathname.startsWith('/uploads/') && maintOn()) { const wh = res.writeHead.bind(res); res.writeHead = (c, h) => { if (c === 200 && h && String(h['Content-Type'] || '').startsWith('text/html')) { c = 503; h = { ...h, 'Retry-After': '3600', 'X-Robots-Tag': 'noindex' }; } return wh(c, h); }; }   // maintenance mode: pages answer 503 so Google keeps your listings
  try {
    if (url.pathname.startsWith('/api/')) {
      if (req.method === 'OPTIONS') return send(res, 204, '', 'text/plain', cors);
      const r = routes.find(r => r.method === req.method && r.re.test(url.pathname));
      if (!r) throw new HttpError(404, 'Not found');
      if (maintOn() && req.method !== 'GET' && req.method !== 'HEAD' && !url.pathname.startsWith('/api/admin/') && url.pathname !== '/api/paystack/webhook') throw new HttpError(503, getGeneral().maintenance_msg || 'We are updating the store. Please check back soon.');   // no new orders or sign-ups; admin and the Paystack webhook keep working
      const ctx = { req, ip, params: Object.fromEntries(Object.entries(url.pathname.match(r.re).groups || {}).map(([k, v]) => { try { return [k, decodeURIComponent(v)]; } catch { throw new HttpError(400, 'Bad request'); } })), query: url.searchParams, body: {} };
      if (r.opts.limit) limit(ip, r.opts.limit[0], r.opts.limit[1]);
      if (r.opts.auth) {
        const t = verify((req.headers.authorization || '').replace(/^Bearer /, ''));
        if (r.opts.auth === 'admin') { if (!t || t.role !== 'admin' || (t.v || 0) !== getRev('admin_pwv')) throw new HttpError(401, 'Admin sign-in required'); }
        else { let u = t && t.sub ? db.prepare('SELECT * FROM users WHERE id=?').get(t.sub) : null; if (u && t.pv && t.pv !== pvOf(u)) u = null; if (!u && r.opts.auth === 'user') throw new HttpError(401, 'Please sign in'); ctx.user = u; }
      }
      if (req.method !== 'GET' && req.method !== 'DELETE' && !r.opts.stream) {
        const raw = await readBody(req, r.opts.big ? 25e6 : 2e5); ctx.raw = raw;
        if (!r.opts.raw && raw.length) { try { ctx.body = JSON.parse(raw.toString()); } catch { throw new HttpError(400, 'Invalid JSON'); } }
      }
      const out = await r.fn(ctx);
      if (out && out.html) return send(res, 200, out.html, 'text/html; charset=utf-8', cors);
      if (out && out.csv) return send(res, 200, '\ufeff' + out.csv, 'text/csv; charset=utf-8', { ...cors, 'Content-Disposition': `attachment; filename="${out.name || 'export.csv'}"` });
      if (out && out.bin) return send(res, 200, out.bin.data, 'application/octet-stream', { ...cors, 'Cache-Control': 'no-store', 'Content-Disposition': `attachment; filename="${out.bin.name}"` });
      if (out && out.file) return send(res, 200, out.file.data, out.file.type + '; charset=utf-8', { ...cors, 'Content-Disposition': `attachment; filename="${out.file.name}"` });
      return send(res, 200, JSON.stringify(out), undefined, { ...cors, 'Cache-Control': 'no-store' });
    }
    if (req.method !== 'GET' && req.method !== 'HEAD') throw new HttpError(405, 'Method not allowed');
    if (SEO(req, res, url)) return;
    if (url.pathname.startsWith('/uploads/')) return serveUpload(req, res, path.basename(decodeURIComponent(url.pathname)));
    let f = path.normalize(path.join(PUB, url.pathname === '/' ? 'index.html' : decodeURIComponent(url.pathname)));
    if (!f.startsWith(PUB + path.sep) || path.basename(f).startsWith('.')) throw new HttpError(404, 'Not found');
    if (!fs.existsSync(f) || fs.statSync(f).isDirectory()) throw new HttpError(404, 'Not found');
    const ext = path.extname(f).toLowerCase();
    if (ext === '.html' && path.basename(f) === 'index.html') return send(res, 200, injectHtml(fs.readFileSync(f, 'utf8'), url.pathname === '/'), 'text/html; charset=utf-8', { 'Cache-Control': 'no-cache' });
    return send(res, 200, fs.readFileSync(f), MIME[ext] || 'application/octet-stream', { 'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=86400' });
  } catch (e) {
    const code = e instanceof HttpError ? e.code : 500; if (code === 500) console.error(e);
    send(res, code, JSON.stringify({ error: code === 500 ? 'Something went wrong. Please try again.' : e.message }), undefined, cors);
  }
});
server.requestTimeout = 0; server.headersTimeout = 65000;   // allow very large / slow video uploads (Node's default cuts requests off after 5 minutes)
process.on('unhandledRejection', e => console.error('Unhandled error (kept running):', e && e.message || e));
process.on('uncaughtException', e => console.error('Unexpected error (kept running):', e && e.stack || e));
setInterval(() => quiet(reconcile()), 5 * 60000).unref(); setTimeout(() => quiet(reconcile()), 15000).unref();
server.listen(PORT, () => {
  console.log(`Skinsentials22 running on ${SITE}`);
  if (!ENV.ADMIN_PASSWORD) console.warn('! ADMIN_PASSWORD is not set: admin login is disabled.');
  const nt = notifyStatus(); if (!nt.telegram && !nt.webhook && !nt.adminEmail) console.warn('! No order alerts set up (Telegram / email / webhook). You will still see everything live in Admin > Payments.');
  if (!psKey()) console.warn('! No Paystack secret key yet: online payments are off (bank transfer, pay on delivery and DM still work). Add the key in .env or in Admin > Settings.');
});


/* ---------- homepage products: chosen automatically, by hand, or both (Admin > Homepage Products) ----------
   Saved in settings > 'featured'. Pinned products always come first; in Automatic mode the rest are filled in by the
   chosen rule every time a visitor loads the page, so a new product or a new sale changes the list on its own.
   Manual mode shows only the pinned products. It only decides what the homepage shows; it never changes products. */
const FEATURED = (() => {
  const DEF = { mode: 'auto', rule: 'newest', count: 8, hideSoldOut: false, pinned: [] };
  const clean = c => { c = c && typeof c === 'object' ? c : {}; return { mode: c.mode === 'manual' ? 'manual' : 'auto', rule: c.rule === 'bestsellers' ? 'bestsellers' : 'newest', count: Math.min(24, Math.max(1, Math.round(+c.count) || DEF.count)), hideSoldOut: c.hideSoldOut === true, pinned: (Array.isArray(c.pinned) ? c.pinned : []).map(x => str(x, 64)).filter((x, i, a) => x && a.indexOf(x) === i).slice(0, 24) }; };
  const read = () => { try { const d = JSON.parse((db.prepare("SELECT v FROM settings WHERE k='featured'").get() || {}).v || 'null'); if (d) return clean(d); } catch {} return { ...DEF, pinned: [] }; };
  const sales = () => { const m = new Map(); try { for (const r of db.prepare("SELECT items FROM orders WHERE paid=1 AND status<>'Cancelled' AND created>?").all(now() - 90 * 864e5)) for (const l of JSON.parse(r.items || '[]')) if (l && l.id && l.id !== 'giftcard') m.set(l.id, (m.get(l.id) || 0) + (+l.qty || 0)); } catch {} return m; };
  function ids(products, cfg) {
    products = products || productsAll(); cfg = cfg || read();
    const all = products.filter(p => p && p.id && !p.hidden && !p.draft), pos = new Map(all.map((p, i) => [p.id, i]));
    const pool = all.filter(p => !(cfg.hideSoldOut && typeof p.stock === 'number' && p.stock <= 0)), have = new Set(pool.map(p => p.id));
    const pinned = cfg.pinned.filter(id => have.has(id));
    const newest = list => list.slice().sort((a, b) => pos.get(b.id) - pos.get(a.id));
    if (cfg.mode === 'manual') return pinned.length ? pinned : newest(pool).slice(0, cfg.count).map(p => p.id);   // an empty hand-picked list falls back, so the homepage is never blank
    let rest = pool.filter(p => !pinned.includes(p.id));
    if (cfg.rule === 'bestsellers') { const s = sales(); rest = rest.sort((a, b) => (s.get(b.id) || 0) - (s.get(a.id) || 0) || pos.get(b.id) - pos.get(a.id)); } else rest = newest(rest);
    return [...pinned, ...rest.map(p => p.id)].slice(0, cfg.count);
  }
  route('GET', '/api/featured', { limit: ['feat', 120] }, () => ({ ids: ids() }));
  route('GET', '/api/admin/featured', { auth: 'admin' }, () => ({ cfg: read(), ids: ids() }));
  route('PUT', '/api/admin/featured', { auth: 'admin' }, ({ body }) => {
    const cfg = clean(body); db.prepare("INSERT INTO settings VALUES('featured',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify(cfg)); return { cfg, ids: ids() };
  });
  return { ids };
})();

/* ---------- the store owner's real starter products (Eucerin, IUNIK, Baby Dove, Olay) ----------
   Added once to a store that has no products, and available any time as Admin > Bulk Upload > "Replace all products".
   The pictures are written to the uploads folder like any picture you upload yourself. A safety backup is taken first. */
const STARTER = (() => {
  const IMG = {
  "starter-v1-share.jpg": "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AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP/2Q==",
  "starter-v1-eucerin-oil-control.webp": "UklGRuIzAABXRUJQVlA4INYzAABQ/gCdASpYAlgCPjEYi0QiIaEhIvPIgEAGCWdu8kXR7tLFZ31z5N6wacCTfzwww7v/mednzD4x+4vEv5Tfd9eo84f2/LH56/1n5wf6T5s/7n2Bf2L/C/+T3F/1X/yH9X/JH4uPXr/av+R6oP6Z/j//P/hvds/3f7Ye9H+3f4b2D/6Z/YeuY/rP/I9jD90/WY/4v7mfEj/X/9X+2nwx/tTdGPtQ9O/J764/gP2m/wHuo6O7Sf5b94PyX90/b38zvvF/gfcB6k/En/Y9QX8Z/nH+N/t/5F8Xdp//F9Av1u+mf83/F/lB8KX0H+x/wHq19hP+F7gP8s/o3+m/uf70/3/6B7/T8N/rfYC/oH9s/4n+T/JT5J/+n/X+m/9H/0v/t/yPwO/zb+2f9P/Ghw1GXBGmfeJMSYkxJiTEmJMSYkxJiTEmJMSYkxJiTEmJMSYkxJiTEmJGRGMekgQcddb8Aaa01G5LeEBXT1pMbCtkTpfxDz/Cy51HQRyv87LJGHBCHiXEuJcS4lxLiXEghijJziDbMaxDt7ANrO5zRz560VWmTMyZ7c0xk5v+LNjDxLiXEuJcS4kEakQRambtyhr99vQugjZx3NFjnVt6BX+SMgmgq/XcPBbIRDCz+ECBdm7N2bs3Zuzdmw+EdVj24Qjw/K87a0ugh/YmcpeIuu2rEeSVpoyVG630ICBdm7N2bs3Zuzdk5dX0zxkgflMgmhy2B5Ag2b6yorkoy4I0z7xJiTEmJEYuh4L0c5NrpefFs1/6YnUikhJ9WcDFlm77gjTPvEmJMSYkxJddcVRoABhAfbCIfX5wEfyUWCK6jmb3QHr4TST2bs3Zuzdm7N2bs3TV2xCH/7E838KH5CTi3fz7P7+4uLZalGXBGmfeJMSYkw9gien+jFg197XdsgASYG5v68gMYeJcS4lxLiXEuJcIIgIBoWqAEIzwpzH3/J+swf6biSCRZJD9U2R8Jh3bW40LQmQJD1aC2+yaTf4s2MPEuJcS4lxLiUpY00HorZYs5zv7u6xgY7vNLCQEgJASAkBICQEgHqFadsX7/JCmLn/UXsCl/hc6l2YofGB6Mz3azSP2/S4lxLiXEuJcS4lxJYI3w6mE6e2eXq6do9xd+Y80uJcS4lxLiXEuJcOL9eCRCedsa1RKJBlQbjGaVqxmgS7BJqXSgwZ9NtVTlsgWsabnLeH1gJASAkBICQEgJASFcfq2hMYOVFpiMNyNKOuRGvaqBapX4ZgKmgfmO1yi/FYVOSRtFZj1XQpfL1MWbvmB/z3rJMSYkxJiTEmJMSXiYov4R0JRjHz2ltL80y04q9I4OZ8V5lh5Wbfn3UPhrLpHlzAhx3KrGl7RBgSv20XWBOu6O5xMDuBzAQLs3Zuzdm7N2bs3rgQ7Zc1xUsZZ/GDYzGXSw/pSdu9U5bokExai/bXj+UhejEbwSDhHeF7SdNQQACF2bs3Zuzdm7N2bs68Y9ep4YX9rHBY6Aa790UNGzOoLv9977AptP5XfoT/LePs3Zuzdm7N2bs3ZsnzHuIJWHUGs0rYpIAB6Is3RuzgOEsm/dmk5c2iXUtoXVJKMuCNM+8SYkxJiTA6suB6SfvPOmRRpg0Axb5+RrgBAtFtgQDh3l1govVWSNjF/aLLxJiTEmJMSYkxJiQ55LHkZ4Onxupl5yelSxQqgKDG5ubiECytdj+PF5eH0ynPM0diVBOG5CyDMj7AgmmvAjxfZk+FGxeJMSYkxJiTEmJLpdOSOtsMxbFZ2vG5apyVZ9DyQrlNMS0KzwALlvEED18XXxtZuzdm7N2bs3ZuzdLkrEAwVKQEyvrhwWAi+mG5ut3MrR7r1uc6xYoQ4blqUZcEaZ94kxJiS6XRJQt5GSHDSq7AMHNdiWlU/suJ897HeNR9NSJprNsZcEaZ94kxJiTEmJEDKnYlrmWONCPWAS6dq2sKO/ZtEx4yTBWKsRBata+Cot2Slc0SJE0UsFPFC/kM+dko+8SYkxJiTEmJMSGpXTOtPYJiScx3mOJKEdK2M/T+S0OUPirthJBPLnCvLD98j7tiRP9iJ/yhn2dqaLZI/zr4N/YnYVVEKUZcEaZ94kxJiTD2C2d/BS5o6ECR62711ODlvdgtJginjBSIEmjLaQE9V6s9k+5MESv4cQDkBbqTsnTKFqMiW0Y2t8vu4UnDYBpn3iTEmJMSYkxJh68WVDM1shij2CzM5angAmMSw84HOHsyl0xl7b1dFitqfDd6RAaNwYpvTr7mo0wDT/Krrzj1FljEZcEaZ94kxJiTEmIbJaheunj4JhFujM2XG7NSNmg7eAiEPj9a1ieXDX8Earz849m7N2bs3Zuzdm7N2db1Oc+gIdOqAjLwtFAsyLQ3J27Vnm1dthouqYYKCaDeudM91S42dyodTmh6qDWTUNYEIf8jLgjTPvEmJMSYkxIpfrqLrhRk9Amp/Ehu9JPTXFu9lLfHmpktAEADoCoZOSlKCqvCcd2I0z7xJiTEmJMSYkv665WCfLJU+wI3oRl/HLAQ4ADmJbvsCzknLSnYWH1umuU8cT21OMxcpgbPvWBXePDiiJLMtnLVYz1MiJfcEaZ94kxJiTEmJL/EqgVLKXD69YtugyURanHtB3uLafHGdlUiV0k4w8S4lxLiXEuJcS4fyIkP3vMWpw7QVcuvCm99SP7OzKvxYiiZ94kxJiTEmJMSYkxJiTEmJMSYkxJiTEmJMSYkxJiTEmJMSYkxJiTEmHYAA/v/qgwAAGEEIlyACgD2gFRQxsVQ+fzEiAPq9V7FEA/AUoO+H4lQtZQ5nyV1k7euhcxvHOL/qoFhGAJPL8bXgFYc/9O7ka1mr+n4nhKgCU7mste6EGTfz4uzc9HgGsPyI1iuXem0ZQ6WNEmM98UuboEACeZwOPpbhXRxBeRk9R+yU/r/mv4utPFbfsUOJTvUMmV7PW8+vdJcOrqpGnW2K0Exbf7VzOa7yiLTW6cp19y7kgrRZYiC/eByzZb5BUzL7KV9rLLE5nZa8VuBszzWLb2Ne2H/ehuvknhDB7ILmqiJCRMRFAAuNOX7ksSoYAY6vzGWg4BEIuMQiGEbcIqS9Uvlw96CVDzXk3vC4ItjdjwDbCb5GyaMkjJKsJtEEMdaWVaHNPCI/J7hZw0AUJgIUuKQxc5Ksw32g3voDXozMuIf/rmOXccfMn+4HoDDBWlEU+s1HK/CJetUxwfg8TGtjVun4W7WqQMlt61kAhDs0V2YT1NoGMnABLi1tumbIGAWw4jDRGpHCd+kWyoi7bbCdySvLh6vlwRlNcsz8QGpzw0BfiBT1alOEeDJ/T1BY255QL4Pp3AFrk1+A2Eq+OAYdYS3O/XTiwnu0LZfHf6kyzDeLtJQEnp2ge+EhxBBz5vVd0rhnZLxIhK+z92RfKhhZqO7OMY6xvWF3/e0C+7lKPbZsBfukn6LYh12dG0wnpb8mBg/kgAP/uk5bk8YodgW/BFzC5UjwYAwEPtS5OHif95D2ySyYWuK5ZrgsJZGQdVgiu5Rp0ExyTx+8hDmHm0jRZQHSXc7gX8lZx2OfRmss4BxlM72MHJ0YI5gu3TEPS175xhpUh8n92uHjsBRwxkS25Q76ZJFiaQuB9ARaSeTWPW6q+ro6Wtn+d4++sUEAB8XBOjv+/c4FQocP3INU0yNHABUDlPK6YothOTSrD7mH8oJjQpyMcxGZy8jwfxsqeLa4Cs7yBnryLstApZpBo+ng3/E+GRMH4BOBAiZp5pMFFA6DqW0FdRQA6Vy8qpBPn27LlQ79oLhNiF5+RAIHyOm3SIjy6v4czBoXssmHto43U4debVVie1Y3NXjDO+UKAoAjYSEGlJXekf+Rdx2aDTuy7BILr4W/GkaoAjZEeO9MJIWz0eKNBcjqrRWIKNntELLHUBTycfmednCLN8/yTQKkwWpwzc+83M+OSzbpp2jZqh/tKIdqEVb15coX6GRKPJI3NtDAATspEzEdB3PWM5x6F00pcHAmHdLA0GdGlPkBTaHpqz/hqJBrr/WhnVkhA1CNcWPYNKtGYkbpSoiz4u4SLt+8T0U7KbK6lrpjYMn7XsYv/2xOt5P+rwOwRRVvbtEAa6NOUQX4wetzABcIWXXm2dUkEG4aaAt/X+omBJlTMnuNhXubBJ/QZ0Lr0uFVybEyE2pv4wwrT5H22fLgRvVo1rcYbpp8L3FUezFbgud6GiA4X8azajdeJyPpMzP87ffboKwyvyDuBNAlzuVdgK/jZpmOAVaeyCvWuT19EGbUtMtWBGZsVbuy/MpHAAK7UYm+ogNO8g+Wyhm6AEWjhs7nqtgwOT+UofWUDP/9s2OonernWgl8DjVhp5Q3PifTwI1bckdrfEnpw2TJJqgVWJQAldsFDvNNXDfTvNJ0Cc81Jqyg3SYEu/wElz9jghcNwAAvkNaJiWhAjaGqhAnxgMHo/OEqFel4qY5mdCoIMpMZq9b6bGnVkqUbRnq1q+sIoiH1GOZgQngn6ND0Zz0aVyniIGbilanpHBVuvHuFx6plGaT4KEoL45H84lmhE4xJ3hxzQwGb5spdV+P+ug5LijubVJJxEsBXWAD3WnvDbuoS35ErA6ekfVDz1wcQysiYrwrBSCTMUlhXfkBLnWTs8RBejipAUvjC328ebSUu1d/TeuHghO0/c+K8XyzVZOJILfHPH6B/zLdPbVbigL+CFbBLRcq28L0L8bYWsSIIugrKjrDKxDQcEUJ4gELxpuoo7swoPLsFLlcGFiYJEtv6Bj9GUwzCwOPH2j8zq+HzZGcgHj6s7Mt/DJsTd9HxqSdCRenfIHQolc1B98EPhfxtUxNr2v3AxIK6qfi4Q/L6kXSfUGVftZrve/lYTv644aFrGGUOvZpF/qW//FKLgehCKziYODXEZe4bdpvXJLiJ8Pe5JhtFpOn26EJo5hGbK/1dbbuxG0vPoJYJlSsLLXrSSNTBrjEJBiJkKfYQx6KPWO/NQFnMGBAJsmBszCO5FCEfAVyZCFAZ5OpI2iXAAKEn+GbGfRTJgBWhlZEa5g8mhoj68neR/7SGPBhBFh27YHBeeYKkAzelKfdkA5h/zRD+My1c0uCbuHrLNb91MznNYA03zSEEaXc+1mVZO7MseCluTPc5xwACVtMwQYZqZFCeqCmQ+E6rtcPx+Ppi9jlfBwWNBEYYHhUyvFohh4A0DBUQP1DQn+gQpIgXIgk+5p++Vsj6eunoB31NlKTbgyP7E0/J3uTjsZujQmOfAJwL6+rQ6eF7UZS12EhADdHr0KzGdFYAxuFMWEhDHlBk+mNJiWWCGEE+co2RhqTIsK1QaOhJfxqaN2BASBGL8uVA+Q6tetAeJg5QSleZZ8p78fNUE+15whOfeVuqMHHpnzjQOo8ar3f4DryUCL5S0HWnB3k0iXRep3cPXRP35EzvCN+9kK3octU6yHLucoRlsvF1u6B1V4QoUW25tBg3pbsU4x07wM/GYlllRWO0wWfnJNN5ADM45H6agDzW35sOi72BMRX1haq9vlJVFvVTAYv+Vn+BTh/R1n0d22UbYpwQNjBFAd7Yga7ft93dE+L9j31pRcGjw13WaVGiXfndkfvlacHonrIJTzFugGxkSCqH2Mg5zyTGOcnqtOOZbMjkL8RdPfPJhSbpok6TfQIfQNLGB3/saATE8wAAgQI/ghycuVk56onxV0kZSbe+NEvpK7FYsIfrr765QP2eC3kkgmd7sJWGoOKFJ0ZnOH3ioWap4VUlQq8f3t54sYywfcv8DPvCJD4MlOG9kupSM51Ui+503fthmIIH5DvS8vtCnD6GU1FEp19lAxMYbaxLbCJD1aciqg42f6KayqdPy1vlJx/S4Qn+z/kErT6gqzGMmTYOaWc/Pcg48NPSeD18LZuJ9LwlnzG5Aar3v0HZc/PQummroaKDyq2iYxncc/wAhMMb79Zx5RUC7QbzMToj4yxxij/f8Z0NNeBBfwp2RMb5TGpVHzS+mVLdy/olii6+5G8l2SXXc0wcYpdpUqK1u1Rc0kWTVsS/sQ+XdrqckYIPpBWDPcLYJXzJcN+p9yEPFPLNu9uWiLuJdd3NjbkV91Wbkod1mwqYQbPI0E1bw26X9mRDBdw/1qTWcZr5DHRRoAdsCByt+PF1LXwKEynCbTj/bZ3a+cD2opq1z8XqJ+VmZfmRmSCiPJ9QFW3fb/pFkVpXAWcpfHHXVbKmI/PEdu1egxR4DN9FjAklRikWa4Bj2uW9/oSrr8xKP+GLYhhTLM2GfPFWa08oifjYdLn9+uzEhy7jh3QHejx/nSq4GBiFRExV15CjUPxfVXRLJIVWopvJH/v8aqep/XbJ+lzbgFd6ic5SdSw9PiGM0CcAFgNYSdoX2pDBtKAXjukhQKSGdY1rJTkws5WmJb07rbMgZ4GP5442P+bNNhCMg5AdvmEEBCLAFUlZ2M5IB73gDFp6a2kyXJNaKcEzKe/Th46WWAUOf087x7ZmNlT5GTjklvQnXEG/SgdjR3vIl5+aRq3yGMa8ChKHEcpYj5jjAhR98mNzHtW/2qrFJxkH2UD09UYBC8rS+QZmBzgB1P8nADSQxYrw0BZekGOg+8ZTmiTH/JOjg/QLFZ1TUqGhTP9/fMgVao4MoQe7foFoERAaVa6OrJht+Z8ZpHVLmeQ2CyNRrZdnofT584JOGSgxGgTxoAAnc6/NW3Ogk3KoIZPSFj2x4+Kt+DR3GPt+szB0bPgjowlVmJAchuc1zv7Uc7t7ULRMN8IC5Lz3R85FYs1ohXR3CyjobwBbb1dnCgEnO21SjfJurIKVAIpW5JzhRHaw1OOaH5P6Lv55PXDf0X1dDdV5Y2Ga2TuXqnJ4Ozs9EEK3wggIiUwaiA94AMF/oEgb5Dn5O0t1SxF6pdNslnfX0IGX/m20hz5VJPIa9AnxzCym95rCl3Eq6QaOhuG43/VnjuZJTyp53g/OrLwdXqTdNimQkl8cnIbxGZ4DmLYXMZ/tvnjNh2KZKMX0lJ2rX5KfXEYNSt+KFtRkEOwMQ6FgqtpS/KKWidt+OXTLwAtbr3Jr/XdoP3ZBqBbJ5M82R+Qta1tW2qNZ0U3Pdkfa41VMRH7K9vTPWiWRmIeoi1a4pT6eukhHXEeDtgsNVBYMIoG/Tm43TRayc+6Z2vqSh0lNB+d7idsLYlW0QjF+EyYI7Gg4Rs+4zhWEcOVflFF4sECU7vCGBye1w86n6VYCa49uVHb4HC8DhZV6j6CSSNUlTYv26lI7Cro5yeWDmru+lAZJwCYzMoyjHUygqCyGkkgUSTQ+PjgngmFgQwyminfPhd/XLFqPHeVYolueOn8bcs+En+vLfnltlHhE1+kQdaLcAa2boDobj/gs6AdG2Ja1FvgQqfEATQDbHJCV0uyvdEqZTQd25gzZNgOHh8VtXHAWG0lxkRDFQVmYPWFYq9Tb38G80HSJ/JitixHxqj0l3ju0DAE6PEVgGLw+AQkR5ifn8llMkkn1mEw5/TwFHPPMXCBHqRf3VgI943JRQLQ1otSwbdrUlvyqdZ10M+/UggWpYabubDg1as8LTIf+Nw/l4qmfVRwSEbMB9uKSqB6gSudUVKMtHu0aUPRJ+TL6U/ydBzsJtTcsznvNQ1K7vm/bCIA0Tdp/8UvBoZAGiAagfMRJX/SJu98Knz7Xk/oD3vBU0b5094Q2DsFAGq3/t4ZMRBy3EIV2S1ZHJd/BitbNyQRuZLHoDpWzo7U8rETuIAej90dHXNo925yUwOb+Q8tAETlaih9QQq7VpjG/iIduHCDLJxIzal5RctX9X7OZkk50dNwT1g5j2Fd1tEM7aB5gGTlkw4GvmQIgyEIvUl+Yt4l8CfxFRgr2ZebgZz0Czaoc4aipxGxiukditc6V80t/qVnAkUeX8stj7lj0QFJemK6/vBjpkqJ1N0uVthy9+hqmUgoRDOyxu3cbNYb7BHUDEZ3pC84ABzom34I29TEX1NosO2lIFo5kVegXteVuVS1s4DL6HTuFZEcgNBUCynvMvTMu/ymmG/iENn8spvKw9pTkPbfVjj4qvGWrf5syrs4HxcLuCCNr4fxo6Xg44FYe0sP/xtWQyc6te9yCuCdlTfrZjPpL4osbI2aTgSLDjD97IrMlL8TzM6u33be0YiHbadNW0ZFZ6Yw7yOOUHI2gcGQAADzfyl2jjG88YSVSE7C3C2ADO4ABGBRSau3bUPaA5/qyO9YoBkz4WjAmlOJfpCoIsWntlnm5jvorQwA3aCGgft7MfdNJbBPsLobawLmZQDeCn8h7n7llNaKA0iw+KW/5UuHfWXUIzXDoXcZB/3TcpP6375C+E2gN/QxJOKGkQlW8w0K0sjztMB/4qD0giaHdi7SRfZpRmPDHyDmylAO+wTyCQ6Be5dWI1MasJjlPowf4DEDfzJyTA3soLyXwjyWbB9X3qTRqjvJ60zmjZdL5UKgH2BDA6FEn9mlEz9KlHMZQ+KW7z3qRQXrng5IdsbrRk9BfUe1iZ1YlyTz9sVmpNQ/JpRbizIj0E843ioKVbSliZxc4ucNwjiyAaFTmsQzxjj4wkAio1zAGa2+Ia23WcAEYHYJB+yH1RHEbWkCDzhbov0JtOURiS1syp1OnQSqGRqYY3YFCKQPmzB+RRMOy1FNcpujmtZ64GozHd5iuvS8Bf2ipFt2EUaPKA0LxHbsoSJaqnrm7JIkEMWJQFYkCrKa74Hp31wy40rds4/QF+OzO/fcy/vcIsfwYyEpNCO0Oych21Vp/nCt5Tp3881UjSeZR4RnRPga0PGvACyHLYvHoV8uzJNMfk2o9JMtSB3wmh1p7GELNxTBLEPaCDnmSDJTFWwaUWxAQKusswW6JqNEffejbk2wyg65OyAL+UtRoIqWj40ROrPIqCDUbUg+U4I0n4ltubi1Y4u/nhzADX7diFrEBFVn+fjHI0JDrxAI61/ryqJ/hBKSoIBFjOHeE/NkUWmRgPusjI4+l0C99pa0OPRR8ixuYY9dB3jrsoScR+bbicFiQHFSNs6U12glxbCp32cYtKqCSS6HVDb7jr4Bi+6kxQXvYfAWDLs/yVA003scIcQpsID3KIbHkSEwBu5j0QQezjOqEHgH58kBnoYQBtpkSVi1aOw/j3UPORvRlfQFnok+7w89uUgaCOPqhm05O50o+6M5utrxZGT+RZWTR3BgW3PdryZwqD7OLgNgMnLFpcUHQrcQnHMGP3aBPZlyWUftVZOlF7m0UHl4ag6ac0YU65pZWTHjU1A2tWJOkdhEDztYCSzz5UZGgDC66B+yGyji7c+RdoWy7J3KVW/s+DthTalEMJ9AhJDP2QaQExXkILbnRZCZTepIkWh0OKdAaYVOu/Kyq8I36X1sMuPbRX03TPuw5ktL25Aea/emWDSM+Cqg4f4zKN5GZTGfBHvqtfUB0W5Huyn7zDN5y93DyGDzV1P0u9McMLc0iuO/1HXcu+2kZoHxtq1i/DT0yuj2G51gl6ArIFc5UeFsodYDOFHOd3Wf1ia7w1vkKcQAsHQW7oQOwIE2S2Sdqe08+kwzCUdlNxbqe19OWf7zrwseuBFbbzo6Vz24+/+7iJ63Lo4rjttI4eXU36DAMTh/8bit6orNQBFsx8bfCrTF36kO9k3mzTxaSlLKz6GU1kt1z9XK0Wzga2+N7713+XIGjGiQlVaxqpSKYN9jOUpK5Vpg7i/vLbz8FheOtD1KYb+4/ZToHIFKdQHtq/n1OHMtPiqYa/kHiFfuhOKrGaxYEkk2BqO366SaZ8IgZ8ql27aFbEw/muZP0WhHwSFDhe3dkoujFzuQDeVb2E/q8IQ4R0lm130s1e5nbtGNlejb5DNier1MpI2AAAJB5I69NasN/8n0n6uy0POf9i1beqgPa1Qx4iqz2D4NZlQ2pIqgqKd35V5pYqycIJUUrISpDpVmUfE3XUkQ1R8S82U3p8phcUQTXKf6Es1mXnCoF/MI4ZIqTaMLyDiv17AY3yRMwrSlC050KTEUi/1Ma8V8LpMd6la4XBJCsF9vRZ6lUM7HAlX4Gf2uy1rXoapwuTw6U5PNw2XItnb0Ai3SQkoybeWPMhVsMe9fmygS1/4fE2fmDqzFqdU/yAC+yM2sqLjFhx+MmyG6BcYXtK5EU5a3+Kx/g3Hp1z/JQsDmFc/BnsgZ0A96VZ6BFb6FVZuwmFbC5Uq75ufAzEnZ2mM5jgD5v9d7eAax63RBOc22HoZ3a7lmDXNc+57oj/sHllOxb+B5SgbJZXVja63g5TyDlOQOaZk4/nCbwhcAYk4o7nb57KbXWiximfz9PNhhbpoEclJgZn51IKapK1QmrlWZn6xBblnsxroYhDqzEiyRRk07o6tA6CFMfAwHTmYg/vGVpR7GmcQ4aNrFbRAfQAkSBO18IWyQQ3b/IwOPQzXCyo9jv0ohYtNn6TnZo7R1ZFx3vdBngjKkZ2vN2KsQSlRJ4h5LMFt+CBk/h62wRG5quSS9PMoy//ju5NH0Dz+XydlUowWvsFI+WSrnJyr8hv0USb0ov1V8w//mDC0U9JVVHobBiylssuENTtUaFYawqk7ojkU2ueYi2FxaYTVVuHIyYAh/qZN/llvx5Hu4Yd8fjlyJQ8ZmirgZtgVlNSKbylIdW2u/5kYnb0Ydt73Q/RXj9HLrAk/UgiBF1MZbpAG1MvhrZhIg1IAq6cYBfXnsqRqwi3u7bOxuI5G/l7P3AMlted+qJaXASmyvSVCP8znB26m0Cbxyk025bL8jXWTNOg5N9HEwKv3k6ZAmAA7zjkSHRp0UQSjbF3Ss+ACVqujTrUvJoiYFjq04YXdXg3DeAyPQtqWB6oJ3XK/85fsn2CmlB2+B0VViu5taHktAcXJxa30DW+GwC/vlxOmm+1m7OtdfXp5XDzaA2B+mHx+RX3qLqdW4OQfjsiriDkx5Bx1+wh380nBPZDs0XfZPl3nahNz7SLa9I2rlr9LQuaDcLdh+/gC8eJHs8ch3OLvtQ2dJt014qcaXGd/yKowT5SqWLqVlOZXEaB14FRKBbbNqQEx9vg9RC2xgTZXCwLrDF6+jeEoIU7HOSfxhD7G7A/lRUtaBtpwWplhrM32vtDC6eq4gGIRliTHY3lJU1QYzV2oeFO3vvHE38AKdGCsa+oHm8Hu7z4iC1pr+EvYWW6NDz9UCUPegXh2aVTHHKzmpMcdmdc2C5zUgTKNxG12FFAn+wxA7c9CNGqfaaL9jSiYEMYrFVaVkPDtSHRyD2vKhUsUXE3yXRe1tJFwFI0yASYxmvUa95Q7V9IiGnvvpd0pR/RmWb/l1LxSdmgMPXf/Y+btAWgLi62X3myAnoCa6GrjmMdP0hpNjVAVu/FwzQkGmhbCUMkFsjjKffuOd2Lfia8bpeZtlQHyIIVKlv6Eiaf5mis361A0MY2K540JdQGN4r/cFn7oLUkfrY0/q7do3SbC6zwyrrLG2Mrg3h7hsmeuk29Oe2eLiQFXzzHgM7lZYW6Ady1kLgZ4KgUHWlfOHA8+fseKY8NyjnqLAIN6FEEPeiWfyR4RwdCImwu8aBQc9+ih0Vkj5vQ9+5jo/r4nn5VTFLJ1LD7eNx/4F/LVhxjTJgSMhl02r+JP6D0jIdJdwnJW+eggoNABBR/YXH5I83qDv/6TofaQamcBmmWJgYSbyakszEMnm5vumdoxejWEbl5/h47dS9A8QyqA9k+yEVuyPAiSuZLMpY04FX5Cgj1bpNlJg5GgAK00Z6auLfl/tgnRImoEaapLEmxpM/17+QCs90pPbl0hlZl5uloP0ZfmGIeR2i2ctzhbHgRUFzA8c1EMCJJplTbGS3D8uvSBRSUmg7nz+M+rNIdx+k567+Eh3tMSvfkqvaPugPxImLiH3zDjQi3QyB2f8hDHXSR7Xi0lei1m+cWTD+4DbOGbubNs2L5bzBlYZXbzCYe5yUbkX1Dg2k4XrxlKFkoAmm8Om+/RTiP+1G+NKKsOjrEecbUt4VCTFnn/Z1VQLemOJj9NX4sIBhRP7famUDd3bXwmKyf/tVp+yL3M+CPQ/vEpvvOAuTvSLCIhIMtXnzSba0sEyf5Oi/5L1YohtRKX6LbNGDWUDD4iuQ9Qj1BATz9myyVlm2Rcy55Cokj9NYm40rTgN5kFCMu1G31y633kSFOZCIV902WWntKH0uj4GL5IaTt/gjdDV90q2hRPwAR+fBE/+EnhXkItsmKwtn//g0EqHLP876dQfqvjTX8GH4hV9GPpJQ56lJ2rqx3Z8U/OO9DQVV52h8JVgfBf+4lsGeHMhFv17Re5QhFteMT5b6dWbMU7ZXLTGwkam3IyJNK/Hhy5dOUpW3aXPaX3vWrKKFxsyw+IzosEuLO9A7W+ay6Fsw8az3NRedubv9NMrtmGpmzoUbzOSKeiNjjpGrOZXp5kHXmoCeg/JoXvlwR/NZlqVGhI5v0LM+x/Xckz4fzwILl7cGGfN+HQMgs6jSUz8vvAiRKdZLlF99uIZjwmimgqDD7uRmT5ccsW8DNW0pmb4u25vYA2rjuw0A0PS8XLRMIY9cadeoCVt7ImrI/kp6R3jVHzaKOHP9ZcnMJCMAoJc+4RnbnbyqbEEXrjHdLSdOBf5I6algVx4UZdLqpOIYu1vWohAw0SO4hBMYTgnCE+SN8xLgEQcL2WuEMwRJp/bo/wzi/DfzXf8Jn7KlcppWlQ7q0ouzTYB09YbZRQPug9tQ05Gwm0kz7TBvmcPnaTThb1mfyUAyCOOotgjy6sn7YvINut96WbAYgEAQTM8L98bC0GjMCCWSYBVk3mPOW3+ix2Dg71Ul4AexKueuTB33JgAf1JB0EXuK/WOMTfcPLtqA5YJS0Tx3+7c5NhDgIYKdR5ORl2t9TwpD1vLPgEzmY7uIAzXjaf6+w+HiFgFGbzGbQ0/gukRWZfW7HwCeqZkdbzhbxUumQWVsjkwWsANV8a0d95wxPfBAOveVF4SJfQxqZ3nrUoRqiR6n/2nqVCa3DhBZEujv4lvapRgys83egs6SYAzLyDGOgljscOFAVcTeUrFyM5PzH1F9SxjOQ8YsSaF6np4O/2swEps/RYpi0QcPdQyvfWH3VAzVuMmCdMl2xOjgngfCCjNfVF7MF2/G60v+44UHuOn0nYxczd9x2vElcj7OUPOqKijO//7O0hKmt9Ppvl1ROQQVNVvY2Q4KeEn18oXlgAm6jXvAAlpM5Na1+fSnIcOXofC0dTGp6R4AqW7LhUtJUDl9e/yBtoyM+76wh6xXrGeRprxdEofqQYdxQacAGIF/j30g6/4QKXiRQu3+237au8jY74lFyTLx65n5dnw9luAdK62g8CuCaZZqA0husjY7aIYir0wvE4a3sREb5Z9ojXZ2uOnqT/HN2mG+sqEwrCtev90wIYzZw6iiaTS0oV8xUOxdiZ6QxfujJL7PZ150B8Bk96dvq6+S5UDc5Rec8kSZY8vz3wBq8kiUQ6VoO8Ed5UdjV2fpIH9F321cAbfJCujBEZDGj9P5uy3WbNoHcC+AWAiT8ERXwkceO6WQKfFHYq04UlM76mPWr9AG91OjlePADANKcoUxqtN5CdynpHjajJ+DIJHdFEqQ4JYLQtOD19VWRnhNkO+um1COCTJ9GKwt2XDNGE0QYOhvQgL72oj9eHAR72bKyZaM5h2SYPKXN4KeHmIXtJK5Yb2C/8qCYaxdTLtI6cqlUzP+3AEQMWWsxiBTemD/dEiQCWvo3Je8c1X2JtNgbVWB3rWmtWHg39JPmqhnYCHqgEQcCufsi5PfFQqcZbTaAuknSv54n15+9NN8830HV1TZ9ORKYg2z7Ztq8Np9bMYEyzAhHCHcAMyG7WsBTWSMnZCTQVzuQGtoA24QzrKUDxyjZtwgEw+LN+E9NvgBFJgvqGZYTU0N/2XKf12IABhsW/Q1nko4I2CLZ3yc9vPQBIeX5lYOitvUfeCaVobap8bxn43WJNCBFiAkonK//T0KDAzrEOOMZ3x5RW4Immook0IYmOjV0qIaQyNEnjJoqrBcp2Ce6BzhA2CURqet94ANKLJhb+zdHC7uCdYEMzyAv52o2tTrM6Md3tepHNzKZRUV8ce2RtWOid5M+J3LPXxo//8DCpMqTDx9xxLO7ImyfoqPhwzH1+ovCA2xq9/Jv0crvE0mLAS6UT0mFI35mcSe/WlqvkHvWKSVkGRpKQDGYVbuiObNRs/4dxOQtQjQykyBQ4/fy0svmKdtm8mt81DLGhCUf3d8yHDJlJ7J5uF8xkDW37ZEncWq/fy33B1EPHoATyikEHhwULLUVMhaxGTAMWdcozRntMHn/zWdfL0H4jhwPAFHwNkRFtop0lLcrRvhySxnkuj5DaoGO96JzkeEmWoNYYoOBTcM1XsQ2WFVnLp9ZIPuoMgg9dmGnLxYXjwwY2fD8z2bMgAh/AK4BVhgym8OrCQ7S/c6a9XMdeZ2i/mxjDqPXkkOvLUHPWr1PJzqaQ29GoNJzUFhhMM2xHwj0Ok6Q1uog6UWHzSsfIMKVTjZ+w3v1cbSFE0ZzqYxwlSfpI+PbVDsQ5/+18s99ddAoOu+shK2EkDaTbfIGHSqs4rSwBuTquzPebtMv11MKIPZiZTXLVcvalMKsc+KL26hYTUI2twC4n8IX0aXUWweObu/PHA2h4exnIca6gDvbWUj3O+QOxr5TTJ+qrUZd5kKEP6UI+ft3t92Iroj48INwyCIoHE7N3K+Oqtm4vHYZtnZp7uUI6LWWeroRZhzEl9SaTGKi11YqFlo5sH8NnnFo5viib+nTvkCfet0m6/nkjqvcuz0Fb9vgZdliIoONj0wzrceSFYzvPtdrRfFZc7hEj7CWDGkNmwVU388M4IC23Xb6A19UbdI/nc1s9vUXHWj4ldEwUpKZvf/iCBgrgbC4cne9y9fj2BXHjQEupowMPPYvbvORvEPHPwVWX99/98M+/tURGY74/E38jU59twpgCcF0TmHwkZuOhKgNPRXAOSySu8Bq4qfZBe1QW3La0HUtnj0ByHosiC3lAHszvMDTuZjgjSzKE/thII9uvaZXcSy8NF29wcamPFJcZeeVZDT88QK81CRx26YT2EzA8ZoX65yPmuAt+YAziH7uu6Q6kL4wh9x0RU8vJ/SAgNUQf6T20NnsdMhc1vUmP/P61XKU4Ev5rD2hFBFwpScGWjN5j3xvKXtTMhLbcVzU3WbDFbOs5a2iwy5ozjc8UXHaIKiVVwjJmxBnTu6OMBlh4wN5x1F4qEGo/qH4yP8KNT3swzBqFmfi4je/8AWTOoGePoqTOn6YHdpqWg3LvxrZRQCjw88MZotrRXyxe2XUdHdXwxLFXMKyGzBgzbkLVklz/NzJqwbUqB/c7/GXHruIYkb9nPVmS4HVe9gWFrgdM3k/QkD+R6k0B/BOyL2VpFsxkMSxMKexODb4Kw/7uXWdxDzbmNlS6Cx9rXdXRKzNesx7Do6cIqa/nOQ/scL/hxRot36dIoQVEeLP8SuWMPcfunvrFzdOrNKBmjr224DYUvMrS/0wsfJ6E6fOcCQKAAUAqNnrtfM0rJSPzugfZX0m60SvXXQaZ/2r1KOTqNegjNMYAX9YsC5A++3NA+BRU4vRnp0cgsXxKBJfd4n43s3z8wtDdnoV8D9ICVnbiQ8SstkLxP9oB1yqR6YbtER+f7/Zqfj+PhpfHDfkp9QF1oKAdnLg22l6TB3a1p6z+L9Zn8eojc4PfoTtd3GkmhBUWJ0Lg6ZMVaeQ/2tflcGvw3OwDSG8Rb+vLO7hFmR7hPW30Q7hqBcxmrgRX/EeC9BaE1ZJM89xx5pBekAmCNAQDKd7M05cZEpHd5hyanpZvHyH6fWftl13IloSe3275INETPccxDz+O8mBVL1ArkzydZc/BGPyZGXhuI37031r9sz9/q+nWwX3b0fxNnH0ajPCUEKp6SUG54CVGiAgRI/JFL6hZmJU8fCdACdjXU0KVt2niu4YRsBV8uOTF4XNbgMAPBuVcIRN83HaXzwEI1yk0kIAHPZfrVShnK2vhhIHH0MF6PO4lcdcT7cxjUmYN3A22R3cZpk+iUv7tdZQwp4Qvs/roLszi7NPYmL84lS+1S9dtwzgziB3V7JF1lpnnFrupJM3mwuX1CXxcDU8o5cAXzqKCz43wmZhA1fF+WoF0vwPRO5IfvwNwCmCeY56JDiJZRhcArO8gVrCnHW/7ddS9taFkxdnlY/obhFgmB/AFMe3wR5/WKlU0lb1WfgaGsMMLzUCP1Pm0xwayI/GjopA1PPwpZEyRayMAOWpMZ4q/umdTklnsp8vDt2Wi+lhUSFwy2F5vKy8NtNrs/81GpgMIy4dkC91R3i+b0f/Lw4MVvyIN43Umc41I2jDDGwLJ3N49vpNphqNDdHvOqIzzJIF9loVv66GfIeuIfXpi8iSTKiJkHf5pmzSGivf9H7daUYCauAgIaGJQWTPzNavk72r4KLtk560ftr74szq4To2BS7x6AEIbm89PnKxEtCpFwmSceLwP1jWllCAekPNcOclaqHrswwKqnyegiO8xStDmFaTdl3ol6P2naQZI6m9nTPZUJWHioRU4w07qdkAE2oJy3v/+lX7rHR3UE/DL7ZAbRj//xWvdzxbZmzSQFClSUV46Io+NDRVMM6v7veIhFSeLMWL2p2WxrOw3Pn/6sOpOJJb9wLGe1OREJhs9cjP62n2u31WjgQHryNdBfWFbv/pCQkbaINKlIgbDCYMvg5jZU6Diaxx0fkLPAaufO1zWduNU+OxJe2KIuCDlFx9HrFvCnTvoCzNuSg+6mNpBzP1xdaXXEoe0vEuv6EshWbgUEB+bfpN+3CyGH0deSXYftVwQ+Jd6Bg7C0XA8YWQJcihfr4HieqzDthyO47d5HR8lkWb5GQMxUIL8SLgBuvBN8sX98L9Phj8XmByjUD+cf9pvFzSt5IaND/wWbBcX4zkW3UoI1JOwECDEa+1SQ0GSJ8DyS9iZF2lmub8WhYc1NS3SO2w1OhuIZ/aVPCOcpTMoUnAbQ80e36tre3979vbYYr9nWxniEgpfJgaOt2ZzZUKH+HZBSaPhCsG9QcEEA6xTfEnN5neXnudBOZr5oomNRz9Aojizm2fOxfIbPnMvXFlDTYFXL2L6QL9BLTZyURCWjTUCj6AcDwgchu1nlY+j8/utptxlxcYbcK4vjz1lnvEr3p6y7Xm124T9WPAdFdB+p91JXTw58PeQz7AZ7THW7pcILGTJ/5It/RbN+uaTTg7rq01zyF6JFw4M/tY7MlOCgkFViULYhGtMt0hmvb0ijEvesMWCZ2Q6Pwwk3TSXOKuLXU+sCbZcBzIVvd6f6fkluEaCdZFXUxl79cbLS2HEbBZULpt66SYUbfzaRdzeaRWPTulgL14P2QwrowrOvTeYUCcYX5t4o0HHwyZES7OUQlL26YEGZ4Q2MhyjQNOfl5sVyfQjrX+fbQ6BtFlq9wmzlql/aIiExzvAr5lG1BmhmrNdcWChWrceX2ndtCmpR1QvD+cufrYsaq5wrKf7ZYz4uELKkQJNLMkI/K3x7XCmR3R4tLT/bZEBpAtkv0h8EEARbvt2FIe7Ym1zB908zKZ+FKaYEX7Vlqkgb+QMlV7M2Y+utIAYkBClL8AP9plXR/BaDK7vuuSmJosCamg9hJdl1Y4jNLyEhaoMiNSiSTydpbzkkkfZbXQInO+S+Egt53r7r37EWLYCAFqQXPb5BEwlONWpXLc6TVeXEZBrogA24kOMyWdchzq19tw3LpUJkGbUMuNxNMjRFuhXAHvJaFv1tJQa2+Wy5x3ypbHbZX58SGoMpFQnD8UAAAAAAAAAAA",
  "starter-v1-iunik-centella-sun-water.webp": "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",
  "starter-v1-baby-dove-rich-moisture-wash.webp": "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",
  "starter-v1-olay-bump-erasing-body-wash.webp": "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"
};
  const PRODUCTS = [
  {
    "id": "eucerin-oil-control-sun-gel-cream-spf50-50ml",
    "name": "Eucerin Oil Control Sun Gel-Cream SPF50+ \u2013 50ml",
    "price": 22000,
    "brand": "Eucerin",
    "dept": "skincare",
    "category": "blemish-prone",
    "categoryLabel": "Blemish-prone",
    "tag": "Sun Protection",
    "rating": 0,
    "image": "/uploads/starter-v1-eucerin-oil-control.webp",
    "images": [],
    "description": "An ultra-light sun gel-cream with a dry-touch finish, made for blemish-prone skin. SPF 50+ gives very high protection against UVB and UVA rays. Oil Control formula with advanced spectral technology. Dermatological skincare by Eucerin. Size: 50ml.",
    "ingredients": "",
    "usageSteps": [
      "Apply generously and evenly to the face and any exposed skin about 15 minutes before going outside.",
      "Reapply after swimming, sweating or towel drying, and at least every 2 hours in the sun."
    ],
    "usageWhen": "Morning",
    "usageTips": "Use it as the last step of your morning skincare routine.",
    "usageWarn": "For external use only. Avoid contact with the eyes. Stop using if irritation occurs.",
    "hasVideo": false,
    "usage": "Apply generously and evenly to the face and any exposed skin about 15 minutes before going outside. Reapply after swimming, sweating or towel drying, and at least every 2 hours in the sun."
  },
  {
    "id": "iunik-centella-calming-daily-sun-water-120ml",
    "name": "IUNIK Centella Calming Daily Sun Water 120ml",
    "price": 21000,
    "brand": "IUNIK",
    "dept": "kbeauty",
    "category": "all",
    "tag": "Sun Protection",
    "rating": 0,
    "image": "/uploads/starter-v1-iunik-centella-sun-water.webp",
    "images": [],
    "description": "A sheer, lightweight sun water that calms and hydrates while defending your skin from UV. SPF 50+ PA++++. Size: 120ml (4.05 fl oz).",
    "ingredients": "",
    "usageSteps": [
      "Apply generously and evenly to the face and any exposed skin about 15 minutes before going outside.",
      "Reapply after swimming, sweating or towel drying, and at least every 2 hours in the sun."
    ],
    "usageWhen": "Morning",
    "usageTips": "Use it as the last step of your morning skincare routine.",
    "usageWarn": "For external use only. Avoid contact with the eyes. Stop using if irritation occurs.",
    "hasVideo": false,
    "usage": "Apply generously and evenly to the face and any exposed skin about 15 minutes before going outside. Reapply after swimming, sweating or towel drying, and at least every 2 hours in the sun."
  },
  {
    "id": "baby-dove-rich-moisture-hypoallergenic-wash-20oz",
    "name": "Baby Dove Rich Moisture Hypoallergenic Wash \u2013 20oz",
    "price": 35000,
    "brand": "Baby Dove",
    "dept": "baby",
    "category": "sensitive",
    "tag": "Baby Care",
    "rating": 0,
    "image": "/uploads/starter-v1-baby-dove-rich-moisture-wash.webp",
    "images": [],
    "description": "A gentle, hypoallergenic wash for baby's sensitive skin, with rich moisture. Part of the Baby Dove sensitive skin care range. Size: 20oz.",
    "ingredients": "",
    "usageSteps": [
      "Squeeze a small amount onto a wet sponge or your hand.",
      "Gently lather over baby's body and rinse well with water."
    ],
    "usageWhen": "Bath time",
    "usageTips": "Pat the skin dry gently after bath time.",
    "usageWarn": "For external use only. Avoid contact with the eyes; if it gets in, rinse well with water. Never leave a baby alone in the bath.",
    "hasVideo": false,
    "usage": "Squeeze a small amount onto a wet sponge or your hand. Gently lather over baby's body and rinse well with water."
  },
  {
    "id": "olay-bump-erasing-exfoliating-body-wash-20-fl-oz",
    "name": "Olay Bump Erasing Exfoliating Body Wash \u2013 20 fl oz",
    "price": 20000,
    "brand": "Olay",
    "dept": "bath",
    "category": "all",
    "tag": "Body Wash",
    "rating": 0,
    "image": "/uploads/starter-v1-olay-bump-erasing-body-wash.webp",
    "images": [],
    "description": "A body wash that exfoliates and smooths rough, bumpy skin, with an AHA serum complex. Made with Vitamin B3 and lamellar technology. Size: 20 fl oz (591 mL).",
    "ingredients": "",
    "usageSteps": [
      "Apply to a wet washcloth, pouf or your hands and lather over the body.",
      "Rinse well. Start with a few times a week to see how your skin feels."
    ],
    "usageWhen": "Shower",
    "usageTips": "Moisturise after your shower.",
    "usageWarn": "For external use only. Avoid contact with the eyes. Exfoliating products can make skin more sensitive to the sun, so use sun protection. Stop using if irritation occurs.",
    "hasVideo": false,
    "usage": "Apply to a wet washcloth, pouf or your hands and lather over the body. Rinse well. Start with a few times a week to see how your skin feels."
  }
];
  function install() {
    for (const [name, b64] of Object.entries(IMG)) { try { fs.writeFileSync(path.join(UPLOADS, name), Buffer.from(b64, 'base64')); } catch (e) { console.error('Starter picture not written:', name, e.message); } }
    tx(() => { db.exec('DELETE FROM products'); db.exec('DELETE FROM stock'); const ins = db.prepare('INSERT INTO products VALUES(?,?,?)'); PRODUCTS.forEach((p, i) => ins.run(p.id, i, JSON.stringify(p))); bumpRev('products_rev'); });
    return PRODUCTS.length;
  }
  route('POST', '/api/admin/starter-products', { auth: 'admin', limit: ['starter', 3] }, ({ body }) => {
    if (body.confirm !== true) throw new HttpError(400, 'Please confirm that you want to replace every product.');
    const d = new Date(), z = n => String(n).padStart(2, '0'), backup = `store-${d.getUTCFullYear()}${z(d.getUTCMonth() + 1)}${z(d.getUTCDate())}-${z(d.getUTCHours())}${z(d.getUTCMinutes())}${z(d.getUTCSeconds())}-update.db`;
    try { const dir = path.join(DATA, 'backups'); fs.mkdirSync(dir, { recursive: true }); const f = path.join(dir, backup); if (!fs.existsSync(f)) db.exec(`VACUUM INTO '${f.replace(/'/g, "''")}'`); }
    catch (e) { console.error('Safety backup failed:', e.message); throw new HttpError(500, 'The safety backup could not be made, so nothing was changed.'); }
    return { ok: true, count: install(), backup, rev: getRev('products_rev') };
  });
  if (!db.prepare("SELECT 1 FROM settings WHERE k='starter_done'").get()) {
    if (!db.prepare('SELECT 1 FROM products LIMIT 1').get()) { install(); console.log('Added your 4 real starter products (the store had none).'); }
    db.prepare("INSERT OR IGNORE INTO settings VALUES('starter_done','1')").run();
  }
  return { install };
})();
