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
const UPLOADS = path.join(DATA, 'uploads'); fs.mkdirSync(UPLOADS, { recursive: true });
const MAX_UPLOAD = (+ENV.MAX_UPLOAD_MB || 0) * 1048576;   // 0 = no limit set by this app (your host / disk may still have one)
const SECRET = ENV.JWT_SECRET || (() => { const f = path.join(DATA, '.secret'); try { return fs.readFileSync(f, 'utf8'); } catch { const s = crypto.randomBytes(48).toString('hex'); fs.writeFileSync(f, s, { mode: 0o600 }); return s; } })();
const PS_API = ENV.PAYSTACK_API || 'https://api.paystack.co', PAYSTACK = ENV.PAYSTACK_SECRET_KEY || '', SITE = (ENV.SITE_URL || `http://localhost:${PORT}`).replace(/\/$/, '');

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
if (!db.prepare('SELECT 1 FROM products LIMIT 1').get()) {
  try { const seed = JSON.parse(fs.readFileSync(path.join(ROOT, 'products.seed.json'), 'utf8')); const ins = db.prepare('INSERT INTO products VALUES(?,?,?)'); seed.forEach((p, i) => ins.run(p.id, i, JSON.stringify(p))); } catch {}
}
const DEFAULTS = { bank_name: '', account_number: '', account_name: '', extra_accounts: '[]', pay_off: '', cod_note: '', ship_lagos: '4000', ship_other: '6500', days_lagos: '1-3', days_other: '3-7', process_time: '24 hours', free_over: '0', intl_note: '', whatsapp: '2349032078594', instagram: 'skinsentials22', announcement: '' };
const LIMITS = { extra_accounts: 1500, pay_off: 120, cod_note: 300, ship_lagos: 9, ship_other: 9, free_over: 9, days_lagos: 6, days_other: 6, process_time: 40, intl_note: 300 };
const getSettings = () => { const s = { ...DEFAULTS }; for (const r of db.prepare('SELECT k,v FROM settings').all()) if (r.k in DEFAULTS) s[r.k] = r.v; return s; };
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
const emailOk = e => /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(e) && e.length <= 254;
const naira = n => '₦' + n.toLocaleString('en-NG');
const hits = new Map();
function limit(ip, bucket, max, win = 60000) {
  const k = bucket + ip, t = now(), h = (hits.get(k) || []).filter(x => t - x < win); h.push(t); hits.set(k, h);
  if (h.length > max) throw new HttpError(429, 'Too many attempts. Please wait a minute and try again.');
}
setInterval(() => { const t = now(); for (const [k, v] of hits) if (!v.some(x => t - x < 60000)) hits.delete(k); }, 60000).unref();
const userOut = u => ({ id: u.id, name: u.name, email: u.email, phone: u.phone, address: u.address || '', created: u.created, newsletter: !!db.prepare('SELECT active FROM subscribers WHERE email=? AND active=1').get(u.email) });
const orderOut = o => ({ id: '#' + (1000 + o.id), ref: o.ref, customer: o.customer, email: o.email, phone: o.phone, address: o.address, items: o.item_count, lines: JSON.parse(o.items), total: o.total, shipping: o.shipping || 0, state: o.state || '', zone: o.zone || '', method: o.method, status: o.status, paid: !!o.paid, claimed: !!o.claimed, note: o.note, date: o.created, payment: o.pstatus ? { id: o.pid, status: o.pstatus, channel: o.pchannel, detail: o.pdetail, fees: o.pfees, received: o.preceived } : null });

function subscribe(email, name) {
  const ex = db.prepare('SELECT * FROM subscribers WHERE email=?').get(email);
  if (ex) { db.prepare('UPDATE subscribers SET active=1, name=COALESCE(NULLIF(?,\'\'),name) WHERE id=?').run(name, ex.id); return false; }
  db.prepare('INSERT INTO subscribers(email,name,token,created) VALUES(?,?,?,?)').run(email, name, crypto.randomBytes(18).toString('hex'), now()); return true;
}

/* ---------- notifications (all optional; configured in .env) ---------- */
const TG = { token: ENV.TELEGRAM_BOT_TOKEN || '', chat: ENV.TELEGRAM_CHAT_ID || '', api: ENV.TELEGRAM_API || 'https://api.telegram.org' };
const MAILCFG = { key: ENV.RESEND_API_KEY || '', from: ENV.MAIL_FROM || '', admin: ENV.ADMIN_EMAIL || '', api: ENV.RESEND_API || 'https://api.resend.com' };
const HOOK = ENV.NOTIFY_WEBHOOK_URL || '';
const mailOn = () => !!(MAILCFG.key && MAILCFG.from);
const notifyStatus = () => ({ telegram: !!(TG.token && TG.chat), webhook: !!HOOK, adminEmail: mailOn() && !!MAILCFG.admin, customerEmail: mailOn() });
const hx = v => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
async function postJson(url, body, headers = {}) {
  let r; try { r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body), signal: AbortSignal.timeout(10000) }); } catch (e) { throw new Error('could not reach ' + new URL(url).host); }
  if (!r.ok) throw new Error(new URL(url).host + ' answered HTTP ' + r.status); return r;
}
const sendMail = (to, subject, html) => mailOn() && to ? postJson(MAILCFG.api + '/emails', { from: MAILCFG.from, to: [to], subject, html }, { Authorization: 'Bearer ' + MAILCFG.key }).then(() => true) : Promise.resolve(false);
async function notifyAdmin(title, lines) {
  const text = [title, ...lines].join('\n'), jobs = [];
  if (TG.token && TG.chat) jobs.push(postJson(`${TG.api}/bot${TG.token}/sendMessage`, { chat_id: TG.chat, text }));
  if (HOOK) jobs.push(postJson(HOOK, { text, content: text, title, lines }));
  if (mailOn() && MAILCFG.admin) jobs.push(sendMail(MAILCFG.admin, title, `<p>${lines.map(hx).join('<br>')}</p>`));
  const rs = await Promise.allSettled(jobs); rs.forEach(r => r.status === 'rejected' && console.error('Admin notification failed:', r.reason.message)); return rs.every(r => r.status === 'fulfilled') && jobs.length > 0;
}
const mailShell = (h, body) => `<div style="font-family:Arial,sans-serif;max-width:520px;margin:auto;color:#1b2a3a"><h2 style="font-family:Georgia,serif">SKINSENTIALS22</h2><h3>${h}</h3>${body}<p style="color:#778;font-size:12px;margin-top:24px">Questions? Reply to this email or message us on WhatsApp.</p></div>`;
const linesHtml = o => `<table style="width:100%;border-collapse:collapse;font-size:14px">${JSON.parse(o.items).map(i => `<tr><td style="padding:4px 0">${i.qty} × ${hx(i.name)}</td><td style="text-align:right">${naira(i.price * i.qty)}</td></tr>`).join('')}${o.shipping ? `<tr><td style="padding:4px 0">Delivery</td><td style="text-align:right">${naira(o.shipping)}</td></tr>` : ''}<tr><td style="padding-top:8px;border-top:1px solid #ddd"><b>Total</b></td><td style="text-align:right;border-top:1px solid #ddd"><b>${naira(o.total)}</b></td></tr></table>`;
const quiet = p => { Promise.resolve(p).catch(e => console.error('Background task failed:', e.message)); };

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
  quiet(notifyAdmin(`💰 Payment received: ${naira(got)}`, [`Order ${oid(o)} · ${o.customer}`, `Method: ${label(o.method)}${i.channel ? ' (' + i.channel + ')' : ''}`, `Reference: ${o.ref}`]));
  quiet(sendMail(o.email, `Payment received for order ${oid(o)}`, mailShell('Thank you! We have received your payment.', `<p>Hi ${hx(o.customer)}, your order <b>${oid(o)}</b> is confirmed and we are getting it ready.</p>${linesHtml(o)}<p>Delivery to: ${hx(o.address)}${o.state ? ', ' + hx(o.state) : ''}</p><p style="color:#778">Reference ${hx(o.ref)}</p>`)));
  return true;
}
function unsettle(o, note) {
  const p = payOf(o.id); if (!p || p.status !== 'paid') return false;
  tx(() => { db.prepare("UPDATE payments SET status='pending', received=0, paid_at=NULL, confirmed_by='', updated=? WHERE id=?").run(now(), p.id); db.prepare('UPDATE orders SET paid=0, paid_at=NULL WHERE id=?').run(o.id); ev(p.id, 'unpaid', 'Marked as not paid by admin' + (note ? ': ' + note : '')); }); return true;
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
const deliveryCfg = s => ({ lagos: { fee: money0(s.ship_lagos, 4000), days: daysOk(s.days_lagos, '1-3') }, other: { fee: money0(s.ship_other, 6500), days: daysOk(s.days_other, '3-7') }, processing: s.process_time || '24 hours', freeOver: money0(s.free_over, 0), intlNote: s.intl_note || 'International orders: please contact us on WhatsApp for shipping rates.' });
const shipFor = (s, zone, sub, physical) => { const d = deliveryCfg(s); return physical && (zone === 'lagos' || zone === 'other') && !(d.freeOver && sub >= d.freeOver) ? d[zone].fee : 0; };
const etaText = (s, zone) => { const d = deliveryCfg(s); return `Orders are processed within ${d.processing}. Delivery takes ${zone === 'lagos' ? d.lagos.days + ' working days within Lagos' : d.other.days + ' working days outside Lagos'}.`; };
const csvCell = v => { let t = String(v == null ? '' : v); if (/^[=+\-@\t\r]/.test(t)) t = "'" + t; return '"' + t.replace(/"/g, '""') + '"'; };   // the leading quote stops spreadsheet formula injection
const contentRaw = () => { try { return JSON.parse((db.prepare("SELECT v FROM settings WHERE k='content'").get() || {}).v || 'null'); } catch { return null; } };

route('GET', '/api/health', () => ({ ok: true }));
route('GET', '/api/config', () => {
  const s = getSettings(), methods = Object.entries(PM).filter(([, m]) => !m.hidden).map(([id, m]) => { const st = methodState(id, s); return { id, label: m.label, desc: m.desc, group: m.manual ? 'direct' : 'online', available: st.ok, reason: st.reason || '' }; });
  return { paystack: !!psKey(), transfer: methodState('transfer', s).ok, dm: methodState('dm', s).ok, methods, whatsapp: s.whatsapp, instagram: s.instagram, announcement: s.announcement, codNote: s.cod_note, email: mailOn(), delivery: { ...deliveryCfg(s), states: NG_STATES, intl: INTL } };
});
route('GET', '/api/products', () => ({ products: productsAll(), rev: getRev('products_rev') }));
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
  return { token: userToken(u), user: userOut(u) };
});
route('POST', '/api/auth/login', { limit: ['login', 10] }, ({ body }) => {
  const u = db.prepare('SELECT * FROM users WHERE email=?').get(str(body.email, 254).toLowerCase());
  const ok = u ? checkPw(String(body.password || ''), u.pw) : (crypto.scryptSync('x', 'y', 64), false);
  if (!ok) throw new HttpError(401, 'Incorrect email or password.');
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
  db.prepare('UPDATE users SET pw=? WHERE id=?').run(hashPw(n), user.id); return { ok: true, token: userToken(db.prepare('SELECT * FROM users WHERE id=?').get(user.id)) };
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
  const prods = new Map(productsAll().map(p => [p.id, p])), lines = []; let total = 0, count = 0;
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
  const shipping = shipFor(s, zone, total, physical); total += shipping;
  const ref = 'SK22-' + crypto.randomBytes(5).toString('hex').toUpperCase();
  let authUrl = '';
  if (!PM[method].manual) { const b2 = { email, amount: total * 100, currency: 'NGN', reference: ref, callback_url: `${SITE}/?pay=${ref}`, metadata: { customer, phone, method } }; if (PM[method].ps) b2.channels = PM[method].ps; authUrl = (await paystack('POST', '/transaction/initialize', b2)).authorization_url; }
  /* order + payment record + stock hold happen together or not at all */
  const o = tx(() => {
    for (const [id, n] of needs) if (db.prepare('SELECT 1 FROM stock WHERE product_id=?').get(id) && !db.prepare('UPDATE stock SET qty=qty-? WHERE product_id=? AND qty>=?').run(n, id, n).changes) throw new HttpError(409, `${prods.get(id).name} has just sold out. Please update your bag.`);
    const t = now(), info = db.prepare('INSERT INTO orders(ref,user_id,customer,email,phone,address,items,item_count,total,shipping,method,created,state,zone) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run(ref, user ? user.id : null, customer, email, phone, address, JSON.stringify(lines), count, total, shipping, method, t, state, zone);
    const pid = db.prepare('INSERT INTO payments(order_id,ref,method,amount,status,created,updated) VALUES(?,?,?,?,?,?,?)').run(info.lastInsertRowid, ref, method, total, 'pending', t, t).lastInsertRowid;
    ev(pid, 'order', `New order: ${naira(total)} via ${label(method)}${PM[method].manual ? '' : ', waiting for the customer to pay'}`, 1);
    return db.prepare('SELECT * FROM orders WHERE id=?').get(info.lastInsertRowid);
  });
  const out = { ref, id: oid(o), total, shipping, method, state, zone, eta: physical ? etaText(s, zone) : '' };
  if (!PM[method].manual) out.authorization_url = authUrl;
  else if (method === 'transfer') { const accs = accountsOf(s); out.bank = { ...accs[0], narration: ref }; out.accounts = accs; }
  else if (method === 'cod') out.cod = { note: s.cod_note || '' };
  else out.dm = dmLinks(o, s);
  quiet(notifyAdmin(`🛒 New order ${oid(o)}: ${naira(total)}`, [`${customer} · ${phone}`, `Pay by: ${label(method)}`, `Items: ${lines.map(l => l.qty + 'x ' + l.name).join(', ')}`, `Deliver to: ${address}`, `Ref: ${ref}`]));
  if (PM[method].manual) quiet(sendMail(email, `We received your order ${oid(o)}`, mailShell('Thank you for your order!', `<p>Hi ${hx(customer)}, we have your order <b>${oid(o)}</b>.</p>${linesHtml(o)}${physical ? `<p style="color:#556">${hx(etaText(s, zone))}</p>` : ''}${method === 'transfer' ? `<p><b>Please transfer ${naira(total)}</b> to one of these accounts using <b>${hx(ref)}</b> as the narration:</p>${out.accounts.map(a => `<p style="background:#f4f1ea;padding:10px;border-radius:8px">${hx(a.bank_name)}<br><b>${hx(a.account_number)}</b><br>${hx(a.account_name)}</p>`).join('')}<p>Then tap "I have made the transfer" on the website and we will confirm.</p>` : method === 'cod' ? `<p>You will pay <b>${naira(total)}</b> when it arrives. We will call to confirm delivery.</p>` : '<p>Please continue the chat with us on WhatsApp or Instagram to agree payment.</p>'}`)));
  return out;
});
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
  }
  if (typeof body.paid === 'boolean') { const fresh = db.prepare('SELECT * FROM orders WHERE id=?').get(o.id); if (body.paid) { if (fresh.status === 'Cancelled') throw new HttpError(409, 'This order is cancelled.'); settle(fresh, { by: 'admin' }); } else unsettle(fresh); }
  return { order: orderOut(adminOrder(params.id)) };
});
route('PUT', '/api/admin/products', { auth: 'admin', big: true }, ({ body }) => {
  const list = body.products; if (!Array.isArray(list) || list.length > 500) throw new HttpError(400, 'Invalid product list');
  if (+body.rev !== getRev('products_rev')) throw new HttpError(409, 'The catalogue was changed somewhere else (another tab or device). Reload this page, then repeat your change.');
  if (!list.length && db.prepare('SELECT 1 FROM products').get() && !body.allowEmpty) throw new HttpError(400, 'Refusing to delete every product. Reload the page and try again.');
  const seen = new Set(); for (const p of list) { if (!p || typeof p.id !== 'string' || !p.id || p.id.length > 64 || seen.has(p.id) || typeof p.name !== 'string' || !(+p.price >= 0)) throw new HttpError(400, 'Each product needs a unique id, a name and a valid price.'); seen.add(p.id); p.price = Math.round(+p.price); delete p.stock;
    for (const k of ['videos', 'images']) if (p[k] != null) p[k] = (Array.isArray(p[k]) ? p[k] : []).map(u => str(u, 2000)).filter(Boolean);
    for (const k of ['brand', 'dept', 'deptLabel', 'category', 'categoryLabel', 'usageWhen']) if (p[k] != null) p[k] = str(p[k], 80);
    for (const [k, n] of [['usage', 3000], ['usageTips', 800], ['usageWarn', 600], ['ingredients', 3000], ['description', 4000]]) if (p[k] != null) p[k] = str(p[k], n);
    if (p.usageSteps != null) p.usageSteps = (Array.isArray(p.usageSteps) ? p.usageSteps : []).map(x => str(x, 500)).filter(Boolean).slice(0, 20);
    if (Array.isArray(p.videos)) p.hasVideo = p.hasVideo || p.videos.length > 0; }
  const rev = tx(() => { db.exec('DELETE FROM products'); const ins = db.prepare('INSERT INTO products VALUES(?,?,?)'); list.forEach((p, i) => ins.run(p.id, i, JSON.stringify(p))); for (const r of db.prepare('SELECT product_id FROM stock').all()) if (!seen.has(r.product_id)) db.prepare('DELETE FROM stock WHERE product_id=?').run(r.product_id); return bumpRev('products_rev'); });
  return { ok: true, count: list.length, rev };
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
  db.prepare("INSERT INTO settings VALUES('content',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v").run(JSON.stringify(c)); return { ok: true, rev: bumpRev('content_rev') };
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
  restock(db.prepare('SELECT * FROM orders WHERE id=?').get(o.id)); return payDetail(p.id);
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
  return { file: { name: 'skinsentials22-backup-' + new Date().toISOString().slice(0, 10) + '.json', type: 'application/json', data: JSON.stringify({ exported: new Date().toISOString(), products: productsAll(), stock: [...stockMap()], orders: db.prepare('SELECT * FROM orders ORDER BY id').all(), payments: db.prepare('SELECT * FROM payments ORDER BY id').all(), payment_events: db.prepare('SELECT * FROM payment_events ORDER BY id').all(), users: db.prepare('SELECT * FROM users').all().map(strip), subscribers: db.prepare('SELECT * FROM subscribers').all(), brands: db.prepare('SELECT * FROM brands').all(), settings: getSettings(), content: contentRaw() }, null, 1) } };
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

/* ---------- server ---------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.webp': 'image/webp', '.gif': 'image/gif', '.txt': 'text/plain', '.mp4': 'video/mp4', '.m4v': 'video/mp4', '.mov': 'video/quicktime', '.webm': 'video/webm', '.ogv': 'video/ogg', '.jpeg': 'image/jpeg' };
function readBody(req, max) {
  return new Promise((ok, no) => { const c = []; let n = 0; req.on('data', d => { n += d.length; if (n > max) { no(new HttpError(413, 'Request too large')); req.destroy(); } else c.push(d); }); req.on('end', () => ok(Buffer.concat(c))); req.on('error', no); });
}
const send = (res, code, data, type = 'application/json; charset=utf-8', extra = {}) => { res.writeHead(code, { 'Content-Type': type, 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY', 'Referrer-Policy': 'strict-origin-when-cross-origin', ...(SITE.startsWith('https') ? { 'Strict-Transport-Security': 'max-age=15552000' } : {}), ...extra }); res.end(data); };

/* serves uploaded files with HTTP Range support, which videos need to start quickly, seek and play on iPhone/Safari */
function serveUpload(req, res, name) {
  const f = path.join(UPLOADS, name);
  if (!/^[\w.-]+$/.test(name) || name.endsWith('.part') || !fs.existsSync(f)) throw new HttpError(404, 'Not found');
  const size = fs.statSync(f).size, type = MIME[path.extname(name).toLowerCase()] || 'application/octet-stream';
  const h = { 'Content-Type': type, 'Accept-Ranges': 'bytes', 'Cache-Control': 'public, max-age=31536000, immutable', 'X-Content-Type-Options': 'nosniff' };
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

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x'), ip = (ENV.TRUST_PROXY === '1' && String(req.headers['x-forwarded-for'] || '').split(',').pop().trim()) || req.socket.remoteAddress || '';   // last entry = the one our own proxy added; the first can be forged by the visitor
  const cors = ENV.CORS_ORIGIN ? { 'Access-Control-Allow-Origin': ENV.CORS_ORIGIN, 'Access-Control-Allow-Headers': 'Content-Type, Authorization', 'Access-Control-Allow-Methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS', Vary: 'Origin' } : {};
  try {
    if (url.pathname.startsWith('/api/')) {
      if (req.method === 'OPTIONS') return send(res, 204, '', 'text/plain', cors);
      const r = routes.find(r => r.method === req.method && r.re.test(url.pathname));
      if (!r) throw new HttpError(404, 'Not found');
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
      if (out && out.file) return send(res, 200, out.file.data, out.file.type + '; charset=utf-8', { ...cors, 'Content-Disposition': `attachment; filename="${out.file.name}"` });
      return send(res, 200, JSON.stringify(out), undefined, { ...cors, 'Cache-Control': 'no-store' });
    }
    if (req.method !== 'GET' && req.method !== 'HEAD') throw new HttpError(405, 'Method not allowed');
    if (url.pathname.startsWith('/uploads/')) return serveUpload(req, res, path.basename(decodeURIComponent(url.pathname)));
    let f = path.normalize(path.join(PUB, url.pathname === '/' ? 'index.html' : decodeURIComponent(url.pathname)));
    if (!f.startsWith(PUB + path.sep) || path.basename(f).startsWith('.')) throw new HttpError(404, 'Not found');
    if (!fs.existsSync(f) || fs.statSync(f).isDirectory()) throw new HttpError(404, 'Not found');
    const ext = path.extname(f).toLowerCase();
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
