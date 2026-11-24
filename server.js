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
if (!db.prepare("SELECT 1 FROM settings WHERE k='seeded'").get()) {   // the starter products are added once, on a brand-new store only: deleting products never brings them back
  if (!db.prepare('SELECT 1 FROM products LIMIT 1').get()) {
  try { const seed = JSON.parse(fs.readFileSync(path.join(ROOT, 'products.seed.json'), 'utf8')); const ins = db.prepare('INSERT INTO products VALUES(?,?,?)'); seed.forEach((p, i) => ins.run(p.id, i, JSON.stringify(p))); } catch {}
  }
  try { db.prepare("INSERT OR IGNORE INTO settings VALUES('seeded','1')").run(); } catch {}
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
const orderOut = o => ({ id: '#' + (1000 + o.id), ref: o.ref, customer: o.customer, email: o.email, phone: o.phone, address: o.address, items: o.item_count, lines: JSON.parse(o.items), total: o.total, discount: (o.discount || 0) + (o.glow_disc || 0), glow: o.glow_disc || 0, credit: o.credit_used || 0, shipping: o.shipping || 0, state: o.state || '', zone: o.zone || '', method: o.method, status: o.status, paid: !!o.paid, claimed: !!o.claimed, note: o.note, date: o.created, payment: o.pstatus ? { id: o.pid, status: o.pstatus, channel: o.pchannel, detail: o.pdetail, fees: o.pfees, received: o.preceived } : null });

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
  quiet(notifyAdmin(`🛒 New order ${oid(o)}: ${naira(total)}`, [`${customer} · ${phone}`, `Pay by: ${label(method)}`, `Items: ${lines.map(l => l.qty + 'x ' + l.name).join(', ')}`, `Deliver to: ${address}`, `Ref: ${ref}`]));
  if (PM[method].manual) quiet(sendMail(email, `We received your order ${oid(o)}`, mailShell('Thank you for your order!', `<p>Hi ${hx(customer)}, we have your order <b>${oid(o)}</b>.</p>${linesHtml(o)}${GLOW.note(o)}${physical ? `<p style="color:#556">${hx(etaText(s, zone))}</p>` : ''}${method === 'transfer' ? `<p><b>Please transfer ${naira(total)}</b> to one of these accounts using <b>${hx(ref)}</b> as the narration:</p>${out.accounts.map(a => `<p style="background:#f4f1ea;padding:10px;border-radius:8px">${hx(a.bank_name)}<br><b>${hx(a.account_number)}</b><br>${hx(a.account_name)}</p>`).join('')}<p>Then tap "I have made the transfer" on the website and we will confirm.</p>` : method === 'cod' ? `<p>You will pay <b>${naira(total)}</b> when it arrives. We will call to confirm delivery.</p>` : '<p>Please continue the chat with us on WhatsApp or Instagram to agree payment.</p>'}`)));
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
  restock(db.prepare('SELECT * FROM orders WHERE id=?').get(o.id)); refHook('onReverse', o, 'Order refunded', 'void'); return payDetail(p.id);
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
  return { file: { name: 'skinsentials22-backup-' + new Date().toISOString().slice(0, 10) + '.json', type: 'application/json', data: JSON.stringify({ exported: new Date().toISOString(), products: productsAll(), stock: [...stockMap()], orders: db.prepare('SELECT * FROM orders ORDER BY id').all(), payments: db.prepare('SELECT * FROM payments ORDER BY id').all(), payment_events: db.prepare('SELECT * FROM payment_events ORDER BY id').all(), users: db.prepare('SELECT * FROM users').all().map(strip), subscribers: db.prepare('SELECT * FROM subscribers').all(), brands: db.prepare('SELECT * FROM brands').all(), settings: getSettings(), content: contentRaw(), glow: Object.fromEntries(['glow_ledger', 'glow_claims', 'glow_codes', 'glow_refs'].map(t => [t, db.prepare('SELECT * FROM ' + t).all()])), referral: Object.fromEntries(['ref_versions', 'ref_codes', 'ref_people', 'ref_orders', 'ref_ledger', 'ref_events', 'ref_flags', 'ref_audit'].map(t => [t, db.prepare('SELECT * FROM ' + t).all()])) }, null, 1) } };
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
REF = referralInit({ db, route, tx, now, str, HttpError, naira, csvCell, hx, sendMail, mailShell, quiet, productsAll, shipFor, zoneOf, getSettings, PM, crypto, SITE, mailOn, SECRET });

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
  const lineOf = items => {
    if (!Array.isArray(items) || items.length > 50) throw new HttpError(400, 'Your bag is empty.');
    const prods = new Map(productsAll().map(p => [p.id, p])), out = [];
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

/* ---------- SEO: crawlable URLs, sitemap, robots, real titles/descriptions/Product data in the HTML ----------
   Google and WhatsApp/Facebook previews read the HTML the server sends. This gives every product, blog post and
   main page its own address (/p/ID/name, /blog/ID/title, /shop ...) with its own title, description, canonical link,
   social preview and Product structured data, plus /sitemap.xml and /robots.txt. It only reads; it never changes products. */
const SEO = (() => {
  const PAGES = { '/shop': ['Shop Skincare in Nigeria — Serums, Moisturisers & Cleansers | Skinsentials22', 'Browse all skincare essentials for every skin type. Authentic products, prices in naira, delivery across Nigeria.', 'catalog'],
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
  const prods = () => { try { return productsAll().filter(p => p && p.id && !p.hidden && !p.draft); } catch { return []; } };
  const posts = () => { try { return ((contentRaw() || {}).posts || []).filter(p => p && p.pub && p.id); } catch { return []; } };
  const pUrl = p => `${SITE}/p/${encodeURIComponent(p.id)}/${slug(p.name)}`, bUrl = p => `${SITE}/blog/${encodeURIComponent(p.id)}/${slug(p.title)}`;
  const day = () => new Date().toISOString().slice(0, 10);
  function page(m) {
    let h; try { h = fs.readFileSync(path.join(PUB, 'index.html'), 'utf8'); } catch { return null; }
    for (const re of [/<title>[\s\S]*?<\/title>/, /<meta name="description"[^>]*>/, /<link rel="canonical"[^>]*>/, /<meta property="og:(title|description|url|type|image)"[^>]*>/g, /<meta name="twitter:(title|description|image)"[^>]*>/g]) h = h.replace(re, '');
    const tags = `<title>${e(m.title)}</title><meta name="description" content="${e(m.desc)}"><link rel="canonical" id="sk-canon" href="${e(m.url)}"><meta property="og:title" content="${e(m.title)}"><meta property="og:description" content="${e(m.desc)}"><meta property="og:url" content="${e(m.url)}"><meta property="og:type" content="${m.type || 'website'}">` +
      (m.image ? `<meta property="og:image" content="${e(m.image)}"><meta name="twitter:image" content="${e(m.image)}">` : '') + `<meta name="twitter:title" content="${e(m.title)}"><meta name="twitter:description" content="${e(m.desc)}"><script>window.__SK_SEO__=${jsn(m.boot || { route: 'page' })};</script>`;
    h = h.replace('</head>', () => tags + '</head>');
    if (m.ld) h = h.replace('<script type="application/ld+json" id="sk-ld-page"></script>', () => `<script type="application/ld+json" id="sk-ld-page">${jsn(m.ld)}</script>`);
    if (m.body) h = h.replace(/(<body[^>]*>)/, (x, b) => b + '<noscript>' + m.body + '</noscript>');
    return h;
  }
  const links = () => '<ul>' + prods().slice(0, 200).map(p => `<li><a href="/p/${encodeURIComponent(p.id)}/${slug(p.name)}">${e(p.name)}</a> ₦${Math.round(+p.price || 0).toLocaleString('en-NG')}</li>`).join('') + '</ul>';
  const crumbs = l => ({ '@type': 'BreadcrumbList', itemListElement: l.map((c, i) => ({ '@type': 'ListItem', position: i + 1, name: c[0], item: c[1] })) });
  const nf = (res) => false;
  /* returns true when it handled the request */
  const setting = k => { try { return (db.prepare('SELECT v FROM settings WHERE k=?').get(k) || {}).v || ''; } catch { return ''; } };
  const keySet = (k, v) => { try { db.prepare('INSERT INTO settings VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v').run(k, v); } catch {} };
  const ixKey = () => { let k = setting('indexnow_key'); if (!/^[a-f0-9]{32}$/.test(k)) { k = crypto.randomBytes(16).toString('hex'); keySet('indexnow_key', k); } return k; };
  const handle = function (req, res, url) {
    const p = url.pathname, html = (code, body) => { send(res, code, req.method === 'HEAD' ? '' : body, 'text/html; charset=utf-8', { 'Cache-Control': 'no-cache' }); return true; };
    if (/^\/[a-f0-9]{32}\.txt$/.test(p) && p.slice(1, 33) === ixKey()) { send(res, 200, p.slice(1, 33), 'text/plain; charset=utf-8'); return true; }   // proves to search engines that this site owns the IndexNow key
    if (p === '/robots.txt') { send(res, 200, `User-agent: *\nAllow: /\nDisallow: /api/\nDisallow: /uploads/private/\n\nSitemap: ${SITE}/sitemap.xml\n`, 'text/plain; charset=utf-8', { 'Cache-Control': 'public, max-age=3600' }); return true; }
    if (p === '/sitemap.xml') {
      const u = (l, pr, img) => `<url><loc>${e(l)}</loc><lastmod>${day()}</lastmod><priority>${pr}</priority>${img ? `<image:image><image:loc>${e(img)}</image:loc></image:image>` : ''}</url>`;
      send(res, 200, '<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">' + u(SITE + '/', '1.0') + Object.keys(PAGES).map(k => u(SITE + k, '0.8')).join('') + prods().map(x => u(pUrl(x), '0.9', abs(x.image))).join('') + posts().map(x => u(bUrl(x), '0.6', abs(x.cover))).join('') + '</urlset>', 'application/xml; charset=utf-8', { 'Cache-Control': 'public, max-age=3600' }); return true;
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
      const body = page({ title, desc, url: SITE + key, boot: { route: 'page' }, ld, body: `<h1>${e(title.split(' | ')[0])}</h1><p>${e(desc)}</p>${key === '/shop' ? links() : ''}<a href="/">Home</a>` });
      return body ? html(200, body) : false;
    }
    return false;
  };
  handle.key = ixKey; handle.setting = setting; handle.save = keySet;
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
      const cur = SEO.urls(); let known = {}; try { known = JSON.parse(SEO.setting('seo_known') || '{}'); } catch {}
      const changed = Object.keys(cur).filter(u => known[u] !== cur[u]).concat(Object.keys(known).filter(u => !(u in cur))); if (!changed.length) return;
      const key = SEO.key(), r = await fetch('https://api.indexnow.org/indexnow', { method: 'POST', headers: { 'Content-Type': 'application/json; charset=utf-8' }, body: JSON.stringify({ host: new URL(SITE).host, key, keyLocation: `${SITE}/${key}.txt`, urlList: [...new Set([SITE + '/', SITE + '/shop', ...changed.slice(0, 9000)])] }), signal: AbortSignal.timeout(15000) });
      if (r.status === 200 || r.status === 202) { SEO.save('seo_known', JSON.stringify(cur)); console.log(`Search engines told about ${changed.length} new or changed page(s).`); } else console.error('IndexNow answered', r.status, '(will retry)');
    } catch (e) { console.error('IndexNow failed (will retry):', e.message); }
  };
  setTimeout(run, 30e3).unref(); setInterval(run, 120e3).unref();
})();

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
