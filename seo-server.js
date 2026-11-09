/* Skinsentials22 — SEO routes for Express.
   Gives every product, blog post and main page its own crawlable URL, with real <title>, description,
   canonical, Open Graph (WhatsApp / Facebook / X previews) and Product JSON-LD in the HTML, plus /sitemap.xml and /robots.txt.

   In server.js (before your catch-all / static handler):
     const seo = require('./seo-server');
     app.use(seo({
       indexPath: require('path').join(__dirname, 'public', 'index.html'),
       getProducts: async () => <array of products, same shape as products.seed.json / GET /api/products>,
       getPosts:    async () => <array of published posts {id,title,body,cover,date}> (optional, can return [])
     }));
*/
const fs = require('fs');
const SITE = process.env.SITE_URL || 'https://skinsentials22.com.ng';
const BRAND = 'Skinsentials22';
const PAGES = {'/shop': ['Shop Skincare in Nigeria — Serums, Moisturisers & Cleansers | Skinsentials22', 'Browse all skincare essentials for every skin type. Authentic products, prices in naira, delivery across Nigeria.'],
  '/videos': ['Skincare Video Guides — Watch & Shop | Skinsentials22', 'Watch expert skincare video guides and shop the products featured.'],
  '/skin-quiz': ['Skin Finder Quiz — Find Your Routine | Skinsentials22', 'Answer a few questions and get a personalised skincare routine.'],
  '/skin-consultation': ['Free Skin Consultation in Nigeria | Skinsentials22', 'Talk to our skincare team and get routine advice that fits your skin and budget.'],
  '/ingredients': ['Shop Skincare by Ingredient | Skinsentials22', 'Find products by the ingredient your skin needs.'],
  '/gift-cards': ['Skincare Gift Cards | Skinsentials22', 'Send a Skinsentials22 gift card with a personal message.'],
  '/brands': ['Skincare Brands We Stock | Skinsentials22', 'Explore the skincare and beauty brands available at Skinsentials22.'],
  '/journal': ['Skincare Journal — Routines, Ingredients & Tips | Skinsentials22', 'Practical skincare advice and ingredient guides from the Skinsentials22 team.']};
const esc = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const slug = s => String(s || '').toLowerCase().replace(/&/g, ' and ').replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 80) || 'item';
const trim = (s, n) => { s = String(s || '').replace(/\s+/g, ' ').trim(); return s.length > n ? s.slice(0, n - 1).replace(/\s\S*$/, '') + '…' : s; };
const abs = u => !u ? '' : /^https?:/i.test(u) ? u : /^data:/.test(u) ? '' : SITE + (u[0] === '/' ? '' : '/') + u;
const json = o => JSON.stringify(o).replace(/</g, '\\u003c');

module.exports = function (opt) {
  const router = require('express').Router();
  const getP = async () => { try { return (await opt.getProducts()) || []; } catch (e) { return []; } };
  const getB = async () => { try { return opt.getPosts ? (await opt.getPosts()) || [] : []; } catch (e) { return []; } };
  const send = (res, m) => {
    let h = fs.readFileSync(opt.indexPath, 'utf8');
    const strip = re => { h = h.replace(re, ''); };
    strip(/<title>[\s\S]*?<\/title>/); strip(/<meta name="description"[^>]*>/); strip(/<link rel="canonical"[^>]*>/);
    strip(/<meta property="og:(title|description|url|type|image)"[^>]*>/g); strip(/<meta name="twitter:(title|description|image)"[^>]*>/g);
    const tags = `<title>${esc(m.title)}</title><meta name="description" content="${esc(m.desc)}"><link rel="canonical" id="sk-canon" href="${esc(m.url)}">` +
      `<meta property="og:title" content="${esc(m.title)}"><meta property="og:description" content="${esc(m.desc)}"><meta property="og:url" content="${esc(m.url)}"><meta property="og:type" content="${m.type || 'website'}">` +
      (m.image ? `<meta property="og:image" content="${esc(m.image)}"><meta name="twitter:image" content="${esc(m.image)}">` : '') +
      `<meta name="twitter:title" content="${esc(m.title)}"><meta name="twitter:description" content="${esc(m.desc)}">` +
      (m.ld ? `<script type="application/ld+json">${json(m.ld)}</script>` : '') +
      `<script>window.__SK_SEO__=${json(m.boot || {route: 'page'})};</script>`;
    h = h.replace('</head>', tags + '</head>');
    if (m.body) h = h.replace(/(<body[^>]*>)/, '$1<noscript>' + m.body + '</noscript>');
    res.status(m.status || 200).type('html').send(h);
  };

  router.get('/robots.txt', (req, res) => res.type('text').send(`User-agent: *\nAllow: /\nDisallow: /api/\nDisallow: /admin\n\nSitemap: ${SITE}/sitemap.xml\n`));

  router.get('/sitemap.xml', async (req, res) => {
    const [P, B] = await Promise.all([getP(), getB()]), day = new Date().toISOString().slice(0, 10);
    const u = (loc, pr) => `<url><loc>${esc(loc)}</loc><lastmod>${day}</lastmod><priority>${pr}</priority></url>`;
    res.type('application/xml').send('<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' +
      u(SITE + '/', '1.0') + Object.keys(PAGES).map(p => u(SITE + p, '0.8')).join('') +
      P.map(p => u(`${SITE}/p/${encodeURIComponent(p.id)}/${slug(p.name)}`, '0.9')).join('') +
      B.filter(p => p.pub !== false).map(p => u(`${SITE}/blog/${encodeURIComponent(p.id)}/${slug(p.title)}`, '0.6')).join('') + '</urlset>');
  });

  const productRoute = async (req, res, next) => {
    const P = await getP(), p = P.find(x => String(x.id) === req.params.id);
    if (!p) return next();
    const url = `${SITE}/p/${encodeURIComponent(p.id)}/${slug(p.name)}`;
    if (req.params.slug !== slug(p.name)) return res.redirect(301, url.replace(SITE, ''));
    const price = Math.round(+p.price || 0), img = abs(p.image);
    send(res, {title: `${p.name} — Buy in Nigeria | ${BRAND}`, desc: `${trim(p.description, 150)} ₦${price.toLocaleString('en-NG')}.`, url, type: 'product', image: img, boot: {route: 'product', id: String(p.id)},
      ld: {'@context': 'https://schema.org', '@graph': [{'@type': 'Product', name: p.name, description: trim(p.description, 300), sku: String(p.id), category: p.category, image: img ? [img] : undefined, brand: {'@type': 'Brand', name: p.brand || BRAND}, url,
        offers: {'@type': 'Offer', url, priceCurrency: 'NGN', price: String(price), itemCondition: 'https://schema.org/NewCondition', availability: (typeof p.stock === 'number' && p.stock <= 0) ? 'https://schema.org/OutOfStock' : 'https://schema.org/InStock'}},
        {'@type': 'BreadcrumbList', itemListElement: [['Home', SITE + '/'], ['Shop', SITE + '/shop'], [p.name, url]].map((c, i) => ({'@type': 'ListItem', position: i + 1, name: c[0], item: c[1]}))}]},
      body: `<article><h1>${esc(p.name)}</h1><p>${esc(p.description)}</p><p>₦${price.toLocaleString('en-NG')}</p><p>${esc(p.ingredients || '')}</p><a href="/shop">Shop all skincare</a></article>`});
  };
  router.get('/p/:id', productRoute); router.get('/p/:id/:slug', productRoute);

  const postRoute = async (req, res, next) => {
    const B = await getB(), p = B.find(x => String(x.id) === req.params.id && x.pub !== false);
    if (!p) return next();
    const url = `${SITE}/blog/${encodeURIComponent(p.id)}/${slug(p.title)}`;
    if (req.params.slug !== slug(p.title)) return res.redirect(301, url.replace(SITE, ''));
    send(res, {title: `${p.title} | ${BRAND} Journal`, desc: trim(String(p.body || '').replace(/^## /gm, ''), 155), url, type: 'article', image: abs(p.cover), boot: {route: 'post', id: String(p.id)},
      ld: {'@context': 'https://schema.org', '@type': 'BlogPosting', headline: p.title, image: abs(p.cover) || undefined, datePublished: p.date ? new Date(p.date).toISOString() : undefined, author: {'@type': 'Organization', name: BRAND}, mainEntityOfPage: url},
      body: `<article><h1>${esc(p.title)}</h1><p>${esc(trim(p.body, 1500))}</p></article>`});
  };
  router.get('/blog/:id', postRoute); router.get('/blog/:id/:slug', postRoute);

  Object.keys(PAGES).forEach(path => router.get(path, (req, res) => send(res, {title: PAGES[path][0], desc: PAGES[path][1], url: SITE + path, boot: {route: 'page'}})));
  return router;
};
