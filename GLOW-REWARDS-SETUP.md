# Skinsentials22 Glow Rewards: setup

Two files changed. Nothing else, no new packages, no new environment variables.

| File | What changed |
|---|---|
| `server.js` | New Glow module (points ledger, reward claims, Glow Friend codes, admin routes) + 6 small hooks in the order route, order output, order emails and backup |
| `public/index.html` | New `<script id="glow-layer-js">` at the end: rewards page, account tab, checkout code box, admin screen, home strip, menu link |

## Deploy
1. Back up `data/store.db*`, `server.js`, `public/index.html`.
2. Replace `server.js` and `public/index.html`, restart. Tables are created automatically (additive). One column, `orders.glow_disc`, is added.
3. Open the site: the page is at `/#glow` (also in the mobile menu, footer and a strip on the home page).
4. **Admin > Glow Rewards > Settings**: check the numbers below, then Save.

## How it behaves
- **Earning:** a signed-in customer's order earns points when it is **paid and marked Delivered** in Admin > Orders. Cancelled or unpaid orders take the points back; reinstated ones give them back. Gift cards earn nothing.
- **How many points:** by default per category (cleanser 10, toner 15, serum 15, moisturizer 20, sunscreen 20, mask 10, bundle 50) per unit, matched on words in the product name (editable). Anything no category matches earns 1 point per ₦400. Switch to "by total spend only" in Settings if you prefer.
- **Expiry:** each batch of points expires 12 months after it is earned. Spending uses the oldest points first.
- **Rewards:** customer taps **Claim**, points are held immediately, you get an alert, they get a WhatsApp link. In **Reward claims** you tap *Mark delivered* or *Decline* (points returned).
- **Glow Friend:** every account gets `GLOW-[FIRSTNAME]1000` (a number is added if two people share a first name, e.g. `GLOW-AMAKA2-1000`). A friend enters it at checkout (or opens the shared link, which saves it for 30 days). ₦1,000 off a first order, checked on the server. The referrer gets 30 points when the friend's order is paid and Delivered. Own code, same email (ignoring dots and +tags), same phone, repeat use and non-first orders are refused.
- A Glow Friend code replaces the older referral-programme code on that order (one friend offer per order). The older programme is untouched.

## Decisions for you (these were open in your brief)
1. **Points basis:** category values (default) vs total spend. Both work; change in Settings.
2. **"Free delivery":** the Glow Starter reward is recorded and confirmed by you on WhatsApp; it is **not** applied automatically at checkout. Decide whether it covers Lagos only or all states, and apply it by hand or tell me and I will automate it.
3. **Vouchers / ₦5,000 off:** same: confirmed by you, applied by you. Nothing in checkout redeems them yet.
4. **Early access and birthday gift:** no birthday is stored on accounts, so you action these manually.
5. **Guest orders earn nothing.** Points are tied to a signed-in account (matching by email alone would let someone claim another person's points). The checkout tells guests to sign in; you can credit a past guest order with Members > Adjust points.
6. **Terms wording** on the page is standard text built from your settings. Have it checked before launch, like the referral terms.

## Tests run
Real HTTP against a temporary database (29 checks): points by category with gift cards excluded; pending vs credited; no double-earn; claim, insufficient points, reject returns points; Glow Friend discount computed on the server (tampered price ignored); own-code, +tag email, unknown code and repeat use refused; bad code refused instead of silently charging full price; bonus after delivery; cancel reverses bonus and earned points; admin routes reject customers; settings save. A headless Chromium run drove the link, checkout box, member dashboard and all admin screens with no script errors. Not verifiable here: visual layout (the sandbox cannot load Tailwind) and real Paystack/WhatsApp. Please place one test order and one friend order before launch.

## Homepage and SEO update
- The four "Shop by Skin Concern" tiles are removed. The homepage product grid now shows **every product in your shop** (it reads the same live list as the shop and updates when you add, edit or delete). Nothing in the update writes to products.
- **SEO was not working because `seo-server.js` needs Express, and your store runs on plain Node, so it was never connected.** The same logic is now built into `server.js`: `/sitemap.xml`, `/robots.txt`, a real page for every product (`/p/ID/name`), blog post (`/blog/ID/title`) and main page (`/shop`, `/glow-rewards` ...), each with its own title, description, canonical link, WhatsApp/Facebook preview and Product price data (naira, in stock or not). `seo-server.js` is no longer needed.
- **After deploying (this is what brings customers):** 1) set `SITE_URL=https://skinsentials22.com.ng` in your environment; 2) open https://skinsentials22.com.ng/sitemap.xml to confirm it lists your products; 3) add the site to Google Search Console, submit `sitemap.xml`, and use "Request indexing" on the homepage and shop; 4) create a free Google Business Profile and share product links (`/p/...`) on WhatsApp and Instagram.

## Per-product points (Admin > Glow Rewards > Product points)
- Every product shows its **Auto** points (from its category). Type a number in **Your points** to give that product its own points per unit; empty = Auto.
- **Each:** type in a row. **Group:** group by department, skin type, brand or points category, then use *Set group* on a group header. **All:** tick *Select all shown*, type points, *Set for selected*. *Back to automatic* clears your numbers.
- Saved inside Glow Rewards settings only. Products are never modified. Settings saves keep your product numbers.

## Guest orders, emails, account line, and protecting existing data
- **Earlier guest orders:** a signed-in customer opens Account > Glow Points > "Ordered before without an account?" and enters the **order reference** (SK22-..., from the confirmation or email) plus the **email or phone used on that order**. Both must match. The order then earns points like any other (if already delivered, they are credited straight away). Order references cannot be guessed, wrong answers all get the same message, an order can belong to only one account, and attempts are rate-limited. Email alone is never enough.
- **Emails and confirmation:** order emails and the on-screen confirmation now say how many Glow Points the order will earn (signed in), or how to claim them with the order reference (guest).
- **Account:** the account dialog starts with a line showing the Glow Points balance, points to the next reward, and points on the way. Tap it to open the Glow Points tab.
- **Your existing products, posts, hero slides and orders are protected:** Glow and SEO only read them. Product points, codes, claims and links live in their own tables and settings. In addition, the server now saves a full copy of the database in `data/backups/` at startup and every 6 hours (14 newest kept). **When you update, replace only `server.js` and `public/index.html`. Never replace or delete the `data/` folder** (it holds products, posts and uploads).
- To restore an earlier copy: stop the server, copy a file from `data/backups/` over `data/store.db`, delete `store.db-wal` and `store.db-shm`, start again.

## Starter products removed, and everything you post reaches search engines
- The four built-in demo products (Barrier Gel, Vitamin C, Cleansing Balm, BHA Solution) are **no longer written into the page**. The homepage and shop show only what is in **Admin > Products**.
- The server used to add those four again whenever the product list was empty. It now adds them **only once, on a brand-new store**, so deleting products never brings them back.
- **If the four still show after you deploy, they are saved in your shop: delete them once in Admin > Products.** (Nothing is deleted for you.)
- **Automatic SEO for every post:** a product or blog post you save gets its own page and a sitemap entry **instantly**. Within about 2 minutes the server also tells Bing, Yandex and other IndexNow search engines about every new, changed or deleted page. Google picks it up from the sitemap: submit `https://skinsentials22.com.ng/sitemap.xml` once in Google Search Console and it rechecks on its own. This needs `SITE_URL` set to your real address (it is skipped on localhost). Set `INDEXNOW=0` to switch the notifications off.

## Download database backup button
**Admin > Settings > Backup > Download database backup** saves a complete, restorable copy of the store database (products, posts, orders, payments, customers, Glow points, settings) to your computer. Only a signed-in admin can use it. It contains customer details, so keep the file private.
- It does **not** include uploaded videos and images (`data/uploads/`); copy that folder separately.
- **To restore:** stop the server, copy the downloaded file over `data/store.db`, delete `store.db-wal` and `store.db-shm`, put your `uploads/` folder back, start the server.
- The older **Download full backup** button (a readable export, no passwords or keys) is still there.
