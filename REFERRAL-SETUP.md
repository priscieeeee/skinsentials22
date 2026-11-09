# Skinsentials22 Referral Programme: setup, activation and rollback

The programme is **OFF by default**. Nothing changes for customers until an administrator completes the checklist in Admin > Referrals and presses **Activate**.

## What you are deploying: two files

| File | What changed |
|---|---|
| `server.js` | Your original server with the referral module merged in, plus small guarded hooks (checkout pricing, payment settled / marked unpaid, cancel, refund, Paystack chargeback webhook, backup) |
| `public/index.html` | Your original page with the referral interface merged in as `<script id="referral-layer-js">`, a Referral Programme policy and privacy wording in the policies layer, and small accessibility / SEO fixes |

No separate referral files, no new npm packages, no new environment variables. Built on your existing stack: Node 22.5+ built-in SQLite, Paystack, bank transfer / pay on delivery / DM, Resend email, the single admin password and the `settings` table.

**Also improved in the same pass:** a meta description and social-share tags, a skip-to-content link, accessible names for 7 icon-only buttons (close, menu, bag, mute toggles) and every unlabeled search/sort/admin field, a "Refer a friend" strip on the home page and a mobile-menu link (shown only while a programme is live), and focus moved to the page heading when the referral page opens.

## Deploy

1. **Back up first**: copy `data/store.db*`, your current `server.js` and `public/index.html`.
2. Replace `server.js` and `public/index.html` with the new files. Make sure `SITE_URL` in `.env` is `https://skinsentials22.com.ng`, because referral links are built from it.
3. Restart. Tables are created automatically (additive, safe to re-run). Two columns, `orders.discount` and `orders.credit_used`, are added with default 0.
4. Sign in to Admin > **Referrals**. State shows OFF; customers see no offer, no home strip, no menu link.

Optional (already used by the store): `RESEND_API_KEY` and `MAIL_FROM` for reward emails; `TRUST_PROXY=1` behind a proxy; `PAYSTACK_SECRET_KEY`.

## Activation checklist (Admin > Referrals)

Complete the worksheet fields, **Save as new version**, read the plain-language summary and checkout preview, then tick the three merchant confirmations and press Activate with a reason.

- Campaign name, headline, support contact, full terms
- Friend offer (fixed or percent, cap), minimum spend, exclusions, regions, online-only option
- Referrer reward (store credit), expiry, who may refer, per-person limit, budget cap
- Attribution rule (first or last link) and window, return buffer, "code required at checkout"
- Dates and timezone (a start date schedules it; blank starts on activation)
- Notification switches and wording
- Fraud-review owner: **named person in your team who checks Admin > Referrals > review queue (badge shows open flags)**
- **Tax / accounting treatment of store credit confirmed with your accountant** and **terms reviewed against Nigerian consumer law and your returns policy**. The system refuses activation without these confirmations.

Pause stops new referrals; orders already placed and rewards already earned are still honoured. End is final for new referrals.

## How it works

1. A signed-in eligible customer gets a code (`SK-` plus 8 unambiguous characters, about 40 bits, rate-limited) and link.
2. A visitor opens the link (code saved in the visitor's own browser storage for the configured window; first or last link wins per admin setting) or types the code at checkout. No account is needed.
3. Checkout calls the server, which validates the code and returns the discount, credit and final total before payment. The order route repeats the same calculation; browser-supplied amounts are ignored.
4. Order is `pending`. When payment is confirmed (Paystack, admin confirmation, any method) it becomes `qualifying` and a reward ledger row is created that becomes available after the return buffer. A background job (every minute) moves it to `qualified` only if the order is still paid and not cancelled.
5. Cancel, refund, "marked unpaid" or a Paystack `charge.dispute.create` event reverse the reward with an offsetting ledger row (never deleted). Store credit spent on a cancelled/refunded order is returned. A chargeback places the referral under review.

**Duplicate protection:** one referral per order (`UNIQUE order_id`), one friend per offer, ledger idempotency keys, state-guarded transitions, all inside SQLite transactions. Repeated webhooks do nothing the second time (tested).

**Referral states:** pending → review | qualifying | void; review → pending | qualifying | rejected | void; qualifying → qualified | reversed | review | rejected | pending | void; qualified → reversed | review | rejected; reversed / rejected → qualifying (reinstate); void is final. Customer, admin and ledger views all read the same status.

**Fraud signals (flag for human review, never silent rejection):** same phone as referrer, same delivery address, high volume in 24 h, chargeback. Same account or same email (ignoring dots and `+tags`) is blocked with the same generic message as an unknown code, so codes and identities cannot be probed. Admins approve / reject / reverse / reinstate with a mandatory reason, recorded in the audit log and the referral's event history. Customers can contact the support address shown on the page to appeal.

**Privacy:** referrers see status and amount only. Friend identity is stored as a keyed hash (HMAC) of the email, only to enforce one-use-per-friend. Visits are daily per-code counts with no IP or device data, deleted after 90 days. Exports contain no names, emails or order contents, and every export is logged.

## Reports (definitions)

Sales = qualifying subtotal of paid referred orders (excluding refunded). Discounts = friend offers on those orders. Rewards (net) = reward ledger rows minus reversals. **Net contribution = sales − discounts − net rewards. This is not profit**: product cost, Paystack fees, delivery and tax are not included.

## Permissions

The store has one admin role, so every `/api/admin/referral/*` route requires the existing server-side admin sign-in (customer tokens are rejected, tested). Per-permission roles do not exist in the store and were not invented; the audit actor is recorded as `admin`. If you add staff accounts later, add role checks at the `A` constant in `referral.js`.

## Rollback

1. Admin > Referrals > **End** (or Pause) with a reason. No new referrals; customers see no offer.
2. To remove the code: restore the backed-up `server.js` and `public/index.html`, restart. The `ref_*` tables and two order columns can stay unused (older code ignores them; defaults are 0). Orders already placed with an offer stay valid and priced as paid. Unspent store credit stays in `ref_ledger` for you to honour or settle manually.
3. Restore `store.db` only if you also accept losing orders taken since the backup.

## Assumptions, gaps and manual steps

- **Reward type:** store credit only (usable on products at checkout, not delivery or gift cards). The store has no points system or payout method, so none was added.
- **Stacking:** the store has no sale prices, coupon engine or loyalty scheme, so the only stackable items are the friend offer and store credit (admin switch). Free-delivery threshold is judged on the pre-discount subtotal.
- **Channels:** email via your existing Resend setup only (transactional, to the account holder): reward available, reward reversed, reward on hold, and a once-only reminder 7 days before credit expires. No SMS/WhatsApp sender exists in the store. Nothing is sent to invited friends and no address books are touched. Not built: pause/end announcement emails.
- **Not built:** repeated-payment-method and device-pattern fraud signals (deliberately no fingerprinting); partial refunds (the store only supports full refunds); per-credit FIFO expiry (balance = unexpired rewards minus spend, floored at zero).
- **Chargebacks:** wired to Paystack's `charge.dispute.create` event. Confirm the event is enabled and named so in your Paystack dashboard (test mode), and resolve the dispute outcome manually with reject / reinstate.
- **Abandoned online payment:** a friend's one-time offer stays attached to their unpaid online order until the store's existing 24-hour auto-cancel frees it. The friend is told this in plain words at checkout. This is deliberate: freeing it earlier would let someone get the discount on two orders.
- **Disabled codes:** if you disable a customer's code, they cannot create a new one; only you can issue one ("New code") after review.
- **Retention:** only visit counts are purged automatically. Decide how long to keep referral, order and ledger records (accounting and dispute needs) and delete or anonymise afterwards. Data-subject requests (access, correction, erasure) are handled manually through your support address; the store has no self-service deletion.
- **Policies:** the referral page generates key terms from your settings (so they always match behaviour) and carries a privacy block. Add a matching "Referral programme" paragraph to the Privacy and Terms pages in the policies layer (purposes, lawful basis you rely on under the NDPA, processors Paystack / email provider / host, retention, rights). This needs your or your lawyer's confirmation; it was not written as legal advice.
- **Before launch, merchant must:** confirm local terms and tax/accounting treatment, review the checkout example and report definitions above, name the fraud-review owner, and test one real order end to end in Paystack test mode.

## Tests run

Server flow tested with real HTTP against a temporary database: disabled by default; activation gate; server-side discount (tampered client discount ignored); self-referral and unknown code give identical replies; second use by the same friend blocked; reward issued once despite repeated payment signals; referrer response contains no friend details; credit redeemed then restored on cancel; refund reverses reward; fraud flag holds reward until approved; double-approve refused; reverse then reinstate nets to one reward; ledger and event history preserved; export logged; admin API rejects anonymous and customer tokens; pause stops new referrals. A real-browser run (headless Chromium, desktop and mobile size) drove the whole journey through the interface: admin signs in, saves settings and activates; a friend arrives from a link, sees the saved code and the discounted total at checkout, places an order, and attribution clears; zero page errors. Also verified: the expiry reminder fires once and does not change balances; the referral policy page opens; accessibility fixes apply. Not verifiable here: visual layout (the sandbox cannot load the Tailwind stylesheet), real Paystack payments and real email delivery, so please test one order in Paystack test mode before launch.
