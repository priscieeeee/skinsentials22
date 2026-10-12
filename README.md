# Skinsentials22 store

One Node server serves your website **and** the API. No `npm install` needed (zero dependencies). Needs **Node 22.5+**.

## Files
```
server.js
products.seed.json
package.json
.env.example     (copy to .env)
public/index.html   <- the website goes in the public folder
```

## Run it
```
cp .env.example .env     # then edit: ADMIN_PASSWORD, PAYSTACK_SECRET_KEY, SITE_URL
npm start                # http://localhost:3000
```
Admin: open `/#admin`, sign in with `ADMIN_PASSWORD`.

## First-time setup (5 minutes)
1. **Admin → Settings → Payment methods** has everything in one place:
   - **Paystack key**: paste your *secret* key (`sk_test_...` to try, `sk_live_...` to go live) and press **Save key**, then **Test connection**. One key switches on **Card, Pay with bank, USSD and Instant bank transfer**. (You can still put it in `.env` as `PAYSTACK_SECRET_KEY` instead; a key in `.env` wins.) In the Paystack dashboard (Settings → API Keys & Webhooks) set the **Webhook URL** shown on that page.
   - **Bank accounts**: your main account plus up to 3 more (OPay, Moniepoint, Kuda...). Customers see all of them for *Direct bank transfer*.
   - **Method switches**: tick or untick any method. *Pay on delivery* and *DM* work without any setup.
   - **WhatsApp number and Instagram handle** (used by *DM* orders).
3. Your products were imported from the old page automatically. From now on edit them in **Admin → Products** (saved to the database, same for every visitor).

## What the backend does
| Feature | How |
|---|---|
| Accounts | Register / sign in. Passwords hashed with scrypt, 30-day signed tokens, rate-limited. |
| Profile | Account icon → **Overview / Profile / Orders / Password**. Edit name, phone, delivery address and the newsletter choice; checkout fills itself in from the profile; order history shows items, payment method, status and a "Check payment status" button; customers can change their own password. |
| Favourites (hearts) | Guests: saved on the device. Signed in: synced to the account, merged on sign-in. |
| Newsletter | Footer box + sign-up checkbox. Admin → Customers & Subscribers → Download CSV. |
| Pay online (card, pay with bank, USSD, instant transfer) | Server starts the Paystack transaction limited to the channel the customer picked, customer pays on Paystack, server **verifies the amount** (and the signed webhook confirms even if the customer closes the tab). |
| Direct bank transfer | Customer sees your account(s) + a unique reference, taps "I have made the transfer", you tick **Mark paid** in admin. |
| Pay on delivery | Order is saved, you confirm by phone, tick **Mark paid** when the rider collects. |
| DM | Order is saved first, then opens a pre-written WhatsApp / Instagram message with the order reference. |
| Prices | Always read from the database. Browser-sent prices are ignored (gift cards are range-checked). |
| Admin | Orders (status, mark paid), products, customers, subscribers, password reset for customers (generates a temporary password). |
| Departments and skin types | On a product, **type** any department or skin type, or pick a suggestion. New departments appear in the shop menu automatically; new skin types appear as filters in the shop. |
| Brands and logos | Type a brand on a product and upload its logo (or manage logos under **Admin → Products → Brands and logos**). Logos show on the Brands page, the brand page and product cards. |
| How to use | On a product (**Admin → Products**) write the steps one per line, when to use it, a pro tip and a caution. The product page shows numbered steps. Products with nothing written yet show general guidance for their type (cleanser, serum, SPF...) until you add your own. When signed in as admin you can also edit it straight on the product page with **Edit how to use**. |
| Product videos | Upload as many videos per product as you like. They play (and can be skipped through) on the product page and in Video Discovery. Product and brand images are also saved as files now. |

## Deploying
Any host that runs Node 22.5+ **and keeps a persistent disk** (the SQLite file lives in `data/`): a VPS, Render, Railway, Fly.io. Set the `.env` values as environment variables, mount the disk and set `DATA_DIR` to it, and put HTTPS in front (these hosts do it for you). Standard shared cPanel hosting is usually too old and won't work. **Back up `data/store.db` and the `data/uploads/` folder** regularly (uploads hold your videos and logos).

## Video uploads: what "unlimited" means
The site sets no limit on video size or how many videos you add; files stream straight to disk (`data/uploads/`), so large videos do not use up memory. Real limits come from outside the site: free disk space on your host, and any upload cap your host or proxy applies (for example Cloudflare's free plan caps a request at 100 MB). To cap uploads yourself, set `MAX_UPLOAD_MB` in `.env`. Use **MP4 (H.264)** for videos that play everywhere; iPhone `.mov` files may not play in Chrome or Firefox.

## Honest limitations
- No emails are sent yet (order confirmations, password reset, newsletter blasts). The subscriber list exports to CSV for Mailchimp, Brevo etc. Adding an email provider is the natural next step.
- Blog posts, hero slides and the announcement bar are still saved in the admin's browser, not the database.
- Order stock levels are not tracked.


---

## Delivery fees (new)

| Where | Fee | Time |
|---|---|---|
| Within Lagos | ₦4,000 | 1-3 working days |
| Outside Lagos / other states | ₦6,500 | 3-7 working days |
| International | not charged online: the customer is sent to WhatsApp for a shipping rate | n/a |

Orders are processed within 24 hours.

- At checkout the customer picks their **state**. If they type an address first (for example "Lekki" or "Port Harcourt") the state is **filled in automatically**, and the fee and the estimated delivery dates update live.
- The fee is checked **again on the server**, so it cannot be changed from the browser. If someone selects Lagos but the address is clearly elsewhere, the order is refused with a clear message.
- International orders cannot be paid online. The checkout shows your message and a WhatsApp button with the bag already written out.
- The delivery information also appears on every product page, in the bag, in the footer, in the order confirmation and in the order emails.
- **Admin → Delivery**: change both fees, the delivery days, the processing time, an optional "free delivery over ₦X", and the international message. It also has a **fee calculator**, a **Ready to dispatch** queue (paid and pay-on-delivery orders, oldest first, with an overdue warning), an **On the way** list, **Mark shipped / Mark delivered**, and a printable **dispatch slip** for every order.

## Payments dashboard (new)

**Admin → Payments** records every payment from every method (card, pay with bank, USSD, instant transfer, direct transfer, pay on delivery, DM):

- Totals received (today / 7 days / 30 days / all time), money waiting, refunds, a 14-day chart and a breakdown by method.
- Live alerts while the dashboard is open: a sound, a pop-up, a red badge and the page title count for **new orders, payments received, transfers to confirm and amounts to check**. You can also turn on desktop notifications.
- **Confirm** a bank transfer, pay-on-delivery or DM payment (with the amount you actually received), mark it **Not received**, **Re-check** an online payment with Paystack, or **Refund** (online refunds go through Paystack; the order is cancelled and stock released).
- Full history for each payment, filters, search, and **Export CSV**.
- Money from Paystack payments is paid out by Paystack to the settlement account in **your Paystack dashboard** (Settings → Preferences). The Payments page shows your Paystack balance. Transfers go straight to your own bank accounts.
- Safety nets: a signed webhook, a check every 5 minutes for customers who paid and closed the tab, wrong-amount payments flagged for you, and unpaid online checkouts cancelled after 24 hours with their stock released.

### Phone alerts (optional, in `.env`)

| Want | Add to `.env` |
|---|---|
| Telegram message | `TELEGRAM_BOT_TOKEN` (from @BotFather) and `TELEGRAM_CHAT_ID` |
| Email to you + receipts and password-reset emails to customers | `RESEND_API_KEY`, `MAIL_FROM` (a sender on your Resend domain), `ADMIN_EMAIL` |
| Slack / Discord / any webhook | `NOTIFY_WEBHOOK_URL` |

Use **Admin → Settings → Send a test alert** to check. Without any of these, everything still works and you see alerts live in the dashboard.

## Other improvements and fixes

- Hero images, **blog posts and the announcement bar are now saved on the server**, so customers see what you publish (before, they only existed in your own browser). Uploaded images go to the server.
- The shopping bag **survives a page refresh**, has + / - quantity buttons, and is re-checked against the live catalogue.
- Optional **stock** per product (Admin → Products): sold-out items can't be bought, and cancelled or abandoned orders return their stock. Blank means unlimited.
- **Forgot password** by email link (needs `RESEND_API_KEY`); otherwise customers are pointed to WhatsApp. Changing a password signs older logins out.
- **Admin password can be changed in Settings** (it used to do nothing). `RESET_ADMIN_PASSWORD=1` in `.env` restores the `.env` password if you ever lose it.
- Catalogue saves are now protected: if the admin page has stale data it is refused instead of overwriting the real catalogue.
- Fixed: a new account could see another person's guest orders if it used the same email; admin revenue counted unpaid orders; order data was kept in the admin's browser; the login rate limit trusted a forgeable header; the webhook signature was not compared safely; spreadsheet formula injection in CSV exports; missing escaping of product names; broken images now show a placeholder.
- **Admin → Settings → Download full backup** exports products, orders, payments, customers and settings (no passwords or keys).

## Featured Guide (homepage video box)
- It is a real slider: plays one video after another, slides to the next when a video ends, with arrows, dots, swipe, pause and sound buttons.
- **Automatic by default:** it plays every video uploaded to your products (Admin → Products → edit → Upload product videos).
- **Admin → Featured Guide** lets you add your own guides (title, label, linked product, video, cover), edit, reorder, delete, change how long picture slides stay, or switch the box off. Saved on the server for every visitor.
- If there is nothing to show (no videos and no guides), or you switch it off, the box is hidden and the headline uses the full width.
