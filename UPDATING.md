# Updating Skinsentials22 without losing anything

Your products, blog posts, customers, orders, rewards, settings and uploaded pictures live in the **data folder** (`data/` by default, or wherever `DATA_DIR` points).
Updating `server.js` or `public/index.html` never changes that data. Data is only lost when a host or an upload replaces or deletes the data folder, or when the app runs on a disk that is reset on every deploy.
z
## Every update, in this order
1. Admin > Settings > Backups: click **Make a backup**, then **Download** it to your computer.
2. Replace ONLY these files: `server.js` and `public/index.html` (plus any other file in `public/` that I changed).
3. Do NOT upload, replace, rename or delete: the `data/` folder, the `.env` file.
4. Restart the app.
5. Check the start-up log. You should see `Safety copy: N products are in the store.` If you see `DATA RECOVERY:` lines, the database was missing and was restored automatically from the copy kept outside the app folder. 

## The two copies that now protect you
- Inside the data folder: `data/backups` (every 6 hours, and before updates).
- Outside the app folder: `BACKUP_DIR` (default: a folder called `skinsentials22-backups` next to the app folder). It holds the newest 12 database copies (every 3 hours, and when the app stops for a redeploy), every uploaded picture, and the login secret.
If the database is missing at start-up, the newest copy is restored automatically, with the pictures and the login secret, so customers stay signed in.

## If you host the app online (Render, Railway, a VPS...)
- Use a persistent disk or volume, and point both folders at it in your environment settings, for example:
  `DATA_DIR=/var/data/skinsentials22`  and  `BACKUP_DIR=/var/data/skinsentials22-backups`
- Without a persistent disk, a host that rebuilds the app wipes everything inside it on each push. Both copies must be on the disk that survives.
- Still download a backup to your computer regularly. A copy on the same disk does not help if that disk is lost.

## If you use git / GitHub pushes
- Never commit `data/`, `.env` or any `.db` file (use the `.gitignore` provided). A committed database overwrites your live one on every push.

## Keep a private copy of `.env`
It holds your admin password and payment keys. For safety it is NOT included in the backups.
