# Deploying Pravaha for free

**Read this first — the honest version.** Pravaha is ~14 containers (Kafka + Postgres
+ Redis + API + 10 workers + Next.js), needs **~4 GB RAM**, and Kafka is stateful. It
**does not fit** any of the "free web service" tiers (Render / Fly / Railway / Vercel
functions) as a whole. What *is* free:

| Path | Cost | Always‑on? | Card needed? | Best for |
| --- | --- | --- | --- | --- |
| **A. GitHub Codespaces** | free tier: 120 core-h/mo | no — stops when idle | no | a real "click → the whole thing runs" demo |
| **B. Oracle Cloud Always Free (Ampere A1)** | free forever | yes | yes (ID verify only, never charged) | a public URL that stays up |
| **C. Vercel — dashboard only** | free | yes (frontend) | no | showing the UI; needs a backend elsewhere to have data |

---

## A. GitHub Codespaces (no card, whole stack, ~5 min)

The repo ships a `.devcontainer/`, so a Codespace boots with Docker + Python + Node
ready.

1. On the repo page: **Code ▸ Codespaces ▸ Create codespace on main**.
   Pick a **4‑core / 8 GB** machine (the smaller 2‑core / 4 GB is tight but works if
   you skip the `seed` generator).
2. When the terminal is ready:
   ```bash
   docker compose up -d --build
   docker compose run --rm migrate
   docker compose --profile demo up -d seed          # optional demo traffic
   ```
3. The **Ports** tab shows `3000` (dashboard) and `8000` (API). Click the globe
   icon on `3000` to open it. To share a link, set that port's visibility to
   **Public** (right‑click ▸ Port Visibility).
4. Login `admin@pravaha.local` / `admin12345`. Run the marquee demo:
   ```bash
   docker compose run --rm --no-deps seed \
     python -m pravaha.scripts.scenarios run payment_failure_spike --api http://api:8000 --bootstrap
   ```

Caveats: the Codespace **stops after 30 min idle** (data persists on its volume;
just restart it). GitHub Free personal accounts include **120 core‑hours + 15 GB‑month**
of Codespaces per month — a 4‑core machine spends that in ~30 h of uptime, a 2‑core in
~60 h, so start/stop it around demos. It is not meant to be a 24/7 host (use path B).

---

## B. Oracle Cloud — Always Free VM (public, 24/7)

Oracle's Always Free tier includes an **Ampere A1 (arm64)** allowance of up to
**4 OCPU / 24 GB RAM** at no cost, indefinitely. A credit card is required **only for
identity verification** — Always Free resources are never billed. (If you're not
willing to add a card, use path A or C.)

### 1. Create the VM
* Oracle Cloud console ▸ **Compute ▸ Instances ▸ Create**.
* Image **Ubuntu 22.04**, shape **VM.Standard.A1.Flex**, **2 OCPU / 12 GB** (plenty;
  stays in Always Free). Add your SSH public key. Create.
* **Networking ▸ Virtual Cloud Network ▸ Security List**: add ingress rules for TCP
  **80** and **443** from `0.0.0.0/0`.
* On the VM also open the host firewall:
  ```bash
  sudo iptables -I INPUT -p tcp --dport 80  -j ACCEPT
  sudo iptables -I INPUT -p tcp --dport 443 -j ACCEPT
  sudo netfilter-persistent save
  ```

### 2. Point a name at it
No domain? Use a free one:
* **DuckDNS**: create `something.duckdns.org` → set it to the VM's public IP.
* or **nip.io**: `<public-ip-with-dashes>.nip.io` resolves automatically
  (e.g. `140-238-1-2.nip.io`).

### 3. Deploy
```bash
ssh ubuntu@<vm-ip>
git clone https://github.com/HxrshRaj/Pravaha && cd Pravaha
cp deploy/.env.prod.example .env
nano .env       # set PRAVAHA_DOMAIN, PRAVAHA_ACME_EMAIL, and 3 secrets
sudo bash deploy/deploy.sh
```
`deploy.sh` installs Docker, pulls the prebuilt `ghcr.io/<owner>/pravaha-*` images
(published by `.github/workflows/images.yml`), runs migrations + topic creation, and
starts everything behind **Caddy**, which fetches a Let's Encrypt certificate for
`PRAVAHA_DOMAIN` automatically.

Open `https://<PRAVAHA_DOMAIN>`. Generate traffic:
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
  --profile demo up -d seed
```

> **Make the images pullable:** after the `images` workflow runs once, go to your
> GitHub profile ▸ **Packages** ▸ `pravaha-app` / `pravaha-web` ▸ *Package settings*
> ▸ **Change visibility → Public**. Otherwise the VM can't pull them (or run
> `docker compose ... build` on the VM instead — slower, needs the RAM).

### Notes for arm64
The base images (`postgres`, `redis`, `apache/kafka`, `caddy`, `python:3.11-slim`,
`node:22-slim`) are all multi‑arch, so the same compose works on the A1 VM. If you
build on the VM instead of pulling, that's fine too.

---

## C. Vercel — dashboard only (free, no backend data)

```bash
npm i -g vercel
cd apps/web
vercel            # root: apps/web ; framework: Next.js ; build: next build
```
Set env var `NEXT_PUBLIC_API_BASE_URL` to wherever your API is reachable (a
Codespace public URL, or an Oracle deploy). Without a backend the pages render but
every call 404s — this path only makes sense paired with A or B.

---

## What you cannot do for free (and why)

* **All‑in‑one PaaS free tiers** (Render/Railway/Fly free): 256–512 MB per service
  and no managed Kafka. Pravaha needs ~4 GB and a stateful broker.
* **Upstash/Confluent free Kafka**: message caps (e.g. 10k/day) are below the demo
  generator's rate; fine only for a trickle of events.
* This project deliberately keeps a **real Kafka** (see `docs/decisions/0001`), so
  there is no "drop Kafka to fit a free tier" build.
