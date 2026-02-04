# Config

Two env templates for two deployment modes. Copy the right one to `config/.env` before starting services.

---

## 1. Local (single machine)

All components run in Docker on one machine.

```bash
cp config/.env.single-machine config/.env
```

Then start each component's docker-compose as usual.

---

## 2. Distributed (multi-machine)

Each component runs on a separate machine. All machines must have the same `config/.env`.

```bash
# 1. Copy the template
cp config/.env.distributed config/.env

# 2. Edit config/.env:
#    Update IPs in the INSTANCE REGISTRY section (top of file).
#    Per-component compose files derive connection strings from these
#    IPs automatically via Docker Compose ${VAR} substitution.

# 3. Edit ingestor/nginx.distributed.conf:
#    Replace the upstream IPs to match your INGESTOR_*_IP values.
#    Nginx cannot read env vars in upstream blocks.

# 4. Copy config/.env to all machines
scp config/.env <user>@<machine>:~/project/config/.env   # repeat for each machine
```

---

## Which version is active?

```bash
head -1 config/.env   # Shows "VERSION 1" or "VERSION 2"
```

---

## Do not commit

`config/.env` (the active config) should not be committed. Only the templates (`.env.single-machine`, `.env.distributed`) are version-controlled.
