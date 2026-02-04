# Config

Two env templates for two deployment modes. Copy the right one to `config/.env` before starting services.

---

## 1. Local (single machine)

All components run in Docker on one machine. Order matters — each layer depends on the one before it.

### Startup order (recommended)
If you start out of order, services may fail to connect at boot and require a restart.
1. Kafka
2. ClickHouse
3. Ingestors
4. Nginx
5. Flink
6. Generator
7. Kafka UI (optional, anytime after Kafka)

```bash
# 0. Activate local config
cp config/.env.single-machine config/.env

# 1. Kafka (3-broker KRaft cluster)
docker compose -f ops/compose/single-machine/kafka.yml up -d

# 2. ClickHouse
docker compose -f ops/compose/single-machine/clickhouse.yml up -d

# 3. Kafka UI  (optional, anytime after Kafka is up)
docker compose -f ops/compose/single-machine/kafka-ui.yml up -d

# 4. Ingestors + Nginx load balancer
docker compose -f ops/compose/single-machine/ingestor.yml up -d
docker compose -f ops/compose/single-machine/ingestor-nginx.yml up -d

# 5. Flink (JobManager + TaskManager)
docker compose -f ops/compose/single-machine/flink.yml up -d

# 6. Generator  (native binary, not Docker)
cd services/generator && ./build/generate
```

---

## 2. Distributed (multi-machine)

Each component runs on a separate machine. All machines must have the **same** `config/.env`.

### Startup order (recommended)
If you start out of order, services may fail to connect at boot and require a restart.
1. Kafka brokers (E, F, G)
2. ClickHouse (I)
3. Ingestors (B, C, D)
4. Nginx (A)
5. Flink (H)
6. Generator (A)
7. Kafka UI (optional, anytime after Kafka)

### 2a. Prepare config (do once, on any machine)

```bash
# 1. Start from the distributed template
cp config/.env.distributed config/.env

# 2. Edit config/.env — update IPs in the INSTANCE REGISTRY at the top.
#    Per-component compose files derive connection strings from these
#    IPs automatically via Docker Compose ${VAR} substitution.

# 3. Edit ops/compose/distributed/nginx.distributed.conf — replace upstream IPs manually.
#    Nginx cannot read env vars in upstream {} blocks.

# 4. Push config/.env to every machine
scp config/.env <user>@<machine>:~/project/config/.env   # repeat for each
```

### 2b. Start components (per machine, in order)

Start the layers bottom-up. Kafka and ClickHouse must be ready before the layers that depend on them.

```bash
# ── Machine E  (Kafka broker 1)
docker compose -f ops/compose/distributed/kafka-1.yml up -d

# ── Machine F  (Kafka broker 2)
docker compose -f ops/compose/distributed/kafka-2.yml up -d

# ── Machine G  (Kafka broker 3)
docker compose -f ops/compose/distributed/kafka-3.yml up -d

# ── Machine I  (ClickHouse)
docker compose -f ops/compose/distributed/clickhouse.yml up -d

# ── Machine B  (Ingestor 1)   — after Kafka is up
docker compose -f ops/compose/distributed/ingestor-1.yml up -d

# ── Machine C  (Ingestor 2)
docker compose -f ops/compose/distributed/ingestor-2.yml up -d

# ── Machine D  (Ingestor 3)
docker compose -f ops/compose/distributed/ingestor-3.yml up -d

# ── Machine A  (Nginx + Generator)   — after Ingestors are up
docker compose -f ops/compose/distributed/ingestor-nginx.yml up -d
cd services/generator && ./build/generate

# ── Machine H  (Flink)   — after Kafka and ClickHouse are up
docker compose -f ops/compose/distributed/flink.yml up -d

# ── Kafka UI   (optional, deploy on any machine that can reach Kafka)
docker compose -f ops/compose/distributed/kafka-ui.yml up -d
```

---

## Which version is active?

```bash
head -1 config/.env   # Shows "VERSION 1" or "VERSION 2"
```

---

## Do not commit

`config/.env` (the active config) should not be committed. Only the templates (`.env.single-machine`, `.env.distributed`) are version-controlled.
