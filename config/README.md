# Config

Two env templates for two deployment modes. Copy the right one to `config/.env` before starting services.

---

## 1. Local (single machine)

All components run in Docker on one machine. Order matters — each layer depends on the one before it.

```bash
# 0. Activate local config
cp config/.env.single-machine config/.env

# 1. Kafka (3-broker KRaft cluster)
docker compose -f infra/kafka/docker-compose.yml up -d

# 2. ClickHouse
docker compose -f infra/clickhouse/docker-compose.yml up -d

# 3. Kafka UI  (optional, anytime after Kafka is up)
docker compose -f infra/kafka/docker-compose.kafka-ui.yml up -d

# 4. Ingestors + Nginx load balancer
docker compose -f ingestor/docker-compose.yml up -d
docker compose -f ingestor/docker-compose.nginx.yml up -d

# 5. Flink (JobManager + TaskManager)
docker compose -f infra/flink/docker-compose.yml up -d

# 6. Generator  (native binary, not Docker)
cd generator && ./build/generate
```

---

## 2. Distributed (multi-machine)

Each component runs on a separate machine. All machines must have the **same** `config/.env`.

### 2a. Prepare config (do once, on any machine)

```bash
# 1. Start from the distributed template
cp config/.env.distributed config/.env

# 2. Edit config/.env — update IPs in the INSTANCE REGISTRY at the top.
#    Per-component compose files derive connection strings from these
#    IPs automatically via Docker Compose ${VAR} substitution.

# 3. Edit ingestor/nginx.distributed.conf — replace upstream IPs manually.
#    Nginx cannot read env vars in upstream {} blocks.

# 4. Push config/.env to every machine
scp config/.env <user>@<machine>:~/project/config/.env   # repeat for each
```

### 2b. Start components (per machine, in order)

Start the layers bottom-up. Kafka and ClickHouse must be ready before the layers that depend on them.

```bash
# ── Machine E  (Kafka broker 1)
docker compose -f infra/kafka/docker-compose.kafka-1.yml up -d

# ── Machine F  (Kafka broker 2)
docker compose -f infra/kafka/docker-compose.kafka-2.yml up -d

# ── Machine G  (Kafka broker 3)
docker compose -f infra/kafka/docker-compose.kafka-3.yml up -d

# ── Machine I  (ClickHouse)
docker compose -f infra/clickhouse/docker-compose.yml up -d

# ── Machine B  (Ingestor 1)   — after Kafka is up
docker compose -f ingestor/docker-compose.ingestor-1.yml up -d

# ── Machine C  (Ingestor 2)
docker compose -f ingestor/docker-compose.ingestor-2.yml up -d

# ── Machine D  (Ingestor 3)
docker compose -f ingestor/docker-compose.ingestor-3.yml up -d

# ── Machine A  (Nginx + Generator)   — after Ingestors are up
docker compose -f ingestor/docker-compose.nginx.yml up -d
cd generator && ./build/generate

# ── Machine H  (Flink)   — after Kafka and ClickHouse are up
docker compose -f infra/flink/docker-compose.flink.yml up -d

# ── Kafka UI   (optional, deploy on any machine that can reach Kafka)
docker compose -f infra/kafka/docker-compose.kafka-ui.yml up -d
```

---

## Which version is active?

```bash
head -1 config/.env   # Shows "VERSION 1" or "VERSION 2"
```

---

## Do not commit

`config/.env` (the active config) should not be committed. Only the templates (`.env.single-machine`, `.env.distributed`) are version-controlled.
