.PHONY: test test-unit test-e2e bench up down logs psql count topic status windows realtime redis-cli api dashboard-install dashboard-test dashboard-dev dashboard-build

COMPOSE := docker compose -f infrastructure/docker/docker-compose.yml

test-unit:
	go test ./pkg/... ./event-generator/... ./stream-processor/internal/... ./stream-processor/ ./analytics-api/ ./analytics-api/internal/... ./analytics-api/api/

# Colima users: DOCKER_HOST is set automatically when ~/.colima/default/docker.sock exists.
test-e2e:
	@if [ -S "$$HOME/.colima/default/docker.sock" ]; then \
		export DOCKER_HOST="unix://$$HOME/.colima/default/docker.sock"; \
		export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock; \
		export TESTCONTAINERS_RYUK_DISABLED=true; \
	fi; \
	go test ./stream-processor/e2e ./analytics-api/e2e -count=1 -timeout 15m

bench:
	go test ./stream-processor/internal/analytics -bench=BenchmarkOnEvent -benchmem -count=3

test: test-unit test-e2e

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f stream-processor event-generator analytics-api

psql:
	$(COMPOSE) exec postgres psql -U streamscale -d streamscale

count:
	$(COMPOSE) exec postgres psql -U streamscale -d streamscale -c "SELECT COUNT(*) AS events FROM events;"

topic:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic events
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic events.dlq

status:
	$(COMPOSE) exec postgres psql -U streamscale -d streamscale -c "SELECT status, COUNT(*) FROM processing_status GROUP BY 1 ORDER BY 1;"

windows:
	$(COMPOSE) exec postgres psql -U streamscale -d streamscale -c "SELECT spec_name, window_start, event_count, events_per_second, purchases_per_minute, revenue, active_users FROM metrics_windows ORDER BY window_start DESC LIMIT 20;"

realtime:
	curl -s "http://127.0.0.1:8081/v1/realtime?window=60"; echo
	curl -s http://127.0.0.1:8081/v1/realtime/stats; echo

redis-cli:
	$(COMPOSE) exec redis redis-cli

api:
	curl -s http://127.0.0.1:8082/health; echo
	curl -s http://127.0.0.1:8082/metrics/overview; echo

# ---- dashboard (React + TypeScript) ----
# Runs Node in Docker, so no local Node.js install is needed. With Node 20+
# installed you can instead run the npm scripts in ./dashboard directly.
NODE := docker run --rm -it -v $(CURDIR)/dashboard:/app -w /app
NODE_IMAGE := node:20.18.1-alpine

dashboard-install:
	$(NODE) $(NODE_IMAGE) npm ci

dashboard-test:
	$(NODE) $(NODE_IMAGE) sh -c "npm run typecheck && npm test"

dashboard-build:
	$(NODE) $(NODE_IMAGE) npm run build

# Dev server with hot reload on http://localhost:5173, proxying /api to the
# analytics-api on the host (start the backend with `make up` first).
dashboard-dev:
	$(NODE) -p 5173:5173 --add-host=host.docker.internal:host-gateway \
		-e API_TARGET=http://host.docker.internal:8082 $(NODE_IMAGE) npm run dev
