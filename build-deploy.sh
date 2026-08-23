#!/usr/bin/env bash
# Build the Spring Boot jar, build the Docker images, and start the whole stack.
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Building Spring Boot jar..."
if [ -x "./mvnw" ]; then
  ./mvnw -q clean package -DskipTests
else
  mvn -q clean package -DskipTests
fi

echo "==> Building images and starting the stack (3x kafka + app)..."
docker compose build
docker compose up -d

echo
echo "==> Up. Open http://localhost:8080"
echo "    logs:   docker compose logs -f app"
echo "    stop:   docker compose down"
echo "    wipe:   docker compose down -v"
