#!/bin/bash
set -e

echo "🛠️  [Entrypoint] Starting Airflow Entrypoint Script..."

# Wait for the MySQL DB to be ready
echo "⌛ [Entrypoint] Waiting for MySQL to be available..."
until nc -z -v -w30 mysql 3306; do
  echo "⏳ [Entrypoint] Waiting for MySQL database connection..."
  sleep 5
done
echo "✅ [Entrypoint] MySQL is up and running!"

# Run DB migrations
echo "🔄 [Entrypoint] Running airflow db upgrade..."
airflow db upgrade

# Create admin user if not already present
echo "👤 [Entrypoint] Ensuring Airflow admin user exists..."
if ! airflow users list | grep -wq admin; then
  echo "👤 [Entrypoint] Creating admin user..."
  airflow users create \
    --username admin \
    --password admin \
    --firstname Suraj \
    --lastname Jadhav \
    --role Admin \
    --email sj4456046@gmail.com
else
  echo "✅ [Entrypoint] Admin user already exists."
fi

echo "🚀 [Entrypoint] Launching: $@"
exec "$@"
