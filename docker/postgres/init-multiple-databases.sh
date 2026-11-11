#!/bin/sh

set -e

# Debug: Print environment variables (redact passwords)
echo "=== Postgres initialization starting ==="
echo "POSTGRES_USER: ${POSTGRES_USER}"
echo "METADATA_DATABASE_NAME: ${METADATA_DATABASE_NAME}"
echo "METADATA_DATABASE_USERNAME: ${METADATA_DATABASE_USERNAME}"
echo "CELERY_BACKEND_NAME: ${CELERY_BACKEND_NAME}"
echo "CELERY_BACKEND_USERNAME: ${CELERY_BACKEND_USERNAME}"
echo "ELT_DATABASE_NAME: ${ELT_DATABASE_NAME}"
echo "ELT_DATABASE_USERNAME: ${ELT_DATABASE_USERNAME}"
echo ""

# Validate required environment variables
validate_var() {
    local var_name=$1
    local var_value=$2
    if [ -z "$var_value" ]; then
        echo "ERROR: Required environment variable '$var_name' is not set or empty"
        exit 1
    fi
}

validate_var "POSTGRES_USER" "$POSTGRES_USER"
validate_var "METADATA_DATABASE_NAME" "$METADATA_DATABASE_NAME"
validate_var "METADATA_DATABASE_USERNAME" "$METADATA_DATABASE_USERNAME"
validate_var "METADATA_DATABASE_PASSWORD" "$METADATA_DATABASE_PASSWORD"
validate_var "CELERY_BACKEND_NAME" "$CELERY_BACKEND_NAME"
validate_var "CELERY_BACKEND_USERNAME" "$CELERY_BACKEND_USERNAME"
validate_var "CELERY_BACKEND_PASSWORD" "$CELERY_BACKEND_PASSWORD"
validate_var "ELT_DATABASE_NAME" "$ELT_DATABASE_NAME"
validate_var "ELT_DATABASE_USERNAME" "$ELT_DATABASE_USERNAME"
validate_var "ELT_DATABASE_PASSWORD" "$ELT_DATABASE_PASSWORD"

echo "All required environment variables are set. Proceeding with database creation..."
echo ""

create_user_and_database() {
    local database=$1
    local username=$2
    local password=$3
    echo "Creating user '$username' and database '$database'..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<EOSQL
        CREATE USER $username WITH PASSWORD '$password';
        CREATE DATABASE $database OWNER $username;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $username;
        GRANT ALL ON SCHEMA public TO $username;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $username;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $username;
EOSQL
    echo "✓ User '$username' and database '$database' created successfully"
}

# Metadata database
create_user_and_database "$METADATA_DATABASE_NAME" "$METADATA_DATABASE_USERNAME" "$METADATA_DATABASE_PASSWORD"

# Celery result backend database
create_user_and_database "$CELERY_BACKEND_NAME" "$CELERY_BACKEND_USERNAME" "$CELERY_BACKEND_PASSWORD"

# ELT database
create_user_and_database "$ELT_DATABASE_NAME" "$ELT_DATABASE_USERNAME" "$ELT_DATABASE_PASSWORD"

echo ""
echo "=== All databases and users created successfully ==="
