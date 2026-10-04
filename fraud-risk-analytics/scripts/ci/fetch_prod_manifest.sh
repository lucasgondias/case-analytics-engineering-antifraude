#!/usr/bin/env bash
# Baixa o manifest.json do último deploy de produção para Slim CI (state:modified+ / --defer).
# PROD_MANIFEST_URI: s3://bucket/dbt/prod/manifest.json | gs://bucket/dbt/prod/manifest.json
set -euo pipefail
dest="${1:-prod-manifest}"
mkdir -p "$dest"
: "${PROD_MANIFEST_URI:?defina PROD_MANIFEST_URI}"
case "$PROD_MANIFEST_URI" in
  s3://*) aws s3 cp "$PROD_MANIFEST_URI" "$dest/manifest.json" ;;
  gs://*) gcloud storage cp "$PROD_MANIFEST_URI" "$dest/manifest.json" ;;
  *) echo "esquema não suportado: $PROD_MANIFEST_URI" >&2; exit 1 ;;
esac
