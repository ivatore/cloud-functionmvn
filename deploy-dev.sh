#!/bin/bash

# Para detener el script si falla un comando
set -e

# === CONFIGURACION GENERAL ===
FUNCTION_NAME=func-jumpstart-events-streaming-processor
ENTRY_POINT=com.jumpstart.PubSubMessageHandler
RUNTIME=java17
TOPIC=tp-jumpstart-events-receiver-streaming
MEMORY=2GB
TIMEOUT=120s
REGION=us-central1
SERVICE_ACCOUNT=sa-jumpstart-events-streaming@gs-mx-sandboxanalitica-prod.iam.gserviceaccount.com

# === VARIABLES DE ENTORNO ===
ENV_VARS="BUCKET_NAME=jumpstart_events_raw,SATELITES_STATUS=vertex-clevertap-"

# === DESPLIEGUE ===
echo "Desplegando función $FUNCTION_NAME..."

gcloud functions deploy $FUNCTION_NAME \
  --entry-point $ENTRY_POINT \
  --runtime $RUNTIME \
  --trigger-topic $TOPIC \
  --source . \
  --memory $MEMORY \
  --timeout $TIMEOUT \
  --region $REGION \
  --set-env-vars "$ENV_VARS"\
  --service-account $SERVICE_ACCOUNT

echo "✔️✔️✔️ Despliegue completo"