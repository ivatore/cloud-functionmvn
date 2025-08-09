#!/bin/bash

# Para detener el script si falla un comando
set -e

# === CONFIGURACION GENERAL ===
FUNCTION_NAME=func-jumpstart-events-streaming-processor
ENTRY_POINT=com.jumpstart.PubSubMessageHandler
RUNTIME=java17
TOPIC=tp-jumpstart-events-receiver-streaming
MEMORY=4GB
TIMEOUT=120s
REGION=us-central1
SERVICE_ACCOUNT=sa-jumpstart-events-streaming@celtic-acumen-419200.iam.gserviceaccount.com

# === VARIABLES DE ENTORNO ===
ENV_VARS="BUCKET_NAME=jumpstart_events_raw_prod,SATELITES_STATUS=OFF"


echo "Desplegando función en ⚠️PRODUCCION⚠️ $FUNCTION_NAME..."

gcloud functions deploy $FUNCTION_NAME \
  --entry-point $ENTRY_POINT \
  --runtime $RUNTIME \
  --trigger-topic $TOPIC \
  --source . \
  --memory $MEMORY \
  --region $REGION \
  --set-env-vars "$ENV_VARS"\
  --service-account $SERVICE_ACCOUNT\
  --cpu=2

echo "✔️✔️✔️ Despliegue completo"