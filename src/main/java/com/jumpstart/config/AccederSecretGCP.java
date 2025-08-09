package com.jumpstart.config;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AccederSecretGCP {

	public static String obtenerSecret(String secretId) {
		String payload = "";
		String projectId = obtenerProjectId();

		try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
			SecretVersionName secretVersionName = SecretVersionName.of(projectId, secretId, "latest");
			AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
			payload = response.getPayload().getData().toStringUtf8();
			System.out.println("Secreto leido correctamente.");
		} catch (Exception e) {
			System.err.println("Error al acceder al secreto: " + e.getMessage());
			e.printStackTrace();
		}

		return payload;
	}

	public static JsonObject getSateliteConfig(String secretId) {
		try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
			SecretVersionName secretVersionName = SecretVersionName.of(obtenerProjectId(), secretId, "latest");

			String payload = client
					.accessSecretVersion(
							AccessSecretVersionRequest.newBuilder().setName(secretVersionName.toString()).build())
					.getPayload().getData().toStringUtf8();

			return JsonParser.parseString(payload).getAsJsonObject();

		} catch (Exception e) {
			JsonObject json = new JsonObject();
			json.addProperty("accountId", "TEST-75W-96W-566Z");
			json.addProperty("passcode", "UTW-TIE-UEUL");
			json.addProperty("region", "us1");
			json.addProperty("appsflyer-key", "JXL8dJbVbx2qLSUTiV8tr");

			return json;
//			throw new RuntimeException("Error al obtener secreto: " + e.getMessage(), e);
		}
	}

	private static String obtenerProjectId() {
		try {
			// Intenta obtener el project ID desde el metadata server de GCP
			URL url = new URL("http://metadata.google.internal/computeMetadata/v1/project/project-id");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			con.setRequestProperty("Metadata-Flavor", "Google");

			try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
				StringBuilder response = new StringBuilder();
				String line;
				while ((line = in.readLine()) != null) {
					response.append(line);
				}
				return response.toString();
			}
		} catch (Exception e) {
			// Fallback: intenta obtenerlo de variable de entorno
			System.err.println("No se pudo obtener el Project ID desde metadata. Intentando variable de entorno...");
			String fallbackProjectId = System.getenv("GCP_PROJECT");
			if (fallbackProjectId != null && !fallbackProjectId.isBlank()) {
				return fallbackProjectId;
			} else {
				System.err.println("No se pudo obtener el Project ID. Usando valor por defecto.");
				return "project-id-no-definido";
			}
		}
	}
}