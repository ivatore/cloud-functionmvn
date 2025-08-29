package com.jumpstart.service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.jumpstart.config.AccederSecretGCP;
import com.jumpstart.entity.enums.CamposJson;
import com.jumpstart.service.payload.PayloadClaverTap;
import com.jumpstart.utils.JsonUtilsEvent;

@Service
public class CleverTapService {
	private static final Logger log = LoggerFactory.getLogger(CleverTapService.class);

	private static final JsonObject config = AccederSecretGCP.getSateliteConfig("secret_jumpstart_config_satelites");

	private static final String CLEVERTAP_URL = "https://us1.api.clevertap.com/1/upload";
	private static final String CLEVERTAP_ID = config.get("accountId").getAsString();
	private static final String CLEVERTAP_PASS = config.get("passcode").getAsString();

	private final HttpClient httpClient = HttpClient.newHttpClient();
	private final ObjectMapper mapper = new ObjectMapper();
	private final PayloadClaverTap payloadClaverTap;

	private final JsonUtilsEvent jsonUtilsEvent;

	public CleverTapService(PayloadClaverTap payloadClaverTap, JsonUtilsEvent jsonUtilsEvent) {
		this.payloadClaverTap = payloadClaverTap;
		this.jsonUtilsEvent = jsonUtilsEvent;
	}

	public String enviarEvento(JsonObject event) {
		String payload = "";
		try {
			JsonObject usuario = event.getAsJsonObject(CamposJson.USUARIO);

			String nombreEvento = event.get(CamposJson.EVENTO).getAsJsonArray().get(0).getAsJsonObject().get("nombre")
					.getAsString();

			String idevento = jsonUtilsEvent.obtenerSiExiste(usuario, "icu")
					.orElse(jsonUtilsEvent.obtenerSiExiste(usuario, "sicu").orElse("NA"));

			if (nombreEvento.equals("purchase")) {
				payload = payloadClaverTap.construirPayloadChargedEventCleverTap(event);
			} else {
				payload = payloadClaverTap.construirPayloadGeneralEventCleverTap(event);
			}
			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(CLEVERTAP_URL))
					.timeout(Duration.ofSeconds(180)).header("Content-Type", "application/json")
					.header("X-CleverTap-Account-Id", CLEVERTAP_ID).header("X-CleverTap-Passcode", CLEVERTAP_PASS)
					.POST(HttpRequest.BodyPublishers.ofString(payload)).build();

			HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

			log.info("Payload Clevertap---> {}" + payload);
			log.info("CleverTap response: {}" + idevento + response.body());
			return payload;

		} catch (Exception e) {
			log.error("Error enviando evento a CleverTap" + e.getMessage());
			e.printStackTrace();
			return payload;
		}
	}

	public String enviarUserProfile(JsonObject evento) {
		String payloadsend = "";

		try {
			Map<String, Object> payload = payloadClaverTap.construirPayloadUserCleverTap(evento);
			payloadsend = mapper.writeValueAsString(payload);

			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(CLEVERTAP_URL))
					.timeout(Duration.ofSeconds(180)).header("Content-Type", "application/json")
					.header("X-CleverTap-Account-Id", CLEVERTAP_ID).header("X-CleverTap-Passcode", CLEVERTAP_PASS)
					.POST(HttpRequest.BodyPublishers.ofString(payloadsend)).build();

			HttpClient httpClient = HttpClient.newHttpClient();
			HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

			if (response.statusCode() >= 200 && response.statusCode() < 300) {
				log.info("CleverTap: Usuario enviado");
			} else {
				log.info("CleverTap User Error: HTTP " + response.statusCode());
			}

		} catch (Exception ex) {
			log.error("CleverTap Error: " + ex.getMessage());
		}

		return payloadsend;
	}

}
