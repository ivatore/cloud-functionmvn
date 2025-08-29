package com.jumpstart.service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.gson.JsonObject;
import com.jumpstart.config.AccederSecretGCP;
import com.jumpstart.entity.enums.CamposJson;
import com.jumpstart.service.payload.PayloadAppFlyer;

@Service
public class AppsFlyerService {
	private static final Logger log = LoggerFactory.getLogger(AppsFlyerService.class);
	private static final JsonObject config = AccederSecretGCP.getSateliteConfig("secret_jumpstart_config_satelites");

	private static final String DEV_KEY = config.get("appsflyer-key").getAsString();
	private static final String URL = "https://api2.appsflyer.com/inappevent/";

	private final HttpClient httpClient = HttpClient.newHttpClient();
	private final PayloadAppFlyer payloadAppFlyer;

	public AppsFlyerService(PayloadAppFlyer payloadAppFlyer) {
		this.payloadAppFlyer = payloadAppFlyer;
	}

	public String enviarEvento(JsonObject event) {
		String payload = "";
		try {
			String appId = event.get(CamposJson.EVENTO).getAsJsonArray().get(0).getAsJsonObject().get("idAppsFlyer")
					.getAsString();
			String endpoint = URL + appId;

			payload = payloadAppFlyer.construirPayloadAppsFlyer(event);

			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(endpoint)).timeout(Duration.ofSeconds(180))
					.header("accept", "application/json").header("Content-Type", "application/json")
					.header("authentication", DEV_KEY).POST(HttpRequest.BodyPublishers.ofString(payload)).build();

			HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

			log.info("AppsFlyer payload: {}", payload);
			log.info("AppsFlyer response: {}", response.body());
			return payload;

		} catch (Exception e) {
			log.error("Error enviando evento a AppsFlyer {}", e.getMessage());
			return payload;
		}
	}

}
