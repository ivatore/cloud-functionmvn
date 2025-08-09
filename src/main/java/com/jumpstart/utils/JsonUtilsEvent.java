package com.jumpstart.utils;

import java.util.Optional;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

@Component
public class JsonUtilsEvent {
	public boolean validarCampoExistente(JsonNode nodo, String... path) {
		JsonNode actual = nodo;
		for (String p : path) {
			if (p.matches("\\d+")) {
				int index = Integer.parseInt(p);
				if (!actual.isArray() || index >= actual.size()) {
					return false;
				}
				actual = actual.get(index);
			} else {
				actual = actual.path(p);
				if (actual.isMissingNode()) {
					return false;
				}
			}
		}
		return !actual.isNull() && !actual.asText().isEmpty();
	}

	public Optional<String> obtenerValorCampo(JsonNode nodo, String... path) {
		JsonNode actual = nodo;
		for (String p : path) {
			if (p.matches("\\d+")) {
				int index = Integer.parseInt(p);
				if (!actual.isArray() || index >= actual.size()) {
					return Optional.empty();
				}
				actual = actual.get(index);
			} else {
				actual = actual.path(p);
				if (actual.isMissingNode()) {
					return Optional.empty();
				}
			}
		}
		if (actual.isValueNode()) {
			return Optional.of(actual.asText());
		}
		return Optional.empty();
	}

	public boolean validarCampoExistente(JsonObject root, String... path) {
		JsonElement actual = root;

		for (String p : path) {
			if (actual == null || actual.isJsonNull()) {
				return false;
			}

			if (p.matches("\\d+")) { // índice de array
				int index = Integer.parseInt(p);
				if (!actual.isJsonArray())
					return false;

				JsonArray array = actual.getAsJsonArray();
				if (index >= array.size())
					return false;

				actual = array.get(index);
			} else { // propiedad de objeto
				if (!actual.isJsonObject())
					return false;
				JsonObject obj = actual.getAsJsonObject();

				if (!obj.has(p))
					return false;

				actual = obj.get(p);
			}
		}

		return actual != null && !actual.isJsonNull() && !actual.getAsString().isEmpty();
	}

	public Optional<String> obtenerValorCampo(JsonObject root, String... path) {
		JsonElement actual = root;

		for (String p : path) {
			if (actual == null || actual.isJsonNull()) {
				return Optional.empty();
			}

			if (p.matches("\\d+")) { // índice de array
				if (!actual.isJsonArray())
					return Optional.empty();

				int index = Integer.parseInt(p);
				JsonArray array = actual.getAsJsonArray();
				if (index >= array.size())
					return Optional.empty();

				actual = array.get(index);
			} else { // propiedad de objeto
				if (!actual.isJsonObject())
					return Optional.empty();

				JsonObject obj = actual.getAsJsonObject();
				if (!obj.has(p))
					return Optional.empty();

				actual = obj.get(p);
			}
		}

		if (actual != null && !actual.isJsonNull() && actual.isJsonPrimitive()) {
			return Optional.of(actual.getAsString());
		}

		return Optional.empty();
	}

	public Optional<String> obtenerSiExiste(JsonObject root, String... path) {
		JsonElement actual = root;

		for (String p : path) {
			if (actual == null || actual.isJsonNull()) {
				return Optional.empty();
			}

			if (p.matches("\\d+")) {
				if (!actual.isJsonArray())
					return Optional.empty();

				int index = Integer.parseInt(p);
				JsonArray array = actual.getAsJsonArray();
				if (index >= array.size())
					return Optional.empty();

				actual = array.get(index);
			} else {
				if (!actual.isJsonObject())
					return Optional.empty();

				JsonObject obj = actual.getAsJsonObject();
				if (!obj.has(p))
					return Optional.empty();

				actual = obj.get(p);
			}
		}

		// Solo devuelve el valor si es primitivo (string, number, boolean)
		if (actual != null && !actual.isJsonNull() && actual.isJsonPrimitive()) {
			return Optional.of(actual.getAsString());
		}

		return Optional.empty();
	}
}
