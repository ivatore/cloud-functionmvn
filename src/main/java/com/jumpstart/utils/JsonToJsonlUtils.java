package com.jumpstart.utils;

import java.util.Iterator;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

@Component
public class JsonToJsonlUtils {

	ObjectMapper mapper = new ObjectMapper();

	public String getJsonlByEvent(String payload) {
		StringBuilder jsonl = new StringBuilder();
		JsonNode root;
		try {
			root = mapper.readTree(payload);
			JsonNode headers = root.get("headers");
			JsonNode usuario = root.get("usuario");
			JsonNode eventos = root.get("eventos");

			if (eventos == null || !eventos.isArray()) {
				throw new IllegalArgumentException("El campo 'eventos' debe ser un arreglo.");
			}

			for (JsonNode evento : eventos) {
				ObjectNode combined = mapper.createObjectNode();
				combined.set("headers", headers);
				combined.set("usuario", usuario);
				combined.set("evento", evento);
				jsonl.append(mapper.writeValueAsString(combined)).append("\n");
			}
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return jsonl.toString();
	}

	public String getJsonlByClaverTap(String payload) {
		System.out.println(payload);
		StringBuilder jsonl = new StringBuilder();
		JsonNode root;
		try {
			root = mapper.readTree(payload);

			JsonNode arreglo = root.get("d");
			if (arreglo == null || !arreglo.isArray()) {
				throw new IllegalArgumentException("El campo 'd' debe ser un arreglo.");
			}

			for (JsonNode item : arreglo) {
				jsonl.append(mapper.writeValueAsString(item)).append("\n");
			}
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return jsonl.toString();
	}

	public String convertirJsonAJsonl(String jsonInput) throws Exception {
		System.out.println(jsonInput);
		JsonNode root = mapper.readTree(jsonInput);
		root = normalizarClaves(root); // <<< Normaliza las claves

		StringBuilder jsonl = new StringBuilder();

		// Caso 1: contiene "eventos" (eventos + headers + usuario)
		if (root.has("eventos") && root.get("eventos").isArray()) {
			JsonNode headers = root.get("headers");
			JsonNode usuario = root.get("usuario");

			for (JsonNode evento : root.get("eventos")) {
				ObjectNode combined = mapper.createObjectNode();
				combined.set("headers", headers);
				combined.set("usuario", usuario);
				ObjectNode eventoClonado = (ObjectNode) normalizarClaves(evento);
//				if (!eventoClonado.has("propiedades")) {
//					System.out.println("NO TRAE PROPIEDADES");
//					ObjectNode propiedadesPorDefecto = mapper.createObjectNode();
//					propiedadesPorDefecto.put("nombre", "N/A");
//					propiedadesPorDefecto.put("correo", "N/A");
//					propiedadesPorDefecto.put("idOfertaCredito", "N/A");
//					propiedadesPorDefecto.put("segmento", "General");
//					insertarDespues(eventoClonado, "parametros", "propiedades", propiedadesPorDefecto);
////					eventoClonado.set("propiedades", propiedadesPorDefecto);
//				}
				combined.set("evento", eventoClonado);
				jsonl.append(mapper.writeValueAsString(combined)).append("\n");
			}

			// Caso 2: contiene arreglo "d"
		} else if (root.has("d") && root.get("d").isArray()) {
			for (JsonNode item : root.get("d")) {
				jsonl.append(mapper.writeValueAsString(item)).append("\n");
			}

			// Caso 3: JSON plano con "eventValue" como texto que debe expandirse
		} else if (root.has("eventValue") && root.get("eventValue").isTextual()) {
			ObjectNode objeto = (ObjectNode) root;
			JsonNode expanded = mapper.readTree(root.get("eventValue").textValue());
			expanded = normalizarClaves(expanded);
			objeto.set("eventValue", expanded);
			jsonl.append(mapper.writeValueAsString(objeto)).append("\n");

			// Caso 4: objeto simple, solo imprimir como está
		} else {
			jsonl.append(mapper.writeValueAsString(root)).append("\n");
		}

		return jsonl.toString();
	}

	public String getJsonlByAppFlyer(String payload) throws JsonMappingException, JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(payload);
		JsonNode rootNormalized = normalizarClaves(root);
		ObjectNode objeto = (ObjectNode) rootNormalized;

		JsonNode eventValue = rootNormalized.get("eventValue");
		if (eventValue != null && eventValue.isTextual()) {
			JsonNode deserializado = mapper.readTree(eventValue.textValue());
			objeto.set("eventValue", deserializado);
		}

		return mapper.writeValueAsString(objeto) + "\n";
	}

	public JsonNode normalizarClaves(JsonNode input) {
		if (input.isObject()) {
			ObjectNode result = JsonNodeFactory.instance.objectNode();
			Iterator<Map.Entry<String, JsonNode>> fields = input.fields();
			while (fields.hasNext()) {
				Map.Entry<String, JsonNode> entry = fields.next();
				String nuevaClave = entry.getKey().replaceAll("[\\s\\-]", "_");
				result.set(nuevaClave, normalizarClaves(entry.getValue()));
			}
			return result;
		} else if (input.isArray()) {
			ArrayNode result = JsonNodeFactory.instance.arrayNode();
			for (JsonNode item : input) {
				result.add(normalizarClaves(item));
			}
			return result;
		} else {
			return input; // valores primitivos
		}
	}

	
	public JsonElement normalizarClaves(JsonElement input) {
	    if (input.isJsonObject()) {
	        JsonObject jsonObject = input.getAsJsonObject();
	        JsonObject result = new JsonObject();
	        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
	            String nuevaClave = entry.getKey().replaceAll("[\\s\\-]", "_");
	            result.add(nuevaClave, normalizarClaves(entry.getValue()));
	        }
	        return result;
	    } else if (input.isJsonArray()) {
	        JsonArray jsonArray = input.getAsJsonArray();
	        JsonArray result = new JsonArray();
	        for (JsonElement item : jsonArray) {
	            result.add(normalizarClaves(item));
	        }
	        return result;
	    } else {
	        return input; // valores primitivos
	    }
	}
	
	public ObjectNode insertarDespues(ObjectNode original, String claveObjetivo, String nuevaClave,
			JsonNode nuevoValor) {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode resultado = mapper.createObjectNode();

		Iterator<Map.Entry<String, JsonNode>> campos = original.fields();

		while (campos.hasNext()) {
			Map.Entry<String, JsonNode> entrada = campos.next();
			String clave = entrada.getKey();
			JsonNode valor = entrada.getValue();

			resultado.set(clave, valor);

			if (clave.equals(claveObjetivo)) {
				resultado.set(nuevaClave, nuevoValor);
			}
		}

		return resultado;
	}
}
