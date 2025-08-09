package com.jumpstart.service.payload;

import java.util.Optional;

import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jumpstart.entity.enums.CamposJson;
import com.jumpstart.utils.JsonToJsonlUtils;
import com.jumpstart.utils.JsonUtilsEvent;

@Service
public class PayloadAppFlyer {

	private final JsonToJsonlUtils jsonToJsonlUtils;

	private final JsonUtilsEvent jsonUtilsEvent;

	private static final Gson gson = new Gson();

	public PayloadAppFlyer(JsonUtilsEvent jsonUtilsEvent, JsonToJsonlUtils jsonToJsonlUtils) {
		this.jsonUtilsEvent = jsonUtilsEvent;
		this.jsonToJsonlUtils = jsonToJsonlUtils;
	}

	public String construirPayloadAppsFlyer(JsonObject event) {
		JsonObject original = gson.fromJson(event, JsonObject.class);

		boolean hasProductos = jsonUtilsEvent.validarCampoExistente(event, CamposJson.EVENTO, "0",
				CamposJson.EVENTOPRODUCTO, "0", "id");
		boolean hasPropiedades = jsonUtilsEvent.validarCampoExistente(event, CamposJson.EVENTO, "0",
				CamposJson.EVENTOPROPIEDAD, "nombre");
		boolean hasParametros = jsonUtilsEvent.validarCampoExistente(event, CamposJson.EVENTO, "0",
				CamposJson.EVENTOPARAMETRO, "idaccion");

		JsonObject usuario = original.getAsJsonObject(CamposJson.USUARIO);
		JsonObject headers = original.getAsJsonObject(CamposJson.HEADERS);
		JsonObject evento = original.getAsJsonArray(CamposJson.EVENTO).get(0).getAsJsonObject();
		JsonObject ubicacion = evento.getAsJsonObject(CamposJson.EVENTOUBICACION);
		JsonObject fuente = evento.getAsJsonObject(CamposJson.EVENTOFUENTE);

		JsonObject producto = hasProductos ? evento.getAsJsonArray(CamposJson.EVENTOPRODUCTO).get(0).getAsJsonObject()
				: null;
		JsonObject propiedades = hasPropiedades ? evento.getAsJsonObject(CamposJson.EVENTOPROPIEDAD) : null;
		JsonObject paramentros = hasParametros ? evento.getAsJsonObject(CamposJson.EVENTOPARAMETRO) : null;

		String customerUserId = Optional.ofNullable(usuario.get("icu").getAsString())
				.orElse(usuario.get("sicu").getAsString());

		JsonObject eventValue = new JsonObject();
		eventValue.add("id", evento.get("id"));
		eventValue.add("idPlataforma", evento.get("idAppsFlyer"));
		eventValue.add("idSesion", evento.get("idSesion"));
		eventValue.add("registroPlataforma",
				evento.has("registroPlataforma") ? evento.get("registroPlataforma") : new JsonObject());
		eventValue.add(CamposJson.HEADERS, jsonToJsonlUtils.normalizarClaves(headers));
		eventValue.add(CamposJson.EVENTOUBICACION, ubicacion);
		if (hasParametros) {
			eventValue.add(CamposJson.EVENTOPARAMETRO, paramentros);
		}
		if (hasPropiedades) {
			eventValue.add(CamposJson.EVENTOPROPIEDAD, propiedades);
		}

		eventValue.add(CamposJson.EVENTOFUENTE, fuente);
		if (hasProductos) {
			eventValue.add(CamposJson.EVENTOPRODUCTO, producto);
		}
		eventValue.add(CamposJson.USUARIO, usuario);

		JsonObject resultado = new JsonObject();
		resultado.addProperty("appsflyer_id", evento.get("idAppsFlyer").getAsString());
		resultado.addProperty("customer_user_id", customerUserId);
		resultado.addProperty("device_id", headers.has("x-modelo") ? headers.get("x-modelo").getAsString() : "NA");
		if (hasParametros) {
			resultado.addProperty("eventCurrency", paramentros.get("moneda").getAsString());
		}
		resultado.addProperty("eventName",
				reglaNombreEvent(evento.get("nombre").getAsString(), evento.get("aliasAppsFlyer").getAsString()));
		resultado.addProperty("eventTime", evento.get("fechaHoraRegistro").getAsString());
		resultado.addProperty("eventValue", eventValue.toString()); // debe ir como String JSON
		resultado.addProperty("platform", headers.get("x-version-sistema-operativo").getAsString());

		return gson.toJson(resultado);

	}

	private String reglaNombreEvent(String nombre, String alias) {
		StringBuilder nombrenuevo = new StringBuilder();

		switch (nombre) {
		case "purchase": {
			nombrenuevo.append("af_purchase");
			break;
		}
		case "add_to_cart": {
			nombrenuevo.append("af_add_to_cart");
			break;
		}
		case "begin_checkout": {
			nombrenuevo.append("af_begin_checkout");
			break;
		}
		case "search": {
			nombrenuevo.append("af_search");
			break;
		}
		case "view_item_list": {
			nombrenuevo.append("af_list_view");
			break;
		}
		case "login": {
			nombrenuevo.append("af_login");
			break;
		}
		default:
			nombrenuevo.append(nombre);
			if (alias != null && !alias.isEmpty()) {
				nombrenuevo.append("_");
				nombrenuevo.append(alias);
			}

			break;
		}

		return nombrenuevo.toString();
	}
}