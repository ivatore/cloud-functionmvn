package com.jumpstart.service.payload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.jumpstart.entity.enums.CamposJson;
import com.jumpstart.utils.JsonUtilsEvent;

public class PayloadClaverTap {

	public static final String MONTOTOTAL = "montoTotal";
	public static final String IDOFERTACREDITO = "idOfertaCredito";
	public static final String SEGMENTO = "segmento";

	public static final String NA = "";

	private static final Gson gson = new Gson();

	private final JsonUtilsEvent jsonvalida;

	public PayloadClaverTap(JsonUtilsEvent jsonvalida) {
		this.jsonvalida = jsonvalida;
	}

	public String construirPayloadChargedEventCleverTap(JsonObject event) {
//		JsonObject usuario = event.getAsJsonObject("usuario");
		JsonObject evento = event.getAsJsonArray(CamposJson.EVENTO).get(0).getAsJsonObject();
		JsonObject parametros = evento.getAsJsonObject(CamposJson.EVENTOPARAMETRO);
		JsonObject headers = event.getAsJsonObject(CamposJson.HEADERS);
		JsonArray productos = evento.getAsJsonArray(CamposJson.EVENTOPRODUCTO);

		JsonObject evtData = new JsonObject();

		evtData.addProperty("x_id_emisor", headers.get("x-id-emisor").getAsString());
		evtData.addProperty("x_nombre_aplicacion", headers.get("x-nombre-aplicacion").getAsString());
		evtData.addProperty("x_version_aplicacion", headers.get("x-version-aplicacion").getAsString());
		evtData.addProperty("x_sistema_operativo", headers.get("x-sistema-operativo").getAsString());
		evtData.addProperty("x_version_sistema_operativo", headers.get("x-version-sistema-operativo").getAsString());
		evtData.addProperty("x_modelo", headers.get("x-modelo").getAsString());
		evtData.addProperty("x_resolucion_pantalla", headers.get("x-resolucion-pantalla").getAsString());
		evtData.addProperty("x_idioma", headers.get("x-idioma").getAsString());
		evtData.addProperty("x_zona_horaria", headers.get("x-zona-horaria").getAsString());
		evtData.addProperty("x_estado_red", headers.get("x-estado-red").getAsString());

		// Valores principales
		evtData.addProperty("Amount", parametros.get(MONTOTOTAL).getAsBigDecimal());
		evtData.addProperty("Currency", parametros.get("moneda").getAsString());
		evtData.addProperty("Payment mode", parametros.get("formaPago").getAsString());
		evtData.addProperty("Delivery By", parametros.get("tipoEnvio").getAsString());
		evtData.addProperty("Store Name", parametros.get("nombreTienda").getAsString());
		evtData.addProperty("Coupon", parametros.get("cupon").getAsString());
		evtData.addProperty("Discount Amount", parametros.get("montoDescuento").getAsString());
		evtData.addProperty("Tax Amount", parametros.get("montoImpuestos").getAsString());
		evtData.addProperty("Points Redeemed", parametros.get("puntosCanjeados").getAsString());

		// Detalle si existe
		if (evento.has("detalle")) {
			evtData.add("Detalle", evento.get("detalle"));
		}

		// Productos
		JsonArray itemsArr = new JsonArray();
		for (JsonElement prodElem : productos) {
			JsonObject prod = prodElem.getAsJsonObject();
			JsonObject item = new JsonObject();
			item.addProperty("Category", prod.get("categoria").getAsString());
			item.addProperty("Product name", prod.get("nombre").getAsString());
			item.addProperty("Quantity", prod.get("cantidad").getAsInt());
			item.addProperty("Price", prod.get("precio").getAsBigDecimal());
			item.addProperty("Brand", prod.get("marca").getAsString());
			itemsArr.add(item);
		}
		evtData.add("Items", itemsArr);

		// Evento general
		JsonObject eventoObj = new JsonObject();
		eventoObj.addProperty("objectId", evento.get("idClevertap").getAsString());
		eventoObj.addProperty("type", "event");
		eventoObj.addProperty("evtName",
				homologacionNombre(evento.get("nombre").getAsString(), evento.get("aliasClevertap").getAsString()));
		eventoObj.add("evtData", evtData);

		// Armar respuesta final
		JsonArray eventos = new JsonArray();
		eventos.add(eventoObj);

		JsonObject result = new JsonObject();
		result.add("d", eventos);

		return gson.toJson(result);
	}

	public String construirPayloadGeneralEventCleverTap(JsonObject event) {
//		JsonObject usuario = event.getAsJsonObject(CamposJson.USUARIO);
		JsonObject headers = event.getAsJsonObject(CamposJson.HEADERS);
		JsonObject evento = event.getAsJsonArray(CamposJson.EVENTO).get(0).getAsJsonObject();

		JsonObject evtData = new JsonObject();

		// Campos parametrizados opcionales
		Map<String, String> parametrosOpcionales = Map.of("Amount", MONTOTOTAL, "Currency", "moneda", "formaPago",
				"formaPago", "skuGrupo", "skuGrupo", "tipoLogeo", "tipoLogeo", "estatusTransaccion",
				"estatusTransaccion", "nombreTienda", "nombreTienda", MONTOTOTAL, MONTOTOTAL, "tipoEnvio", "tipoEnvio");

		parametrosOpcionales.forEach((clave, ruta) -> {
			String valor = jsonvalida.obtenerSiExiste(event, CamposJson.EVENTO, "0", "parametros", ruta).orElse(NA);
			evtData.addProperty(clave, valor);
		});

		// Detalle
		if (evento.has("detalle")) {
			evtData.add("Detalle", evento.get("detalle"));
		}

		// Headers obligatorios
		addPropertyIfPresent(evtData, headers, "x_id_emisor");
		addPropertyIfPresent(evtData, headers, "x_nombre_aplicacion");
		addPropertyIfPresent(evtData, headers, "x_version_aplicacion");
		addPropertyIfPresent(evtData, headers, "x_sistema_operativo");
		addPropertyIfPresent(evtData, headers, "x_version_sistema_operativo");
		addPropertyIfPresent(evtData, headers, "x_modelo");
		addPropertyIfPresent(evtData, headers, "x_resolucion_pantalla");
		addPropertyIfPresent(evtData, headers, "x_idioma");
		addPropertyIfPresent(evtData, headers, "x_zona_horaria");
		addPropertyIfPresent(evtData, headers, "x_estado_red");

		// Ubicación
		if (evento.has(CamposJson.EVENTOUBICACION)) {
			JsonObject ubicacion = evento.getAsJsonObject(CamposJson.EVENTOUBICACION);
			addPropertyIfPresent(evtData, ubicacion, "pais");
			addPropertyIfPresent(evtData, ubicacion, "ciudad");
			addPropertyIfPresent(evtData, ubicacion, "municipio");
			addPropertyIfPresent(evtData, ubicacion, "codigoPostal");
			addPropertyIfPresent(evtData, ubicacion, "latitud");
			addPropertyIfPresent(evtData, ubicacion, "longitud");
		}

		// Fuente
		if (evento.has(CamposJson.EVENTOFUENTE)) {
			JsonObject fuente = evento.getAsJsonObject(CamposJson.EVENTOFUENTE);
			addPropertyIfPresent(evtData, fuente, "nombre", "nombreFuente");
			addPropertyIfPresent(evtData, fuente, "url");
		}

		// Construcción final
		JsonObject eventoObj = new JsonObject();
		eventoObj.addProperty("objectId", evento.get("idClevertap").getAsString());
		eventoObj.addProperty("type", "event");
		eventoObj.addProperty("evtName", evento.get("nombre").getAsString());
		eventoObj.add("evtData", evtData);

		JsonArray eventos = new JsonArray();
		eventos.add(eventoObj);

		JsonObject result = new JsonObject();
		result.add("d", eventos);

		return gson.toJson(result);
	}

	private void addPropertyIfPresent(JsonObject target, JsonObject source, String field) {
		if (source.has(field)) {
			target.addProperty(field, source.get(field).getAsString());
		}
	}

	private void addPropertyIfPresent(JsonObject target, JsonObject source, String sourceField, String targetField) {
		if (source.has(sourceField)) {
			target.addProperty(targetField, source.get(sourceField).getAsString());
		}
	}

	public Map<String, Object> construirPayloadUserCleverTap(JsonObject event) {
		Map<String, Object> root = new HashMap<>();
		List<Object> dArray = new ArrayList<>();

		Map<String, Object> item = new HashMap<>();
		jsonvalida.validarCampoExistente(event, "");
		boolean hasIcu = jsonvalida.validarCampoExistente(event, CamposJson.USUARIO, "icu");
		boolean hasSicu = jsonvalida.validarCampoExistente(event, CamposJson.USUARIO, "sicu");
		String userId = "";

		if (hasIcu) {
			userId = jsonvalida.obtenerSiExiste(event, CamposJson.USUARIO, "icu").orElse("");
		}
		if (hasSicu) {
			userId = jsonvalida.obtenerSiExiste(event, CamposJson.USUARIO, "sicu").orElse("");
		}

		item.put("objectId", userId);
		item.put("type", "profile");

		Map<String, Object> profileData = new HashMap<>();
		profileData.put("Nombre", jsonvalida
				.obtenerSiExiste(event, CamposJson.EVENTO, "0", CamposJson.EVENTOPROPIEDAD, "nombre").orElse(NA));
		if (jsonvalida.validarCampoExistente(event, CamposJson.EVENTO, "0", CamposJson.EVENTOPROPIEDAD, "correo")) {
			profileData.put("Correo", jsonvalida
					.obtenerSiExiste(event, CamposJson.EVENTO, "0", CamposJson.EVENTOPROPIEDAD, "correo").orElse(NA));
		}
		if (jsonvalida.validarCampoExistente(event, CamposJson.EVENTO, "0", CamposJson.EVENTOPROPIEDAD,
				IDOFERTACREDITO)) {
			profileData.put(IDOFERTACREDITO,
					jsonvalida
							.obtenerSiExiste(event, CamposJson.EVENTO, "0", CamposJson.EVENTOPROPIEDAD, IDOFERTACREDITO)
							.orElse(NA));
		}
		if (jsonvalida.validarCampoExistente(event, CamposJson.EVENTO, "0", CamposJson.EVENTOPROPIEDAD, SEGMENTO)) {
			profileData.put(SEGMENTO, jsonvalida
					.obtenerSiExiste(event, CamposJson.EVENTO, "0", CamposJson.EVENTOPROPIEDAD, SEGMENTO).orElse(NA));
		}

		item.put("profileData", profileData);

		dArray.add(item);

		root.put("d", dArray);
		return root;
	}

	private String homologacionNombre(String eventOrigin, String alias) {
		StringBuilder nombrenuevo = new StringBuilder();

		switch (eventOrigin) {
		case "pageview":
			nombrenuevo.append("flow_start");
			break;
		case "purchase":
			nombrenuevo.append("Charged");
			break;

		default:
			nombrenuevo.append(eventOrigin);
			if (alias != null && !alias.isEmpty()) {
				nombrenuevo.append("_");
				nombrenuevo.append(alias);
			}
			break;
		}

		return nombrenuevo.toString();
	}

}
