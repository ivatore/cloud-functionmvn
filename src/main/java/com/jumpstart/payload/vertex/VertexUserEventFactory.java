package com.jumpstart.payload.vertex;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.google.cloud.retail.v2.PriceInfo;
import com.google.cloud.retail.v2.Product;
import com.google.cloud.retail.v2.ProductDetail;
import com.google.cloud.retail.v2.PurchaseTransaction;
import com.google.cloud.retail.v2.UserEvent;
import com.google.cloud.retail.v2.UserInfo;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.jumpstart.entity.enums.CamposJson;
import com.jumpstart.utils.JsonUtilsEvent;

@Service
public class VertexUserEventFactory {

	private static final String EXPERIMENTIDS = "origen";
	private static final String FECHAHORAREGIST = "fechaHoraRegistro";
	private static final String IDCARRITO = "idCarrito";
	private static final String ATTRTOKEN = "attributionToken";
	private static final String CANTIDAD = "cantidad";

	private final JsonUtilsEvent jsonUtilsEvent;

	public VertexUserEventFactory(JsonUtilsEvent jsonUtilsEvent) {
		this.jsonUtilsEvent = jsonUtilsEvent;
	}

	private Timestamp parseTimestamp(String isoDateTime) {
		Instant instant = Instant.parse(isoDateTime);
		return Timestamps.fromMillis(instant.toEpochMilli());
	}

	public UserEvent buildAddToCartEvent(JsonObject json, String nombreEvento) {

		JsonObject usuario = json.getAsJsonObject(CamposJson.USUARIO);
		JsonObject evento = json.getAsJsonArray(CamposJson.EVENTO).get(0).getAsJsonObject();
		JsonObject parametros = evento.getAsJsonObject(CamposJson.EVENTOPARAMETRO);

		String eventType = nombreEvento;
		String visitorId = jsonUtilsEvent.obtenerSiExiste(usuario, "sicu")
				.orElse(jsonUtilsEvent.obtenerSiExiste(usuario, "icu").orElse("NA"));
		Timestamp eventTime = parseTimestamp(
				jsonUtilsEvent.obtenerSiExiste(evento, FECHAHORAREGIST).orElse(Instant.now().toString()));
		List<String> experimentIds = new ArrayList<>();
		experimentIds.add(jsonUtilsEvent.obtenerSiExiste(parametros, EXPERIMENTIDS).orElse("NA"));
		String attributionToken = jsonUtilsEvent.obtenerSiExiste(parametros, ATTRTOKEN)
				.orElse(jsonUtilsEvent.obtenerSiExiste(evento, CamposJson.EVENTOPRODUCTO, "0", ATTRTOKEN).orElse("NA"));
		String cartId = jsonUtilsEvent.obtenerSiExiste(parametros, IDCARRITO).orElse("NA");
		String userId = visitorId;

		String productId = jsonUtilsEvent.obtenerSiExiste(evento, CamposJson.EVENTOPRODUCTO, "0", "id").orElse("NA");

		Int32Value quantity = Int32Value.of(Integer.parseInt(
				jsonUtilsEvent.obtenerSiExiste(evento, CamposJson.EVENTOPRODUCTO, "0", CANTIDAD).orElse("0")));

		ProductDetail detail = ProductDetail.newBuilder().setProduct(Product.newBuilder().setId(productId).build())
				.setQuantity(quantity).build();

		UserInfo userInfo = UserInfo.newBuilder().setUserId(userId).build();

		return UserEvent.newBuilder().setEventType(eventType).setVisitorId(visitorId).setEventTime(eventTime)
				.addAllExperimentIds(experimentIds).setAttributionToken(attributionToken).addProductDetails(detail)
				.setCartId(cartId).setUserInfo(userInfo).build();
	}

	public UserEvent buildDetailPageViewEvent(JsonObject json, String nombreEvento) {

		JsonObject usuario = json.getAsJsonObject(CamposJson.USUARIO);
		JsonObject evento = json.getAsJsonArray(CamposJson.EVENTO).get(0).getAsJsonObject();
		JsonObject parametros = evento.getAsJsonObject(CamposJson.EVENTOPARAMETRO);

		String eventType = nombreEvento;
		String visitorId = jsonUtilsEvent.obtenerSiExiste(usuario, "sicu")
				.orElse(jsonUtilsEvent.obtenerSiExiste(usuario, "icu").orElse("NA"));
		Timestamp eventTime = parseTimestamp(
				jsonUtilsEvent.obtenerSiExiste(evento, FECHAHORAREGIST).orElse(Instant.now().toString()));
		List<String> experimentIds = new ArrayList<>();
		experimentIds.add(jsonUtilsEvent.obtenerSiExiste(parametros, EXPERIMENTIDS).orElse("NA"));
		String attributionToken = jsonUtilsEvent.obtenerSiExiste(parametros, ATTRTOKEN)
				.orElse(jsonUtilsEvent.obtenerSiExiste(evento, CamposJson.EVENTOPRODUCTO, "0", ATTRTOKEN).orElse("NA"));
		String userId = visitorId;
		String productId = jsonUtilsEvent.obtenerSiExiste(evento, CamposJson.EVENTOPRODUCTO, "0", "id").orElse("NA");

		ProductDetail detail = ProductDetail.newBuilder().setProduct(Product.newBuilder().setId(productId).build())
				.build();

		UserInfo userInfo = UserInfo.newBuilder().setUserId(userId).build();

		return UserEvent.newBuilder().setEventType(eventType).setVisitorId(visitorId).setEventTime(eventTime)
				.addAllExperimentIds(experimentIds).setAttributionToken(attributionToken).addProductDetails(detail)
				.setUserInfo(userInfo).build();
	}

	public UserEvent buildHomePageViewEvent(JsonObject json, String nombreEvento) {

		JsonObject usuario = json.getAsJsonObject(CamposJson.USUARIO);
		JsonObject evento = json.getAsJsonArray(CamposJson.EVENTO).get(0).getAsJsonObject();

		String eventType = nombreEvento;
		String visitorId = jsonUtilsEvent.obtenerSiExiste(usuario, "sicu")
				.orElse(jsonUtilsEvent.obtenerSiExiste(usuario, "icu").orElse("NA"));
		Timestamp eventTime = parseTimestamp(
				jsonUtilsEvent.obtenerSiExiste(evento, FECHAHORAREGIST).orElse(Instant.now().toString()));
		String userId = visitorId;

		UserInfo userInfo = UserInfo.newBuilder().setUserId(userId).build();

		return UserEvent.newBuilder().setEventType(eventType).setVisitorId(visitorId).setEventTime(eventTime)
				.setUserInfo(userInfo).build();
	}

	public UserEvent buildSearchEvent(JsonObject json, String nombreEvento) {

		JsonObject usuario = json.getAsJsonObject(CamposJson.USUARIO);
		JsonObject evento = json.getAsJsonArray(CamposJson.EVENTO).get(0).getAsJsonObject();
		JsonObject parametros = evento.getAsJsonObject(CamposJson.EVENTOPARAMETRO);
		JsonArray productos = evento.getAsJsonArray(CamposJson.EVENTOPRODUCTO);

		String eventType = nombreEvento;

		String visitorId = jsonUtilsEvent.obtenerSiExiste(usuario, "sicu")
				.orElse(jsonUtilsEvent.obtenerSiExiste(usuario, "icu").orElse("NA"));
		Timestamp eventTime = parseTimestamp(
				jsonUtilsEvent.obtenerSiExiste(evento, FECHAHORAREGIST).orElse(Instant.now().toString()));
		List<String> experimentIds = new ArrayList<>();
		experimentIds.add(jsonUtilsEvent.obtenerSiExiste(parametros, EXPERIMENTIDS).orElse("NA"));
		String attributionToken = jsonUtilsEvent.obtenerSiExiste(parametros, ATTRTOKEN)
				.orElse(jsonUtilsEvent.obtenerSiExiste(evento, CamposJson.EVENTOPRODUCTO, "0", ATTRTOKEN).orElse("NA"));
		String userId = visitorId;

		String searchQuery = jsonUtilsEvent.obtenerSiExiste(parametros, "terminoBuscado").orElse("NA");

		JsonArray products = productos;
		List<ProductDetail> details = new ArrayList<>();
		for (int i = 0; i < products.size(); i++) {
			String productId = jsonUtilsEvent.obtenerSiExiste(evento, CamposJson.EVENTOPRODUCTO, i + "", "id")
					.orElse("0");
			details.add(ProductDetail.newBuilder().setProduct(Product.newBuilder().setId(productId).build()).build());
		}

		UserInfo userInfo = UserInfo.newBuilder().setUserId(userId).build();

		return UserEvent.newBuilder().setEventType(eventType).setVisitorId(visitorId).setSearchQuery(searchQuery)
				.setEventTime(eventTime).addAllExperimentIds(experimentIds).setAttributionToken(attributionToken)
				.addAllProductDetails(details).setUserInfo(userInfo).build();
	}

	public UserEvent buildShoppingCartPageViewEvent(JsonObject json, String nombreEvento) {

		JsonObject usuario = json.getAsJsonObject(CamposJson.USUARIO);
		JsonObject evento = json.getAsJsonArray(CamposJson.EVENTO).get(0).getAsJsonObject();
		JsonObject parametros = evento.getAsJsonObject(CamposJson.EVENTOPARAMETRO);
		JsonArray productos = evento.getAsJsonArray(CamposJson.EVENTOPRODUCTO);

		String eventType = nombreEvento;

		String visitorId = jsonUtilsEvent.obtenerSiExiste(usuario, "sicu")
				.orElse(jsonUtilsEvent.obtenerSiExiste(usuario, "icu").orElse("NA"));
		Timestamp eventTime = parseTimestamp(
				jsonUtilsEvent.obtenerSiExiste(evento, FECHAHORAREGIST).orElse(Instant.now().toString()));
		List<String> experimentIds = new ArrayList<>();
		experimentIds.add(jsonUtilsEvent.obtenerSiExiste(parametros, EXPERIMENTIDS).orElse("NA"));
		String attributionToken = jsonUtilsEvent.obtenerSiExiste(parametros, ATTRTOKEN)
				.orElse(jsonUtilsEvent.obtenerSiExiste(evento, CamposJson.EVENTOPRODUCTO, "0", ATTRTOKEN).orElse("NA"));
		String cartId = jsonUtilsEvent.obtenerSiExiste(parametros, IDCARRITO).orElse("NA");
		String userId = visitorId;

		JsonArray products = productos;
		List<ProductDetail> details = new ArrayList<>();
		for (int i = 0; i < products.size(); i++) {
			JsonObject item = products.get(i).getAsJsonObject();
			String productId = jsonUtilsEvent.obtenerSiExiste(item, "id").orElse("0");
			Int32Value quantity = Int32Value
					.of(Integer.parseInt(jsonUtilsEvent.obtenerSiExiste(item, CANTIDAD).orElse("0")));
			details.add(ProductDetail.newBuilder().setProduct(Product.newBuilder().setId(productId).build())
					.setQuantity(quantity).build());
		}

		UserInfo userInfo = UserInfo.newBuilder().setUserId(userId).build();

		return UserEvent.newBuilder().setEventType(eventType).setVisitorId(visitorId).setEventTime(eventTime)
				.addAllExperimentIds(experimentIds).setAttributionToken(attributionToken).addAllProductDetails(details)
				.setCartId(cartId).setUserInfo(userInfo).build();
	}

	public UserEvent buildPurchaseCompleteEvent(JsonObject json, String nombreEvento) {

		JsonObject usuario = json.getAsJsonObject(CamposJson.USUARIO);
		JsonObject evento = json.getAsJsonArray(CamposJson.EVENTO).get(0).getAsJsonObject();
		JsonObject parametros = evento.getAsJsonObject(CamposJson.EVENTOPARAMETRO);
		JsonArray productos = evento.getAsJsonArray(CamposJson.EVENTOPRODUCTO);

		String eventType = nombreEvento;

		String visitorId = jsonUtilsEvent.obtenerSiExiste(usuario, "sicu")
				.orElse(jsonUtilsEvent.obtenerSiExiste(usuario, "icu").orElse("NA"));
		Timestamp eventTime = parseTimestamp(
				jsonUtilsEvent.obtenerSiExiste(evento, FECHAHORAREGIST).orElse(Instant.now().toString()));
		List<String> experimentIds = new ArrayList<>();
		experimentIds.add(jsonUtilsEvent.obtenerSiExiste(parametros, EXPERIMENTIDS).orElse("NA"));
		String attributionToken = jsonUtilsEvent.obtenerSiExiste(parametros, ATTRTOKEN)
				.orElse(jsonUtilsEvent.obtenerSiExiste(evento, CamposJson.EVENTOPRODUCTO, "0", ATTRTOKEN).orElse("NA"));
		String cartId = jsonUtilsEvent.obtenerSiExiste(parametros, IDCARRITO).orElse("NA");
		String userId = visitorId;

		JsonArray products = productos;
		List<ProductDetail> details = new ArrayList<>();
		for (int i = 0; i < products.size(); i++) {
			JsonObject item = products.get(i).getAsJsonObject();
			String productId = jsonUtilsEvent.obtenerSiExiste(item, "id").orElse("0");
			float price = Float.parseFloat(jsonUtilsEvent.obtenerSiExiste(item, "precio").orElse("0.00"));
			String currency = jsonUtilsEvent.obtenerSiExiste(item, "moneda").orElse("MXN");
			Int32Value quantity = Int32Value
					.of(Integer.parseInt(jsonUtilsEvent.obtenerSiExiste(item, CANTIDAD).orElse("0")));

			ProductDetail detail = ProductDetail.newBuilder()
					.setProduct(Product.newBuilder().setId(productId)
							.setPriceInfo(PriceInfo.newBuilder().setPrice(price).setCurrencyCode(currency).build())
							.build())
					.setQuantity(quantity).build();

			details.add(detail);
		}

		float revenue = Float.parseFloat(jsonUtilsEvent.obtenerSiExiste(parametros, "montoTotal").orElse("0.00"));

		String currencyCode = jsonUtilsEvent.obtenerSiExiste(parametros, "moneda").orElse("MXN");

		PurchaseTransaction transaction = PurchaseTransaction.newBuilder().setRevenue(revenue)
				.setCurrencyCode(currencyCode).build();

		UserInfo userInfo = UserInfo.newBuilder().setUserId(userId).build();

		return UserEvent.newBuilder().setEventType(eventType).setVisitorId(visitorId).setEventTime(eventTime)
				.addAllExperimentIds(experimentIds).setAttributionToken(attributionToken).addAllProductDetails(details)
				.setCartId(cartId).setPurchaseTransaction(transaction).setUserInfo(userInfo).build();
	}

	private List<String> jsonArrayToList(JsonArray array) {
		List<String> list = new ArrayList<>();
		if (array != null) {
			array.forEach(e -> list.add(e.getAsString()));
		}
		return list;
	}

}