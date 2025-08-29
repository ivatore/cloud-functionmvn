package com.jumpstart.service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.retail.v2.UserEvent;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jumpstart.entity.enums.CamposJson;
import com.jumpstart.payload.vertex.VertexUserEventFactory;
import com.jumpstart.utils.JsonToJsonlUtils;
import com.jumpstart.utils.JsonUtilsEvent;

public class EventoProcessorService {
	private static final Logger log = Logger.getLogger(EventoProcessorService.class.getName());

	private final ObjectMapper mapper = new ObjectMapper();

	private static final String BUCKETNAME = System.getenv("BUCKET_NAME");
	private static final String SATELITES_STATUS = System.getenv("SATELITES_STATUS");

	private static final String PURCHASE = "purchase";
	private static final String NO_PURCHASE = "no_purchase";

	private final Storage storage;
	private final CleverTapService cleverTapService;
	private final AppsFlyerService appsFlyerService;
	private final JsonUtilsEvent jsonvalida;
	private final JsonToJsonlUtils jsonlUtils;
	private final ParquetServicesolito parquetService;
	private final ParquetPayloadService parquetPayloadService;
	private final VertexUserEventFactory eventFactory;
	private final VertexService vertexService;

	public EventoProcessorService(CleverTapService cleverTapService, AppsFlyerService appsFlyerService, Storage storage,
			JsonUtilsEvent jsonvalida, JsonToJsonlUtils jsonlUtils, ParquetServicesolito parquetService,
			ParquetPayloadService parquetPayloadService, VertexUserEventFactory eventFactory,
			VertexService vertexService) {
		this.cleverTapService = cleverTapService;
		this.appsFlyerService = appsFlyerService;
		this.storage = storage;
		this.jsonvalida = jsonvalida;
		this.jsonlUtils = jsonlUtils;
		this.parquetService = parquetService;
		this.parquetPayloadService = parquetPayloadService;
		this.eventFactory = eventFactory;
		this.vertexService = vertexService;
	}

	public void procesarMensaje(String json) {
		try {
			JsonNode original = mapper.readTree(json);
			JsonNode normalizado = jsonlUtils.normalizarClaves(original);
			JsonNode root = mapper.readTree(mapper.writeValueAsString(normalizado));

			Optional<String> optionalNombreEvento = jsonvalida.obtenerValorCampo(root, CamposJson.EVENTO, "0",
					"nombre");
			if (optionalNombreEvento.isEmpty()) {
				log.warning("El campo 'nombre' no está presente en el evento.");
				return;
			}

			String nombreEvento = optionalNombreEvento.get();
			boolean esCompra = PURCHASE.equals(nombreEvento);
			JsonObject event = JsonParser.parseString(json).getAsJsonObject();
			JsonObject evento = event.getAsJsonArray(CamposJson.EVENTO).get(0).getAsJsonObject();

			enviarAVertex(nombreEvento, event);

			saveMessageDataToStorage(json, nombreEvento, PURCHASE, false);

			if (!jsonvalida.validarCampoExistente(root, CamposJson.EVENTO, "0", "registroPlataforma", "0"))
				return;

			JsonArray plataformas = evento.getAsJsonArray("registroPlataforma");
			if (plataformas == null || plataformas.size() == 0) {
				log.warning("Plataformas nulas o vacías.");
				return;
			}

			Set<String> plataformasSet = new HashSet<>();
			plataformas.forEach(p -> plataformasSet.add(p.getAsString().toLowerCase()));

			if (plataformasSet.contains("clevertap") && SATELITES_STATUS.toLowerCase().contains("clevertap")) {
				enviarACleverTap(event, evento, nombreEvento, esCompra);
			} else if (!SATELITES_STATUS.toLowerCase().contains("clevertap")) {
				log.info("Satelite CleverTap OFF");
			}

			if (plataformasSet.stream().anyMatch(p -> p.contains("appsflyer"))
					&& SATELITES_STATUS.toLowerCase().contains("appsflyer")) {
				enviarAAppsFlyer(event, nombreEvento, esCompra);
			} else if (!SATELITES_STATUS.toLowerCase().contains("appsflyer")) {
				log.info("Satelite AppsFlyer OFF");
			}

		} catch (

		Exception ex) {
			log.info("Error al procesar mensaje: {}" + ex.getMessage() + ex);
		}
	}

	private void enviarAVertex(String nombreEvento, JsonObject event) {
		try {
			UserEvent eventUser;
			String vertexName = switch (nombreEvento) {
			case "add_to_cart" -> "add-to-cart";
			case "view_item" -> "detail-page-view";
			case "screen_view" -> "home-page-view";
			case "view_item_list" -> "search";
			case "view_cart" -> "shopping-cart-page-view";
			case "purchase" -> "purchase-complete";
			default -> "";
			};

			eventUser = switch (vertexName) {
			case "add-to-cart" -> eventFactory.buildAddToCartEvent(event, vertexName);
			case "detail-page-view" -> eventFactory.buildDetailPageViewEvent(event, vertexName);
			case "home-page-view" -> eventFactory.buildHomePageViewEvent(event, vertexName);
			case "search" -> eventFactory.buildSearchEvent(event, vertexName);
			case "shopping-cart-page-view" -> eventFactory.buildShoppingCartPageViewEvent(event, vertexName);
			case "purchase-complete" -> eventFactory.buildPurchaseCompleteEvent(event, vertexName);
			default -> UserEvent.getDefaultInstance();
			};

			if (SATELITES_STATUS.toLowerCase().contains("vertex") && !vertexName.isBlank()) {
				log.info("Evento generado a vertex: " + vertexName + eventUser.toString());
				vertexService.envioUserEventServiceClient(eventUser);
			} else if (!SATELITES_STATUS.toLowerCase().contains("vertex")) {
				log.info("Satelite Vertex OFF");
			}
		} catch (Exception e) {
			log.severe("Evento vertex malformado: {} {}" + e.getMessage()
					+ jsonvalida.obtenerSiExiste(event, CamposJson.USUARIO, "icu")
							.orElse(jsonvalida.obtenerSiExiste(event, CamposJson.USUARIO, "sicu").orElse("NA")));
		}
	}

	private void enviarACleverTap(JsonObject event, JsonObject evento, String nombreEvento, boolean esCompra) {
		String payload = cleverTapService.enviarEvento(event);
		saveMessageDataToStorageSegmento(payload, nombreEvento, esCompra ? PURCHASE : NO_PURCHASE, true, "clevertap");

		if (jsonvalida.validarCampoExistente(evento, "propiedades", "nombre")) {
			String profile = cleverTapService.enviarUserProfile(event);
			saveMessageDataToStorage(profile, nombreEvento, "usuario", true);
		}
	}

	private void enviarAAppsFlyer(JsonObject event, String nombreEvento, boolean esCompra) {
		String payload = appsFlyerService.enviarEvento(event);
		saveMessageDataToStorageSegmento(payload, nombreEvento, esCompra ? PURCHASE : NO_PURCHASE, true, "appsflyer");
	}

	private void saveMessageDataToStorage(String data, String evento, String tipo, boolean isPayload) {
		saveMessageData(data, evento, tipo, isPayload, null);
	}

	private void saveMessageDataToStorageSegmento(String data, String evento, String tipo, boolean isPayload,
			String segmento) {
		saveMessageData(data, evento, tipo, isPayload, segmento);
	}

	private void saveMessageData(String data, String evento, String tipo, boolean isPayload, String segmento) {
		ZoneId zona = ZoneId.of("America/Mexico_City");
		LocalDate fecha = LocalDate.now(zona);

		String rutaBase = buildRuta(tipo, evento, segmento, isPayload, fecha);
		String nombreArchivo = String.format("%s-%s.parquet", evento, UUID.randomUUID());
		String rutaCompleta = rutaBase + nombreArchivo;

		log.info("Ruta construida: {}" + rutaCompleta);
		BlobId blobId = BlobId.of(Optional.ofNullable(BUCKETNAME).orElse("jumpstart_events_raw_prod"), rutaCompleta);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();

		try {
			byte[] bytes = null;
			switch (tipo) {
			case PURCHASE, NO_PURCHASE:
				if (segmento == null) {
					bytes = parquetService.processJsonAndWriteParquet(data);
					break;
				}
				if ("clevertap".equals(segmento)) {
					bytes = parquetPayloadService.processClaverJsonToBytes(data);
				} else if ("appflyer".equals(segmento)||"appsflyer".equals(segmento)) {
					bytes = parquetPayloadService.processAppsflyerJsonToBytes(data);
				}
				break;
			case "usuario":
				bytes = parquetPayloadService.processUsuarioJsonToBytes(data);
				break;
			default:
				break;
			}
			storage.create(blobInfo, bytes);
		} catch (Exception e) {
			log.severe("Error al convertir a Parquet, se guarda JSON como fallback: {}" + e.getMessage() + e);
			if (Optional.ofNullable(data).isPresent()) {
				storage.create(BlobInfo
						.newBuilder(BlobId.of(Optional.ofNullable(BUCKETNAME).orElse("jumpstart_events_raw_prod"),
								rutaCompleta.replace(".parquet", ".json")))
						.setContentType("application/json").build(), data.getBytes(StandardCharsets.UTF_8));
			}

		}
	}

	private static String buildRuta(String tipo, String evento, String segmento, boolean isPayload, LocalDate fecha) {
		String prefix = isPayload ? "Payloads/" : "events_raw/";

		if (segmento != null && !segmento.isEmpty()) {
			return String.format("%s%s/year=%d/month=%02d/day=%02d/", prefix, segmento, fecha.getYear(),
					fecha.getMonthValue(), fecha.getDayOfMonth());
		}
		if (List.of("usuario", PURCHASE, NO_PURCHASE).contains(tipo)) {
			if (List.of(PURCHASE, NO_PURCHASE).contains(tipo)) {
				return String.format("%s%s/month=%02d/day=%02d/", prefix, "year=" + fecha.getYear(),
						fecha.getMonthValue(), fecha.getDayOfMonth());
			}
			return String.format("%s%s/year=%d/month=%02d/day=%02d/", prefix, tipo, fecha.getYear(),
					fecha.getMonthValue(), fecha.getDayOfMonth());
		}
		return String.format("%s%s/month=%02d/day=%02d/", prefix, "year=" + fecha.getYear(), fecha.getMonthValue(),
				fecha.getDayOfMonth());
	}

}
