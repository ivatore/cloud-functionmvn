package com.jumpstart;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.Storage;
import com.jumpstart.config.StorageConfig;
import com.jumpstart.payload.vertex.VertexUserEventFactory;
import com.jumpstart.service.AppsFlyerService;
import com.jumpstart.service.CleverTapService;
import com.jumpstart.service.EventoProcessorService;
import com.jumpstart.service.ParquetPayloadService;
import com.jumpstart.service.ParquetServicesolito;
import com.jumpstart.service.VertexService;
import com.jumpstart.service.payload.PayloadAppFlyer;
import com.jumpstart.service.payload.PayloadClaverTap;
import com.jumpstart.utils.JsonToJsonlUtils;
import com.jumpstart.utils.JsonUtilsEvent;

public class PubSubMessageHandler implements BackgroundFunction<PubSubMessageHandler.PubSubMessage> {

	private static final Logger logger = Logger.getLogger(PubSubMessageHandler.class.getName());
	private final EventoProcessorService processor;

	public PubSubMessageHandler() {
		Storage storage = new StorageConfig().storage();
		JsonToJsonlUtils jsonToJsonlUtils = new JsonToJsonlUtils();
		JsonUtilsEvent jsonUtilsEvent = new JsonUtilsEvent();
		PayloadClaverTap payloadClaverTap = new PayloadClaverTap(jsonUtilsEvent);
		ParquetServicesolito parquetService = new ParquetServicesolito();
		ParquetPayloadService parquetPayloadService = new ParquetPayloadService();
		VertexUserEventFactory eventFactory = new VertexUserEventFactory(jsonUtilsEvent);
		VertexService vertexService = new VertexService();
		CleverTapService cleverTapService = new CleverTapService(payloadClaverTap,jsonUtilsEvent);
		PayloadAppFlyer appFlyer = new PayloadAppFlyer(jsonUtilsEvent, jsonToJsonlUtils);
		AppsFlyerService appsFlyerService = new AppsFlyerService(appFlyer);

		this.processor = new EventoProcessorService(cleverTapService, appsFlyerService, storage, jsonUtilsEvent,
				jsonToJsonlUtils, parquetService, parquetPayloadService, eventFactory, vertexService);
	}

	@Override
	public void accept(PubSubMessage message, Context context) {

		if (message.data == null) {
			logger.warning("Mensaje vacío");
			return;
		}

		String decoded = new String(Base64.getDecoder().decode(message.data), StandardCharsets.UTF_8);
		logger.info("Mensaje recibido: " + decoded);
		processor.procesarMensaje(decoded);

	}

	public static class PubSubMessage {
		public String data;
		public Map<String, String> attributes;
	}
}
