package com.jumpstart;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.commons.io.IOUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jumpstart.entity.enums.CamposJson;
import com.jumpstart.service.ParquetPayloadService;
import com.jumpstart.service.payload.PayloadAppFlyer;
import com.jumpstart.service.payload.PayloadClaverTap;
import com.jumpstart.utils.JsonToJsonlUtils;
import com.jumpstart.utils.JsonUtilsEvent;

public class Local {

	public static void main(String[] args) throws Exception {
		InputStream is = new FileInputStream("hola-mundo.json");
//		InputStream is = new FileInputStream("clever-no-p.json");
//		InputStream is = new FileInputStream("add_icu.json");
		String jsonTxt = IOUtils.toString(is, "UTF-8");

		JsonObject event = JsonParser.parseString(jsonTxt).getAsJsonObject();

		JsonUtilsEvent jsonu = new JsonUtilsEvent();
		String visitorId = jsonu.obtenerSiExiste(event.getAsJsonObject(CamposJson.USUARIO), "sicu").get();
		System.out.println(visitorId.isEmpty());
//		TestParquetAppsFlyer(jsonTxt);
//		TestParquetCleverTap(jsonTxt);
	}

	public static void saveParquetBytes(byte[] parquetBytes, Path out) throws Exception {
		if (out.getParent() != null) {
			Files.createDirectories(out.getParent());
		}
		Files.write(out, parquetBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
				StandardOpenOption.WRITE);
	}

	static void TestParquetAppsFlyer(String json) {
		try {
			PayloadAppFlyer payloadAppFlyer = new PayloadAppFlyer(new JsonUtilsEvent(), new JsonToJsonlUtils());
			ParquetPayloadService parquetPayloadService = new ParquetPayloadService();
			JsonObject event = JsonParser.parseString(json).getAsJsonObject();
			String payload = payloadAppFlyer.construirPayloadAppsFlyer(event);
			System.out.println(payload);
			byte[] parquetBytes = parquetPayloadService.processAppsflyerJsonToBytes(payload);
			Path destino = Path.of("BORRAR/outapp.parquet");
			saveParquetBytes(parquetBytes, destino);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	static void TestParquetCleverTap(String json) {
		try {

			PayloadClaverTap payloadClaverTap = new PayloadClaverTap(new JsonUtilsEvent());
			JsonObject event = JsonParser.parseString(json).getAsJsonObject();
			System.out.println(event.toString());

			ParquetPayloadService parquetPayloadService = new ParquetPayloadService();
			String payload = payloadClaverTap.construirPayloadChargedEventCleverTap(event);
//			String payload = payloadClaverTap.construirPayloadGeneralEventCleverTap(event);

			byte[] parquetBytes = parquetPayloadService.processClaverJsonToBytes(payload);
			Path destino = Path.of("BORRAR/out.parquet");
			saveParquetBytes(parquetBytes, destino);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
