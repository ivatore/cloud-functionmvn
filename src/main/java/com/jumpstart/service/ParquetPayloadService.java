package com.jumpstart.service;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jumpstart.utils.InMemoryOutputFile;

public class ParquetPayloadService {
	private static final ObjectMapper mapper = new ObjectMapper();

	private String getText(JsonNode node, String fieldName) {
		JsonNode value = node.get(fieldName);
		return (value != null && !value.isNull()) ? value.asText() : null;
	}

	public byte[] processClaverJsonToBytes(String json) throws IOException {
		JsonNode root = mapper.readTree(json);
		JsonNode dataList = root.get("d");

		Schema schema = SchemaBuilder.record("ClaverEvent").fields().optionalString("evtName").optionalString("FBID")
				.optionalString("type").optionalString("evtData").endRecord();

		InMemoryOutputFile outputFile = new InMemoryOutputFile();

		try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
				.withSchema(schema).build()) {

			for (JsonNode event : dataList) {
				GenericRecord record = new GenericData.Record(schema);
				record.put("evtName", getText(event, "evtName"));
				record.put("FBID", getText(event, "FBID"));
				record.put("type", getText(event, "type"));
				JsonNode evtDataNode = event.get("evtData");
				String evtDataJson = evtDataNode != null ? mapper.writeValueAsString(evtDataNode) : null;
				record.put("evtData", evtDataJson);
				writer.write(record);
			}
		}
		return outputFile.getContent();
	}

	public byte[] processAppsflyerJsonToBytes(String json) throws IOException {
		JsonNode root = mapper.readTree(json);

		Schema schema = SchemaBuilder.record("AppsflyerEvent").fields().optionalString("eventTime")
				.optionalString("customer_user_id").optionalString("appsflyer_id").optionalString("device_id")
				.optionalString("platform").optionalString("eventCurrency").optionalString("eventName")
				.optionalString("eventValue").endRecord();

		InMemoryOutputFile outputFile = new InMemoryOutputFile();

		try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
				.withSchema(schema).build()) {

			GenericRecord record = new GenericData.Record(schema);
			record.put("eventTime", getText(root, "eventTime"));
			record.put("customer_user_id", getText(root, "customer_user_id"));
			record.put("appsflyer_id", getText(root, "appsflyer_id"));
			record.put("device_id", getText(root, "device_id"));
			record.put("platform", getText(root, "platform"));
			record.put("eventCurrency", getText(root, "eventCurrency"));
			record.put("eventName", getText(root, "eventName"));
			record.put("eventValue", getText(root, "eventValue"));

			writer.write(record);
		}
		return outputFile.getContent();
	}

	public byte[] processUsuarioJsonToBytes(String json) throws IOException {
		JsonNode root = mapper.readTree(json);
		JsonNode dataList = root.get("d");

		Schema schema = SchemaBuilder.record("UsuarioProfile").fields().optionalString("type")
				.optionalString("objectId").optionalString("idOfertaCredito").optionalString("Nombre")
				.optionalString("segmento").optionalString("Correo").endRecord();

		InMemoryOutputFile outputFile = new InMemoryOutputFile();

		try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
				.withSchema(schema).build()) {

			for (JsonNode item : dataList) {
				GenericRecord record = new GenericData.Record(schema);
				record.put("type", getText(item, "type"));
				record.put("objectId", getText(item, "objectId"));

				JsonNode profileData = item.get("profileData");
				if (profileData != null) {
					record.put("idOfertaCredito", getText(profileData, "idOfertaCredito"));
					record.put("Nombre", getText(profileData, "Nombre"));
					record.put("segmento", getText(profileData, "segmento"));
					record.put("Correo", getText(profileData, "Correo"));
				}

				writer.write(record);
			}
		}
		return outputFile.getContent();
	}
}
