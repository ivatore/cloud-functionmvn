package com.jumpstart.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jumpstart.utils.InMemoryOutputFile;

@Service
public class ParquetService {
	private static final ObjectMapper mapper = new ObjectMapper();

	public byte[] processJsonAndWriteParquetToBytes(String json) throws IOException {
		JsonNode root = mapper.readTree(json);
		JsonNode headers = root.get("headers");
		JsonNode usuario = root.get("usuario");
		JsonNode eventos = root.get("eventos");

		Schema schema = SchemaBuilder.record("Evento").fields().optionalString("usuario").optionalString("header")
				.optionalString("evento").endRecord();

		InMemoryOutputFile outputFile = new InMemoryOutputFile();

		try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
				.withSchema(schema).build()) {

			for (JsonNode evento : eventos) {
				GenericRecord record = new GenericData.Record(schema);
				record.put("usuario", usuario.toString());
				record.put("header", headers.toString());
				record.put("evento", evento.toString());
				writer.write(record);
			}
		}

		return outputFile.getContent();
	}

	public File processJsonAndWriteParquetToFile(String json) throws IOException {
		JsonNode root = mapper.readTree(json);
		JsonNode headers = root.get("headers");
		JsonNode usuario = root.get("usuario");
		JsonNode eventos = root.get("eventos");

		// Construcción del esquema y data
		Schema schema = SchemaBuilder.record("Evento").fields().optionalString("usuario").optionalString("header")
				.optionalString("evento").endRecord();

		File tempFile = File.createTempFile("data-", ".parquet");
		Path path = new Path(tempFile.getAbsolutePath());

		try (@SuppressWarnings("deprecation")
		ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path).withSchema(schema)
				.build()) {

			for (JsonNode evento : eventos) {
				GenericRecord record = new GenericData.Record(schema);
				record.put("usuario", usuario.toString());
				record.put("header", headers.toString());
				record.put("evento", evento.toString());
				writer.write(record);
			}
		}

		return tempFile;
	}

	@SuppressWarnings({ "unused", "incomplete-switch" })
	private GenericRecord createRecord(JsonNode node, Schema schema) {
		GenericRecord record = new GenericData.Record(schema);
		for (Schema.Field field : schema.getFields()) {
			JsonNode value = node.get(field.name());
			if (value == null || value.isNull())
				continue;

			switch (field.schema().getType()) {
			case STRING -> record.put(field.name(), value.isContainerNode() ? value.toString() : value.asText());
			case LONG -> record.put(field.name(), value.asLong());
			case DOUBLE -> record.put(field.name(), value.asDouble());
			case BOOLEAN -> record.put(field.name(), value.asBoolean());
			case ARRAY -> {
				List<String> list = new ArrayList<>();
				value.forEach(v -> list.add(v.asText()));
				record.put(field.name(), list);
			}
			}
		}
		return record;
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

	@SuppressWarnings("unused")
	private String getText(JsonNode node, String fieldName) {
		JsonNode value = node.get(fieldName);
		return value != null && !value.isNull() ? value.asText() : null;
	}

	@SuppressWarnings("unused")
	private JsonNode merge(JsonNode... nodes) {
		var result = mapper.createObjectNode();
		for (JsonNode node : nodes) {
			result.setAll((ObjectNode) node);
		}
		return result;
	}

	@SuppressWarnings("unused")
	private Schema inferSchema(JsonNode jsonNode) {
		SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("EventRecord").namespace("example").fields();

		jsonNode.fields().forEachRemaining(entry -> {
			String name = entry.getKey();
			JsonNode value = entry.getValue();
			Schema fieldSchema;

			switch (value.getNodeType()) {
			case NUMBER -> fieldSchema = value.isIntegralNumber() ? Schema.create(Schema.Type.LONG)
					: Schema.create(Schema.Type.DOUBLE);
			case BOOLEAN -> fieldSchema = Schema.create(Schema.Type.BOOLEAN);
			case STRING -> fieldSchema = Schema.create(Schema.Type.STRING);
			case ARRAY -> fieldSchema = Schema.createArray(Schema.create(Schema.Type.STRING));
			case OBJECT -> fieldSchema = Schema.create(Schema.Type.STRING); // nested serialized
			default -> fieldSchema = Schema.create(Schema.Type.STRING);
			}

			fields.name(name).type(fieldSchema).noDefault();
		});

		return fields.endRecord();
	}

}
