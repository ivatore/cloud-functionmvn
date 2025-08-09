package com.jumpstart.service;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
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
public class ParquetServicesolito {

	private static final ObjectMapper mapper = new ObjectMapper();

	public byte[] processJsonAndWriteParquet(String json) throws IOException {
		JsonNode root = mapper.readTree(json);
		root = normalizarClaves(root);
		JsonNode headers = root.get("headers");
		JsonNode usuario = root.get("usuario");
		JsonNode eventos = root.get("eventos").get(0);

		if (headers == null || usuario == null || eventos == null) {
			throw new IllegalArgumentException("JSON debe contener 'headers', 'usuario', y 'eventos'");
		}

		// Merge nodes
		JsonNode merged = mergeAndFlatten(headers, usuario, eventos);
		Schema schema = inferSchema(merged);
		GenericRecord recordParquet = createRecord(merged, schema);

		InMemoryOutputFile outputFile = new InMemoryOutputFile();

		try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
				.withSchema(schema).build()) {
			writer.write(recordParquet);
		}

		return outputFile.getContent();
	}

	public String processJsonAndWriteParquetLocal(String json) throws IOException {
		JsonNode root = mapper.readTree(json);
		root = normalizarClaves(root);
		JsonNode headers = root.get("headers");
		JsonNode usuario = root.get("usuario");
		JsonNode eventos = root.get("eventos").get(0);

		if (headers == null || usuario == null || eventos == null) {
			throw new IllegalArgumentException("JSON debe contener 'headers', 'usuario', y 'eventos'");
		}

		JsonNode merged = mergeAndFlatten(headers, usuario, eventos);
		Schema schema = inferSchema(merged);
		GenericRecord recordParquet = createRecord(merged, schema);

		String outputPath = "output_" + System.currentTimeMillis() + ".parquet";

		try (@SuppressWarnings("deprecation")
		ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(outputPath))
				.withSchema(schema).build()) {
			writer.write(recordParquet);
		}

		return new File(outputPath).getAbsolutePath();
	}

	private JsonNode mergeAndFlatten(JsonNode... nodes) {
		ObjectNode result = mapper.createObjectNode();
		for (JsonNode node : nodes) {
			flatten("", node, result);
		}
		return result;
	}

	private void flatten(String prefix, JsonNode node, ObjectNode result) {
		if (node.isObject()) {
			node.fields().forEachRemaining(entry -> {
				String key = prefix.isEmpty() ? entry.getKey() : prefix + "_" + entry.getKey();
				flatten(key, entry.getValue(), result);
			});
		} else if (node.isArray()) {
			result.put(prefix, node.toString()); // for arrays of String/Json
		} else {
			result.put(prefix, node.asText());
		}
	}

	private Schema inferSchema(JsonNode jsonNode) {
		SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("EventRecord").namespace("example").fields();

		jsonNode.fields().forEachRemaining(entry -> {
			fields.name(entry.getKey()).type().nullable().stringType().noDefault();
		});

		return fields.endRecord();
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

	private GenericRecord createRecord(JsonNode node, Schema schema) {
		GenericRecord recordParquet = new GenericData.Record(schema);
		schema.getFields().forEach(field -> {
			JsonNode value = node.get(field.name());
			if (value != null && !value.isNull()) {
				recordParquet.put(field.name(), value.asText());
			}
		});
		return recordParquet;
	}
}