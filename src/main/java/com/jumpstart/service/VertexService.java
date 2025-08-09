package com.jumpstart.service;

import java.util.logging.Logger;

import org.springframework.stereotype.Service;

import com.google.cloud.retail.v2.CatalogName;
import com.google.cloud.retail.v2.UserEvent;
import com.google.cloud.retail.v2.UserEventServiceClient;
import com.google.cloud.retail.v2.WriteUserEventRequest;
import com.jumpstart.config.AccederSecretGCP;

@Service
public class VertexService {
	private static final Logger log = Logger.getLogger(VertexService.class.getName());

	public void envioUserEventServiceClient(UserEvent userEvent) {
		String parent = CatalogName.of(AccederSecretGCP.obtenerSecret("vertex_projectId"), "global", "default_catalog")
				.toString();
		try (UserEventServiceClient client = UserEventServiceClient.create()) {

			WriteUserEventRequest request = WriteUserEventRequest.newBuilder().setParent(parent).setUserEvent(userEvent)
					.build();
			UserEvent response = client.writeUserEvent(request);
			log.info("Evento enviado:{} " + response);
		} catch (Exception e) {
			log.info("Event error vertex: {}" + e.getMessage());
		}
	}
}
