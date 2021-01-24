package com.heb.pm.core.endpoint.requests;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * ProductEventRequest.
 */
@Data
@Accessors(chain = true)
public class ProductEventRequest implements EventStagingRequest {

	private String userId;
	private List<Long> productIds;

	@Override
	public List<Long> getIdsToStage() {
		return this.productIds;
	}
}
