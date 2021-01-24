package com.heb.pm.core.endpoint.requests;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * Request object to stage legacy events for Item Codes.
 *
 * @author d116773
 * @since 1.9.0
 */
@Data
@Accessors(chain = true)
public class ItemEventRequest implements EventStagingRequest {

	private String userId;
	private List<Long> itemCodes;

	@Override
	public List<Long> getIdsToStage() {
		return this.itemCodes;
	}
}
