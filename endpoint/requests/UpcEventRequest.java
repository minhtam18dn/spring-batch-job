package com.heb.pm.core.endpoint.requests;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * Request object to stage legacy events for UPCs.
 *
 * @author d116773
 * @since 1.9.0
 */
@Data
@Accessors(chain = true)
public class UpcEventRequest implements EventStagingRequest {

	private String userId;
	private List<Long> upcs;

	@Override
	public List<Long> getIdsToStage() {
		return this.upcs;
	}
}
