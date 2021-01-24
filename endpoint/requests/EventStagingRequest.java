package com.heb.pm.core.endpoint.requests;

import java.util.List;

/**
 * Interface for classes that repesent requests to stage legacy events.
 *
 * @author d116773
 * @since 1.9.0
 */
public interface EventStagingRequest {

	/**
	 * Returns the ID of the user who triggered this request.
	 *
	 * @return The ID of the user who triggered this request.
	 */
	String getUserId();

	/**
	 * Returns the list of IDs the request is for (products, items, UPCs, etc.).
	 *
	 * @return The list of IDs the request is for.
	 */
	List<Long> getIdsToStage();
}
