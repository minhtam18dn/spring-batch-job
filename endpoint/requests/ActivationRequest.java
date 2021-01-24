package com.heb.pm.core.endpoint.requests;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * Represents a request to kic off activation.
 *
 * @author d116773
 * @since 1.10.0
 */
@Getter
@Setter
@Accessors(chain = true)
@ToString
public class ActivationRequest {

	private Long workRequestId;
	private Long trackingId;
	private String userId;
	private Boolean runAsynchronously;
}
