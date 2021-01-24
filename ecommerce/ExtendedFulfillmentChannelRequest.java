package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.maintenance.FulfillmentChannelRequest;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * Extends FulfillmentChannelRequest from pm-lib-model with additional information
 * that is needed when processing the request.
 *
 * @author m314029
 * @since 1.24.0
 */
@Getter
@Setter
@ToString(callSuper = true)
@Accessors(chain = true)
public final class ExtendedFulfillmentChannelRequest extends FulfillmentChannelRequest {

	private Long productId;
}
