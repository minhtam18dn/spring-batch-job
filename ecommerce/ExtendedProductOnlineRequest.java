package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.maintenance.ProductOnlineRequest;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * Extends ProductOnlineRequest from pm-lib-model with additional information
 * that is needed when processing the request.
 *
 * @author d116773
 * @since 1.22.0
 */
@Getter
@Setter
@ToString(callSuper = true)
@Accessors(chain = true)
public final class ExtendedProductOnlineRequest extends ProductOnlineRequest {

	private Long productId;
	private Boolean idIsProduct;
}
