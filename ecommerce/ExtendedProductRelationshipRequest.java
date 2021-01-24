package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.maintenance.ProductRelationshipRequest;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.text.DecimalFormat;

/**
 * Extends ProductRelationshipRequest from pm-lib-model with additional information
 * that is needed when processing the request.
 *
 * @author d116773
 * @since 1.23.0
 */
@Getter
@Setter
@Accessors(chain = true)
public class ExtendedProductRelationshipRequest extends ProductRelationshipRequest {

	private Long productId;

	@Override
	public String toString() {

		DecimalFormat df = new DecimalFormat("0.0000");

		return String.format("{%d (%s) %d, %s, %d}", this.productId, this.getRelationshipType(),
				this.getRelatedProductId(), df.format(this.getProductQuantity()), this.getUpc());
	}
}

