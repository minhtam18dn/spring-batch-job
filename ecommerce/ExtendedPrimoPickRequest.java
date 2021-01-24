package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.model.PrimoPick;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.Objects;

/**
 * Adds additional fields to the model's PrimoPick request.
 *
 * @author d116773
 * @since 1.23.0
 */
@Getter
@Setter
@Accessors(chain = true)
@ToString(callSuper = true)
public class ExtendedPrimoPickRequest extends PrimoPick {

	private Long productId;
	private boolean makeDistinctive;

	/**
	 * Returns a new ExtendedPrimoPickRequest.
	 *
	 * @param productId The product the request is for.
	 * @param primoPick A PrimoPick to copy the rest of the data from.
	 * @return
	 */
	public static ExtendedPrimoPickRequest of(long productId, PrimoPick primoPick) {

		ExtendedPrimoPickRequest extendedPrimoPickRequest = new ExtendedPrimoPickRequest()
				.setProductId(productId);

		extendedPrimoPickRequest.setEffectiveDate(primoPick.getEffectiveDate())
				.setExpirationDate(primoPick.getExpirationDate());

		return extendedPrimoPickRequest;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ExtendedPrimoPickRequest)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		ExtendedPrimoPickRequest that = (ExtendedPrimoPickRequest) o;
		return productId.equals(that.productId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), productId);
	}
}
