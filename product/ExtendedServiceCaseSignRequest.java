package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.model.ServiceCaseSign;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.Objects;

/**
 * Adds additional fields to the model's ServiceCaseSign request.
 *
 * @author m314029
 * @since 1.25.0
 */
@Getter
@Setter
@Accessors(chain = true)
@ToString(callSuper = true)
public class ExtendedServiceCaseSignRequest extends ServiceCaseSign {

	private Long productId;
	private String tagType;
	private String userId;

	/**
	 * Returns a new ExtendedServiceCaseSignRequest.
	 *
	 * @param productId The product the request is for.
	 * @param userId The user who initiated the request.
	 * @return The created object.
	 */
	public static ExtendedServiceCaseSignRequest of(long productId, String userId) {

		return new ExtendedServiceCaseSignRequest()
				.setProductId(productId)
				.setUserId(userId);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ExtendedServiceCaseSignRequest)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		ExtendedServiceCaseSignRequest that = (ExtendedServiceCaseSignRequest) o;
		return productId.equals(that.productId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), productId);
	}
}
