package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.core.maintenance.ProductCustomHierarchyRequest;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Objects;

/**
 * Extends ProductCustomHierarchyRequest with additional properties needed by the service.
 *
 * @author d116773
 * @since 1.27.0
 */
@Getter
@Setter
@Accessors(chain = true)
public class ExtendedProductCustomHierarchyRequest extends ProductCustomHierarchyRequest {

	private String hierarchyContext;

	/**
	 * Makes an ExtendedProductCustomHierarchyRequest from a ProductCustomHierarchyRequest and hierarchy context.
	 *
	 * @param originalRequest The ProductCustomHierarchyRequest to copy into a new request.
	 * @param hierarchyContext The hierarchy context to add to the new request.
	 * @return The new ExtendedProductCustomHierarchyRequest.
	 */
	public static ExtendedProductCustomHierarchyRequest from(ProductCustomHierarchyRequest originalRequest, String hierarchyContext) {

		ExtendedProductCustomHierarchyRequest newRequest = new ExtendedProductCustomHierarchyRequest().setHierarchyContext(hierarchyContext);

		if (Objects.nonNull(originalRequest)) {
			newRequest.setId(originalRequest.getId())
					.setIdType(originalRequest.getIdType())
					.setParentHierarchyLevel(originalRequest.getParentHierarchyLevel())
					.setUserId(originalRequest.getUserId());
		}
		return newRequest;
	}
}
