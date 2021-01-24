package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.dao.core.entity.ProductDiscountThreshold;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.LinkedList;
import java.util.List;

/**
 * Extends ProductMaintenanceRequest with information needed for processing.
 *
 * @author d116773
 * @since 1.25.0
 */
@Getter
@Setter
@ToString(callSuper = true)
@Accessors(chain = true)
public class ExtendedProductMaintenanceRequest extends ProductMaintenanceRequest {

	private Long productId;

	@Setter(AccessLevel.PRIVATE)
	private List<ProductDiscountThreshold> existingDiscountThresholds = new LinkedList<>();
}
