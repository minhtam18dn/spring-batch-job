package com.heb.pm.core.endpoint;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.model.ProductOnlineRange;
import com.heb.pm.core.service.maintenance.ecommerce.ExtendedProductOnlineRequest;
import com.heb.pm.core.service.maintenance.ecommerce.ProductOnlineService;
import com.heb.pm.util.security.wsag.ClientInfoService;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Objects;

/**
 * Endpoint for maintaining product online information.
 *
 * @author d116773
 * @since 1.22.0
 */
@RestController()
@RequestMapping("/productOnline")
public class ProductOnlineEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(ProductOnlineEndpoint.class);

	@Autowired
	private transient ProductOnlineService productOnlineService;

	@Autowired
	private transient ClientInfoService clientInfoService;

	/**
	 * Handles the POST request which will modify a product or product group's product online information.
	 *
	 * @param productId The ID of the product or product group to update.
	 * @param productOnlineUpdateRequest The ExtendedProductOnlineRequest with the information the user is sending to perform the update.
	 * @param request The HttpServlet request that initiated this call.
	 * @return The updated list of product online information.
	 */
	@PostMapping("/{productId}")
	@PreAuthorize("hasAuthority('ECOMMERCE')")
	@ApiOperation("Updates the product online information for a product or product group.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK", response = ProductOnlineRange.class),
			@ApiResponse(code = 409, message = "Unable to validate request."),
			@ApiResponse(code = 404, message = "Not Found.")})
	public List<ProductOnlineRange> post(@ApiParam("The ID of the product or product group to update.") @PathVariable("productId") Long productId,
										 @ApiParam(value = "The update request.", example = "See ProductOnlineRequest in pm_lib_model")
										 @RequestBody ExtendedProductOnlineRequest productOnlineUpdateRequest,
										 HttpServletRequest request) {

		if (Objects.isNull(productOnlineUpdateRequest.getUserId())) {
			throw new ValidationException("Unable to validate request.", List.of("User ID is required."));
		}

		productOnlineUpdateRequest.setProductId(productId);

		// Pad the sales channel code out to 5.
		productOnlineUpdateRequest.getSalesChannel().setSalesChannelCode(
				StringUtils.rightPad(productOnlineUpdateRequest.getSalesChannel().getSalesChannelCode(), 5));

		ProductOnlineEndpoint.logger.info(String.format("User %s using application %s from IP %s has requested to update product online information: %s.",
				productOnlineUpdateRequest.getUserId(),
				this.clientInfoService.getClientApplicationName(),
				request.getRemoteAddr(),
				request));

		return this.productOnlineService.processRequest(productOnlineUpdateRequest);
	}

	/**
	 * Returns product online information for a product or product group. This only returns current and future information.
	 *
	 * @param productId The ID of the product or product group to lookup product online information for.
	 * @param request The HttpServlet request that initiated this call.
	 * @return The current product online information for the supplied product.
	 */
	@GetMapping("/{productId}")
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns the product online information for a product or product group.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK", response = ProductOnlineRange.class),
			@ApiResponse(code = 404, message = "Not Found.")})
	public List<ProductOnlineRange> getByProductId(@ApiParam("The ID of the product or product group to return information for.")
													   @PathVariable("productId") Long productId,
												   HttpServletRequest request) {

		ProductOnlineEndpoint.logger.info(String.format("Application %s from IP %s has requested product online for product: %d",
				this.clientInfoService.getClientApplicationName(),
				request.getRemoteAddr(),
				productId));

		return this.productOnlineService.getByProductId(productId);
	}

	/**
	 * Returns product online information for a product or product group for a supplied sales channel. This only returns current and future information.
	 *
	 * @param productId The ID of the product or product group to lookup product online information for.
	 * @param salesChannel The sales channel to lookup product online information for.
	 * @param request The HttpServlet request that initiated this call.
	 * @return The current product online information for the supplied product for the supplied sales channel.
	 */
	@GetMapping("/{productId}/{salesChannel}")
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns the product online information for a product or product group for a specific sales channel.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK", response = ProductOnlineRange.class),
			@ApiResponse(code = 404, message = "Not Found.")})
	public List<ProductOnlineRange> getByProductIdAndSalesChannel(@ApiParam("The ID of the product or product group to return information for.")
																	   @PathVariable("productId") Long productId,
																   @ApiParam("The sales channel to return information for.")
																   @PathVariable("salesChannel") String salesChannel,
																   HttpServletRequest request) {

		ProductOnlineEndpoint.logger.info(String.format("Application %s from IP %s has requested product online for product %d and sales channel %s",
				this.clientInfoService.getClientApplicationName(),
				request.getRemoteAddr(),
				productId,
				salesChannel));

		// Pad the sales channel code out to 5.
		return this.productOnlineService.getByProductIdAndSalesChannel(productId, StringUtils.rightPad(salesChannel, 5));
	}
}
