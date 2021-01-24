package com.heb.pm.core.endpoint;

import com.heb.pm.core.maintenance.ProductCustomHierarchyRequest;
import com.heb.pm.core.service.CustomHierarchyService;
import com.heb.pm.core.service.maintenance.ecommerce.ExtendedProductCustomHierarchyRequest;
import com.heb.pm.util.ListUtils;
import com.heb.pm.util.security.wsag.ClientInfoService;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rest endpoint for custom hierarchy data.
 *
 * @author d116773
 * @since 1.27.0
 */
@RestController()
@RequestMapping("/custom-hierarchy")
@Api(value = "CustomHierarchyAPI", produces = MediaType.APPLICATION_JSON_VALUE)
public class CustomHierarchyEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(CustomHierarchyEndpoint.class);

	@Autowired
	private transient CustomHierarchyService customHierarchyService;


	@Autowired
	private transient ClientInfoService clientInfoService;

	/**
	 * Adds a list of products to a custom hierarchy.
	 *
	 * @param hierarchyContext The hierarchy to add the products to.
	 * @param requests The list of requests to process.
	 * @param request The HTTP request that triggered this call.
	 * @return A success string.
	 */
	@PostMapping("/{hierarchy}/products")
	@ApiOperation("Adds products to custom hierarchies.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "An update message.", response = String.class),
			@ApiResponse(code = 409, message = "Invalid request."),
			@ApiResponse(code = 500, message = "Operation failed.")})
	@PreAuthorize("hasAuthority('CUSTOM_HIERARCHY')")
	public String addProductsToHierarchy(@ApiParam("The ID of the hierarchy to add products to.") @PathVariable("hierarchy") String hierarchyContext,
										 @ApiParam("The requests to process") @RequestBody List<ProductCustomHierarchyRequest> requests,
										 HttpServletRequest request) {

		String requestsAsString = ListUtils.toString(requests);

		logger.info(String.format("Application %s from IP %s has requested to add products to hierarchy %s: '%s'.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), hierarchyContext, requestsAsString));

		List<ExtendedProductCustomHierarchyRequest> extendedRequests = requests.stream()
				.map(r -> ExtendedProductCustomHierarchyRequest.from(r, hierarchyContext))
				.collect(Collectors.toList());

		this.customHierarchyService.addProductsToHierarchy(extendedRequests);
		return "SUCCESS: RESPONSE NOT YET DEFINED";
	}

	/**
	 * Removes a list of products from a custom hierarchy.
	 *
	 * @param hierarchyContext The hierarchy to remove the products from.
	 * @param requests The list of requests to process.
	 * @param request The HTTP request that triggered this call.
	 * @return A success string.
	 */
	@DeleteMapping("/{hierarchy}/products")
	@ApiOperation("Removes products to custom hierarchies.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "An update message.", response = String.class),
			@ApiResponse(code = 409, message = "Invalid request."),
			@ApiResponse(code = 500, message = "Operation failed.")})
	@PreAuthorize("hasAuthority('CUSTOM_HIERARCHY')")
	public String removeProductsFromHierarchy(@ApiParam("The ID of the hierarchy to add products to.") @PathVariable("hierarchy") String hierarchyContext,
											  @ApiParam("The requests to process") @RequestBody List<ProductCustomHierarchyRequest> requests,
											  HttpServletRequest request) {

		String requestsAsString = ListUtils.toString(requests);

		logger.info(String.format("Application %s from IP %s has requested to remove products from hierarchy %s: '%s'.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), hierarchyContext, requestsAsString));

		List<ExtendedProductCustomHierarchyRequest> extendedRequests = requests.stream()
				.map(r -> ExtendedProductCustomHierarchyRequest.from(r, hierarchyContext))
				.collect(Collectors.toList());

		this.customHierarchyService.removeProductsFromHierarchy(extendedRequests);
		return "SUCCESS: RESPONSE NOT YET DEFINED";
	}
}
