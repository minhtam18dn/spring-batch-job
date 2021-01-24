package com.heb.pm.core.endpoint;

import com.heb.pm.core.exception.NotFoundException;
import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.core.maintenance.UpdateProductPrimaryRequest;
import com.heb.pm.core.model.Product;
import com.heb.pm.core.service.ProductService;
import com.heb.pm.core.service.maintenance.ProductMaintenanceService;
import com.heb.pm.core.service.search.ExtendedProductSearchCriteria;
import com.heb.pm.util.ListUtils;
import com.heb.pm.util.endpoint.PageableResult;
import com.heb.pm.util.security.wsag.ClientInfoService;
import com.heb.pm.util.tuples.Duple;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;


/**
 * Rest endpoint for finding product information.
 */
@RestController()
@RequestMapping(ProductEndpoint.PRODUCT_BASE_URL)
@Api(value = "ProductAPI", produces = MediaType.APPLICATION_JSON_VALUE, description = "Endpoint for product level data.")
public class ProductEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(ProductEndpoint.class);

	protected static final String PRODUCT_BASE_URL = "/product";

	private static final String BY_PRODUCT_ID_PARAMETER = "{productId}";

	private static final String BY_SEARCH_CRITERIA = "/search";

	private static final String BY_SEARCH_CRITERIA_STREAMING = "/search/stream";

	private static final String MAINTENANCE = BY_PRODUCT_ID_PARAMETER + "/maintenance";

	private static final String NEW_PRIMARY = MAINTENANCE + "/updatePrimary";

	private static final String PUBLISH = "/publish";

	private static final String PRODUCT_SEARCH_MESSAGE = "Application %s from IP %s has requested information for product: %d";

	private static final int MAX_PROGRAM_NAME_LENGHT = 7;

	@Autowired
	private transient ProductService productService;

	@Autowired
	private transient ProductMaintenanceService productMaintenanceService;

	@Autowired
	private transient ClientInfoService clientInfoService;

	/**
	 * Searches for a product by ID. Will set the response status to 404 if it does not exist.
	 *
	 * @param productId The ID of the product to search for.
	 * @param filters An optional list of filters to apply to the result.
	 * @param request The HTTP request that initiated the call.
	 * @return A Product with the requested ID
	 */
	@RequestMapping(method = RequestMethod.GET, value = BY_PRODUCT_ID_PARAMETER, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation("Returns product information linked to the given product id.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "\"productId\" : {productId}", response = Product.class),
			@ApiResponse(code = 404, message = "error : Product id not found.")})
	public Product findProductById(@ApiParam(value = "The product id being requested.", example = "127127", required = true)
								   @PathVariable("productId") Long productId,
								   @ApiParam("Filter names of the fields that the user wants removed from results. If no filters are included, then all fields are " +
										   "returned. If any are included, only the requested fields are returned." +
										   " E.G. 'SUPPLY-CHAIN', 'ECOMMERCE', 'NUTRITION', 'SHELF-EDGE'")
								   @RequestParam(value = "filters", required = false) List<String> filters,
								   @ApiParam("Optional list of AP numbers to filter products on. If no filters are included, " +
										   "then all fields are returned. If AP numbers are included, then products not tied " +
										   "to these AP numbers are not returned. Items and Supplier items not attached to " +
										   "the AP numbers aren't returned as well.")
									@RequestParam(value = "apNumbers", required = false) List<Long> apNumbers,
								   HttpServletRequest request) {

		ProductEndpoint.logger.info(String.format(ProductEndpoint.PRODUCT_SEARCH_MESSAGE, this.clientInfoService.getClientApplicationName(),
				request.getRemoteAddr(), productId));

		return this.productService.getProductById(productId, filters, apNumbers)
				.orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	/**
	 * Searches for a list of products. Any product not found in the list will not be returned. If no products are
	 * found, then the response is an empty list.
	 *
	 * @param productSearchCriteria The search criteria to use when looking for products.
	 * @param filters An optional list of filters to apply to the result.
	 * @param request The HTTP request that initiated the call.
	 * @return A list of Products with the requested IDs.
	 */
	@RequestMapping(method = RequestMethod.POST, value = BY_SEARCH_CRITERIA, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Searches for a collection of product IDs based on search criteria.", httpMethod = "POST")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "A list of products that match the search criteria.", response = Product.class),
			@ApiResponse(code = 400, message = "Search criteria invalid.")
	})
	public PageableResult<Product> findProductBySearchCriteria(@ApiParam(value = "A search criteria object to use to find products with.",
			example = "{ \"productIds\": [1977448,1626795,1422289,124999,2171990]}")
									@RequestBody ExtendedProductSearchCriteria productSearchCriteria,
									@ApiParam("Optional filter of the fields that the user wants removed from results. " +
										"If no filters are included, then all fields are " +
										"returned. If any are included, only the requested fields are returned." +
										" E.G. 'SUPPLY-CHAIN', 'ECOMMERCE', 'NUTRITION', 'SHELF-EDGE'")
									@RequestParam(value = "filters", required = false) List<String> filters,
									@ApiParam("Optional list of AP numbers to filter products on. If no filters are included, " +
										"then all fields are returned. If AP numbers are included, then products not tied " +
										"to these AP numbers are not returned. Items and Supplier items not attached to " +
										"the AP numbers aren't returned as well.")
									@RequestParam(value = "apNumbers", required = false) List<Long> apNumbers,
									@ApiParam("Optional value to indicate if this is the first search. The Default value is true.")
									@RequestParam(value = "firstSearch", required = false) Boolean firstSearch,
									@ApiParam("Optional value to indicate which page of information to request. Page numbers start at 0, which is the default.")
									@RequestParam(value = "page", required = false) Integer page,
									@ApiParam("Optional value to indicate the number of products to return per page. The Default value is 25.")
									@RequestParam(value = "pageSize", required = false) Integer pageSize,
															   HttpServletRequest request) {

		ProductEndpoint.logger.info(String.format("Application %s from IP %s has requested information for the following search criteria: %s",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), productSearchCriteria.toString()));

		return this.productService.getProductsBySearchCriteria(productSearchCriteria, filters, apNumbers, firstSearch, page, pageSize);
	}

	/**
	 * Searches for a list of products. Any product not found in the list will not be returned. This endpoint will stream
	 * data as it becomes available rather than generate the full list as one message.
	 *
	 * @param productSearchCriteria The search criteria to use when looking for products.
	 * @param filters An optional list of filters to apply to the result.
	 * @param apNumbers An optional list of AP number to filter products, items, and
	 * @param request The HTTP request that initiated the call.
	 * @param response The HTTP response to write the products to.
	 * @throws IOException
	 */
	@RequestMapping(method = RequestMethod.POST, value = BY_SEARCH_CRITERIA_STREAMING, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Searches for a collection of product IDs based on search criteria. This operation will stream" +
			"the data as it becomes available.	", httpMethod = "POST")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "A list of products that match the search criteria.", response = Product.class),
			@ApiResponse(code = 400, message = "Search criteria invalid.")
	})
	public void streamProductsBySearchCriteria(@ApiParam(value = "A search criteria object to use to find products with.",
												example = "{ \"productIds\": [1977448,1626795,1422289,124999,2171990]}")
												@RequestBody ExtendedProductSearchCriteria productSearchCriteria,
												@ApiParam("Optional filter of the fields that the user wants removed from results. " +
													   "If no filters are included, then all fields are " +
													   "returned. If any are included, only the requested fields are returned." +
													   " E.G. 'SUPPLY-CHAIN', 'ECOMMERCE', 'NUTRITION', 'SHELF-EDGE'")
											   	@RequestParam(value = "filters", required = false)List<String> filters,
												@ApiParam("Optional list of AP numbers to filter products on. If no filters are included, " +
														"then all fields are returned. If AP numbers are included, then products not tied " +
														"to these AP numbers are not returned. Items and Supplier items not attached to " +
														"the AP numbers aren't returned as well.")
											   	@RequestParam(value = "apNumbers", required = false)List<Long> apNumbers,
											   	HttpServletRequest request, HttpServletResponse response) throws IOException {

		ProductEndpoint.logger.info(String.format("Application %s from IP %s has requested to stream information for the following search criteria: %s",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), productSearchCriteria.toString()));

		response.setContentType(MediaType.APPLICATION_JSON_VALUE);
		this.productService.streamProductsBySearchCriteria(productSearchCriteria, filters, apNumbers, response.getOutputStream());
	}

	/**
	 * Updates a product primary UPC. This endpoint will figure out a new primary.
	 *
	 * @param productId The product ID to update.
	 * @param updatePrimaryRequest The request triggering the update.
	 * @param request The HTTP Servlet request that triggerd this call.
	 * @return The updated product.
	 */
	@PreAuthorize("hasAuthority('UPDATE_PRIMARY')")
	@RequestMapping(method = RequestMethod.POST, value = NEW_PRIMARY, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Calculates a new product primary UPC and updates it. If one cannot be calculated, it makes no change.", httpMethod = "POST")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "The updated product. The resonse is very parely populated, but will include the new product primary.", response = Product.class),
			@ApiResponse(code = 409, message = "The request was invalid.")
	})
	public Product updateProductPrimary(@ApiParam(value = "The products to update the primary UPC of.", example = "127127", required = true)
											@PathVariable("productId") Long productId,

										@ApiParam(value = "The request to update the product primary.",
												example = "{\"programName\": \"I18X016\",\"userId\": \"d116773\"}")
										@RequestBody UpdateProductPrimaryRequest updatePrimaryRequest,

										HttpServletRequest request) {

		// Validate the request.
		List<String> errors = new LinkedList<>();
		if (Objects.isNull(updatePrimaryRequest.getUserId())) {

			errors.add("User ID is required.");
		}
		if (Objects.isNull(updatePrimaryRequest.getProgramName())) {
			errors.add("Program name is required.");
		} else if (updatePrimaryRequest.getProgramName().length() > MAX_PROGRAM_NAME_LENGHT) {
			errors.add("Program name must be seven characters or less.");
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to update product primary.", errors);
		}

		logger.info(String.format("User %s from application %s from IP %s requested an update to product %d's primary UPC.", updatePrimaryRequest.getUserId(),
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), productId));

		// Update the product primary.
		this.productMaintenanceService.updateProductPrimary(productId, updatePrimaryRequest.getProgramName(), updatePrimaryRequest.getUserId());

		// Return the updated product.
		return this.productService.getProductById(productId, List.of("NONE"), List.of())
				.orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	/**
	 * Production support endpoint that will kick off a job to publish all products.
	 *
	 * @param publishedProductsOnly True to only publish products that have been published to .com.
	 * @param request The HTTP request that triggered this operation.
	 * @return A message for the caller.
	 */
	@PreAuthorize("hasAuthority('PUBLISH_ALL')")
	@RequestMapping(method = RequestMethod.GET, value = PUBLISH, produces = MediaType.TEXT_PLAIN_VALUE)
	// This is an incorrect flagging of the rule.
	@SuppressWarnings("PMD.SimplifiedTernary")
	public String publishAll(@RequestParam(value = "dotComOnly", required = false) Boolean publishedProductsOnly,
							 HttpServletRequest request) {

		logger.info(String.format("Application %s from IP %s requested all products be published.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr()));

		Duple<String, String> startInfo = this.productService.publishProducts(Objects.nonNull(publishedProductsOnly) ? publishedProductsOnly : false);
		return String.format("Job %s was stared with the ID %s.", startInfo.one(), startInfo.two());
	}

	/**
	 * Production support endpoint that will publish a limited set of products.
	 *
	 * @param productIds The list of products to publish.
	 * @param request The HTTP request that triggered this operation.
	 * @return A message for the caller.
	 */
	@PreAuthorize("hasAuthority('PUBLISH')")
	@RequestMapping(method = RequestMethod.POST, value = PUBLISH, produces = MediaType.TEXT_PLAIN_VALUE)
	public String publishAll(@RequestBody List<Long> productIds,
							 HttpServletRequest request) {

		logger.info(String.format("Application %s from IP %s requested the following products be published: %s.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(),
				ListUtils.toString(productIds)));

		int productsPublished = this.productService.publishProductList(productIds);
		return String.format("%,d products published.", productsPublished);
	}

	/**
	 * High-level update to a product.
	 *
	 * @param productId The ID of the product to update.
	 * @param updateProductRequest The update request.
	 * @param request The HTTP request that triggered this change.
	 * @return The updated product.
	 */
	@PreAuthorize("hasAuthority('UPDATE_PRODUCT')")
	@RequestMapping(method = RequestMethod.POST, value = BY_PRODUCT_ID_PARAMETER, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Updates a product.", httpMethod = "POST")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "The updated product. The resonse is very parely populated, but will include the new product primary.", response = Product.class),
			@ApiResponse(code = 409, message = "The request was invalid."),
			@ApiResponse(code = 404, message = "Not Found")
	})
	public Product updateProduct(@ApiParam(value = "The products to update the primary UPC of.", example = "127127", required = true)
										@PathVariable("productId") Long productId,

										@ApiParam(value = "The request to update the product.",
												example = "See ProductMaintenanceRequest in pm-lib-model")
										@RequestBody ProductMaintenanceRequest updateProductRequest,
										HttpServletRequest request) {

		// Validate the request.
		List<String> errors = new LinkedList<>();
		if (Objects.isNull(updateProductRequest.getUserId())) {

			errors.add("User ID is required.");
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to update product.", errors);
		}

		logger.info(String.format("User %s from application %s from IP %s requested an product %d: %s.", updateProductRequest.getUserId(),
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), productId, updateProductRequest));

		// Update the product.
		this.productMaintenanceService.updateProduct(productId, updateProductRequest);

		// Return the updated product.
		return this.productService.getProductById(productId, List.of("NONE"), List.of())
				.orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}
}
