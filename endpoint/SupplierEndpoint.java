package com.heb.pm.core.endpoint;

import com.heb.pm.core.exception.NotFoundException;
import com.heb.pm.core.model.Supplier;
import com.heb.pm.core.service.SupplierService;
import com.heb.pm.util.security.wsag.ClientInfoService;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * Endpoint for supplier level information.
 *
 * @author d116773
 * @since 1.12.0
 */
@RestController()
@RequestMapping("/supplier")
@Api(value = "Supplier API", produces = MediaType.APPLICATION_JSON_VALUE)
public class SupplierEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(SupplierEndpoint.class);

	@Autowired
	private transient SupplierService supplierService;

	@Autowired
	private transient ClientInfoService clientInfoService;


	/**
	 * Returns warehouse supplier information.
	 *
	 * @param apNumber The AP number of the supplier.
	 * @param request The HTTP request that triggered this call.
	 * @return Information about the supplier.
	 */
	@GetMapping("/warehouse/{apNumber}")
	@ApiOperation("Returns information about a particular supplier.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "supplier\" : {apNumber}", response = Supplier.class),
			@ApiResponse(code = 404, message = "error : Supplier not found.")})
	public Supplier getWarehouseSupplier(@ApiParam("The warehouse AP number being requested.")
											 @PathVariable("apNumber") Long apNumber,
										 HttpServletRequest request) {

		SupplierEndpoint.logger.info(String.format("Application %s from IP %s requested supplier information for %d.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), apNumber));

		return this.supplierService.getWarehouseSupplier(apNumber).orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}
}
