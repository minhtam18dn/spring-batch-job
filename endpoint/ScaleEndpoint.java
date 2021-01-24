package com.heb.pm.core.endpoint;

import com.heb.pm.core.exception.NotFoundException;
import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.IngredientStatementMaintenanceRequest;
import com.heb.pm.core.model.IngredientStatement;
import com.heb.pm.core.model.ScaleUpc;
import com.heb.pm.core.model.nutrition.NutritionPanel;
import com.heb.pm.core.service.ScaleService;
import com.heb.pm.util.UpcUtils;
import com.heb.pm.util.security.wsag.ClientInfoService;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Objects;

/**
 * Endpoint for SMPM functionality.
 *
 * @author d116773
 * @since 1.28.0
 */
@RestController()
@RequestMapping("/smpm")
@Api(value = "SMPM API", produces = MediaType.APPLICATION_JSON_VALUE)
public class ScaleEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(ScaleEndpoint.class);

	@Autowired
	private transient ScaleService scaleService;

	@Autowired
	private transient ClientInfoService clientInfoService;

	/**
	 * Returns the next available ingredient statement number higher than a supplied number.
	 *
	 * @param start The number to start looking for free ingredient statements from. If nothing is supplied, then
	 *              the code assumes zero.
	 * @param servletRequest The HTTP Request that initiated this call.
	 * @return The next free ingredient statement number greater than start.
	 * @throws IllegalStateException If there are no statements available higher than the supplied number.
	 */
	@GetMapping("/is/next")
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Finds the next available ingredient statement number.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "OK", response = Long.class),
			@ApiResponse(code = 500, message = "List of ingredient statement numbers exhausted.")
	})
	public Long getNextIngredientStatementId(@ApiParam("A beginning number to start searching from. If provided, " +
			"the number returned will be the next available ingredient statement number higher than the one passed.")
												 @RequestParam(value = "start", defaultValue = "0") Long start,
											 HttpServletRequest servletRequest) {

		logger.info(String.format("Application %s from IP %s requested the next ingredient statement number starting with %d.",
				this.clientInfoService.getClientApplicationName(), servletRequest.getRemoteAddr(), start));

		long startValue = Objects.isNull(start) || start < 0 ? 0 : start;

		return this.scaleService.getNextIngredientStatementNumber(startValue);
	}


	/**
	 * Returns an IngredientStatement for a given ingredient statement number.
	 *
	 * @param ingredientStatementNumber The ingredient statement number.
	 * @param servletRequest The HTTP Request that initiated this call.
	 * @return The IngredientStatement with that number.
	 * @throws NotFoundException If the ingredient statement with that number is not found.
	 */
	@GetMapping("/is/{ingredientStatementNumber}")
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns ingredient statement detail for an ingredient IngredientStatement number.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "OK", response = IngredientStatement.class),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Any error translating the ingredient statement.")
	})
	public IngredientStatement getIngredientStatement(@ApiParam("The ingredient statement number.")
														  @PathVariable("ingredientStatementNumber") Long ingredientStatementNumber,
													  HttpServletRequest servletRequest) {

		logger.info(String.format("Application %s from IP %s requested the ingredient statement %d.",
				this.clientInfoService.getClientApplicationName(), servletRequest.getRemoteAddr(), ingredientStatementNumber));

		return this.getIngredientStatement(ingredientStatementNumber);
	}

	/**
	 * Saves an IngredientStatement.
	 *
	 * @param maintenanceRequest The IngredientStatementMaintenanceRequest with the information about what to save.
	 * @param servletRequest The HTTP Request that initiated this call.
	 * @return The updated IngredientStatement.
	 * @throws ValidationException Any error validating the request.
	 */
	@PostMapping("/is")
	@PreAuthorize("hasAuthority('SMPM')")
	@ApiOperation("Updates an ingredient statement. Will return the updated statement.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "OK", response = IngredientStatement.class),
			@ApiResponse(code = 409, message = "Validation failed"),
			@ApiResponse(code = 500, message = "Any error updating the ingredient statement.")
	})
	public IngredientStatement saveIngredientStatement(@RequestBody IngredientStatementMaintenanceRequest maintenanceRequest ,
													   HttpServletRequest servletRequest) {

		logger.info(String.format("Application %s from IP %s issued the following ingredient statement maintenance request: %s",
				this.clientInfoService.getClientApplicationName(), servletRequest.getRemoteAddr(), maintenanceRequest));

		this.scaleService.saveIngredientStatement(maintenanceRequest);

		try {
			return this.getIngredientStatement(maintenanceRequest.getIngredientStatement().getId());
		} catch (NotFoundException e) {

			// If we get here, the statement was deleted and can't be returned, so return an empty statement.
			return IngredientStatement.of().setId(maintenanceRequest.getIngredientStatement().getId());
		}
	}

	/**
	 * Returns nutrition data for a given UPC.
	 *
	 * @param upc The UPC to look up nutrition for.
	 * @param servletRequest The HTTP Request that initiated this call.
	 * @return The NutritionPanel for that UPC.
	 * @throws NotFoundException If no nutrition exists.
	 */
	@GetMapping("/nutrition/{upc}")
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns nutrition statement detail for a UPC.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "OK", response = Long.class),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Any error translating the nutrition statement.")
	})
	public NutritionPanel getNutrition(@ApiParam("The UPC to look up. This can either be the full pre-digit-two or the identifying portion (PLU) part of the pre-digit-two")
										   @PathVariable("upc") Long upc,
									   HttpServletRequest servletRequest) {

		logger.info(String.format("Application %s from IP %s requested nutrition for  UPC %d.",
				this.clientInfoService.getClientApplicationName(), servletRequest.getRemoteAddr(), upc));

		// Convert any PLUs to pre-digit-2s.
		long upcToLookUp = getFullUpc(upc);

		return this.scaleService.getNutrition(upcToLookUp)
				.orElseThrow(() -> new NotFoundException(String.format("Nutrient statement not found for UPC %d.", upc)));
	}

	/**
	 * Returns all scale data for a given UPC.
	 *
	 * @param upc The UPC to look up.
	 * @param servletRequest The HTTP Request that initiated this call.
	 * @return The ScaleUpc for that UPC.
	 * @throws NotFoundException If scale UPC data does not exist.
	 */
	@GetMapping("/{upc}")
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns all scale data based on a UPC.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "OK", response = Long.class),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 500, message = "Any error translating the scale data.")
	})
	public ScaleUpc getScaleUpc(@ApiParam("The UPC to look up. This can either be the full pre-digit-two or the identifying portion (PLU) part of the pre-digit-two")
									@PathVariable("upc") Long upc,
								HttpServletRequest servletRequest) {

		logger.info(String.format("Application %s from IP %s requested scale data for  UPC %d.",
				this.clientInfoService.getClientApplicationName(), servletRequest.getRemoteAddr(), upc));

		// Convert any PLUs to pre-digit-2s.
		long upcToLookUp = getFullUpc(upc);

		return this.scaleService.getAllScaleData(upcToLookUp)
				.orElseThrow(() -> new NotFoundException(String.format("Scale data not found for UPC %d.", upc)));
	}

	private IngredientStatement getIngredientStatement(Long ingredientStatementNumber) {

		return this.scaleService.getIngredientStatement(ingredientStatementNumber)
				.orElseThrow(() -> new NotFoundException(String.format("Ingredient statement %d not found.", ingredientStatementNumber)));
	}

	private static long getFullUpc(Long upc) {

		if (Objects.isNull(upc)) {
			throw new ValidationException("Unable to validate request.", "UPC is required.");
		}

		// Convert any PLUs to pre-digit-2s.
		return UpcUtils.isPlu(upc) ? UpcUtils.pluToPreDigitTwo(upc) : upc;

	}
}
