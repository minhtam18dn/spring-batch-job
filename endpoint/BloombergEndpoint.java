package com.heb.pm.core.endpoint;

import com.heb.pm.core.exception.NotFoundException;
import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.model.mat.BloombergAttributeMapping;
import com.heb.pm.core.model.mat.BloombergCommodity;
import com.heb.pm.core.model.mat.BloombergHierarchyMapping;
import com.heb.pm.core.service.mat.BloombergService;
import com.heb.pm.util.security.wsag.ClientInfoService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * REST endpoint for handling reuqests related to Bloomberg categories.
 *
 * @author d116773
 * @since 1.22.0
 */
@RestController()
@RequestMapping(BloombergEndpoint.BLOOMBERG_BASE_URL)
public class BloombergEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(BloombergEndpoint.class);

	protected static final String BLOOMBERG_BASE_URL = "/bloomberg";

	private final transient BloombergService bloombergService;
	private final transient ClientInfoService clientInfoService;

	/**
	 * Constructs a new BloombergEndpoint.
	 *
	 * @param bloombergService The BloombergService to handle all the requests related to data.
	 * @param clientInfoService The service that will provide information related to the reuqest that came in.
	 */
	@Autowired
	public BloombergEndpoint(BloombergService bloombergService, ClientInfoService clientInfoService) {

		this.bloombergService = bloombergService;
		this.clientInfoService = clientInfoService;
	}

	/**
	 * Saves a list of new Bloomberg commodities.
	 *
	 * @param commoditiesToSave The list of Bloomberg commodities to save. These should all be new.
	 * @param request The HTTP servlet request that initiated this call.
	 * @return The list of Bloomberg commodities updated after the save.
	 */
	@PutMapping
	@PreAuthorize("hasAuthority('BLOOMBERG')")
	@ApiOperation("Saves a list of Bloomberg commodities.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK.", response = BloombergCommodity.class),
			@ApiResponse(code = 409, message = "Unable to validate commodity.")})
	public Collection<BloombergCommodity> put(@ApiParam(value = "A list of Bloomberg commodities to save", example = "See BloombergCommodity in pm-lib-model.", required = true)
												  @RequestBody List<BloombergCommodity> commoditiesToSave,
											  HttpServletRequest request) {

		StringJoiner commoditesAsString = new StringJoiner(",", "[", "]");
		commoditiesToSave.forEach(c -> commoditesAsString.add(c.toString()));

		logger.info(String.format("Application %s from IP %s requested adding these Bloomberg commodities: %s.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), commoditesAsString.toString()));

		return this.bloombergService.saveNewCommodities(commoditiesToSave);
	}

	/**
	 * Returns Bloomberg commodities. If bloombergId is null, this will return all commodities. If it has a value,
	 * it will return the commodity with that bloombergId. The return will always be wrapped in an array.
	 *
	 * @param bloombergId An optional parmeter with a BloombergId to filter on.
	 * @param request The HTTP servlet request that initiated this call.
	 * @return
	 */
	@GetMapping
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns a list of Bloomberg commodities.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK.", response = BloombergCommodity.class)
			})
	public Collection<BloombergCommodity> get(@ApiParam("An optional filter of a Bloomberg commodity ID.")
												  @RequestParam(name = "bloombergId", required = false) String bloombergId,
											  HttpServletRequest request) {

		if (Objects.isNull(bloombergId)) {

			logger.info(String.format("Application %s from IP %s requested a list of all Bloomberg commodities.",
					this.clientInfoService.getClientApplicationName(), request.getRemoteAddr()));
			return this.bloombergService.getAllCommodities();
		} else {

			logger.info(String.format("Application %s from IP %s requested the Bllomberg commodity with the Boomberg ID %s.",
					this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), bloombergId));
			return List.of(this.bloombergService.findByBloombergId(bloombergId).orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER));
		}
	}

	/**
	 * Returns a specific Bloomberg commodity with a requested ID.
	 *
	 * @param id The ID to look for.
	 * @param request The HTTP servlet request that initiated this call.
	 * @return The Bloomberg commodity with the requested ID.
	 */
	@GetMapping("/{id}")
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns a specific Bloomberg commodities.")
	@ApiResponses(value = {
			@ApiResponse(code = 404, message = "OK.", response = BloombergCommodity.class),
			@ApiResponse(code = 409, message = "Not Found.")})
	public BloombergCommodity get(@ApiParam(value = "The ID of the Bloomberg commodity to find.", required = true)
									  @PathVariable("id") Long id,
								  HttpServletRequest request) {

		logger.info(String.format("Application %s from IP %s requested the Bllomberg commodity with he ID %d.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), id));
		return this.bloombergService.findById(id).orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	/**
	 * Updates a list of existing Bloomberg commodities.
	 *
	 * @param commoditiesToSave The list of existing Bloomberg commodities to update.
	 * @param request The HTTP servlet request that initiated this call.
	 * @return The Bloomberg commodities updated after being saved.
	 */
	@PostMapping
	@PreAuthorize("hasAuthority('BLOOMBERG')")
	@ApiOperation("Updates multiple existing Bloomberg commodities.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK.", response = BloombergCommodity.class),
			@ApiResponse(code = 409, message = "Unable to validate commodity.")})
	public Collection<BloombergCommodity> updateBloombergCommodities(@ApiParam(value = "The list of Bloomberg commodities to update.",
			example = "See BloombergCommodity in pm-lib-model", required = true)
								   @RequestBody List<BloombergCommodity> commoditiesToSave,
								   HttpServletRequest request) {

		StringJoiner commoditesAsString = new StringJoiner(",", "[", "]");
		commoditiesToSave.forEach(c -> commoditesAsString.add(c.toString()));

		logger.info(String.format("Application %s from IP %s requested to save Bloomberg commodities %s.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(),
				commoditiesToSave.toString()));

		return this.bloombergService.updateCommodities(commoditiesToSave);
	}

	/**
	 * Saves a collection of new Bloomberg attribute mappings.
	 *
	 * @param bloombergAttributeMappings The collection of BloombergAttributeMappings to save.
	 * @param request The HTTP servlet request that initiated this call.
	 * @return The updated BloombergAttributeMappings.
	 */
	@PutMapping("/attribute")
	@PreAuthorize("hasAuthority('BLOOMBERG')")
	@ApiOperation("Creates new Bloomberg attribute to commodity mapping.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK.", response = BloombergCommodity.class),
			@ApiResponse(code = 409, message = "Unable to validate mapping.")})
	public Collection<BloombergAttributeMapping> putAttributes(@ApiParam(value = "A list of Bloomberg attribute mappings to update.",
			example = "See BloombergAttributeMapping in pm-lib-model", required = true)
																   @RequestBody List<BloombergAttributeMapping> bloombergAttributeMappings,
															   HttpServletRequest request) {

		StringJoiner mappingsAsString = new StringJoiner(",", "[", "]");
		bloombergAttributeMappings.forEach(c -> mappingsAsString.add(c.toString()));

		logger.info(String.format("Application %s from IP %s requested adding these Bloomberg attribute mappings: %s.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), mappingsAsString.toString()));

		return this.bloombergService.saveNewMappings(bloombergAttributeMappings);
	}

	/**
	 * Returns a list of all BloombergAttributeMappings.
	 *
	 * @param request  The HTTP servlet request that initiated this call.
	 * @return The list of all BloombergAttributeMappings
	 */
	@GetMapping("/attribute")
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns a list of all Bloomberg attribute mappings.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK.", response = BloombergCommodity.class)
	})
	public Collection<BloombergAttributeMapping> getAttributes(HttpServletRequest request) {

		logger.info(String.format("Application %s from IP %s requested a list of all Bloomberg attribute mappings.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr()));

		return this.bloombergService.getAllAttributeMappings();
	}

	/**
	 * Marks a BloombergAttributeMapping as inactive.
	 *
	 * @param bloombergAttributeMapping The BloombergAttributeMapping to inactivate.
	 * @param request  The HTTP servlet request that initiated this call.
	 * @return The BloombergAttributeMapping that was passed in.
	 */
	@DeleteMapping("/attribute")
	@PreAuthorize("hasAuthority('BLOOMBERG')")
	@ApiOperation("Deletes a Bloomberg attribute to commodity mapping.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK.", response = BloombergCommodity.class),
			@ApiResponse(code = 409, message = "Unable to validate mapping.")})
	public BloombergAttributeMapping inactivateAttributes(@ApiParam(value = "The Bloomberg attribute mapping to delete.", example = "See BloombergAttributeMapping in pm-lib-model", required = true)
																	 @RequestBody BloombergAttributeMapping bloombergAttributeMapping,
													  HttpServletRequest request) {

		if (Objects.isNull(bloombergAttributeMapping.getUserId())) {
			throw new ValidationException("Unable to validate mapping.", List.of("User ID is required."));
		}

		logger.info(String.format("User %s using application %s from IP %s requested to delete the Bloomberg attribute mapping %s.",
				bloombergAttributeMapping.getUserId(), this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(),
				bloombergAttributeMapping.toString()));

		return this.bloombergService.invalidate(bloombergAttributeMapping);
	}

	/**
	 * Returns a list of all BloombergHierarchyMappings.
	 *
	 * @param request  The HTTP servlet request that initiated this call.
	 * @return The list of all BloombergHierarchyMappings
	 */
	@GetMapping("/hierarchy")
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns a list of all Bloomberg hierarchy mappings mappings.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK.", response = BloombergCommodity.class)
	})
	public Collection<BloombergHierarchyMapping> getHierarchy(HttpServletRequest request) {

		logger.info(String.format("Application %s from IP %s requested a list of all Bloomberg hierarchy mappings.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr()));

		return this.bloombergService.getAllHierarchyMappings();
	}

	/**
	 * Saves a collection of new Bloomberg hierarchy mappings.
	 *
	 * @param bloombergHierarchyMappings The collection of BloombergHierarchyMapping to save.
	 * @param request The HTTP servlet request that initiated this call.
	 * @return The updated BloombergAttributeMapping.
	 */
	@PutMapping("/hierarchy")
	@PreAuthorize("hasAuthority('BLOOMBERG')")
	@ApiOperation("Creates Bloomberg hierarchy to commodity mapping.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK.", response = BloombergCommodity.class),
			@ApiResponse(code = 409, message = "Unable to validate mapping.")})
	public Collection<BloombergHierarchyMapping> putHierarchy(@ApiParam(value = "A list of Bloomberg attribute mappings to update..",
			example = "See BloombergAttributeMapping in pm-lib-model", required = true)
															   @RequestBody List<BloombergHierarchyMapping> bloombergHierarchyMappings,
															   HttpServletRequest request) {

		StringJoiner mappingsAsString = new StringJoiner(",", "[", "]");
		bloombergHierarchyMappings.forEach(c -> mappingsAsString.add(c.toString()));

		logger.info(String.format("Application %s from IP %s requested adding these Bloomberg hierarchy mappings: %s.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), mappingsAsString.toString()));

		return this.bloombergService.saveAllMappings(bloombergHierarchyMappings);
	}

	/**
	 * Marks a BloombergHierarchyMapping as inactive.
	 *
	 * @param bloombergHierarchyMapping The BloombergHierarchyMapping to inactivate.
	 * @param request  The HTTP servlet request that initiated this call.
	 * @return The BloombergHierarchyMapping that was passed in.
	 */
	@DeleteMapping("/hierarchy")
	@PreAuthorize("hasAuthority('BLOOMBERG')")
	@ApiOperation("Deletes a Bloomberg hierarchy to commodity mapping.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "OK.", response = BloombergCommodity.class),
			@ApiResponse(code = 409, message = "Unable to validate mapping.")})
	public BloombergHierarchyMapping inactivatHerarchy(@ApiParam(value = "The Bloomberg hierarchy mapping to delete.",
			example = "See BloombergHierarchyMapping in pm-lib-model", required = true)
														 @RequestBody BloombergHierarchyMapping bloombergHierarchyMapping,
														 HttpServletRequest request) {

		if (Objects.isNull(bloombergHierarchyMapping.getUserId())) {
			throw new ValidationException("Unable to validate mapping.", List.of("User ID is required."));
		}

		logger.info(String.format("User %s using application %s from IP %s requested to delete the Bloomberg hierarchy mapping %s.",
				bloombergHierarchyMapping.getUserId(), this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(),
				bloombergHierarchyMapping.toString()));

		return this.bloombergService.invalidate(bloombergHierarchyMapping);
	}
}
