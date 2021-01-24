package com.heb.pm.core.endpoint;

import com.heb.pm.core.exception.InvalidRequestException;
import com.heb.pm.core.model.Department;
import com.heb.pm.core.maintenance.MassUpdateRequest;
import com.heb.pm.core.model.SubCommodityDefaults;
import com.heb.pm.core.service.HierarchyService;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Objects;

/**
 * Rest endpoint for merchant hierarchy data.
 */
@RestController()
@RequestMapping(HierarchyEndpoint.HIERARCHY_BASE_URL)
@Api(value = "HierarchyAPI", produces = MediaType.APPLICATION_JSON_VALUE)
public class HierarchyEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(HierarchyEndpoint.class);

	protected static final String HIERARCHY_BASE_URL = "/hierarchy";

	@Autowired
	private transient HierarchyService hierarchyService;

	@Autowired
	private transient MassUpdateEndpoint massUpdateEndpoint;

	/**
	 * Returns a graph of merchant hierarchy data.
	 *
	 * @param toLevel The lowest level of data to return. By default, the method will return all levels.
	 * @param request The HTTP Servlet request that initiated this call.
	 * @return A graph of merchant hierarchy data.
	 */
	@RequestMapping(method = RequestMethod.GET)
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns a graph of the merchant hierarchy.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "An array holding the graph of the merchant hierarchy starting at department.", response = Department.class)
			})
	public List<Department> get(
			@RequestParam(value = "level", defaultValue = HierarchyService.LEVEL_ALL)
			@ApiParam("An optional level at which to stop. Can be empty, ALL, DEPARTMENT, SUB_DEPARTMENT, CLASS, or COMMODITY") String toLevel,
			HttpServletRequest request) {

		HierarchyEndpoint.logger.info("IP %s requested the merchant hierarchy.", request.getRemoteAddr());
		return this.hierarchyService.getFullHierarchy(toLevel);
	}

	/**
	 * Mass Update hierarchy.
	 *
	 * @param massUpdateRequest the request mass update for hierachy.
	 * @return the tracking id.
	 */
	@PostMapping("/massupdate")
	@PreAuthorize("hasAuthority('MASS_UPDATE')")
	@ApiOperation("Hierarchy mass update.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "The tracking ID associated with the hierarchy mass update.", response = Long.class),
			@ApiResponse(code = 400, message = "Mass Update Request request invalid."),
			@ApiResponse(code = 409, message = "Mass Update Request failed.")
	})
	public Long massUpdate(@RequestBody MassUpdateRequest massUpdateRequest, HttpServletRequest request) {

		validateRequest(massUpdateRequest);

		logger.info(String.format("User %s from IP %s has issued the following MassUpdate Request: %s.",
				massUpdateRequest.getUserId(), request.getRemoteAddr(), massUpdateRequest));

		massUpdateRequest.setRequestType(MassUpdateRequest.RequestType.HIERARCHY);

		this.massUpdateEndpoint.massUpdate(massUpdateRequest, request);

		return massUpdateRequest.getTrackingId();
	}

	/**
	 * Checks to see if an mass update request is valid. Will throw an exception if it is not.
	 *
	 * @param massUpdateRequest The mass update request to check.
	 */
	private static void validateRequest(MassUpdateRequest massUpdateRequest) {
		if (Objects.isNull(massUpdateRequest.getUserId())) {
			throw new InvalidRequestException("User Id is required.");
		}
		if (Objects.isNull(massUpdateRequest.getTrackingId())) {
			throw new InvalidRequestException("Tracking Id is required.");
		}
	}

	/**
	 * Returns the defaults for a given sub-commodity.
	 * @param subCommodityCode to return the defaults for
	 * @return the defaults for a given sub-commodity
	 */
	@RequestMapping(method = RequestMethod.GET, value = "/subcommodity/{subCommodityCode}/defaults")
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns the defaults for a given sub-commodity.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "Returns the defaults for a given sub-commodity.", response = SubCommodityDefaults.class)
	})
	public SubCommodityDefaults getDefaultsForSubCommodityCode(HttpServletRequest request,
			@ApiParam("The sub-commodity to return defaults for.")
			@PathVariable Long subCommodityCode) {

		HierarchyEndpoint.logger.info(String.format("IP %s requested defaults for sub commodity %s.", request.getRemoteAddr(), subCommodityCode));

		return this.hierarchyService.getDefaultsForSubCommodityCode(subCommodityCode);
	}
}
