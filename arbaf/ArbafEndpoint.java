package com.heb.pm.arbaf;

import com.heb.pm.util.security.wsag.ClientInfoService;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

/**
 * Rest endpoint for accessing ARBAF data.
 *
 * @author d116773
 * @since 1.1.0
 */
@RestController()
@RequestMapping(ArbafEndpoint.ARBAF_BASE_URL)
@Api(value = "ARBAFAPI", produces = MediaType.APPLICATION_JSON_VALUE)
public class ArbafEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(ArbafEndpoint.class);

	protected static final String ARBAF_BASE_URL = "/arbaf";

	protected static final String USER_LOOKUP_URL = "/{applicationName}/{userId}";

	protected static final String RESOURCE_USERS_LOOKUP_URL = "/{applicationName}/{resource}/users";

	@Autowired
	private transient ClientInfoService clientInfoService;

	@Autowired
	private transient ArbafService arbafService;

	/**
	 * Looks up the permissions for a given user for a given application.
	 *
	 * @param applicationName The application to look for.
	 * @param userId The user to look for.
	 * @param includeJobCode An optional parameter that allows the calling system to exclude looking up permissions
	 *                       by job code.
	 * @return A list of user's permissions.
	 */
	@RequestMapping(method = RequestMethod.GET, value = USER_LOOKUP_URL)
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns a user's permissions for a requested application.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "Collection<Permission>", response = Permission.class),
			@ApiResponse(code = 404, message = "error : application not found.")})
	public Collection<Permission> lookupUser(
			@ApiParam("The application to search for. Replace any spaces in the application name with a plus (+) sign.")
			@PathVariable("applicationName") String applicationName,

			@ApiParam("The one-pass ID of the user.")
			@PathVariable("userId") String userId,

			@ApiParam("Optional parameter saying to include job code in the lookup. The default is true. Can be TRUE or FALSE.")
			@RequestParam(value = "includeJobCode", required = false, defaultValue = "TRUE") Boolean includeJobCode) {

		logger.info(String.format("Application %s requested user access data for application %s for user %s",
				this.clientInfoService.getClientApplicationName(), applicationName, userId));

		// Replace all the +s from the application name with spaces.
		String formattedApplicationName =  applicationName.replaceAll("\\+", " ");
		logger.debug(String.format("Translated application name to \"%s\"", formattedApplicationName));

		return this.arbafService.getUserPermissions(formattedApplicationName, userId, includeJobCode);
	}

	/**
	 * Looks up the resource for a given application and returns user's user id and access level(s).
	 *
	 * @param applicationName The application to look for.
	 * @param resource The resource to look for.
	 * @return A list of user's permissions.
	 */
	@RequestMapping(method = RequestMethod.GET, value = RESOURCE_USERS_LOOKUP_URL)
	@PreAuthorize("hasAuthority('READ')")
	@ApiOperation("Returns a users and their access levels for a give resource and requested application.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "Collection<UserResources>", response = UserResources.class),
			@ApiResponse(code = 404, message = "error : application not found.")})
	public Collection<UserResources> lookupResource(
			@ApiParam("The application to search for. Replace any spaces in the application name with a plus (+) sign.")
			@PathVariable("applicationName") String applicationName,

			@ApiParam("The resource to search.")
			@PathVariable("resource") String resource) {

		logger.info(String.format("Application %s requested user access data for application %s for resource %s",
				this.clientInfoService.getClientApplicationName(), applicationName, resource));

		// Replace all the +s from the application name with spaces.
		String formattedApplicationName =  applicationName.replaceAll("\\+", " ");
		logger.debug(String.format("Translated application name to \"%s\"", formattedApplicationName));

		return this.arbafService.getUserResources(formattedApplicationName, resource);
	}
}
