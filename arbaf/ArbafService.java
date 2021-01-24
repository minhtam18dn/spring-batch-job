package com.heb.pm.arbaf;

import com.heb.pm.core.repository.UserSearchRepository;
import com.heb.pm.util.user.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Holds the logic for looking stuff up on ARBAF.
 *
 * @author d116773
 * @since 1.1.0
 */
@Service
public class ArbafService {

	private static final Logger logger = LoggerFactory.getLogger(ArbafService.class);

	@Autowired
	private transient ArbafDao arbafDao;

	@Autowired
	private transient UserSearchRepository userSearchRepository;

	/**
	 * Returns a collection of the user's permissions.
	 *
	 * @param applicationName The name of the application to look up.
	 * @param userId The ID of the user to look up.
	 * @param includeJobCode Whether or not to include the user's job code when looking up permissions.
	 * @return A collection of the user's permissions.
	 */
	public Collection<Permission> getUserPermissions(String applicationName, String userId, Boolean includeJobCode) {

		String jobCode = null;

		if (includeJobCode) {
			try {
				List<User> users = this.userSearchRepository.getUserList(List.of(userId));
				if (users.isEmpty()) {
					logger.warn(String.format("User %s not found, defaulting to no job code.", userId));
				} else {
					jobCode = users.get(0).getJobCode();
					logger.debug(String.format("Using %s as job code.", jobCode));
				}
			} catch (Exception e) {
				logger.warn(String.format("Caught exception %s looking up user %s, defaulting to no job code.",
						e.getLocalizedMessage(), userId));
			}
		}

		if (!includeJobCode || Objects.isNull(jobCode)) {
			return this.arbafDao.getUserPermissions(applicationName, userId);
		}
		return this.arbafDao.getUserPermissions(applicationName, userId, jobCode);
	}

	/**
	 * Returns a list of users and their access level for a particular resource.
	 *
	 * @param applicationName The application name information requested for.
	 * @param resource The resource the information is requested for.
	 * @return A list of users and their access level.
	 */
	public Collection<UserResources> getUserResources(String applicationName, String resource) {
		return this.arbafDao.getUserResources(applicationName, resource);
	}
}
