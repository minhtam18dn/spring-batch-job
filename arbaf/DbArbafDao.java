package com.heb.pm.arbaf;

import org.neo4j.driver.internal.shaded.io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Arbaf DAO that goes against the SQL Server DB.
 *
 * @author d116773
 * @since 1.1.0
 */
@Repository
public class DbArbafDao implements ArbafDao {

	private static final Logger logger = LoggerFactory.getLogger(DbArbafDao.class);

	private static final String FIND_APPLICATION_SQL = "select appl_id from appl_nm where appl_nm=?";

	private static final String FIND_ROLES_BY_ONEPASS_SQL = "  select usr_sec_grp.usr_role_cd " +
			"from usr_role, usr_sec_grp " +
			"where usr_sec_grp.usr_role_cd=usr_role.usr_role_cd " +
			"and appl_id=? and usr_sec_grp.usr_id=?";

	private static final String FIND_ROLES_BY_ONEPASS_AND_JOBCODE_SQL = "select usr_role.usr_role_cd " +
			"from idm, usr_role, usr_sec_grp  " +
			"where idm.usr_id=usr_sec_grp.usr_id  " +
			"and usr_sec_grp.usr_role_cd=usr_role.usr_role_cd " +
			"and appl_id=? " +
			"and idm.usr_id=?  " +
			"union " +
			"select usr_role.usr_role_cd " +
			"from idm, usr_role, usr_sec_grp, job_cd, usr_role_job_cd " +
			"where job_cd.job_cd=usr_role_job_cd.job_cd " +
			"and usr_role_job_cd.usr_role_cd=usr_role.usr_role_cd " +
			"and appl_id=? " +
			"and job_cd.job_cd=?";

	private static final String FIND_RESOURCES_BY_ROLE_CODE_SQL = "select resrc_nm, acs_abb " +
			"from sec_grp_resrc, access_type, resrc " +
			"where sec_grp_resrc.acs_cd=access_type.acs_cd " +
			"and sec_grp_resrc.resrc_id=resrc.resrc_id " +
			"and usr_role_cd=?";

	private static final String FIND_USERS_AND_ACCESS_LEVEL_BY_RESOURCE_AND_APPLICATION = "select usg.USR_ID, act.ACS_ABB " +
			"from SEC_GRP_RESRC sgr, ACCESS_TYPE act, USR_SEC_GRP usg, APPL_NM n " +
			"where sgr.ACS_CD = act.ACS_CD and sgr.USR_ROLE_CD = usg.USR_ROLE_CD and n.APPL_NM=?" +
			"  and sgr.RESRC_ID in" +
			"    (select r.RESRC_ID from RESRC r where r.APPL_ID = n.APPL_ID and r.RESRC_NM=?) " +
			"order by usg.USR_ID";


	/**
	 * Row mapper to pull the user's roles from the query.
	 */
	private final transient RowMapper<String> roleRowMapper = new RowMapper<String>() {

		@Override
		public String mapRow(ResultSet rs, int rowNum) throws SQLException {
			return rs.getString(1);
		}
	};

	/**
	 * Row mapper to pull the permissions from the query.
	 */
	private final transient RowMapper<Permission> resourcesRowMapper = new RowMapper<Permission>() {

		private static final String RESOURCE_NAME_COLUMN = "resrc_nm";
		private static final String ACCESS_TYPE_COLUMN = "acs_abb";

		/**
		 * Maps a row from the query to pull user's permissions.
		 *
		 * @param rs The result set to read the data from.
		 * @param rowNum The number of the row being processed.
		 * @return A Permission mapped from the DB.
		 * @throws SQLException
		 */
		@Override
		public Permission mapRow(ResultSet rs, int rowNum) throws SQLException {

			return new Permission(rs.getString(RESOURCE_NAME_COLUMN), rs.getString(ACCESS_TYPE_COLUMN));
		}
	};

	/**
	 * Row mapper to pull the UserResource from the query.
	 */
	private final transient RowMapper<UserResource> userResourceRowMapper = new RowMapper<UserResource>() {

		private static final String USER_ID_COLUMN = "usr_id";
		private static final String ACCESS_TYPE_COLUMN = "acs_abb";

		/**
		 * Maps a row from the query to pull user's id and access level for a resource.
		 *
		 * @param rs The result set to read the data from.
		 * @param rowNum The number of the row being processed.
		 * @return A Permission mapped from the DB.
		 * @throws SQLException
		 */
		@Override
		public UserResource mapRow(ResultSet rs, int rowNum) throws SQLException {

			return new UserResource(rs.getString(USER_ID_COLUMN), rs.getString(ACCESS_TYPE_COLUMN));
		}
	};

	@Autowired
	@Qualifier("arbafJdbcTemplate")
	private transient JdbcTemplate arbafJdbcTemplate;

	@Override
	public Collection<Permission> getUserPermissions(String applicationName, String userId) {

		Long applicationId = this.getApplicationId(applicationName).orElseThrow(() -> new ApplicationNotFoundException(applicationName));

		Object[] parms = new Object[2];
		parms[0] = applicationId;
		parms[1] = userId;

		List<String> userRoles = this.arbafJdbcTemplate.query(FIND_ROLES_BY_ONEPASS_SQL, parms, this.roleRowMapper);
		return this.getPermissionsForRoles(userRoles);
	}

	@Override
	public Collection<Permission> getUserPermissions(String applicationName, String userId, String jobCode) {

		Long applicationId = this.getApplicationId(applicationName).orElseThrow(() -> new ApplicationNotFoundException(applicationName));

		Object[] parms = new Object[4];
		parms[0] = applicationId;
		parms[1] = userId;
		parms[2] = applicationId;
		parms[3] = jobCode;

		List<String> userRoles = this.arbafJdbcTemplate.query(FIND_ROLES_BY_ONEPASS_AND_JOBCODE_SQL, parms, this.roleRowMapper);
		return this.getPermissionsForRoles(userRoles);
	}

	@Override
	public Collection<UserResources> getUserResources(String applicationName, String resource) {

		Object[] parms = new Object[2];
		parms[0] = applicationName;
		parms[1] = resource;

		List<UserResource> userResources = null;
		try {
			userResources = this.arbafJdbcTemplate.query(FIND_USERS_AND_ACCESS_LEVEL_BY_RESOURCE_AND_APPLICATION, parms, this.userResourceRowMapper);
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage());
			throw e;
		}

		if (CollectionUtils.isEmpty(userResources)) {
			return new ConcurrentSet<>();
		}

		Map<String, UserResources> map = new ConcurrentHashMap<>();
		for (UserResource userResource : userResources) {
			map.putIfAbsent(userResource.getUserId(), new UserResources(userResource.getUserId(), new ArrayList<>()));
			map.get(userResource.getUserId()).getAccessLevels().add(userResource.getAccessLevel());

		}
		return map.values();
	}

	private Collection<Permission> getPermissionsForRoles(List<String> userRoles) {
		if (userRoles.isEmpty()) {
			return Collections.emptySet();
		}

		Object[] parms = new Object[1];
		Set<Permission> permissions = new HashSet<>();

		for (String userRole : userRoles) {
			parms[0] = userRole;
			permissions.addAll(this.arbafJdbcTemplate.query(FIND_RESOURCES_BY_ROLE_CODE_SQL, parms, this.resourcesRowMapper));
		}

		return permissions;
	}

	private Optional<Long> getApplicationId(String applicationName) {
		try {
			return Optional.of(this.arbafJdbcTemplate.queryForObject(FIND_APPLICATION_SQL,
					Long.class, applicationName));
		} catch (EmptyResultDataAccessException e) {
			logger.debug(String.format("Application \"%s\" not found.", applicationName));
			return Optional.empty();
		}
	}
}
