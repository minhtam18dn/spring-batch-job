package com.heb.pm.core.service.maintenance.ecommerce;

import com.heb.pm.dao.core.entity.GenericEntity;
import com.heb.pm.dao.core.entity.codes.GenericEntityType;
import com.heb.pm.dao.core.rowmappers.EntityDescriptionRowMapper;
import com.heb.pm.dao.core.rowmappers.GenericEntityRowMapper;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.util.JdbcUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

/**
 * Service to lookup values in the ENTY table.
 *
 * @author d116773
 * @since 1.27.0
 */
public class EntityLookup {

	private final transient JdbcTemplate jdbcTemplate;

	private static final String SELECT_BY_ID_SQL = GenericEntityRowMapper.SELECT_SQL + " WHERE ENTY_ID = ?";

	private static final String SELECT_BY_TYPE_AND_EXTERNAL_ID_SQL = GenericEntityRowMapper.SELECT_SQL +
			" WHERE ENTY_TYP_CD = ? AND ENTY_DSPLY_NBR = ?";

	private static final String DESCRIPTION_SELECT_SQL = EntityDescriptionRowMapper.SELECT_SQL + " WHERE ENTY_ID = ?";

	private static final GenericEntityRowMapper ROW_MAPPER = new GenericEntityRowMapper();
	private static final SingleRowResultSetExtractor<GenericEntity> SINGLE_ROW_RESULT_SET_EXTRACTOR = new SingleRowResultSetExtractor<>(ROW_MAPPER);

	/**
	 * Constructs a new EntityLookup.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries.
	 */
	public EntityLookup(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Finds a GenericEntity by its ID.
	 *
	 * @param id The ID to lookup.
	 * @return The GenericEntity with that ID or empty.
	 */
	public Optional<GenericEntity> findById(Long id) {

		return this.singleValueLookup(SELECT_BY_ID_SQL, id);
	}

	/**
	 * Finds a GenericEntity by type and external ID.
	 *
	 * @param type The type of entity to look for.
	 * @param externalId The external ID of the entity.
	 * @return The entity for that type and external ID if present and empty if not.
	 */
	public Optional<GenericEntity> findByTypeAndExternalId(GenericEntityType type, Long externalId) {


		return this.singleValueLookup(SELECT_BY_TYPE_AND_EXTERNAL_ID_SQL, type.getId(), externalId);
	}


	/**
	 * Convenience method to look up a singe row in the table.
	 *
	 * @param sql The SQL to run.
	 * @param parms The bind variables to use.
	 * @return The row returned or empty.
	 */
	private Optional<GenericEntity> singleValueLookup(String sql, Object... parms) {

		Optional<GenericEntity> entity = Optional.ofNullable(this.jdbcTemplate.query(sql,
				JdbcUtils.argsAsArray(parms),
				SINGLE_ROW_RESULT_SET_EXTRACTOR));

		// If it doesn't exist, just return empty.
		if (entity.isEmpty()) {
			return entity;
		}

		// If it does, add the descriptions of the entities and return that.
		this.populateDescriptions(entity.get());
		return entity;
	}

	/**
	 * Given a GenericEntity, adds all descriptions tied to the entity .
	 *
	 * @param entity The GenericEntity to add descriptions for.
	 */
	private void populateDescriptions(GenericEntity entity) {

		EntityDescriptionRowMapper entityDescriptionRowMapper = new EntityDescriptionRowMapper(entity);
		entity.setEntityDescriptions(this.jdbcTemplate.query(DESCRIPTION_SELECT_SQL,
				JdbcUtils.argsAsArray(entity.getId()),
				entityDescriptionRowMapper));
	}
}
