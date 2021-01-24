package com.heb.pm.arbaf;

import io.swagger.annotations.ApiModelProperty;

import java.util.Objects;

/**
 * Represents a user's access to a particular resource.
 *
 * @author d116773
 * @since 1.1.0
 */
public class Permission {

	@ApiModelProperty("The name of the resource the user has access to.")
	private final String resource;

	@ApiModelProperty("The level of access the user has.")
	private final String accessLevel;

	/**
	 * Constructs a new Permission.
	 *
	 * @param resource The resource the user has access to.
	 * @param accessLevel The type of access the user has.
	 */
	/* default */ Permission(String resource, String accessLevel) {
		this.resource = resource;
		this.accessLevel = accessLevel;
	}

	/**
	 * Returns the resource the user has access to.
	 *
	 * @return The resource the user has access to.
	 */
	public String getResource() {
		return resource;
	}

	/**
	 * Return the type of access the user has.
	 *
	 * @return The type of access the user has.
	 */
	public String getAccessLevel() {
		return accessLevel;
	}

	@Override
	public String toString() {
		return "Permission{" +
				"resource='" + resource + '\'' +
				", accessLevel='" + accessLevel + '\'' +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Permission that = (Permission) o;
		return Objects.equals(resource, that.resource) &&
				Objects.equals(accessLevel, that.accessLevel);
	}

	@Override
	public int hashCode() {
		return Objects.hash(resource, accessLevel);
	}
}
