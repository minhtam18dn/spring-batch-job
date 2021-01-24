package com.heb.pm.arbaf;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * Represents all the user's access to a particular resource.
 *
 * @author s793785
 * @since 1.1.0
 */
@Data
@AllArgsConstructor
public class UserResources {

	@ApiModelProperty("The id of the user.")
	private final String userId;

	@ApiModelProperty("The level of access the user has.")
	private final List<String> accessLevels;

}
