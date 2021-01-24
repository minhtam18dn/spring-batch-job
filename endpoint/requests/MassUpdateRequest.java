package com.heb.pm.core.endpoint.requests;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * Represents a request for mass update.
 *
 * @author vn03500
 * @since 1.17.0
 */
@Getter
@Setter
@Accessors(chain = true)
@ToString
@Deprecated
public class MassUpdateRequest {
    private Long trackingId;
    private String userId;
}
