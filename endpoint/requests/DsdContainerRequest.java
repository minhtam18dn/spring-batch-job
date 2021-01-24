package com.heb.pm.core.endpoint.requests;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
@ToString
public class DsdContainerRequest {

    private Long palletId;
    private String userId;
    private int page;
    private int pageSize;
    private Long containerUPC;
    private Long apVendor;
    private String containerStatus;
    private String bdmCode;
}
