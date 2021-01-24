package com.heb.pm.core.endpoint.requests;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.List;

@Getter
@Setter
@Accessors(chain = true)
@ToString
public class DsdAuthorizeItemRequest {

    private String userId;
    private Long itemCode;
    private String itemKeyTypeCode;
    private Long vendorNumber;
    private String vendorType;
    private List<Long> locNbrAuthors;
    private List<Long> locNbrUnAuthors;

}
