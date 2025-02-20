package com.ospreydcs.dp.service.common.protobuf;

import com.ospreydcs.dp.grpc.v1.common.Attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AttributesUtility {

    public static Map<String, String> attributeMapFromList(List<Attribute> attributeList) {
        final Map<String, String> attributeMap = new TreeMap<>();
        for (Attribute attribute : attributeList) {
            attributeMap.put(attribute.getName(), attribute.getValue());
        }
        return attributeMap;
    }

    public static List<Attribute> attributeListFromMap(Map<String, String> attributeMap) {
        final List<Attribute> attributeList = new ArrayList<>();
        for (Map.Entry<String, String> entry : attributeMap.entrySet()) {
            Attribute attribute = Attribute.newBuilder().setName(entry.getKey()).setValue(entry.getValue()).build();
            attributeList.add(attribute);
        }
        return attributeList;
    }
}
