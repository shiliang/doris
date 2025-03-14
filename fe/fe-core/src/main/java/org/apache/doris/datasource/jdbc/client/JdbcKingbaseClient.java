// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

public class JdbcKingbaseClient extends JdbcClient {

    protected JdbcKingbaseClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String kbType = fieldSchema.getDataTypeName().orElse("unknown");
        switch (kbType) {
            case "smallint":
            case "smallserial":
                return Type.SMALLINT;
            case "integer":
            case "serial":
                return Type.INT;
            case "bigint":
            case "bigserial":
                return Type.BIGINT;
            case "numeric":
            case "decimal": {
                int precision = fieldSchema.getColumnSize().orElse(0);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                return createDecimalOrStringType(precision, scale);
            }
            case "real":
                return Type.FLOAT;
            case "double precision":
                return Type.DOUBLE;
            case "char":
                return ScalarType.createCharType(fieldSchema.requiredColumnSize());
            case "timestamp":
            case "timestamptz": {
                // postgres can support microsecond
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "date":
                return ScalarType.createDateV2Type();
            case "boolean":
                return Type.BOOLEAN;
            case "bit":
                if (fieldSchema.getColumnSize().orElse(0) == 1) {
                    return Type.BOOLEAN;
                } else {
                    return ScalarType.createStringType();
                }
            case "text":
            case "varchar":
            case "bpchar":
                return ScalarType.createStringType();
            case "time":
            case "timetz":
                return ScalarType.createStringType();
            case "uuid":
                return ScalarType.createStringType();
            case "json":
            case "jsonb":
                return ScalarType.createStringType();
            case "cidr":
            case "inet":
            case "macaddr":
                return ScalarType.createStringType();
            case "point":
            case "line":
            case "lseg":
            case "box":
            case "path":
            case "polygon":
            case "circle":
                return ScalarType.createStringType();
            case "bytea":
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }


}
