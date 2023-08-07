/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.datasource.highgo.param;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.constants.DataSourceConstants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.AbstractDataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import org.apache.commons.collections4.MapUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.auto.service.AutoService;
import com.google.common.base.Strings;

@AutoService(DataSourceProcessor.class)
public class HighgoDataSourceProcessor extends AbstractDataSourceProcessor {

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, HighgoDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        HighgoConnectionParam connectionParams = (HighgoConnectionParam) createConnectionParams(connectionJson);
        HighgoDataSourceParamDTO HighgoDatasourceParamDTO = new HighgoDataSourceParamDTO();
        HighgoDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        HighgoDatasourceParamDTO.setUserName(connectionParams.getUser());
        HighgoDatasourceParamDTO.setOther(parseOther(connectionParams.getOther()));

        String address = connectionParams.getAddress();
        String[] hostSeperator = address.split(Constants.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constants.COMMA);
        HighgoDatasourceParamDTO.setHost(hostPortArray[0].split(Constants.COLON)[0]);
        HighgoDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constants.COLON)[1]));

        return HighgoDatasourceParamDTO;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam) {
        HighgoDataSourceParamDTO HighgoParam = (HighgoDataSourceParamDTO) datasourceParam;
        String address = String.format("%s%s:%s", DataSourceConstants.JDBC_HIGHGO, HighgoParam.getHost(),
                HighgoParam.getPort());
        String jdbcUrl = String.format("%s/%s", address, HighgoParam.getDatabase());

        HighgoConnectionParam HighgoConnectionParam = new HighgoConnectionParam();
        HighgoConnectionParam.setJdbcUrl(jdbcUrl);
        HighgoConnectionParam.setAddress(address);
        HighgoConnectionParam.setDatabase(HighgoParam.getDatabase());
        HighgoConnectionParam.setUser(HighgoParam.getUserName());
        HighgoConnectionParam.setPassword(PasswordUtils.encodePassword(HighgoParam.getPassword()));
        HighgoConnectionParam.setDriverClassName(getDatasourceDriver());
        HighgoConnectionParam.setValidationQuery(getValidationQuery());
        HighgoConnectionParam.setOther(transformOther(HighgoParam.getOther()));
        HighgoConnectionParam.setProps(HighgoParam.getOther());

        return HighgoConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, HighgoConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.COM_HIGHGO_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.HIGHGO_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        HighgoConnectionParam HighgoConnectionParam = (HighgoConnectionParam) connectionParam;
        if (!Strings.isNullOrEmpty(HighgoConnectionParam.getOther())) {
            return String.format("%s?%s", HighgoConnectionParam.getJdbcUrl(), HighgoConnectionParam.getOther());
        }
        return HighgoConnectionParam.getJdbcUrl();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        HighgoConnectionParam HighgoConnectionParam = (HighgoConnectionParam) connectionParam;
        Class.forName(getDatasourceDriver());
        return DriverManager.getConnection(getJdbcUrl(HighgoConnectionParam),
                HighgoConnectionParam.getUser(), PasswordUtils.decodePassword(HighgoConnectionParam.getPassword()));
    }

    @Override
    public DbType getDbType() {
        return DbType.HIGHGO;
    }

    @Override
    public DataSourceProcessor create() {
        return new HighgoDataSourceProcessor();
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isEmpty(otherMap)) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        otherMap.forEach((key, value) -> stringBuilder.append(String.format("%s=%s&", key, value)));
        return stringBuilder.toString();
    }

    private Map<String, String> parseOther(String other) {
        if (Strings.isNullOrEmpty(other)) {
            return null;
        }
        Map<String, String> otherMap = new LinkedHashMap<>();
        for (String config : other.split("&")) {
            String[] split = config.split("=");
            otherMap.put(split[0], split[1]);
        }
        return otherMap;
    }
}
