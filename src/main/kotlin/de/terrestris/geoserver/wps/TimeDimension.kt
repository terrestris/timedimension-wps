/* Copyright 2020-present terrestris GmbH & Co. KG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.terrestris.geoserver.wps

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.geoserver.catalog.DimensionPresentation
import org.geoserver.catalog.impl.DimensionInfoImpl
import org.geoserver.config.GeoServer
import org.geoserver.security.decorators.ReadOnlyDataStore
import org.geoserver.wps.gs.GeoServerProcess
import org.geoserver.wps.process.RawData
import org.geoserver.wps.process.StringRawData
import org.geotools.jdbc.JDBCDataStore
import org.geotools.process.factory.DescribeParameter
import org.geotools.process.factory.DescribeProcess
import org.geotools.process.factory.DescribeResult
import org.geotools.util.logging.Logging
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.logging.Level

@DescribeProcess(title = "timeDimension", description = "Gets the time dimension of a layer.")
class TimeDimension(private val geoServer: GeoServer) : GeoServerProcess {

    companion object {
        private val LOGGER = Logging.getLogger(TimeDimension::class.java)
    }

    private val objectMapper: ObjectMapper = ObjectMapper()

    @DescribeResult(name = "result", description = "A json object containing two keys: \"presentation\" -> The DimensionPresentation chosen for the layer. \"data\" -> Contains the date valus.", primary = true)
    @Throws(
        JsonProcessingException::class, SQLException::class
    )
    fun execute(
        @DescribeParameter(
            name = "layerName",
            description = "The qualified name of the layer to retrieve the values from. The layer must be based on a " +
                    "JDBC/postgres datastore and be based on a single table with a name equal to the layer name."
        ) layerName: String
    ): RawData {
        var conn: Connection? = null
        var stmt: PreparedStatement? = null
        var rs: ResultSet? = null
        return try {
            val factory = JsonNodeFactory(false)
            val root = factory.objectNode()
            val tableName = layerName.split(":".toRegex())[1]
            val featureType =
                geoServer.catalog.getFeatureTypeByName(layerName) ?: return error("Feature type not found.")
            val source = featureType.getFeatureSource(null, null) ?: return error("Source not found.")
            var store = source.dataStore
            if (store !is JDBCDataStore && store !is ReadOnlyDataStore) {
                return error("Store is not a JDBC data store: ${store.javaClass.canonicalName}")
            }
            if (store is ReadOnlyDataStore) {
                store = store.unwrap(JDBCDataStore::class.java)
            }
            if (store !is JDBCDataStore) {
                return error("Potentially wrapped store is not a JDBC data store: ${store.javaClass.canonicalName}")
            }
            val dataStore = store
            val schema = dataStore.databaseSchema
            conn = dataStore.dataSource.connection
            var sql: String

            val timeData: DimensionInfoImpl = featureType.metadata.get("time") as DimensionInfoImpl
            val attribute: String = timeData.attribute
            val presentation: DimensionPresentation = timeData.presentation

            root.put("presentation", presentation.toString())
            if (presentation === DimensionPresentation.LIST) {
                sql = "SELECT DISTINCT \"$attribute\" FROM $schema.\"$tableName\""
            } else {
                sql = "SELECT MIN(\"$attribute\"), MAX(\"$attribute\") FROM $schema.\"$tableName\""
            }

            LOGGER.fine("Final SQL: $sql")
            stmt = conn.prepareStatement(sql)
            rs = stmt.executeQuery()

            var dataNode = root.putArray("data")
            while (rs.next()) {
                if (presentation === DimensionPresentation.LIST) {
                    var value = rs.getString(1)
                    dataNode.add(value)
                } else {
                    var min = rs.getString(1)
                    var max = rs.getString(2)
                    dataNode.add(min)
                    dataNode.add(max)
                }
            }
            success(root)
        } catch (e: Exception) {
            LOGGER.fine("Error when getting distinct values: " + e.message)
            LOGGER.log(Level.FINE, "Stack trace:", e)
            error("Error: " + e.message)
        } finally {
            conn?.close()
            stmt?.close()
            rs?.close()
        }
    }

    @Throws(JsonProcessingException::class)
    fun error(msg: String): StringRawData {
        val returnMap: MutableMap<String, Any> = HashMap(2)
        returnMap["message"] = msg
        returnMap["success"] = false
        return StringRawData(objectMapper.writeValueAsString(returnMap), "application/json")
    }

    @Throws(JsonProcessingException::class)
    fun success(dataset: JsonNode?): StringRawData {
        return StringRawData(objectMapper.writeValueAsString(dataset), "application/json")
    }

}
