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
import org.geoserver.catalog.FeatureTypeInfo
import org.geoserver.catalog.ResourceInfo
import org.geoserver.catalog.impl.DimensionInfoImpl
import org.geoserver.config.GeoServer
import org.geoserver.security.decorators.ReadOnlyDataStore
import org.geoserver.security.decorators.SecuredFeatureSource
import org.geoserver.security.decorators.SecuredFeatureStore
import org.geoserver.security.decorators.SecuredFeatureTypeInfo
import org.geoserver.wps.gs.GeoServerProcess
import org.geoserver.wps.process.RawData
import org.geoserver.wps.process.StringRawData
import org.geotools.data.DataStoreFinder
import org.geotools.data.FeatureStore
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.store.ContentDataStore
import org.geotools.jdbc.JDBCDataStore
import org.geotools.process.factory.DescribeParameter
import org.geotools.process.factory.DescribeProcess
import org.geotools.process.factory.DescribeResult
import org.geotools.util.logging.Logging
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.*
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
        return try {
            LOGGER.info("Getting time dimensions for layer: $layerName")
            val factory = JsonNodeFactory(false)
            val root = factory.objectNode()
            val tableName = layerName.split(":".toRegex())[1]
            val layer = geoServer.catalog.getLayerByName(layerName)
            val resource = layer.getResource();

            if (resource == null) {
                return error("Resource not found.")
            }

            // Get the time dimension
            val timeData: DimensionInfoImpl = resource.metadata.get("time") as DimensionInfoImpl
            val attribute: String = timeData.attribute
            val presentation: DimensionPresentation = timeData.presentation

            val values = if (presentation === DimensionPresentation.LIST)
                getDistinctValuesFromResource(resource, attribute, tableName) else
                getMinMaxValuesFromResource(resource, attribute, tableName)

            root.put("presentation", presentation.toString())
            val dataNode = root.putArray("data")
            values.forEach { dataNode.add(it) }
            success(root);
        } catch (e: Exception) {
            LOGGER.log(Level.WARNING, "Error getting time dimension", e)
            error("Error getting time dimension: ${e.message}")
        }

    }

    private fun getDistinctValuesFromResource(resource: ResourceInfo, attribute: String, tableName: String): MutableSet<String> {
        if (resource is SecuredFeatureTypeInfo || resource is FeatureTypeInfo) {
            val dataStore = DataStoreFinder.getDataStore(resource.store.connectionParameters);
            return when (dataStore) {
                is JDBCDataStore -> getDistinctValuesFromStore(dataStore, attribute, tableName)
                is ShapefileDataStore -> getDistinctValuesFromStore(dataStore, attribute, tableName)
                else -> throw IllegalArgumentException("Unsupported data store type")
            }
        }
        throw Error("Unsupported resource type")
    }

    private fun getMinMaxValuesFromResource(resource: ResourceInfo, attribute: String, tableName: String): MutableSet<String> {
        if (resource is SecuredFeatureTypeInfo || resource is FeatureTypeInfo) {
            val dataStore = DataStoreFinder.getDataStore(resource.store.connectionParameters);
            return when (dataStore) {
                is JDBCDataStore -> getMinMaxValuesFromStore(dataStore, attribute, tableName)
                is ShapefileDataStore -> getMinMaxValuesFromStore(dataStore, attribute, tableName)
                else -> throw IllegalArgumentException("Unsupported data store type")
            }
        }
        throw Error("Unsupported resource type")
    }

    private fun getDistinctValuesFromStore(store: ReadOnlyDataStore, attribute: String, tableName: String): MutableSet<String> {
        val unwrappedDataStore = unwrapDataStore(store)
        return when (unwrappedDataStore) {
            is JDBCDataStore -> getDistinctValuesFromStore(unwrappedDataStore, attribute, tableName)
            is ShapefileDataStore -> getDistinctValuesFromStore(unwrappedDataStore, attribute, tableName)
            else -> throw IllegalArgumentException("Unsupported data store type")
        }
    }

    private fun getDistinctValuesFromStore(store: JDBCDataStore, attribute: String, tableName: String): MutableSet<String> {
        LOGGER.info("Get distinct values from JDBCDataStore");
        var conn: Connection? = null
        var stmt: PreparedStatement? = null
        val schema = store.databaseSchema
        conn = store.dataSource.connection
        var sql = "SELECT DISTINCT \"$attribute\" FROM $schema.\"$tableName\""

        LOGGER.fine("Final SQL: $sql")
        stmt = conn.prepareStatement(sql)
        var rs = stmt.executeQuery()

        try {
            val distinctValues = mutableSetOf<String>()
            while (rs.next()) {
                distinctValues.add(rs.getString(1))
            }
            return distinctValues
        } finally {
            rs.close()
        }
    }

    private fun getDistinctValuesFromStore(store: ShapefileDataStore, attribute: String, tableName: String): MutableSet<String> {
        LOGGER.info("… from ShapefileDataStore");
        val featureSource = store.featureSource
        val featureCollection = featureSource.features
        val iterator = featureCollection.features()
        val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")

        try {
            val distinctValues = mutableSetOf<String>()
            while (iterator.hasNext()) {
                val feature = iterator.next()
                val value = feature.getAttribute(attribute)
                if (value != null) {
                    distinctValues.add(sdf.format(value as Date))
                }
            }
            return distinctValues
        } finally {
            iterator.close()
        }
    }

    private fun getMinMaxValuesFromStore(store: JDBCDataStore, attribute: String, tableName: String): MutableSet<String> {
        LOGGER.info("Get min/max values from JDBCDataStore");
        var conn: Connection? = null
        var stmt: PreparedStatement? = null
        val schema = store.databaseSchema
        conn = store.dataSource.connection
        var sql = "SELECT MIN(\"$attribute\"), MAX(\"$attribute\") FROM $schema.\"$tableName\""

        LOGGER.fine("Final SQL: $sql")
        stmt = conn.prepareStatement(sql)
        var rs = stmt.executeQuery()

        try {
            val minMaxValues = mutableSetOf<String>()
            while (rs.next()) {
                minMaxValues.add(rs.getString(1))
                minMaxValues.add(rs.getString(2))
            }
            return minMaxValues
        } finally {
            rs.close()
        }
    }

    private fun getMinMaxValuesFromStore(store: ShapefileDataStore, attribute: String, tableName: String): MutableSet<String> {
        LOGGER.info("… from ShapefileDataStore");
        var values = getDistinctValuesFromStore(store, attribute, tableName)
        return mutableSetOf(values.min(), values.max())
    }

    @Throws(IllegalArgumentException::class)
    private fun unwrapDataStore(dataStore: ReadOnlyDataStore): ContentDataStore {
        val dataStoreTypes = listOf(
            JDBCDataStore::class.java,
            ShapefileDataStore::class.java,
        )

        for (type in dataStoreTypes) {
            try {
                val unwrappedDataStore = dataStore.unwrap(type)
                if (unwrappedDataStore != null) {
                    return unwrappedDataStore
                }
            } catch (e: Exception) {
                LOGGER.log(Level.FINE, "Could not unwrap data store of type ${type.simpleName}", e)
            }
        }
        throw IllegalArgumentException("Could not unwrap data store")
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
