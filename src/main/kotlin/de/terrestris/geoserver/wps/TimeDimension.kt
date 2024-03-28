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
import org.geoserver.catalog.*
import org.geoserver.catalog.impl.DimensionInfoImpl
import org.geoserver.config.GeoServer
import org.geoserver.security.decorators.SecuredFeatureTypeInfo
import org.geoserver.wps.gs.GeoServerProcess
import org.geoserver.wps.process.RawData
import org.geoserver.wps.process.StringRawData
import org.geotools.data.DataStoreFinder
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.jdbc.JDBCDataStore
import org.geotools.process.factory.DescribeParameter
import org.geotools.process.factory.DescribeProcess
import org.geotools.process.factory.DescribeResult
import org.geotools.util.logging.Logging
import java.io.File
import java.net.URL
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

    @DescribeResult(name = "result", description = "A json object containing two keys: \"presentation\" -> The DimensionPresentation chosen for the layer. \"data\" -> Contains the date values.", primary = true)
    @Throws(
        JsonProcessingException::class, SQLException::class
    )
    fun execute(
        @DescribeParameter(
            name = "layerName",
            description = "The qualified name of the layer to retrieve the values from."
        ) layerName: String
    ): RawData {
        return try {
            LOGGER.info("Getting time dimensions for layer: $layerName")
            val factory = JsonNodeFactory(false)
            val root = factory.objectNode()
            val resourceName = layerName.split(":".toRegex())[1]
            val layer = geoServer.catalog.getLayerByName(layerName)
            val resource = layer.getResource()
                ?: return error("Resource not found.")

            // Get the time dimension
            val timeData = resource.metadata["time"] as DimensionInfoImpl
            val presentation = timeData.presentation
            var attribute = timeData.attribute
            val values: MutableSet<String>

            LOGGER.fine("Time dimension presentation: $presentation")
            LOGGER.fine("Time dimension attribute: $attribute")
            if (resource is CoverageInfo) {
                LOGGER.fine("Resource is coverage")
                attribute = getTimeAttributeFromCoverage(resource)
                val shapefileDataStore = getShapeFileResourceFromCoverage(resource)
                values = if (presentation === DimensionPresentation.LIST)
                    getDistinctValuesFromStore(shapefileDataStore, attribute) else
                    getMinMaxValuesFromStore(shapefileDataStore, attribute)
            } else {
                LOGGER.fine("Resource is feature type")
                values = if (presentation === DimensionPresentation.LIST)
                    getDistinctValuesFromResource(resource, attribute, resourceName) else
                    getMinMaxValuesFromResource(resource, attribute, resourceName)
            }

            root.put("presentation", presentation.toString())
            val dataNode = root.putArray("data")
            values.forEach { dataNode.add(it) }
            success(root)
        } catch (e: Exception) {
            LOGGER.log(Level.WARNING, "Error getting time dimension", e)
            error("Error getting time dimension: ${e.message}")
        }
    }

    private fun getTimeAttributeFromCoverage(coverageInfo: CoverageInfo): String {
        LOGGER.info("Getting time attribute from coverage")
        val dataStore = coverageInfo.store
        val baseDirectory = dataStore.catalog.resourceLoader.baseDirectory

        // list files in the directory
        val directory = File(baseDirectory, dataStore.url.removePrefix("file:"))

        if (!directory.isDirectory) {
            throw Error("Coverage directory not found:  ${directory.absolutePath}")
        }

        val files = directory.listFiles()

        LOGGER.info("Looking for properties file in coverage directory: ${directory.absolutePath}")
        for (file in files!!) {
            if (file.name.endsWith("properties") && !file.name.equals("indexer.properties") && !file.name.equals("timeregex.properties")) {
                val url = URL("file:${file.absolutePath}")
                val properties = Properties()
                properties.load(url.openStream())
                return properties.getProperty("TimeAttribute")
            }
        }
        throw Error("Could not find time attribute in coverage properties")
    }

    private fun getShapeFileResourceFromCoverage(coverageInfo: CoverageInfo): ShapefileDataStore {
        LOGGER.info("Getting shapefile resource from coverage")
        val dataStore = coverageInfo.store
        val baseDirectory = dataStore.catalog.resourceLoader.baseDirectory

        // list files in the directory
        val directory = File(baseDirectory, dataStore.url.removePrefix("file:"))

        if (!directory.isDirectory) {
            throw Error("Coverage directory not found: ${directory.absolutePath}")
        }

        val files = directory.listFiles()
        // throw error if folder contains more than one file that ends with .shp
        val shapeFiles = files?.filter { it.name.endsWith(".shp") }
        if (shapeFiles === null || shapeFiles.size > 1) {
            throw Error("Coverage contains more than one shapefile or no shapefile.")
        }
        val storeParams = mapOf(
            "url" to shapeFiles[0].toURI().toURL()
        )
        return DataStoreFinder.getDataStore(storeParams) as ShapefileDataStore
    }

    private fun getDistinctValuesFromResource(resource: ResourceInfo, attribute: String, tableName: String): MutableSet<String> {
        if (resource is SecuredFeatureTypeInfo || resource is FeatureTypeInfo) {
            val dataStore = DataStoreFinder.getDataStore(resource.store.connectionParameters)
            return when (dataStore) {
                is JDBCDataStore -> getDistinctValuesFromStore(dataStore, attribute, tableName)
                is ShapefileDataStore -> getDistinctValuesFromStore(dataStore, attribute)
                else -> throw IllegalArgumentException("Unsupported data store type")
            }
        }
        throw Error("Unsupported resource type: ${resource::class.simpleName}")
    }

    private fun getMinMaxValuesFromResource(resource: ResourceInfo, attribute: String, tableName: String): MutableSet<String> {
        if (resource is SecuredFeatureTypeInfo || resource is FeatureTypeInfo) {
            val dataStore = DataStoreFinder.getDataStore(resource.store.connectionParameters)
            return when (dataStore) {
                is JDBCDataStore -> getMinMaxValuesFromStore(dataStore, attribute, tableName)
                is ShapefileDataStore -> getMinMaxValuesFromStore(dataStore, attribute)
                else -> throw IllegalArgumentException("Unsupported data store type")
            }
        }
        throw Error("Unsupported resource type: ${resource::class.simpleName}")
    }

    private fun getDistinctValuesFromStore(store: JDBCDataStore, attribute: String, tableName: String): MutableSet<String> {
        LOGGER.info("Get distinct values from JDBCDataStore")
        val schema = store.databaseSchema
        val conn = store.dataSource.connection
        val sql = "SELECT DISTINCT \"$attribute\" FROM $schema.\"$tableName\""

        LOGGER.fine("Final SQL: $sql")
        val stmt = conn.prepareStatement(sql)
        val resultSet = stmt.executeQuery()

        resultSet.use { rs ->
            val distinctValues = mutableSetOf<String>()
            while (rs.next()) {
                distinctValues.add(rs.getString(1))
            }
            return distinctValues
        }
    }

    private fun getDistinctValuesFromStore(store: ShapefileDataStore, attribute: String): MutableSet<String> {
        LOGGER.info("Get distinct values from ShapefileDataStore")
        val featureSource = store.featureSource
        val featureCollection = featureSource.features
        val iterator = featureCollection.features()
        val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")

        iterator.use { i ->
            val distinctValues = mutableSetOf<String>()
            while (i.hasNext()) {
                val feature = i.next()
                val value = feature.getAttribute(attribute)
                if (value != null) {
                    distinctValues.add(sdf.format(value as Date))
                }
            }
            return distinctValues
        }
    }

    private fun getMinMaxValuesFromStore(store: JDBCDataStore, attribute: String, tableName: String): MutableSet<String> {
        LOGGER.info("Get min/max values from JDBCDataStore")
        val schema = store.databaseSchema
        val conn = store.dataSource.connection
        val sql = "SELECT MIN(\"$attribute\"), MAX(\"$attribute\") FROM $schema.\"$tableName\""

        LOGGER.fine("Final SQL: $sql")
        val stmt = conn.prepareStatement(sql)
        val resultSet = stmt.executeQuery()

        resultSet.use { rs ->
            val minMaxValues = mutableSetOf<String>()
            while (rs.next()) {
                minMaxValues.add(rs.getString(1))
                minMaxValues.add(rs.getString(2))
            }
            return minMaxValues
        }
    }

    private fun getMinMaxValuesFromStore(store: ShapefileDataStore, attribute: String): MutableSet<String> {
        LOGGER.info("Get min/max values from ShapefileDataStore")
        val values = getDistinctValuesFromStore(store, attribute)
        return mutableSetOf(values.min(), values.max())
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
