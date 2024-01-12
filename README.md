## Get timedimensions for a layer ##

A WPS for GeoServer that can be used to retrieve the time dimension infos
for a layer. This works for PostGIS based layers and for layers based
directly on a table. The table name and layer name must be identical.

## Download ##

TODO

## Installation ##

Simply copy the WPS into the `WEB-INF/lib` directory where GeoServer
is deployed. In some versions of GeoServer (also depending on which
extensions are installed) you may have to also add the jackson-databind jar as well as the jackson-annotations jar.
First determine which version of jackson-core is used in your GeoServer
version: in `WEB-INF/lib` there will be a .jar like `jackson-core-2.10.5.jar`.
That means you need to download [jackson-databind-2.10.5.jar](https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind/2.10.5)
and [jackson-annotations-2.10.5.jar](https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-annotations/2.10.5).

## Inputs ##

* use `layerName` to specify the qualified feature type name

The result will be a JSON with objects like this:

```json
{
  "presentation":"CONTINUOUS_INTERVAL",
  "data": [
    "1974-01-01 01:00:00+01",
    "2015-01-01 01:00:00+01"
  ]
}
```

```json
{
  "presentation":"LIST",
  "data": [
    "2002-05-31 02:00:00+02",
    "2014-01-01 01:00:00+01",
    ...
    "2019-01-01 01:00:00+01"
  ]
}
```

## cURL example:
`curl -X POST -F 'file=@req.xml' 'http://localhost:8080/geoserver/ows'`

Contents of `req.xml`:
```
<?xml version="1.0" encoding="UTF-8"?><wps:Execute version="1.0.0" service="WPS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.opengis.net/wps/1.0.0" xmlns:wfs="http://www.opengis.net/wfs" xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" xmlns:wcs="http://www.opengis.net/wcs/1.1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd">
  <ows:Identifier>gs:TimeDimension</ows:Identifier>
  <wps:DataInputs>
    <wps:Input>
      <ows:Identifier>layerName</ows:Identifier>
      <wps:Data>
        <wps:LiteralData>namespace:mylayer</wps:LiteralData>
      </wps:Data>
    </wps:Input>
  </wps:DataInputs>
  <wps:ResponseForm>
    <wps:RawDataOutput mimeType="application/octet-stream">
      <ows:Identifier>result</ows:Identifier>
    </wps:RawDataOutput>
  </wps:ResponseForm>
</wps:Execute>
```
