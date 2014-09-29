/*
 * Copyright 2005-2014 WSO2, Inc. (http://wso2.com)
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package org.wso2.cep.geo;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.*;
import org.apache.log4j.Logger;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

@SiddhiExtension(namespace = "geo", function = "iswithin")
public class GeoIsWithin extends FunctionExecutor {

	Logger log = Logger.getLogger(GeoIsWithin.class);
	private GeometryFactory geometryFactory;
	private Polygon polygon;

	@Override
	public void init(Attribute.Type[] types, SiddhiContext siddhiContext) {
		if (types.length != 3) {
            log.error("Not enough number of method arguments");
			throw new QueryCreationException("Not enough number of method arguments");

		} else {
			if (types[0] != Attribute.Type.DOUBLE || types[1] != Attribute.Type.DOUBLE) {
                log.error("latitude and longitude must be provided as double values");
				throw new QueryCreationException("latitude and longitude must be provided as double values");
			}

			if (types[2] != Attribute.Type.STRING) {
                log.error("polygon parameter should be a GeoJSON feature string");
				throw new QueryCreationException("polygon parameter should be a GeoJSON feature string");
			}

            //Attributes to the siddhi expressions are (lat,lon,polygon) and polygon is given as a constant
          	String strPolygon = (String) attributeExpressionExecutors.get(2).execute(null);
			JsonObject jsonObject = new JsonParser().parse(strPolygon).getAsJsonObject();

			geometryFactory = JTSFactoryFinder.getGeometryFactory();

			JsonArray jLocCoordinatesArray = (JsonArray) jsonObject.getAsJsonArray("coordinates").get(0);
			Coordinate[] coords = new Coordinate[jLocCoordinatesArray.size()];

			for (int i = 0; i < jLocCoordinatesArray.size(); i++) {
				JsonArray jArray = (JsonArray) jLocCoordinatesArray.get(i);
				coords[i] = new Coordinate(Double.parseDouble(jArray.get(0).toString()),
				                           Double.parseDouble(jArray.get(1).toString()));
			}

			LinearRing ring = geometryFactory.createLinearRing(coords);

		    //create a polygon without holes
			polygon = geometryFactory.createPolygon(ring, null);

			if (log.isDebugEnabled()) {
				log.debug("isWithin function initialized successfully with polygon " + polygon.toString());
			}
		}
	}

	@Override
	protected Object process(Object obj) {

		Object functionParams[] = (Object[]) obj;

		double longitude = (Double) functionParams[0];
		double latitude = (Double) functionParams[1];

		// Creating a point
		Coordinate coord = new Coordinate(longitude, latitude);
		Point point = geometryFactory.createPoint(coord);

		return point.within(polygon);
	}

	public Type getReturnType() {
		return Attribute.Type.BOOL;
	}

	public void destroy() {
		if (log.isDebugEnabled()) {
			log.debug("GeoIsWithin function destroyed");
		}
	}
}
