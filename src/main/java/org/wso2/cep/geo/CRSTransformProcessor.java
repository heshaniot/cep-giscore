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

import com.vividsolutions.jts.geom.Coordinate;
import org.apache.log4j.Logger;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SiddhiExtension(namespace = "geo", function = "crstransform")
public class CRSTransformProcessor extends TransformProcessor {
    Logger log = Logger.getLogger(CRSTransformProcessor.class);

	private Map<String, Integer> paramPositions = new HashMap<String, Integer>();
	private String sourcecrs, targetcrs;
	private String latAttrName, longAttrName;

	public CRSTransformProcessor() {
		this.outStreamDefinition =
		                           new StreamDefinition().name("geoStream")
		                                                 .attribute("lattitude",
		                                                            Attribute.Type.DOUBLE)
		                                                 .attribute("longitude",
		                                                            Attribute.Type.DOUBLE);
	}

	@Override
	protected InStream processEvent(InEvent inEvent) {

		double sourceLat = (Double) inEvent.getData(paramPositions.get(latAttrName));
		double sourceLon = (Double) inEvent.getData(paramPositions.get(longAttrName));

		CoordinateReferenceSystem sourceCrs = null;
		CoordinateReferenceSystem targetCrs = null;

		try {
			sourceCrs = CRS.decode(sourcecrs);
			targetCrs = CRS.decode(targetcrs);
		} catch (NoSuchAuthorityCodeException e) {
            log.error("Cannot decode Source and Target CRS",e);
        } catch (FactoryException e) {
            log.error("Cannot decode source and Target CRS",e);
		}

		Coordinate sourceCoordinate = new Coordinate(sourceLat, sourceLon);
		Coordinate targetCoordinate = new Coordinate();

		boolean lenient = true;
		MathTransform mathTransform = null;

		try {
			mathTransform = CRS.findMathTransform(sourceCrs, targetCrs, lenient);
            JTS.transform(sourceCoordinate, targetCoordinate, mathTransform);
		} catch (FactoryException e) {
            log.error("Cannot find a math transform from Source CRS to Target CRS",e);
		}
        catch (TransformException e) {
            log.error("Cannot transform from " + sourcecrs + " to " + targetcrs,e);
		}

		double targetLat = targetCoordinate.x;
		double targetLon = targetCoordinate.y;
		Object[] data = new Object[] { targetLat, targetLon };

		return new InEvent(inEvent.getStreamId(), System.currentTimeMillis(), data);
	}

	@Override
	protected InStream processEvent(InListEvent inListEvent) {
		InListEvent transformedListEvent = new InListEvent();
		for (Event event : inListEvent.getEvents()) {
			if (event instanceof InEvent) {
				transformedListEvent.addEvent((Event) processEvent((InEvent) event));
			}
		}
		return transformedListEvent;
	}

	@Override
	protected Object[] currentState() {
		return new Object[] { paramPositions };
	}

	@Override
	protected void restoreState(Object[] objects) {
		if (objects.length > 0 && objects[0] instanceof Map) {
			paramPositions = (Map<String, Integer>) objects[0];
		}
	}

	@Override
	protected void init(Expression[] parameters, List<ExpressionExecutor> expressionExecutors,
	                    StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition,
	                    String elementId, SiddhiContext siddhiContext) {

		for (Expression parameter : parameters) {
			if (parameter instanceof Variable) {
				Variable var = (Variable) parameter;
				String attributeName = var.getAttributeName();
				paramPositions.put(attributeName,
				                   inStreamDefinition.getAttributePosition(attributeName));
			}
		}
		sourcecrs = ((StringConstant) parameters[0]).getValue();
		targetcrs = ((StringConstant) parameters[1]).getValue();
		latAttrName = ((Variable) parameters[2]).getAttributeName();
		longAttrName = ((Variable) parameters[3]).getAttributeName();

	}

	public void destroy() {
	}
}