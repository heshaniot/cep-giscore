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

package org.wso2.cep.geo.test;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.wso2.cep.geo.GeoIsWithin;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class GeoIsWithinTestCase {

	private SiddhiManager siddhiManager;
	private String withinTrueQueryReference;
	private String withinFalseQueryReference;

	@Before
	public void setUpEnvironment() {
		SiddhiConfiguration conf = new SiddhiConfiguration();
		List<Class> classList = new ArrayList<Class>();
		classList.add(GeoIsWithin.class);
		conf.setSiddhiExtensions(classList);

		siddhiManager = new SiddhiManager();
		siddhiManager.getSiddhiContext().setSiddhiExtensions(classList);
		siddhiManager.defineStream("define stream gpsInputStream (latitude double, longitude double, deviceid string)");

		withinTrueQueryReference = siddhiManager.addQuery("from gpsInputStream[geo:iswithin(longitude, latitude,"
                    +" \"{ 'type': 'Polygon', 'coordinates': [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],"
                    +" [100.0, 1.0], [100.0, 0.0] ] ] }\")==true] select 1 as iswithin insert into gpsOutputStream;");

		withinFalseQueryReference =siddhiManager.addQuery("from gpsInputStream[geo:iswithin(latitude, longitude,"
                    +" \"{ 'type': 'Polygon', 'coordinates': [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],"
                    +" [100.0, 1.0], [100.0, 0.0] ] ] }\")==false] select 0 as iswithin insert into gpsOutputStream;");
	}

	@Test
	public void testGeoIsWithinTrue() throws InterruptedException {
		siddhiManager.addCallback(withinTrueQueryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				Assert.assertTrue((Integer) (inEvents[0].getData(0)) == 1);
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("gpsInputStream");
		inputHandler.send(new Object[] { 100.5, 0.5 });
	}

	@Test
	public void testGeoIsWithinFalse() throws InterruptedException {
		siddhiManager.addCallback(withinFalseQueryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				Assert.assertTrue((Integer) (inEvents[0].getData(0)) == 0);
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("gpsInputStream");
		inputHandler.send(new Object[] { 101.0, 7.0 });
	}

	@After
	public void cleanUpEnvironment() {
		siddhiManager.shutdown();
	}
}
