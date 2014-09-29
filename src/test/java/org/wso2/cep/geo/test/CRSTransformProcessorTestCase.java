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
import org.wso2.cep.geo.CRSTransformProcessor;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class CRSTransformProcessorTestCase {

    private SiddhiManager siddhiManager;
    private String queryReference;

    @Before
    public void setUpEnvironment() {
        SiddhiConfiguration conf = new SiddhiConfiguration();
        List<Class> classList = new ArrayList<Class>();
        classList.add(CRSTransformProcessor.class);
        conf.setSiddhiExtensions(classList);

        siddhiManager = new SiddhiManager();
        siddhiManager.getSiddhiContext().setSiddhiExtensions(classList);
        siddhiManager.defineStream("define stream gpsInputStream (latitude double, longitude double) ");

        queryReference =
                siddhiManager.addQuery("from gpsInputStream#transform.geo:crstransform"
                                       + " (\"EPSG:4326\",\"EPSG:25829\",latitude,longitude) "
                                       + "select latitude,longitude insert into geoStream;");
    }

    @Test
    public void testCRSTransform() throws InterruptedException {
        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                Assert.assertTrue((Double) (inEvents[0].getData(0)) == 1505646.888236971);
            }
        });

        InputHandler inputHandler = siddhiManager.getInputHandler("gpsInputStream");
        inputHandler.send(new Object[]{0.0, 0.0});
    }

    @After
    public void cleanUpEnvironment() {
        siddhiManager.shutdown();
    }
}
