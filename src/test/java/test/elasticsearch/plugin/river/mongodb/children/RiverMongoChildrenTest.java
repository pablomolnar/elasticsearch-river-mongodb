/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package test.elasticsearch.plugin.river.mongodb.children;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import test.elasticsearch.plugin.river.mongodb.RiverMongoDBTestAsbtract;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Test
public class RiverMongoChildrenTest extends RiverMongoDBTestAsbtract {

	private DB mongoDB;
	private DBCollection mongoCollection;

	protected RiverMongoChildrenTest() {
		super("testriver-"    + System.currentTimeMillis(),
              "testdatabase-" + System.currentTimeMillis(),
              "documents-"    + System.currentTimeMillis(),
              "testindex-"    + System.currentTimeMillis());
	}

	@BeforeClass
	public void createDatabase() {
		logger.debug("createDatabase {}", getDatabase());
		try {
			mongoDB = getMongo().getDB(getDatabase());
			mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE);
			logger.info("Start createCollection");
			mongoCollection = mongoDB.createCollection(getCollection(), null);
			Assert.assertNotNull(mongoCollection);
		} catch (Throwable t) {
			logger.error("createDatabase failed.", t);
		}
	}

	@AfterClass
	public void cleanUp() {
		// super.deleteRiver();
		logger.info("Drop database " + mongoDB.getName());
		mongoDB.dropDatabase();
	}



	@Test
	public void testChildrenAttribute() throws Throwable {
		try {
			logger.debug("Create river {}", getRiver());

            // Create river
			super.createRiver(
                    "/test/elasticsearch/plugin/river/mongodb/children/test-mongodb-river-with-children.json", getRiver(),
                    String.valueOf(getMongoPort1()), String.valueOf(getMongoPort2()), String.valueOf(getMongoPort3()),
                    getDatabase(), getCollection(), "tweets", getIndex(), getDatabase()
            );

            // -- INSERT --

            // Insert test document in mongodb
			String mongoDocument = copyToStringFromClasspath("/test/elasticsearch/plugin/river/mongodb/children/test-simple-mongodb-document.json");
			DBObject dbObject = (DBObject) JSON.parse(mongoDocument);
			WriteResult result = mongoCollection.insert(dbObject);
			Thread.sleep(wait);
			String id = dbObject.get("_id").toString();
			logger.info("WriteResult: {}", result.toString());

            // Refresh index just in case
			refreshIndex();

            // Assert index exists
			ActionFuture<IndicesExistsResponse> response = getNode().client().admin().indices().exists(new IndicesExistsRequest(getIndex()));
			assertThat(response.actionGet().isExists(), equalTo(true));

            // Search data by parent
			SearchResponse sr = getNode().client().prepareSearch(getIndex()).setQuery(fieldQuery("_parent", id)).addSort("text", SortOrder.ASC).execute().actionGet();
			logger.debug("SearchResponse {}", sr.toString());

            // Assert hits count
			long totalHits = sr.getHits().getTotalHits();
			logger.debug("TotalHits: {}", totalHits);
			assertThat(totalHits, equalTo(3l));

            // Assert hits data
            SearchHit[] hits = sr.getHits().getHits();
            assert "bar".equals(hits[0].sourceAsMap().get("text"));
            assert "foo".equals(hits[1].sourceAsMap().get("text"));
            assert "zoo".equals(hits[2].sourceAsMap().get("text"));



            // -- UPDATE --




			mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE);

		} catch (Throwable t) {
			logger.error("test failed.", t);
			t.printStackTrace();
			throw t;
		} finally {
			super.deleteRiver();
			super.deleteIndex();
		}
	}
}
