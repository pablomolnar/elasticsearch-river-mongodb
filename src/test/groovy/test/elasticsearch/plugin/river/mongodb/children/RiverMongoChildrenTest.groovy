package test.elasticsearch.plugin.river.mongodb.children

import com.mongodb.*
import com.mongodb.util.JSON
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.SortOrder
import org.testng.Assert
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import test.elasticsearch.plugin.river.mongodb.RiverMongoDBTestAsbtract

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery

class RiverMongoChildrenTest extends RiverMongoDBTestAsbtract {

    DB mongoDB
    DBCollection mongoCollection

    protected RiverMongoChildrenTest() {
        super("testriver-"     + System.currentTimeMillis(),
              "testdatabase-"  + System.currentTimeMillis(),
               "documents-"    + System.currentTimeMillis(),
               "testindex-"    + System.currentTimeMillis())
    }

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase $database")
        try {
            mongoDB = mongo.getDB(database)
            mongoDB.setWriteConcern(WriteConcern.REPLICAS_SAFE)
            logger.info("Start createCollection")
            mongoCollection = mongoDB.createCollection(collection, null)
            Assert.assertNotNull(mongoCollection)
        } catch (e) {
            logger.error("createDatabase failed.", e)
        }
    }

    @AfterClass
    public void cleanUp() {
        logger.info "Drop database $mongoDB.name"
        mongoDB.dropDatabase()
    }


    @Test
    public void testChildrenAttribute() {
        try {
            logger.debug("Create river $river")

            // Create river
            super.createRiver(
                    "/test/elasticsearch/plugin/river/mongodb/children/test-mongodb-river-with-children.json", river,
                    mongoPort1.toString(), mongoPort2.toString(), mongoPort3.toString(),
                    database, collection, "tweets", index, database
            )

            // -- INSERT --

            // Insert test document in mongodb
            String mongoDocument = copyToStringFromClasspath("/test/elasticsearch/plugin/river/mongodb/children/test-simple-mongodb-document.json")
            DBObject dbObject = JSON.parse(mongoDocument)
            WriteResult result = mongoCollection.insert(dbObject)
            Thread.sleep(wait)
            String id = dbObject.get("_id").toString()
            logger.info("WriteResult: $result")

            // Refresh index just in case
            refreshIndex()

            // Assert index exists
            def response = node.client().admin().indices().exists(new IndicesExistsRequest(index))
            assert response.actionGet().isExists() == true

            // Search data by parent
            SearchResponse sr = node.client().prepareSearch(index).setQuery(fieldQuery("_parent", id)).addSort("text", SortOrder.ASC).execute().actionGet()
            logger.debug("SearchResponse $sr")

            // Assert hits count
            long totalHits = sr.hits.totalHits
            logger.debug("TotalHits: $totalHits")
            assert totalHits == 3

            // Assert hits data
            SearchHit[] hits = sr.hits.hits
            assert "bar" == hits[0].sourceAsMap().text
            assert "foo" == hits[1].sourceAsMap().text
            assert "zoo" == hits[2].sourceAsMap().text

            // -- UPDATE --


            mongoCollection.remove(dbObject, WriteConcern.REPLICAS_SAFE)

        } catch (Throwable t) {
            logger.error("test failed.", t)
            t.printStackTrace()
            throw t
        } finally {
            super.deleteRiver()
            super.deleteIndex()
        }
    }
}