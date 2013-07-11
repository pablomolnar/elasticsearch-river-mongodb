package test.elasticsearch.plugin.river.mongodb.children

import com.gmongo.GMongo
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.WriteConcern
import org.bson.types.ObjectId
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.search.SearchHit
import org.testng.Assert
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test
import test.elasticsearch.plugin.river.mongodb.RiverMongoDBTestAsbtract

import static org.elasticsearch.index.query.QueryBuilders.fieldQuery
import static org.elasticsearch.search.sort.SortOrder.ASC

class RiverMongoChildrenTest extends RiverMongoDBTestAsbtract {

    def db
    DBCollection dbCollection

    protected RiverMongoChildrenTest() {
        super("testriver-"     + System.currentTimeMillis(),
              "testdatabase-"  + System.currentTimeMillis(),
               "documents-"    + System.currentTimeMillis(),
               "testindex-"    + System.currentTimeMillis())
    }

    @BeforeClass
    public void createDatabase() {
        logger.debug("createDatabase $database")
        db = new GMongo(mongo).getDB(database)
        db.setWriteConcern(WriteConcern.REPLICAS_SAFE)
        dbCollection = db.createCollection(collection, [:])
        Assert.assertNotNull(dbCollection)
    }

    @AfterClass
    public void cleanUp() {
        logger.info "Drop database $db.name"
        db.dropDatabase()
    }

    @Test
    public void testChildrenAttribute() {
        try {
            logger.debug("Create river $river")

            // Create river
            createRiver(
                    "/test/elasticsearch/plugin/river/mongodb/children/test-mongodb-river-with-children.json", river,
                    mongoPort1.toString(), mongoPort2.toString(), mongoPort3.toString(),
                    database, collection, "tweets", index, database
            )

            // -- INSERT --
            def document = [
                    name: "Richard",
                    tweets: [
                            [_id: "51c8ddbae4b0548e8d233181", text: "foo"],
                            [_id: "51c8ddbae4b0548e8d233182", text: "bar"],
                            [_id: "51c8ddbae4b0548e8d233183", text: "zoo"],
                    ]
            ]

            // Insert test document in mongodb
            def dbObject = new BasicDBObject(document)
            def result = dbCollection.insert(dbObject)
            logger.info("WriteResult: $result")
            Thread.sleep(1000)

            // Refresh index just in case
            refreshIndex()

            // Assert index exists
            def request = new IndicesExistsRequest(index)
            assert node.client().admin().indices().exists(request).actionGet().isExists() == true

            // Search data by parent
            def parentId = dbObject.get("_id").toString()
            def response = node.client().prepareSearch(index).setQuery(fieldQuery("_parent", parentId)).addSort("text", ASC).execute().actionGet()
            logger.debug("SearchResponse $response")

            // Asserts data
            assert response.hits.totalHits == 3
            SearchHit[] hits = response.hits.hits
            assert "bar" == hits[0].sourceAsMap().text
            assert "foo" == hits[1].sourceAsMap().text
            assert "zoo" == hits[2].sourceAsMap().text



            // -- UPDATES SCENARIOS --

            // #1: Replace whole document
            document.tweets[0].text = "fool"
            dbCollection.update([_id: new ObjectId(parentId)], document)
            Thread.sleep(1000)

            response = node.client().prepareSearch(index).setQuery(fieldQuery("_parent", parentId)).addSort("text", ASC).execute().actionGet()
            logger.debug("SearchResponse $response")

            // Asserts data
            assert response.hits.totalHits == 3
            hits = response.hits.hits
            assert "bar"  == hits[0].sourceAsMap().text
            assert "fool" == hits[1].sourceAsMap().text
            assert "zoo"  == hits[2].sourceAsMap().text



            // #2: Push one value to the array
            dbCollection.update([_id: new ObjectId(parentId)], [$push: [tweets:[_id: "51c8ddbae4b0548e8d233184", text: "abc"]]])
            Thread.sleep(1000)

            response = node.client().prepareSearch(index).setQuery(fieldQuery("_parent", parentId)).addSort("text", ASC).execute().actionGet()
            logger.debug("SearchResponse $response")

            // Asserts data
            assert response.hits.totalHits == 4
            hits = response.hits.hits
            assert "abc"  == hits[0].sourceAsMap().text
            assert "bar"  == hits[1].sourceAsMap().text
            assert "fool" == hits[2].sourceAsMap().text
            assert "zoo"  == hits[3].sourceAsMap().text



            // #3: Pull one value from the array
            dbCollection.update([_id: new ObjectId(parentId)], [$pull: [tweets:[text: "bar"]]])
            Thread.sleep(1000)

            response = node.client().prepareSearch(index).setQuery(fieldQuery("_parent", parentId)).addSort("text", ASC).execute().actionGet()
            logger.debug("SearchResponse $response")

            // Asserts data
            assert response.hits.totalHits == 3
            hits = response.hits.hits
            assert "abc"  == hits[0].sourceAsMap().text
            assert "fool" == hits[1].sourceAsMap().text
            assert "zoo"  == hits[2].sourceAsMap().text



            // -- DELETE --
            dbCollection.remove([_id: new ObjectId(parentId)])
            Thread.sleep(1000)

            response = node.client().prepareSearch(index).setQuery(fieldQuery("_parent", parentId)).execute().actionGet()
            logger.debug("SearchResponse $response")

            // Asserts data
            assert response.hits.totalHits == 0

        } catch (e) {
            logger.error("test failed.", e)
            e.printStackTrace()
            throw e
        } finally {
            super.deleteRiver()
            super.deleteIndex()
        }
    }
}