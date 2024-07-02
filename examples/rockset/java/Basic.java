/*
Example program using the official Rockset Java Client with a CrateDB backend.
Here: "Documents" and "Queries" APIs, basic usage.

Usage
=====
- Install dependencies: Run `mvn install`.
- Adjust configuration: Define `ROCKSET_APISERVER` environment variable.
- Run program: Run `mvn compile exec:java`.

Documentation
=============
- https://github.com/rockset/rockset-java-client/blob/master/src/main/java/com/rockset/examples/AddDocumentExample.java
- https://github.com/rockset/rockset-java-client/blob/master/src/main/java/com/rockset/examples/QueryExample.java
- https://docs.rockset.com/documentation/reference/adddocuments
- https://docs.rockset.com/documentation/reference/query
*/
import com.rockset.client.RocksetClient;
import com.rockset.client.model.AddDocumentsRequest;
import com.rockset.client.model.AddDocumentsResponse;
import com.rockset.client.model.CreateCollectionRequest;
import com.rockset.client.model.CreateCollectionResponse;
import com.rockset.client.model.QueryRequest;
import com.rockset.client.model.QueryRequestSql;
import com.rockset.client.model.QueryResponse;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class Basic {
  public static void main(String[] args) throws Exception {

    // Connect to API endpoint.
    String rockset_apiserver = System.getenv("ROCKSET_APISERVER");
    if (rockset_apiserver == null) { rockset_apiserver = "http://localhost:4243"; }
    String rockset_apikey = System.getenv("ROCKSET_APIKEY");
    if (rockset_apikey == null) { rockset_apikey = "abc123"; }

    RocksetClient rs = new RocksetClient(rockset_apikey, rockset_apiserver);

    // TODO: Implement Collections API mock.
    /*
    CreateCollectionRequest request =
        new CreateCollectionRequest().name("my-first-add-document-collection");
    try {
      CreateCollectionResponse response = rs.collections.create("commons", request);
      System.out.println(response);
      String collectionName = response.getData().getName();
      System.out.println("collectionName:", collectionName);
    } catch (Exception e) {
      e.printStackTrace();
    }
    */

    // Add Documents.
    LinkedList<Object> list = new LinkedList<>();
    Map<String, Object> json = new LinkedHashMap<>();
    json.put("name", "foo");
    json.put("address", "bar");
    json.put("dob", LocalDate.of(2000, 7, 4));
    json.put("event_time", Instant.ofEpochSecond(1000));
    list.add(json);

    AddDocumentsRequest documentsRequest = new AddDocumentsRequest().data(list);
    try {
      AddDocumentsResponse documentsResponse =
          rs.documents.add("commons", "foobar", documentsRequest);
      System.out.println(documentsResponse);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Submit SQL query.
    QueryRequest request =
        new QueryRequest().sql(new QueryRequestSql().query("SELECT * FROM \"commons\".\"foobar\""));

    try {
      QueryResponse response = rs.queries.query(request);
      System.out.println(response);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
