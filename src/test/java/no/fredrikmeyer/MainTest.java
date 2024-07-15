package no.fredrikmeyer;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.HashMap;

@Testcontainers
public class MainTest {
    private BigQuery bigQuery;
    @Container
    public BigQueryEmulatorContainer container = new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.6.3");

    @BeforeEach
    public void setUp() {
        String url = container.getEmulatorHttpEndpoint();
        BigQueryOptions options = BigQueryOptions
                .newBuilder()
                .setProjectId(container.getProjectId())
                .setHost(url)
                .setLocation(url)
                .setCredentials(NoCredentials.getInstance())
                .build();
        // Now we have an address and port for Redis, no matter where it is running
        bigQuery = options.getService();
    }

    @Test
    public void test() throws InterruptedException {
        String DATASET = "my_dataset";
        bigQuery.create(DatasetInfo.newBuilder(DATASET).build());

        // Create a table
        var arrayOfRecords =
                Field.newBuilder(
                                "arr",
                                StandardSQLTypeName.STRUCT,
                                Field.of("somestring", StandardSQLTypeName.STRING),
                                Field.of("someotherstring", StandardSQLTypeName.STRING)
                        )
                        .setMode(Field.Mode.REPEATED)
                        .build();

        var schema = Schema.of(arrayOfRecords);

        var tableDefinition = StandardTableDefinition.newBuilder().setSchema(schema).build();

        String TABLE_NAME = "my_table";
        bigQuery.create(TableInfo.of(TableId.of(DATASET, TABLE_NAME), tableDefinition));

        // Insert some data
        HashMap<String, Object> data = new HashMap<>();
        data.put("somestring", "hei");
        data.put("someotherstring", "hei2");

        HashMap<String, Object> root = new HashMap<>();
        root.put("arr", Arrays.asList(data));

        InsertAllRequest request = InsertAllRequest.newBuilder(TableId.of(DATASET, TABLE_NAME))
                .addRow(InsertAllRequest.RowToInsert.of(root))
                .build();
        bigQuery.insertAll(request);

        var query = "select * from my_dataset.my_table";

        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();

        var res2 = bigQuery.query(queryJobConfiguration);

        res2.iterateAll().forEach(r -> {
            // Prints true
            System.out.println(r.hasSchema());
            // Prints false, even though the schema is defined above
            System.out.println(r.get("arr").getRepeatedValue().getFirst().getRecordValue().hasSchema());
        });
    }

}