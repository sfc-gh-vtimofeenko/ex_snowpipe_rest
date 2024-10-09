package com.example.SnowpipeRest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

import java.util.Map;
import java.util.List;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SnowpipeRestRepository {
    Logger logger = LoggerFactory.getLogger(SnowpipeRestRepository.class);

    private ObjectMapper objectMapper = new ObjectMapper();
    private SnowflakeStreamingIngestClient snowpipe_client;
    private SnowflakeStreamingIngestChannel channel;
    private Integer insert_count = 0;
    private SnowpipeRestWAL wal;

    @Value("${snowpipe.name}")
    private String suffix;

    @Value("${snowflake.url}")
    private String snowflake_url;

    @Value("${snowflake.user}")
    private String snowflake_user;

    @Value("${snowflake.role}")
    private String snowflake_role;

    @Value("${snowflake.private_key}")
    private String snowflake_private_key;

    @Value("${snowpipe.database}")
    private String database;

    @Value("${snowpipe.schema}")
    private String schema;

    @Value("${snowpipe.table}")
    private String table;

    @Value("${snowpiperest.wal.enable:1}")
    private int wal_enable;

    @Value("${snowpiperest.wal.flush:1}")
    private int wal_flush;

    @Value("${snowpiperest.wal.dir:wal}")
    private String wal_dir;

    @PostConstruct
    private void init() {
        // get Snowflake credentials and put them in props
        java.util.Properties props = new Properties();
        props.put("url", snowflake_url);
        props.put("user", snowflake_user);
        props.put("role", snowflake_role);
        props.put("private_key", snowflake_private_key);

        // Connect to Snowflake with credentials.
        try {
            // Make Snowflake Streaming Ingest Client
            snowpipe_client = SnowflakeStreamingIngestClientFactory.builder("SNOWPIPE_REST_CLIENT_" + suffix)
                    .setProperties(props).build();
        } catch (Exception e) {
            // Handle Exception for Snowpipe Streaming objects
            throw new RuntimeException(e);
        }

        // Create channel
        if (null == database)
            throw new RuntimeException("Must specify database");
        if (null == schema)
            throw new RuntimeException("Must specify schema");
        if (null == table)
            throw new RuntimeException("Must specify table");
        try {
            OpenChannelRequest request1 = OpenChannelRequest.builder("SNOWPIPE_REST_CHANNEL_" + suffix)
                    .setDBName(database)
                    .setSchemaName(schema)
                    .setTableName(table)
                    .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                    .build();
            channel = snowpipe_client.openChannel(request1);
        } catch (Exception e) {
            // Handle Exception for Snowpipe Streaming objects
            e.printStackTrace();
            throw new SnowpipeRestTableNotFoundException(String.format("Table not found (or no permissions): %s.%s.%s", database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()));
        }

        if (0 != wal_enable)
            wal = new SnowpipeRestWAL(this, wal_dir, wal_flush);
    }

    String getLatestCommittedOffsetToken() {
        return channel.getLatestCommittedOffsetToken();
    }

    public SnowpipeInsertResponse saveToSnowflake(String body) {
        return saveToSnowflake(body, wal_enable);
    }

    public SnowpipeInsertResponse saveToSnowflake(String body, int write_to_wal) {
        // Parse body
        List<Object> rowStrings;
        List<Map<String,Object>> rows;
        try {
            // Parse JSON body
            JsonNode jsonNode = objectMapper.readTree(body);
            // List of strings for error reporting
            rowStrings = objectMapper.convertValue(jsonNode, new TypeReference<List<Object>>() {});
            // List of Map<String,Object> for inserting
            rows = objectMapper.convertValue(jsonNode, new TypeReference<List<Map<String, Object>>>(){});
        }
        catch (JsonProcessingException je) {
            // throw new RuntimeException("Unable to parse body as list of JSON strings.");
            throw new SnowpipeRestJsonParseException("Unable to parse body as list of JSON strings.");
        }

        // Write the rows to the log
        String new_token = Integer.toString(insert_count);
        if (0 != write_to_wal)
            new_token = wal.write_to_log(rows);

        // Issue the insert
        InsertValidationResponse resp = channel.insertRows(rows, new_token);

        // Make response
        try {
            SnowpipeInsertResponse sp_resp = new SnowpipeInsertResponse(rows.size(), rows.size() - resp.getErrorRowCount(), resp.getErrorRowCount());
            for (InsertValidationResponse.InsertError insertError : resp.getInsertErrors()) {
                int idx = (int)insertError.getRowIndex();
                sp_resp.addError(idx, objectMapper.writeValueAsString(rowStrings.get(idx)), insertError.getMessage());
            }

            insert_count++;
            return sp_resp;
        }
        catch (JsonProcessingException je) {
            throw new RuntimeException(je);
        }
    }
}
