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
import java.util.Optional;
import java.util.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private BufferedWriter wal_writer = null;
    private String wal_prefix = "file_";
    private int wal_index = 0;
    private String wal_fname;
    private char token_separator = ':';
    private int cur_row = 0;
    private int rows_per_file = 1000;
    private int replay_chunk_size = 20;

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

        // Replay logs, if enabled
        try {
            if (0 != wal_enable) {
                File wdir = new File(wal_dir);
                wdir.mkdirs();
                replay_if_needed();
                next_wal_writer();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error during WAL replay");
        }
    }

    private void wait_for_flush() {
        // If no current WAL, nothing to flush
        if (null == wal_fname)
            return;

            int maxRetries = 20;
        int retryCount = 0;
        String token = makeToken(wal_fname, cur_row);

        do {
            String offsetTokenFromSnowflake = channel.getLatestCommittedOffsetToken();
            if (offsetTokenFromSnowflake != null
                    && offsetTokenFromSnowflake.equals(token)) {
                System.out.println("SUCCESSFULLY inserted");
                break;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            retryCount++;
        } while (retryCount < maxRetries);
    }

    private String makeToken(String fname, int row) {
        return fname.concat(String.valueOf(token_separator)).concat(String.valueOf(row));
    }

    private String token_to_fname(String token) {
        return token.substring(0, token.indexOf(token_separator));
    }

    private int token_to_row(String token) {
        String row_str = token.substring(token.indexOf(token_separator)+1);
        return Integer.parseInt(row_str);
    }

    private int max_wal_index() {
        return Stream.of(new File(wal_dir).listFiles())
            .map(f -> f.getName().substring(wal_prefix.length() + 1))
            .map(s -> Integer.parseInt(s))
            .max(Integer::compare)
            .orElse(-1);
    }

    private void next_wal_writer() throws IOException {
        if (null != wal_writer)
            wal_writer.close();
        wal_index = max_wal_index() + 1;
        wal_fname = wal_prefix.concat(String.format("%010d", wal_index));
        wal_writer = new BufferedWriter(new FileWriter(new File(wal_dir, wal_fname)));
        cur_row = 0;
        purge_old_log_files();
    }

    private List<String> get_wal_files(int wal_idx) {
        return Stream.of(new File(wal_dir).listFiles())
            .map(File::getName)
            .filter(wf -> (Integer.parseInt(wf.substring(wal_prefix.length())) > wal_idx))
            .collect(Collectors.toList());
    }

    private void replay_file(String fname, int offset) {
        String line;
        StringBuffer sb = new StringBuffer("[");
        int idx = 0;

        try {
            BufferedReader wal_reader = new BufferedReader(new FileReader(new File(wal_dir, fname)));
            wal_fname = fname;
            cur_row = 0;
            for (int i = 0; i < offset; i++) {
                wal_reader.readLine();
                cur_row++;
            }
            // for rows in fname starting at row
            while ((line = wal_reader.readLine()) != null) {
                //   make chunks of rows and insert
                sb.append((0 == idx) ? "" : ",").append(line);
                idx++;
                if (idx > replay_chunk_size) {
                    // save rows
                    sb.append("]");
                    SnowpipeInsertResponse sir = saveToSnowflake(sb.toString(), 0);
                    logger.info(String.format("replay_file: %s", sir.toString()));

                    // reset
                    idx = 0;
                    sb.setLength(1);
                }
            }
            if (idx > 0) {
                // save rows
                sb.append("]");
                SnowpipeInsertResponse sir = saveToSnowflake(sb.toString(), 0);
                logger.info(String.format("replay_file: %s", sir.toString()));        

                // reset
                idx = 0;
                sb.setLength(1);            
            }
            wal_reader.close();
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void replay_if_needed() {
        // Get the last committed offset token
        String last_offset = channel.getLatestCommittedOffsetToken();
        if ((null == last_offset) || (0 == last_offset.length()))
            // Nothing to do
            return;
        logger.info(String.format("replay_if_needed: last_offset: '%s'", last_offset));
        String fname = token_to_fname(last_offset);
        int row = token_to_row(last_offset) + 1;

        // Replay partial file
        replay_file(fname, row);

        // for all files "later" than fname
        int fname_idx = Integer.parseInt(fname.substring(wal_prefix.length()));
        List<String> wal_fnames = get_wal_files(fname_idx);
        logger.info(String.format("replay_if_needed: replaying later files: %s", wal_fnames));
        //   Replay full file
        wal_fnames.stream().forEachOrdered(f -> replay_file(f, 0));

        logger.info(String.format("Replayed %d files", wal_fnames.size() + 1));

        wait_for_flush();
    }

    private void write_to_log(List<Map<String,Object>> rows) {
        try {
            for (Map<String,Object> row : rows) {
                wal_writer.write(objectMapper.writeValueAsString(row));
                wal_writer.newLine();
                cur_row += rows.size();
                if (cur_row >= rows_per_file)
                    next_wal_writer();
            }
            if (0 != wal_flush)
                wal_writer.flush();
        }
        catch (IOException ioe) {
            // Error writing to WAL
        }
    }

    private CompletableFuture<Boolean> purge_file(String f) {
        return CompletableFuture.supplyAsync(() -> 
            {
                try {
                    File file = new File(wal_dir, f);
                    Boolean b = file.delete();
                    logger.info(String.format("Purging file %s: %s", f, b ? "success" : "failure"));
                    return b;
                }
                catch (Exception e) {
                    logger.info(String.format("Error purging file (%s)... skipping", f));
                    return false;
                }
            }
        );
    }

    private Optional<CompletableFuture<Boolean>> purge_old_log_files() {
        String last_offset = channel.getLatestCommittedOffsetToken();
        logger.info(String.format("purge_old_log_files: last_offset: '%s'", last_offset));
        if ((null == last_offset) || (0 == last_offset.length()))
            // Nothing to do
            return Optional.empty();
        String fname = token_to_fname(last_offset);
        int fname_idx = Integer.parseInt(fname.substring(wal_prefix.length() + 1));
        
        List<String> purgable = Stream.of(new File(wal_dir).listFiles())
                .map(File::getName)
                .filter(wf -> (Integer.parseInt(wf.substring(wal_prefix.length())) < fname_idx))
                .collect(Collectors.toList());
        logger.info(String.format("purge_old_log_files: purgable: %s", purgable));

        List<CompletableFuture<Boolean>> futures = purgable.stream().map(f -> purge_file(f)).toList();
        CompletableFuture<Boolean> combinedFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
            .thenApply(v -> futures.stream().map(CompletableFuture::join).reduce(true, (a,b) -> Boolean.logicalAnd(a,b)));  //.collect(Collectors.toList()));
        
        return Optional.of(combinedFuture);
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
        if (0 != write_to_wal)
            write_to_log(rows);

        // Issue the insert
        cur_row += rows.size();
        String new_token = makeToken(wal_fname, cur_row);
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

    public class SnowpipeInsertError {
        public int row_index;
        public String input;
        public String error;

        public SnowpipeInsertError(int row_index, String input, String error) {
            this.row_index = row_index;
            this.input = input;
            this.error = error;
        }

        public int getRow_index() {
            return row_index;
        }

        public String getInput() {
            return input;
        }

        public String getError() {
            return error;
        }

        public String toString() {
            return String.format("{\"row_index\": \"%s\", \"input\": \"%s\", \"error\": \"%s\"}", row_index, input, error);
        }
    }

    public class SnowpipeInsertResponse {
        int num_attempted;
        int num_succeeded;
        int num_errors;
        List<SnowpipeInsertError> errors;

        public SnowpipeInsertResponse(int num_attempted, int num_succeeded, int num_errors) {
            this(num_attempted, num_succeeded, num_errors, new ArrayList<SnowpipeInsertError>());
        }

        public SnowpipeInsertResponse(int num_attempted, int num_succeeded, int num_errors, List<SnowpipeInsertError> errors) {
            this.num_attempted = num_attempted;
            this.num_succeeded = num_succeeded;
            this.num_errors = num_errors;
            this.errors = errors;
        }

        public int getNum_attempted() {
            return num_attempted;
        }

        public int getNum_succeeded() {
            return num_succeeded;
        }

        public int getNum_errors() {
            return num_errors;
        }

        public List<SnowpipeInsertError> getErrors() {
            return errors;
        }

        public SnowpipeInsertResponse addError(int row_index, String input, String error) {
            return addError(new SnowpipeInsertError(row_index, input, error));
        }

        public SnowpipeInsertResponse addError(SnowpipeInsertError e) {
            errors.add(e);
            return this;
        }

        public String toString() {
            StringBuffer resp_body = new StringBuffer("{\n");
            resp_body.append(String.format(
                    "  \"inserts_attempted\": %d,\n  \"inserts_succeeded\": %d,\n  \"insert_errors\": %d,\n",
                    num_attempted, num_succeeded, num_errors));
            resp_body.append("  \"error_rows\":\n    [");
            String delim = " ";
            for (SnowpipeInsertError e: errors) {
                resp_body.append(String.format("\n    %s %s", delim, e.toString()));
                delim = ",";
            }
            resp_body.append("\n    ]");
            resp_body.append("\n}");
            return resp_body.toString();
        }
    }



}
