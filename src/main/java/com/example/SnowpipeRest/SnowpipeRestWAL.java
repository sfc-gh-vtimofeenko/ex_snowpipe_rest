package com.example.SnowpipeRest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

// @Component
public class SnowpipeRestWAL {
    Logger logger = LoggerFactory.getLogger(SnowpipeRestWAL.class);

    private SnowpipeRestRepository repo;
    private ObjectMapper objectMapper = new ObjectMapper();

    private String wal_dir;
    private int wal_flush;

    private BufferedWriter wal_writer = null;
    private String wal_prefix = "file_";
    private int wal_index = 0;
    private String wal_fname;
    private char token_separator = ':';
    private int cur_row = 0;
    private int rows_per_file = 1000;
    private int replay_chunk_size = 20;

    public SnowpipeRestWAL(SnowpipeRestRepository repo, String wal_dir, int wal_flush) {
        this.repo = repo;
        this.wal_dir = wal_dir;
        this.wal_flush = wal_flush;
        try {
            File wdir = new File(this.wal_dir);
            wdir.mkdirs();
            replay_if_needed();
            next_wal_writer();
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
            String offsetTokenFromSnowflake = repo.getLatestCommittedOffsetToken();
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
                    SnowpipeInsertResponse sir = repo.saveToSnowflake(sb.toString(), 0);
                    logger.info(String.format("replay_file: %s", sir.toString()));

                    // reset
                    idx = 0;
                    sb.setLength(1);
                }
            }
            if (idx > 0) {
                // save rows
                sb.append("]");
                SnowpipeInsertResponse sir = repo.saveToSnowflake(sb.toString(), 0);
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
        String last_offset = repo.getLatestCommittedOffsetToken();
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

    String write_to_log(List<Map<String,Object>> rows) {
        try {
            for (Map<String,Object> row : rows) {
                wal_writer.write(objectMapper.writeValueAsString(row));
                wal_writer.newLine();
                cur_row++;
                if (cur_row >= rows_per_file)
                    next_wal_writer();
            }
            if (0 != wal_flush)
                wal_writer.flush();
        }
        catch (IOException ioe) {
            // Error writing to WAL
        }
        return makeToken(wal_fname, cur_row - 1);
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
        String last_offset = repo.getLatestCommittedOffsetToken();
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

        List<CompletableFuture<Boolean>> futures = purgable.stream().map(f -> purge_file(f)).collect(Collectors.toList());
        CompletableFuture<Boolean> combinedFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
            .thenApply(v -> futures.stream().map(CompletableFuture::join).reduce(true, (a,b) -> Boolean.logicalAnd(a,b)));  //.collect(Collectors.toList()));
        
        return Optional.of(combinedFuture);
    }

}
