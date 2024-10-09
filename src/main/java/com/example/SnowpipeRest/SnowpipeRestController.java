package com.example.SnowpipeRest;

import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/snowpipe")
public class SnowpipeRestController {
    Logger logger = LoggerFactory.getLogger(SnowpipeRestController.class);

    @Autowired
    private SnowpipeRestRepository repos;

    @GetMapping("/hello")
    @ResponseBody
    public String hello() {
        return "Hello, there.";
    }

    @PutMapping("/insert")
    @ResponseBody
    public String insert(@RequestBody String body) {
        SnowpipeInsertResponse sp_resp = repos.saveToSnowflake(body);
        return sp_resp.toString();
    }

    @ExceptionHandler(SnowpipeRestTableNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ResponseEntity<String> handleTableNotFound(SnowpipeRestTableNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
    }

    @ExceptionHandler(SnowpipeRestJsonParseException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ResponseEntity<String> handleBadJson(SnowpipeRestJsonParseException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
    }
}
