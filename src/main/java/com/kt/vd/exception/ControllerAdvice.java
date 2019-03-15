package com.kt.vd.exception;

import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(basePackages = "com.kt.vd")
class ElasticSearchControllerAdvice {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchControllerAdvice.class);

    @ExceptionHandler(ElasticsearchException.class)
    @ResponseStatus(value = HttpStatus.NOT_FOUND)
    public String elasticsearchException(ElasticsearchException e){
        log.warn("ElasticSearch Error!!");
        return e.getMessage();
    }

    @ExceptionHandler(org.springframework.data.elasticsearch.ElasticsearchException.class)
    @ResponseStatus(value = HttpStatus.NOT_FOUND)
    public String elaticsearchDataException(org.springframework.data.elasticsearch.ElasticsearchException e){
        log.warn("ElasticSearch Data Error!!");
        return e.getMessage();
    }
}
//
//@RestControllerAdvice(annotations = RestController.class)
//public class RestSecurityControllerAdvice {
//    private static final Logger log = LoggerFactory.getLogger(RestSecurityControllerAdvice.class);
//
//    @ExceptionHandler(UnAuthenticationException.class)
//    @ResponseStatus(value = HttpStatus.UNAUTHORIZED)
//    public ErrorMessage unAuthentication(UnAuthenticationException e) {
//        log.debug("JSON API UnAuthenticationException is happened!");
//        return new ErrorMessage(e.getMessage());
//    }
//}
