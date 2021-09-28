package com.rsanfelice.aws.controller;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.waiters.WaiterParameters;
import lombok.Builder;
import lombok.Value;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@Log4j2
@RequestMapping("/s3")
public class AmazonS3Controller {

    private final AmazonS3Client client;

    public AmazonS3Controller(AmazonS3Client client) {
        this.client = client;
    }

    @PostMapping("/{bucket}")
    public void create(@PathVariable String bucket) {
        // send bucket creation request
        client.createBucket(bucket);
        log.info("Request to create " + bucket + " sent");

        // assure that bucket is available
        client.waiters().bucketExists().run(new WaiterParameters<>(new HeadBucketRequest(bucket)));
        log.info("Bucket " + bucket + " is ready");
    }

    @GetMapping
    public List<Object> listObjectsInBucket(@RequestParam String bucket) {
        var items =
                client.listObjectsV2(bucket).getObjectSummaries().stream()
                        .parallel()
                        .map(S3ObjectSummary::getKey)
                        .map(key -> mapS3ToObject(bucket, key))
                        .collect(Collectors.toList());

        log.info("Found " + items.size() + " objects in the bucket " + bucket);
        return items;
    }

    private Object mapS3ToObject(String bucket, String key) {

        return Object.builder()
                .name(client.getObjectMetadata(bucket, key).getUserMetaDataOf("name"))
                .key(key)
                .url(client.getUrl(bucket, key))
                .isPublic(
                        client.getObjectAcl(bucket, key).getGrantsAsList().stream()
                                .anyMatch(grant -> grant.equals(AmazonS3Controller.publicObjectReadGrant())))
                .build();
    }

    private static Grant publicObjectReadGrant() {
        return new Grant(
                GroupGrantee.parseGroupGrantee(GroupGrantee.AllUsers.getIdentifier()), Permission.Read);
    }

    @Value
    @Builder
    public static class Object {
        String name;
        String key;
        URL url;
        @Builder.Default boolean isPublic = false;
    }


}
