package org.superbiz.moviefun.blobstore;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.tika.Tika;
import org.apache.tika.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

public class S3Store implements BlobStore{

    private final Tika tika = new Tika();
    private final String bucket;
    private final AmazonS3 s3;

    public S3Store(AmazonS3Client s3, String bucket) {
        this.bucket = bucket;
        this.s3 = s3;
    }

    @Override
    public void put(Blob blob) throws IOException {
       s3.putObject(bucket,blob.name,blob.inputStream,new ObjectMetadata());
    }

    @Override
    public Optional<Blob> get(String name) throws IOException {

         if(!s3.doesObjectExist(bucket,name))
         {
             return Optional.empty();
         }

         S3Object s3Object = s3.getObject(bucket,name);

         S3ObjectInputStream content = s3Object.getObjectContent();

         byte[] inputStreamBytes = IOUtils.toByteArray(content);

         return Optional.of(new Blob(name, new ByteArrayInputStream(inputStreamBytes),tika.detect(inputStreamBytes)));

    }

    @Override
    public void deleteAll() {

    }
}
