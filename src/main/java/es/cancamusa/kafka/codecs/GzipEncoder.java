package es.cancamusa.kafka.codecs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class GzipEncoder implements Encoder<String> {
    public GzipEncoder(VerifiableProperties verifiableProperties) {
    }

    public byte[] toBytes(String customMessage) {
        try {
            return compress(customMessage);
        } catch (IOException e) {
            return null;
        }
    }

    private byte[] compress(String string) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream(string.length());
        GZIPOutputStream gos = new GZIPOutputStream(os);
        gos.write(string.getBytes());
        gos.close();
        byte[] compressed = os.toByteArray();
        os.close();
        return compressed;
    }
}