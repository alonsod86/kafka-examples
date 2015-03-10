package es.cancamusa.kafka.codecs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka message decoder for Gzip message stream
 * @author dgutierrez
 *
 */
public class GzipDecoder implements Decoder<String> {
	static Logger log = LoggerFactory.getLogger(GzipDecoder.class.getSimpleName());
	
	public GzipDecoder(VerifiableProperties verifiableProperties) {
	}

	public String fromBytes(byte[] arg0) {
		try {
			return decompress(arg0);
		} catch (IOException e) {
			log.error("Decoding kafka stream", e);
			return null;
		}
	}
	
	public static String decompress(byte[] compressed) throws IOException {
	    final int BUFFER_SIZE = 32;
	    ByteArrayInputStream is = new ByteArrayInputStream(compressed);
	    GZIPInputStream gis = new GZIPInputStream(is, BUFFER_SIZE);
	    StringBuilder string = new StringBuilder();
	    byte[] data = new byte[BUFFER_SIZE];
	    int bytesRead;
	    while ((bytesRead = gis.read(data)) != -1) {
	        string.append(new String(data, 0, bytesRead));
	    }
	    gis.close();
	    is.close();
	    return string.toString();
	}
}
