/*
 * Copyright 2017 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.compress.gzipFpga;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import org.junit.BeforeClass;

/**
 *
 * @author Radu Hobincu &lt;radu.hobincu@upb.ro&gt;
 */
public class GzipFpgaCompressorTest {
    
    private static final Logger LOGGER = Logger.getLogger(GzipFpgaCompressorTest.class.getName());
    private static final int SEED = 100;
    private static final Random GENERATOR = new Random(SEED);
    
    private static byte[] generateRandomData(int size) {
        byte[] buffer = new byte[size];
        GENERATOR.nextBytes(buffer);
        return buffer;
    }
    
    private static byte[] decompress(byte[] compressedStream) throws ZipException, IOException {
        
        ByteArrayInputStream bytein = new ByteArrayInputStream(compressedStream);
        GZIPInputStream gzin = new GZIPInputStream(bytein);
        ByteArrayOutputStream byteout = new ByteArrayOutputStream();
        
        int res = 0;
        byte buf[] = new byte[1024];
        while (res >= 0) {
            res = gzin.read(buf, 0, buf.length);
            if (res > 0) {
                byteout.write(buf, 0, res);
            }
        }
        return byteout.toByteArray();
    }

    //@Test
    public void testIfGzipCoreExists() {
        try {
            boolean coreExists = GzipFpgaCompressor.gzipCoreAvailable();
            LOGGER.log(Level.INFO, "Core exists: {0}", coreExists);
        } catch (Throwable t) {
            fail("Method GzipFpgaCompressor.gzipCoreAvailable() should not throw exception.");
        }
    }
    
    @BeforeClass
    public static void initClass() {
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        LOGGER.addHandler(handler);
        LOGGER.setLevel(Level.ALL);
    }

    //@Test
    public void testStateAfterInit() {
        assumeTrue("System does not have an FPGA GZip compressor.", GzipFpgaCompressor.gzipCoreAvailable());
        GzipFpgaCompressor compressor = new GzipFpgaCompressor(RegisterFile.CompressionType.NO_COMPRESSION);
        assertFalse(compressor.needsInput());
        assertFalse(compressor.finished());
        assertEquals(0, compressor.getBytesRead());
        assertEquals(0, compressor.getBytesWritten());
        compressor.finish();
        while (!compressor.finished());
        
        compressor.end();
    }
    
    @Test
    public void testCompressNoCompression() throws IOException, InterruptedException {
        assumeTrue("System does not have an FPGA GZip compressor.", GzipFpgaCompressor.gzipCoreAvailable());
        GzipFpgaCompressor compressor = new GzipFpgaCompressor(RegisterFile.CompressionType.NO_COMPRESSION);
        assertFalse(compressor.needsInput());
        assertFalse("Compressor should not be finished immediately after init.", compressor.finished());
        assertEquals(0, compressor.getBytesRead());
        assertEquals(0, compressor.getBytesWritten());
        
        byte[] originalData = generateRandomData(1024*1024);
        compressor.setInput(originalData, 0, originalData.length);
        assertFalse(compressor.needsInput());
        assertFalse(compressor.finished());
        assertEquals(originalData.length, compressor.getBytesRead());
        
        compressor.finish();
        
        while (!compressor.finished());

        byte[] compressedData = new byte[1024 * 5 * 1024];
        int compressedSize = compressor.compress(compressedData, 0, compressedData.length);
        assertTrue(compressedSize > 0);
        
        assertTrue(compressor.finished());
        compressor.end();
        try {
            byte[] decompressed = decompress(compressedData);
            assertArrayEquals(originalData, decompressed);
        } catch (ZipException ex) {
            fail("Bad zip format for compressed data: " + ex.getMessage());
        }
    }

    //@Test
    public void testCompressHuffmanCompression() throws IOException {
        assumeTrue("System does not have an FPGA GZip compressor.", GzipFpgaCompressor.gzipCoreAvailable());
        GzipFpgaCompressor compressor = new GzipFpgaCompressor(RegisterFile.CompressionType.FIXED_HUFFMAN);
        assertTrue("Compressor should need input immediately after init.", compressor.needsInput());
        assertFalse("Compressor should not be finished immediately after init.", compressor.finished());
        assertEquals(0, compressor.getBytesRead());
        assertEquals(0, compressor.getBytesWritten());
        
        byte[] originalData = generateRandomData(1024);
        compressor.setInput(originalData, 0, originalData.length);
        assertFalse(compressor.needsInput());
        assertFalse(compressor.finished());
        assertEquals(0, compressor.getBytesRead());
        assertEquals(originalData.length, compressor.getBytesWritten());
        
        compressor.finish();
        
        byte[] compressedData = new byte[1024 * 5];
        int compressedSize = compressor.compress(originalData, 0, compressedData.length);
        assertTrue(compressedSize > 0);
        
        assertTrue(compressor.finished());
        compressor.end();
        try {
            byte[] decompressed = decompress(compressedData);
            assertArrayEquals(originalData, decompressed);
        } catch (ZipException ex) {
            fail("Bad zip format for compressed data: " + ex.getMessage());
        }
    }
}
