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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

/**
 *
 * @author Radu Hobincu &lt;radu.hobincu@upb.ro&gt;
 */
public class GzipFpgaCompressor implements Compressor {

    private static final int DEVICE_ID = 0xBC;
    private static final int MAX_BLOCK_SIZE = 65535;

    private static final Logger LOGGER = Logger.getLogger(GzipFpgaCompressor.class.getName());

    public static boolean gzipCoreAvailable() {
        try {
            File writePipe = new File("/dev/xillybus_write_32");
            File readPipe = new File("/dev/xillybus_read_32");
            File registerFiles = new File("/dev/xillybus_mem_8");
            if (writePipe.exists() && readPipe.exists() && registerFiles.exists()) {
                LOGGER.log(Level.INFO, "Xillibus files exist! Checking for DEVICE_ID...");
                int devId;
                try (RegisterFile registerFile = new RegisterFile(registerFiles.getAbsolutePath())) {
                    devId = registerFile.getDeviceId();
                    LOGGER.log(Level.INFO, "Device ID read: {0}", devId);
                    return devId >= DEVICE_ID;
                }

            }
            LOGGER.log(Level.INFO, "File device does not exist => No GZIP core.");
            return false;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Unable to read from GZIP core devices.", ex);
            return false;
        }
    }

    private static final byte[] HEADER = new byte[]{(byte) 0x1F, (byte) 0x8B, (byte) 0x08, (byte) 0x00, (byte) 0x00,
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0, (byte) 0x00};

    private static byte[] toBytes(int byteCountToWrite) {
        LOGGER.log(Level.INFO, "ByteCount: {0}", String.format("0x%08x", byteCountToWrite));
        byte[] buffer = new byte[4];
        buffer[3] = (byte) byteCountToWrite;
        buffer[2] = (byte) (byteCountToWrite >>> 8);
        buffer[1] = (byte) (byteCountToWrite >>> 16);
        buffer[0] = (byte) (byteCountToWrite >>> 24);
        return buffer;
    }
    private RegisterFile.CompressionType mCompressionType;

    private static class CompressionThread extends Thread {

        private final InputStream mStream;
        private final OutputStream mOutStream;
        private IOException mException;
        private final RegisterFile mRegisterFile;
        private volatile boolean mCompressionComplete;

        public CompressionThread(InputStream inStream, OutputStream outStream,
                RegisterFile registerFile) {
            super("FPGA GZip Compression Thread");
            mStream = inStream;
            mOutStream = outStream;
            mRegisterFile = registerFile;
            mCompressionComplete = false;
        }

        @Override
        public void run() {
            byte[] buffer = new byte[1024 * 64];
            int bytesRead;
            try {
                while (!isInterrupted()) {
                    bytesRead = mStream.read(buffer);

                    LOGGER.log(Level.INFO, "Thread read {0} bytes.", bytesRead);
                    LOGGER.log(Level.INFO, "Compression complete: {0}: ", mRegisterFile.isCompressionComplete());
                    LOGGER.log(Level.INFO, "Isize: {0}: ", String.format("0x%08x", mRegisterFile.getInputSize()));
                    if (bytesRead > 0) {

                        synchronized (mOutStream) {
                            mOutStream.write(buffer, 0, bytesRead);
                        }
                    }
                    mCompressionComplete = mRegisterFile.isCompressionComplete();
                }
            } catch (IOException ex) {
                LOGGER.log(Level.SEVERE, "IOException while reading compressed data: ", ex);
                mException = ex;
            }
        }

        public IOException getException() {
            return mException;
        }

        public boolean hasThrownException() {
            return mException != null;
        }

        public boolean isComplete() {
            return mCompressionComplete;
        }
    }

    private final FileOutputStream mXilibusOutputStream;
    private final FileInputStream mXilibusInputStream;
    private final RegisterFile mRegisterFile;

    private final ByteArrayOutputStream mInputBuffer;
    private final ByteArrayOutputStream mOutputBuffer;

    private int mTotalBytesReceived;
    private int mTotalBytesCompressed;

    private IOException mException;
    private CompressionThread mCompressionThread;

    public GzipFpgaCompressor(RegisterFile.CompressionType compressionType) {
        try {
            mCompressionType = compressionType;
            mXilibusOutputStream = new FileOutputStream("/dev/xillybus_write_32");
            mXilibusInputStream = new FileInputStream("/dev/xillybus_read_32");
            mRegisterFile = new RegisterFile("/dev/xillybus_mem_8");
            mOutputBuffer = new ByteArrayOutputStream();
            mInputBuffer = new ByteArrayOutputStream(65536);
            mCompressionThread = new CompressionThread(mXilibusInputStream, mOutputBuffer, mRegisterFile);
            mCompressionThread.start();
            reset();
        } catch (IOException ex) {
            Logger.getLogger(GzipFpgaCompressor.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException("Unable to open pipes", ex);
        }
    }

    @Override
    public synchronized void setInput(byte[] b, int off, int len) {
        try {

            mTotalBytesReceived += len;

            while (len != 0) {
                if (mInputBuffer.size() + len <= MAX_BLOCK_SIZE) {
                    mInputBuffer.write(b, off, len);
                    return;
                }

                int chunkSize = MAX_BLOCK_SIZE - mInputBuffer.size();
                
                mInputBuffer.write(b, off, chunkSize);
                mXilibusOutputStream.write(toBytes(mInputBuffer.size()));
                mXilibusOutputStream.write(mInputBuffer.toByteArray(), 0, mInputBuffer.size());
                mXilibusOutputStream.flush();
                len -= chunkSize;
                off += chunkSize;
                mInputBuffer.reset();
            }

        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Unable to write data to FPGA compressor: ", ex);
            mException = ex;
        }
    }

    @Override
    public synchronized boolean needsInput() {
        return !finished() && mOutputBuffer.size() == 0;
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {
        // unused
    }

    @Override
    public long getBytesRead() {
        return mTotalBytesReceived;
    }

    @Override
    public long getBytesWritten() {
        return mTotalBytesCompressed;
    }

    @Override
    public synchronized void finish() {
        try {
            final byte[] header = toBytes(mInputBuffer.size() | 0x01000000);
            mXilibusOutputStream.write(header);
            mXilibusOutputStream.write(mInputBuffer.toByteArray(), 0, mInputBuffer.size());
            mXilibusOutputStream.flush();

            LOGGER.log(Level.INFO, "ISize is: {0}", mRegisterFile.getInputSize());
            LOGGER.log(Level.INFO, "Status is: {0}", mRegisterFile.getStatus());

            mInputBuffer.reset();
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Unable to write data to FPGA compressor: ", ex);
            mException = ex;
        }
    }

    @Override
    public boolean finished() {
        return mCompressionThread.isComplete();
    }

    @Override
    public int compress(byte[] b, int off, int len) throws IOException {
        if (mException != null) {
            throw mException;
        }

        if (mCompressionThread.hasThrownException()) {
            throw mCompressionThread.getException();
        }

        synchronized (mOutputBuffer) {
            int bytesToRead = Math.min(len, mOutputBuffer.size());

            if (bytesToRead == 0) {
                return 0;
            }
            mTotalBytesCompressed += bytesToRead;
            byte[] buffer = mOutputBuffer.toByteArray();

            System.arraycopy(buffer, 0, b, off, bytesToRead);
            mOutputBuffer.reset();
            mOutputBuffer.write(buffer, bytesToRead, buffer.length - bytesToRead);

            return bytesToRead;
        }
    }

    @Override
    public void reset() {
        try {
            mRegisterFile.reset();
            mRegisterFile.setCompressionType(mCompressionType);

            LOGGER.log(Level.INFO, "BType value after reset is: {0}", mRegisterFile.getCompressionType());

            mInputBuffer.reset();
            mOutputBuffer.reset();
            mOutputBuffer.write(HEADER, 0, HEADER.length);

            mTotalBytesReceived = 0;
            mTotalBytesCompressed = 0;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Unable to reset FPGA: ", ex);
            mException = ex;
        }
    }

    @Override
    public void end() {
        try {
            mXilibusOutputStream.close();
            mXilibusInputStream.close();
            mRegisterFile.close();

            mCompressionThread.stop();

        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Unable to close streams.", ex);
            mException = ex;
        }
    }

    @Override
    public void reinit(Configuration conf) {

    }
}
