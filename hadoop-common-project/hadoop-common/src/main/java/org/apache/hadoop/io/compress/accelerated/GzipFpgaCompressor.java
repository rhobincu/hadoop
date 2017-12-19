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
package org.apache.hadoop.io.compress.accelerated;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
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

    private static final Logger LOGGER = Logger.getLogger(GzipFpgaCompressor.class.getName());

    private static final int RESET_COMMAND = 0x00;

    public static boolean gzipCoreAvailable() {
        try {
            File writePipe = new File("/dev/xillybus_write_32");
            File readPipe = new File("/dev/xillybus_read_32");
            File registerFiles = new File("/dev/xillybus_mem_8");
            if (writePipe.exists() && readPipe.exists() && registerFiles.exists()) {
                LOGGER.log(Level.INFO, "Xillibus files exist! Checking for DEVICE_ID...");
                RandomAccessFile registerReader = new RandomAccessFile(registerFiles, "r");
                registerReader.seek(Address.DEV_ID.get());
                int devId = registerReader.read();
                LOGGER.log(Level.INFO, "Device ID read: {0}", devId);
                return devId >= DEVICE_ID;
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
        byte[] buffer = new byte[4];
        buffer[0] = (byte) byteCountToWrite;
        buffer[1] = (byte) (byteCountToWrite >>> 8);
        buffer[2] = (byte) (byteCountToWrite >>> 16);
        buffer[3] = (byte) (byteCountToWrite >>> 24);
        return buffer;
    }

    private static enum Address {
        RESET(0x0000000),
        BTYPE(0x00000001),
        STATUS(0x00000002),
        ISIZE_1(0x00000003),
        ISIZE_2(0x00000004),
        ISIZE_3(0x00000005),
        ISIZE_4(0x00000006),
        CRC_1(0x00000007),
        CRC_2(0x00000008),
        CRC_3(0x00000009),
        CRC_4(0x000000010),
        DEV_ID(0x0000000E);
        private final int mAddress;

        private Address(int address) {
            mAddress = address;
        }

        public int get() {
            return mAddress;
        }
    }

    public static enum CompressionType {
        NO_COMPRESSION(0x00),
        FIXED_HUFFMAN(0x01),
        DYNAMIC_HUFFMAN(0x02);
        private final int mType;

        private CompressionType(int type) {
            mType = type;
        }

        public int getIntType() {
            return mType;
        }
    }

    private static class CompressionThread extends Thread {

        private final InputStream mStream;
        private final OutputStream mOutStream;
        private IOException mException;

        public CompressionThread(InputStream inStream, OutputStream outStream) {
            super("FPGA GZip Compression Thread");
            mStream = inStream;
            mOutStream = outStream;
        }

        @Override
        public void run() {
            byte[] buffer = new byte[1024];
            int bytesRead;
            try {
                while (!isInterrupted()) {
                    bytesRead = mStream.read(buffer);
                    if (bytesRead > 0) {
                        synchronized (mOutStream) {
                            mOutStream.write(buffer, 0, bytesRead);
                        }
                    }
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
    }

    private final FileOutputStream mXilibusOutputStream;
    private final FileInputStream mXilibusInputStream;
    private final RandomAccessFile mRegisterSet;
    private final CompressionType mCompressionType;

    private final ByteArrayOutputStream mInputBuffer;
    private final ByteArrayOutputStream mOutputBuffer;

    private boolean mCompressionComplete;
    private int mTotalBytesReceived;
    private int mTotalBytesCompressed;

    private IOException mException;
    private CompressionThread mCompressionThread;

    public GzipFpgaCompressor(CompressionType compressionType) {
        try {
            mCompressionType = compressionType;
            mXilibusOutputStream = new FileOutputStream("/dev/xillybus_write_32");
            mXilibusInputStream = new FileInputStream("/dev/xillybus_read_32");
            mRegisterSet = new RandomAccessFile("/dev/xillybus_mem_8", "rwd");
            mOutputBuffer = new ByteArrayOutputStream();
            mInputBuffer = new ByteArrayOutputStream(65536);
            reset();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(GzipFpgaCompressor.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException("Unable to open pipes", ex);
        }
    }

    @Override
    public synchronized void setInput(byte[] b, int off, int len) {
        try {

            mTotalBytesReceived += len;

            while (len != 0) {
                if (mInputBuffer.size() + len <= 65535) {
                    mInputBuffer.write(b, off, len);
                    return;
                }

                mInputBuffer.write(b, off, 65535 - mInputBuffer.size());
                mXilibusOutputStream.write(toBytes(mInputBuffer.size()));
                mXilibusOutputStream.write(mInputBuffer.toByteArray(), 0, mInputBuffer.size());
                len -= 65535 - mInputBuffer.size();
                off += 65535 - mInputBuffer.size();
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
            mXilibusOutputStream.write(toBytes(mInputBuffer.size() | 0x01000000));
            mXilibusOutputStream.write(mInputBuffer.toByteArray(), 0, mInputBuffer.size());
            mInputBuffer.reset();
            mCompressionComplete = true;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Unable to write data to FPGA compressor: ", ex);
            mException = ex;
        }
    }

    @Override
    public boolean finished() {
        return mCompressionComplete && mOutputBuffer.size() == 0;
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
            mRegisterSet.seek(Address.RESET.get());
            mRegisterSet.write(RESET_COMMAND);

            mRegisterSet.seek(Address.BTYPE.get());
            mRegisterSet.write(mCompressionType.getIntType());

            mInputBuffer.reset();
            mOutputBuffer.reset();
            mOutputBuffer.write(HEADER, 0, HEADER.length);

            mCompressionComplete = false;
            mTotalBytesReceived = 0;
            mTotalBytesCompressed = 0;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Unable to reset core.", ex);
            mException = ex;
        }
    }

    @Override
    public void end() {
        try {
            mCompressionThread.interrupt();
            mCompressionThread.join();
            mXilibusInputStream.close();
            mXilibusOutputStream.close();
            mRegisterSet.close();

        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Unable to close streams.", ex);
            mException = ex;
        } catch (InterruptedException ex) {
            LOGGER.log(Level.WARNING, "Thread interrupted while waiting for compression thread to end.", ex);
        }
    }

    @Override
    public void reinit(Configuration conf) {

    }
}
