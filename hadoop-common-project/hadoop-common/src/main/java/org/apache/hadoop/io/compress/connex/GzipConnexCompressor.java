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
package org.apache.hadoop.io.compress.connex;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

/**
 *
 * @author rhobincu
 */
public class GzipConnexCompressor implements Compressor {

    private static final int DEVICE_ID = 4; //TODO: fix

    public static boolean connexCoreAvailable() {
        try {
            File writePipe = new File("/dev/xillybus_write_32");
            File readPipe = new File("/dev/xillybus_read_32");
            File registerFiles = new File("/dev/xillybus_mem_8");
            if (writePipe.exists() && readPipe.exists() && registerFiles.exists()) {
                RandomAccessFile registerReader = new RandomAccessFile(registerFiles, "r");
                registerReader.seek(Address.INFO.get());
                int devId = registerReader.read();
                return devId == DEVICE_ID;
            }
            return false;
        } catch (IOException ex) {
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
        INFO(0x00000002),
        ISIZE(0x00000003),
        CRC(0x00000007),
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
        private int mType;

        private CompressionType(int type) {
            mType = type;
        }

        public int getIntType() {
            return mType;
        }
    }

    private final static int RESET_COMMAND = 0x00;

    private final FileOutputStream mXilibusOutputStream;
    private final FileInputStream mXilibusInputStream;
    private final RandomAccessFile mRegisterSet;
    private final CompressionType mCompressionType;
    private boolean mCompressionComplete;
    private int mTotalBytesReceived;
    private int mTotalBytesCompressed;
    private volatile boolean mNeedsInput;
    private int mHeaderOffset;
    private boolean mInputDataComplete;

    public GzipConnexCompressor(CompressionType compressionType) {
        try {
            mCompressionType = compressionType;
            mXilibusOutputStream = new FileOutputStream("/dev/xillybus_write_32");
            mXilibusInputStream = new FileInputStream("/dev/xillybus_read_32");
            mRegisterSet = new RandomAccessFile("/dev/xillybus_mem_8", "rwd");
            reset();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(GzipConnexCompressor.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException("Unable to open pipes", ex);
        }
    }

    @Override
    public synchronized void setInput(byte[] b, int off, int len) {
        try {
            mNeedsInput = false;
            mTotalBytesReceived += len;

            while (len != 0) {
                int byteCountToWrite = Math.min(len, 65535);
                mXilibusOutputStream.write(toBytes(byteCountToWrite));
                mXilibusOutputStream.write(b, off, byteCountToWrite);
                off += byteCountToWrite;
                len -= byteCountToWrite;
            }

        } catch (IOException ex) {
            Logger.getLogger(GzipConnexCompressor.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            mNeedsInput = true;
        }
    }

    @Override
    public synchronized boolean needsInput() {
        return mNeedsInput;
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
            mXilibusOutputStream.write(toBytes(0x01000000));
            mInputDataComplete = true;
        } catch (IOException ex) {
            Logger.getLogger(GzipConnexCompressor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public boolean finished() {
        return mCompressionComplete;
    }

    @Override
    public int compress(byte[] b, int off, int len) throws IOException {
        int bytesRead = 0;
        if (mHeaderOffset < 8) {
            bytesRead = Math.min(len, 8);
            System.arraycopy(HEADER, mHeaderOffset, b, off, bytesRead);
            mHeaderOffset += bytesRead;
            off += bytesRead;
            len -= bytesRead;
        }
        bytesRead += mXilibusInputStream.read(b, off, len);
        mCompressionComplete = mInputDataComplete && bytesRead == 0;
        mTotalBytesCompressed += bytesRead;
        return bytesRead;
    }

    @Override
    public void reset() {
        try {
            mRegisterSet.seek(Address.RESET.get());
            mRegisterSet.write(RESET_COMMAND);

            mRegisterSet.seek(Address.BTYPE.get());
            mRegisterSet.write(mCompressionType.getIntType());
            mInputDataComplete = false;
            mHeaderOffset = 0;
            mCompressionComplete = false;
            mTotalBytesReceived = 0;
        } catch (IOException ex) {
            Logger.getLogger(GzipConnexCompressor.class.getName()).log(Level.SEVERE, "Unable to reset core.", ex);
        }
    }

    @Override
    public void end() {
        try {
            mXilibusInputStream.close();
            mXilibusOutputStream.close();
            mRegisterSet.close();
        } catch (IOException ex) {
            Logger.getLogger(GzipConnexCompressor.class.getName()).log(Level.SEVERE, "Unable to close streams.", ex);
        }
    }

    @Override
    public void reinit(Configuration conf) {

    }
}
