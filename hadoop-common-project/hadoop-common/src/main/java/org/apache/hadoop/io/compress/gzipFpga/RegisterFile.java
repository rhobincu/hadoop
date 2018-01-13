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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rhobincu
 */
public class RegisterFile implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(RegisterFile.class.getName());

    public static enum Address {
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
        CRC_4(0x00000000A),
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
        DYNAMIC_HUFFMAN(0x02),
        ERROR(0x03);

        private static CompressionType byValue(int compressionType) {
            for (CompressionType type : values()) {
                if (compressionType == type.mType) {
                    return type;
                }
            }

            return CompressionType.ERROR;
        }
        private final int mType;

        private CompressionType(int type) {
            mType = type;
        }

        public int getIntType() {
            return mType;
        }

        public byte getByteType() {
            return (byte) mType;
        }
    }

    private final RandomAccessFile mRegisterSetFile;
    private final byte[] mRegistersMirror = new byte[32];

    public RegisterFile(String deviceName) throws IOException {
        mRegisterSetFile = new RandomAccessFile("/dev/xillybus_mem_8", "rws");
        readAll();
    }

    private synchronized void readAll() throws IOException {
        mRegisterSetFile.seek(0);
        mRegisterSetFile.read(mRegistersMirror);
    }

    private synchronized void writeAll() throws IOException {
        mRegisterSetFile.seek(0);
        mRegisterSetFile.write(mRegistersMirror);
    }

    public final void dump() throws IOException {
        StringBuilder builder = new StringBuilder("\n");
        readAll();
        for (Address address : Address.values()) {
            builder.append(String.format("REGFILE[%02d] = 0x%02x\n", address.get(), mRegistersMirror[address.get()]));
        }
        LOGGER.log(Level.INFO, "{0}", builder.toString());
    }

    public synchronized void reset() throws IOException {
        readAll();
        mRegistersMirror[Address.RESET.get()] = 0;
        writeAll();
        mRegistersMirror[Address.RESET.get()] = 1;
        writeAll();
        do {
            readAll();
        } while ((mRegistersMirror[Address.RESET.get()] & 0xFF) == 0);
    }

    public synchronized void setCompressionType(CompressionType type) throws IOException {
        if (type == CompressionType.ERROR) {
            throw new IllegalArgumentException("Compression type ERROR can't be set.");
        }
        readAll();
        mRegistersMirror[Address.BTYPE.get()] = type.getByteType();
        writeAll();
    }

    public CompressionType getCompressionType() throws IOException {
        readAll();
        return CompressionType.byValue(mRegistersMirror[Address.BTYPE.get()] & 0xFF);
    }

    public int getInputSize() throws IOException {
        readAll();
        return (mRegistersMirror[Address.ISIZE_1.get()] & 0xFF << 24)
                | (mRegistersMirror[Address.ISIZE_2.get()] & 0xFF << 16)
                | (mRegistersMirror[Address.ISIZE_3.get()] & 0xFF << 8)
                | mRegistersMirror[Address.ISIZE_4.get()] & 0xFF;
    }

    public int getCrc() throws IOException {
        readAll();
        return (mRegistersMirror[Address.CRC_1.get()] & 0xFF << 24)
                | (mRegistersMirror[Address.CRC_2.get()] & 0xFF << 16)
                | (mRegistersMirror[Address.CRC_3.get()] & 0xFF << 8)
                | mRegistersMirror[Address.CRC_4.get()] & 0xFF;
    }

    public int getDeviceId() throws IOException {
        readAll();
        return mRegistersMirror[Address.DEV_ID.get()] & 0xFF;
    }

    public int getStatus() throws IOException {
        readAll();
        return mRegistersMirror[Address.STATUS.get()] & 0xFF;
    }

    public boolean isCompressionComplete() throws IOException {
        return (getStatus() >> 2 & 1) == 1;
    }

    @Override
    public void close() throws IOException {
        mRegisterSetFile.close();
    }
}
