package com.raftfs.storage;

import java.util.zip.CRC32C;

public class ChecksumValidator {

    public long compute(byte[] data) {
        CRC32C crc = new CRC32C();
        crc.update(data);
        return crc.getValue();
    }

    public boolean validate(byte[] data, long expectedChecksum) {
        return compute(data) == expectedChecksum;
    }
}
