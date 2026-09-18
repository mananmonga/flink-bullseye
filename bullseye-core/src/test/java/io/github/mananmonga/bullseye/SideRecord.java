package io.github.mananmonga.bullseye;

/** Test side-input record: a Flink POJO carrying its source coordinates. */
public final class SideRecord {
    public int partition;
    public long offset;
    public String payload;

    public SideRecord() {}

    public SideRecord(int partition, long offset, String payload) {
        this.partition = partition;
        this.offset = offset;
        this.payload = payload;
    }
}
