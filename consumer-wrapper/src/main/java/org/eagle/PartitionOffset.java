package org.eagle;

public class PartitionOffset {
    private final Long offset;
    private final int partition;

    public PartitionOffset(Long offset, int partition) {
        this.offset = offset;
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }
    public int getPartition() {
        return partition;
    }
}
