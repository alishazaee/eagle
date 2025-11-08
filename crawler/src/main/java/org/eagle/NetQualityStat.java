package org.eagle;

import org.xbill.DNS.Record;
import org.xbill.DNS.Resolver;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Type;

public class NetQualityStat {

    public static MetricsProto.DigMetrics digStat(String url, String externalNameserver) {
        int failureCount = 0;
        try {
            Resolver resolver = new SimpleResolver(externalNameserver);
            resolver.setTimeout(2);

            for (int i = 0; i < 10; i++) {
                try {
                    Lookup lookup = new Lookup(url, Type.A);
                    lookup.setResolver(resolver);
                    Record[] records = lookup.run();

                    if (lookup.getResult() != Lookup.SUCCESSFUL || records == null || records.length == 0) {
                        failureCount++;
                    }
                } catch (Exception e) {
                    failureCount++;
                }
            }

        } catch (Exception e) {
            failureCount = 10;
        }

        return MetricsProto.DigMetrics.newBuilder().setExternalFailureRate(failureCount).build();
    }

}
