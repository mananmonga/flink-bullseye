package io.github.mananmonga.bullseye.internal;

import io.github.mananmonga.bullseye.Lane;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.flink.annotation.Internal;

/**
 * The readiness predicate, in one place, with no Flink transport types in its signature.
 *
 * <p>"Every expected partition of every lane has declared an epoch {@code >= N}". Keeping it here
 * — rather than inline in the gate — is what lets the Flink 1.20 broadcast transport be swapped
 * for FLIP-467 generalized watermarks on 2.x without touching the semantics.
 */
@Internal
public final class Readiness {

    private Readiness() {}

    /** Lookup of the highest epoch a (lane, partition) has declared; {@code null} if never. */
    @FunctionalInterface
    public interface DeclaredEpochs {
        Long get(String key) throws Exception;
    }

    /** Broadcast-state key for a (lane, partition) pair. Frozen: it lives in savepoints. */
    public static String key(String lane, int partition) {
        return lane + ":" + partition;
    }

    /**
     * @return {@code true} iff every expected partition of every lane has declared {@code epoch}
     *     or later. Absence counts as not ready.
     */
    public static boolean allReady(Collection<Lane> lanes, long epoch, DeclaredEpochs declared)
            throws Exception {
        for (Lane lane : lanes) {
            for (int p = 0; p < lane.expectedPartitions(); p++) {
                Long e = declared.get(key(lane.id(), p));
                if (e == null || e < epoch) {
                    return false;
                }
            }
        }
        return true;
    }

    /** The (lane, partition) keys that are holding the barrier at {@code epoch}, for diagnostics. */
    public static List<String> missing(Collection<Lane> lanes, long epoch, DeclaredEpochs declared)
            throws Exception {
        List<String> out = new ArrayList<>();
        for (Lane lane : lanes) {
            for (int p = 0; p < lane.expectedPartitions(); p++) {
                String k = key(lane.id(), p);
                Long e = declared.get(k);
                if (e == null || e < epoch) {
                    out.add(k + (e == null ? "(never)" : "(epoch " + e + ")"));
                }
            }
        }
        return out;
    }
}
