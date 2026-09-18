package io.github.mananmonga.bullseye;

/**
 * Operator uid derivation. <b>Frozen across versions.</b>
 *
 * <p>A library must never invent a uid it could silently change between releases: a rename makes
 * every consumer's savepoint unrestorable. The barrier's own uid is caller-supplied and used
 * verbatim; the two per-lane operator uids derive deterministically from the lane id as below.
 * Consumers are encouraged to pin the resulting set of uids in a test.
 *
 * <table border="1">
 *   <caption>uid derivation</caption>
 *   <tr><th>operator</th><th>uid</th></tr>
 *   <tr><td>the barrier (buffer + gate)</td><td>{@code barrierUid} verbatim</td></tr>
 *   <tr><td>per lane: offset stripper</td><td>{@code <lane.id>-offsets}</td></tr>
 *   <tr><td>per lane: readiness tracker</td><td>{@code <lane.id>-readiness}</td></tr>
 * </table>
 *
 * <p>State names inside those operators are likewise frozen: the barrier's buffer is operator list
 * state {@code barrierBuffer}, its broadcast state is {@code readyState}, and the tracker's keyed
 * state is {@code highestOffset} and {@code declaredEpoch}.
 */
public final class BullseyeUids {

    private BullseyeUids() {}

    /** uid of the operator that strips {@code (partition, offset)} from a lane's side-input stream. */
    public static String offsets(String laneId) {
        return laneId + "-offsets";
    }

    /** uid of the keyed operator that tracks per-partition readiness for a lane. */
    public static String readiness(String laneId) {
        return laneId + "-readiness";
    }
}
