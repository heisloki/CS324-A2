
package org.grpc.worker;

import ds.grpc.StateSnapshot;

/**
 * Stores the latest replicated coordinator state on a worker.
 *
 * Snapshots are accepted only when they come from a newer term,
 * or when they advance the sequence number for the same coordinator
 * and term.
 */
public final class ReplicaStore {

    private StateSnapshot snapshot = StateSnapshot.getDefaultInstance();

    /**
     * Store an incoming coordinator snapshot.
     *
     * @param incoming snapshot received from a coordinator
     * @return true if the snapshot was accepted
     */
    public synchronized boolean store(StateSnapshot incoming) {

        if (incoming == null || incoming.getCoordinatorId() <= 0) {
            return false;
        }

        // No valid snapshot has been stored yet.
        if (snapshot.getCoordinatorId() == 0) {
            snapshot = incoming;
            return true;
        }

        // Reject snapshots from an older coordinator term.
        if (incoming.getTerm() < snapshot.getTerm()) {
            return false;
        }

        // A newer term takes precedence over the previous term.
        if (incoming.getTerm() > snapshot.getTerm()) {
            snapshot = incoming;
            return true;
        }

        // In the same term, only the same coordinator may advance state.
        if (incoming.getCoordinatorId() != snapshot.getCoordinatorId()) {
            return false;
        }

        // Reject duplicate or out-of-order snapshots.
        if (incoming.getSequence() <= snapshot.getSequence()) {
            return false;
        }

        snapshot = incoming;
        return true;
    }

    /**
     * Return the latest stored snapshot.
     */
    public synchronized StateSnapshot getSnapshot() {
        return snapshot;
    }
}
