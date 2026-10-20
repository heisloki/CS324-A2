package org.grpc.client;

import ds.grpc.JobProgress;
import ds.grpc.JobState;
import ds.grpc.JobType;
import ds.grpc.SubmitJobRequest;
import ds.grpc.TaskProgress;
import ds.grpc.TaskState;
import org.grpc.bootstrap.BootstrapServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Command-line client for testing the cluster (the GUI comes later).
 *
 *   JobClientCli [--bootstrap host:port] PRIMESUM <start> <end>
 *   JobClientCli [--bootstrap host:port] MAX        <file.csv | 5,3,9,...>
 *   JobClientCli [--bootstrap host:port] PRIMECOUNT <file.csv | 5,3,9,...>
 */
public class JobClientCli {

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = BootstrapServer.DEFAULT_PORT;
        List<String> rest = new ArrayList<>(List.of(args));
        if (rest.size() >= 2 && rest.get(0).equals("--bootstrap")) {
            String[] hp = rest.get(1).split(":");
            host = hp[0];
            port = Integer.parseInt(hp[1]);
            rest = rest.subList(2, rest.size());
        }
        if (rest.size() < 2) {
            System.out.println("Usage: JobClientCli [--bootstrap host:port] PRIMESUM <start> <end> | MAX <file|numbers> | PRIMECOUNT <file|numbers>");
            return;
        }

        SubmitJobRequest.Builder job = SubmitJobRequest.newBuilder().setJobId(UUID.randomUUID().toString());
        switch (rest.get(0).toUpperCase()) {
            case "PRIMESUM":
                job.setType(JobType.JOB_TYPE_PRIMESUM)
                        .setRangeStart(Integer.parseInt(rest.get(1))).setRangeEnd(Integer.parseInt(rest.get(2)));
                break;
            case "MAX":
                job.setType(JobType.JOB_TYPE_MAX);
                readNumbers(rest.get(1)).forEach(job::addNumbers);
                break;
            case "PRIMECOUNT":
                job.setType(JobType.JOB_TYPE_PRIMECOUNT);
                readNumbers(rest.get(1)).forEach(job::addNumbers);
                break;
            default:
                System.out.println("Unknown job type " + rest.get(0));
                return;
        }

        long started = System.currentTimeMillis();
        try (ClusterClient client = new ClusterClient(host, port)) {
            final int[] lastBucket = {-1};
            final String[] lastMessage = {""};
            JobProgress done = client.run(job.build(), new ClusterClient.Listener() {
                @Override
                public void onProgress(JobProgress p) {
                    int bucket = (int) (p.getPercentComplete() / 5);
                    boolean changedMessage = !p.getMessage().equals(lastMessage[0]) && !p.getMessage().isEmpty();
                    if (bucket != lastBucket[0] || changedMessage || p.getState() != JobState.JOB_STATE_RUNNING) {
                        System.out.println(bar(p));
                        if (changedMessage) {
                            System.out.println("      ! " + p.getMessage());
                            lastMessage[0] = p.getMessage();
                        }
                        lastBucket[0] = bucket;
                    }
                }

                @Override
                public void onNote(String note) {
                    System.out.println("      * " + note);
                }
            }, 10 * 60_000L);

            System.out.println();
            System.out.println("Job " + (done.getState() == JobState.JOB_STATE_COMPLETED ? "COMPLETED" : "FAILED")
                    + " in " + (System.currentTimeMillis() - started) / 1000.0 + " s, handled by coordinator Worker "
                    + done.getCoordinatorId() + " (term " + done.getTerm() + ")");
            if (done.getHasResult()) {
                System.out.println("RESULT: " + done.getResultText());
            } else {
                System.out.println("Reason: " + done.getMessage());
            }
            System.out.println("Per-worker breakdown:");
            for (TaskProgress t : done.getTasksList()) {
                System.out.println("   " + t.getTaskId().substring(Math.max(0, t.getTaskId().lastIndexOf('-') + 1))
                        + "  Worker " + t.getWorkerId() + "  " + t.getProcessed() + "/" + t.getTotal() + " items"
                        + (t.getAttempt() > 1 ? "  (reassigned, attempt " + t.getAttempt() + ")" : ""));
            }
        }
    }

    private static String bar(JobProgress p) {
        int filled = (int) Math.round(p.getPercentComplete() / 5);
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < 20; i++) {
            sb.append(i < filled ? '#' : '-');
        }
        sb.append(String.format("] %5.1f%%  ", p.getPercentComplete()));
        for (TaskProgress t : p.getTasksList()) {
            sb.append('W').append(t.getWorkerId() == 0 ? "?" : String.valueOf(t.getWorkerId()))
                    .append(' ').append((int) t.getPercentComplete()).append('%')
                    .append(t.getState() == TaskState.TASK_STATE_QUEUED ? "(queued)" : "")
                    .append(t.getAttempt() > 1 ? "*" : "").append("  ");
        }
        return sb.toString().trim();
    }

    private static List<Integer> readNumbers(String arg) throws Exception {
        String text = Files.exists(Path.of(arg)) ? Files.readString(Path.of(arg)) : arg;
        List<Integer> numbers = new ArrayList<>();
        for (String part : text.split("[,\\s]+")) {
            if (!part.isBlank()) {
                numbers.add(Integer.parseInt(part.trim()));
            }
        }
        return numbers;
    }
}
