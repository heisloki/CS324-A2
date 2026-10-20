
package org.grpc.client;

import ds.grpc.JobProgress;
import ds.grpc.JobState;
import ds.grpc.JobType;
import ds.grpc.SubmitJobRequest;
import ds.grpc.TaskProgress;
import ds.grpc.TaskState;
import org.grpc.bootstrap.BootstrapServer;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableColumn;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClientGUI extends JFrame {

    private final JTextField bootstrapHost =
            new JTextField("localhost", 10);

    private final JTextField bootstrapPort =
            new JTextField(
                    String.valueOf(BootstrapServer.DEFAULT_PORT), 6);

    private final JComboBox<String> jobType =
            new JComboBox<>(
                    new String[]{"PRIMESUM", "MAX", "PRIMECOUNT"});

    private final JTextField rangeStart = new JTextField("2", 8);
    private final JTextField rangeEnd = new JTextField("10000000", 8);

    private final JTextField numbersInput =
            new JTextField("5, 3, 9, 12, 7", 25);

    private final JLabel inputLabel =
            new JLabel("Numbers (comma-separated or file path):");

    private final JButton submitButton =
            new JButton("Submit Job");

    private final JLabel connectionStatus =
            new JLabel("Ready");

    private final JobTableModel jobTableModel =
            new JobTableModel();

    private final JTable jobTable = new JTable(jobTableModel);

    // Each job gets its own background thread of execution.
    private final ExecutorService jobExecutor =
            Executors.newCachedThreadPool();

    private final List<JobRow> jobs = new ArrayList<>();

    private boolean closing = false;

    public ClientGUI() {
        super("Distributed Computing - Client Dashboard");

        setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
        setMinimumSize(new Dimension(900, 580));
        setSize(1100, 680);
        setLocationRelativeTo(null);

        buildInterface();
        updateInputFields();

        jobType.addActionListener(e -> updateInputFields());
        submitButton.addActionListener(e -> submitJob());

        addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosing(java.awt.event.WindowEvent e) {
                closeApplication();
            }
        });
    }

    private void buildInterface() {
        JPanel root = new JPanel(new BorderLayout(12, 12));
        root.setBorder(new EmptyBorder(18, 18, 12, 18));
        root.setBackground(new Color(245, 247, 250));
        setContentPane(root);

        // Header
        JPanel header = new JPanel(new BorderLayout());
        header.setOpaque(false);

        JLabel title = new JLabel("Distributed Job Monitor");
        title.setFont(new Font("SansSerif", Font.BOLD, 25));

        JLabel subtitle = new JLabel(
                "Submit jobs and monitor real distributed work in real time");
        subtitle.setForeground(new Color(90, 100, 115));

        JPanel titlePanel = new JPanel();
        titlePanel.setOpaque(false);
        titlePanel.setLayout(new BoxLayout(titlePanel, BoxLayout.Y_AXIS));
        titlePanel.add(title);
        titlePanel.add(Box.createVerticalStrut(5));
        titlePanel.add(subtitle);

        header.add(titlePanel, BorderLayout.WEST);
        root.add(header, BorderLayout.NORTH);

        // Main content
        JPanel center = new JPanel(new BorderLayout(12, 12));
        center.setOpaque(false);

        JPanel submitPanel = buildSubmitPanel();
        center.add(submitPanel, BorderLayout.NORTH);

        JPanel jobsPanel = buildJobsPanel();
        center.add(jobsPanel, BorderLayout.CENTER);

        root.add(center, BorderLayout.CENTER);

        // Footer
        JPanel footer = new JPanel(new BorderLayout());
        footer.setOpaque(false);
        footer.setBorder(new EmptyBorder(4, 0, 0, 0));

        connectionStatus.setForeground(new Color(70, 80, 95));
        footer.add(connectionStatus, BorderLayout.WEST);

        JLabel hint = new JLabel(
                "Click a progress bar to view task details");
        hint.setForeground(new Color(90, 100, 115));
        footer.add(hint, BorderLayout.EAST);

        root.add(footer, BorderLayout.SOUTH);
    }

    private JPanel buildSubmitPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        panel.setBackground(Color.WHITE);
        panel.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(
                        new Color(220, 225, 232)),
                new EmptyBorder(14, 14, 14, 14)));

        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(5, 6, 5, 6);
        c.anchor = GridBagConstraints.WEST;
        c.fill = GridBagConstraints.HORIZONTAL;

        JLabel sectionTitle = new JLabel("Submit a new job");
        sectionTitle.setFont(
                new Font("SansSerif", Font.BOLD, 16));

        c.gridx = 0;
        c.gridy = 0;
        c.gridwidth = 4;
        panel.add(sectionTitle, c);

        c.gridwidth = 1;

        c.gridx = 0;
        c.gridy = 1;
        panel.add(new JLabel("Bootstrap host:"), c);

        c.gridx = 1;
        panel.add(bootstrapHost, c);

        c.gridx = 2;
        panel.add(new JLabel("Bootstrap port:"), c);

        c.gridx = 3;
        panel.add(bootstrapPort, c);

        c.gridx = 0;
        c.gridy = 2;
        panel.add(new JLabel("Job type:"), c);

        c.gridx = 1;
        panel.add(jobType, c);

        c.gridx = 2;
        panel.add(new JLabel("Range start:"), c);

        c.gridx = 3;
        panel.add(rangeStart, c);

        c.gridx = 0;
        c.gridy = 3;
        panel.add(new JLabel("Range end:"), c);

        c.gridx = 1;
        panel.add(rangeEnd, c);

        c.gridx = 2;
        c.gridwidth = 2;
        panel.add(inputLabel, c);

        c.gridx = 0;
        c.gridy = 4;
        c.gridwidth = 4;
        panel.add(numbersInput, c);

        c.gridx = 0;
        c.gridy = 5;
        c.gridwidth = 4;
        c.fill = GridBagConstraints.NONE;
        c.anchor = GridBagConstraints.EAST;
        panel.add(submitButton, c);

        return panel;
    }

    private JPanel buildJobsPanel() {
        JPanel panel = new JPanel(new BorderLayout(0, 8));
        panel.setOpaque(false);

        JLabel sectionTitle = new JLabel("Submitted jobs");
        sectionTitle.setFont(
                new Font("SansSerif", Font.BOLD, 17));

        panel.add(sectionTitle, BorderLayout.NORTH);

        jobTable.setRowHeight(38);
        jobTable.setFillsViewportHeight(true);
        jobTable.setSelectionMode(
                ListSelectionModel.SINGLE_SELECTION);
        jobTable.setAutoCreateRowSorter(true);
        jobTable.setShowVerticalLines(false);
        jobTable.setGridColor(new Color(230, 233, 238));
        jobTable.setBackground(Color.WHITE);

        jobTable.getTableHeader().setReorderingAllowed(false);
        jobTable.getTableHeader().setFont(
                new Font("SansSerif", Font.BOLD, 12));

        TableColumn progressColumn =
                jobTable.getColumnModel().getColumn(2);

        progressColumn.setCellRenderer(
                new ProgressRenderer());

        progressColumn.setPreferredWidth(220);
        progressColumn.setMinWidth(180);

        jobTable.getColumnModel().getColumn(0)
                .setPreferredWidth(125);
        jobTable.getColumnModel().getColumn(1)
                .setPreferredWidth(105);
        jobTable.getColumnModel().getColumn(3)
                .setPreferredWidth(130);
        jobTable.getColumnModel().getColumn(4)
                .setPreferredWidth(230);

        jobTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                int viewRow = jobTable.rowAtPoint(e.getPoint());
                int viewColumn = jobTable.columnAtPoint(e.getPoint());

                if (viewRow < 0 || viewColumn != 2) {
                    return;
                }

                int modelRow =
                        jobTable.convertRowIndexToModel(viewRow);

                JobRow job = jobTableModel.getJobAt(modelRow);

                if (job != null) {
                    showBreakdown(job);
                }
            }
        });

        JScrollPane scrollPane = new JScrollPane(jobTable);
        scrollPane.setBorder(BorderFactory.createLineBorder(
                new Color(220, 225, 232)));
        scrollPane.getViewport().setBackground(Color.WHITE);

        panel.add(scrollPane, BorderLayout.CENTER);

        return panel;
    }

    private void updateInputFields() {
        String selected = (String) jobType.getSelectedItem();
        boolean primeSum = "PRIMESUM".equals(selected);

        rangeStart.setEnabled(primeSum);
        rangeEnd.setEnabled(primeSum);
        numbersInput.setEnabled(!primeSum);
        inputLabel.setEnabled(!primeSum);

        if (primeSum) {
            inputLabel.setText("Numbers input not required");
        } else {
            inputLabel.setText(
                    "Numbers (comma-separated or file path):");
        }
    }

    private void submitJob() {
        final SubmitJobRequest request;

        try {
            String host = bootstrapHost.getText().trim();
            int port = Integer.parseInt(
                    bootstrapPort.getText().trim());

            if (host.isEmpty() || port < 1 || port > 65535) {
                throw new IllegalArgumentException(
                        "Enter a valid Bootstrap host and port.");
            }

            request = buildRequest();

        } catch (Exception e) {
            JOptionPane.showMessageDialog(
                    this,
                    e.getMessage(),
                    "Invalid job input",
                    JOptionPane.ERROR_MESSAGE);
            return;
        }

        String host = bootstrapHost.getText().trim();
        int port = Integer.parseInt(bootstrapPort.getText().trim());

        JobRow row = new JobRow(request);

        jobs.add(row);
        jobTableModel.addJob(row);

        submitButton.setEnabled(false);
        connectionStatus.setText(
                "Submitting job " + shortId(request.getJobId()) + "...");

        jobExecutor.submit(() -> {
            try (ClusterClient client =
                         new ClusterClient(host, port)) {

                JobProgress done = client.run(
                        request,
                        new ClusterClient.Listener() {
                            @Override
                            public void onProgress(JobProgress progress) {
                                SwingUtilities.invokeLater(
                                        () -> updateJob(row, progress));
                            }

                            @Override
                            public void onNote(String note) {
                                SwingUtilities.invokeLater(() -> {
                                    row.note = note;
                                    connectionStatus.setText(note);
                                    jobTableModel.refresh(row);
                                });
                            }
                        },
                        TimeUnit.MINUTES.toMillis(30));

                SwingUtilities.invokeLater(() -> {
                    updateJob(row, done);
                    row.finished = true;

                    connectionStatus.setText(
                            "Job " + shortId(row.jobId)
                                    + " finished: " + stateText(done.getState()));

                    submitButton.setEnabled(true);

                    if (done.getHasResult()) {
                        JOptionPane.showMessageDialog(
                                this,
                                "Job completed successfully.\n\n"
                                        + "Job ID: " + row.jobId + "\n"
                                        + "Result: " + done.getResultText(),
                                "Job completed",
                                JOptionPane.INFORMATION_MESSAGE);
                    }
                });

            } catch (Exception e) {
                SwingUtilities.invokeLater(() -> {
                    row.finished = true;
                    row.status = "CLIENT ERROR";
                    row.note = e.getMessage() == null
                            ? e.toString() : e.getMessage();

                    jobTableModel.refresh(row);
                    connectionStatus.setText(
                            "Job " + shortId(row.jobId)
                                    + " ended with a client error.");

                    submitButton.setEnabled(true);
                });
            }
        });
    }

    private SubmitJobRequest buildRequest() throws Exception {
        String selected = (String) jobType.getSelectedItem();

        SubmitJobRequest.Builder builder =
                SubmitJobRequest.newBuilder()
                        .setJobId(UUID.randomUUID().toString());

        if ("PRIMESUM".equals(selected)) {
            int start = Integer.parseInt(
                    rangeStart.getText().trim());
            int end = Integer.parseInt(
                    rangeEnd.getText().trim());

            if (start < 2 || end < start) {
                throw new IllegalArgumentException(
                        "PRIMESUM requires start >= 2 and end >= start.");
            }

            builder.setType(JobType.JOB_TYPE_PRIMESUM)
                    .setRangeStart(start)
                    .setRangeEnd(end);

        } else {
            String input = numbersInput.getText().trim();

            if (input.isEmpty()) {
                throw new IllegalArgumentException(
                        "Enter numbers or a path to a numbers file.");
            }

            String content = Files.exists(Path.of(input))
                    ? Files.readString(Path.of(input))
                    : input;

            List<Integer> numbers = new ArrayList<>();

            for (String part : content.split("[,\\s]+")) {
                if (!part.isBlank()) {
                    numbers.add(Integer.parseInt(part.trim()));
                }
            }

            if (numbers.isEmpty()) {
                throw new IllegalArgumentException(
                        "No valid integers were found.");
            }

            if ("MAX".equals(selected)) {
                builder.setType(JobType.JOB_TYPE_MAX);
            } else if ("PRIMECOUNT".equals(selected)) {
                builder.setType(JobType.JOB_TYPE_PRIMECOUNT);
            } else {
                throw new IllegalArgumentException(
                        "Unsupported job type: " + selected);
            }

            numbers.forEach(builder::addNumbers);
        }

        return builder.build();
    }

    private void updateJob(JobRow row, JobProgress progress) {
        row.progress = progress;
        row.percent = Math.max(
                0,
                Math.min(100, (int) Math.round(
                        progress.getPercentComplete())));

        row.status = stateText(progress.getState());

        row.result = progress.getHasResult()
                ? progress.getResultText()
                : progress.getMessage();

        if (progress.getState() != JobState.JOB_STATE_RUNNING) {
            row.finished = true;
        }

        jobTableModel.refresh(row);

        // Keep an open worker-breakdown dialog synchronized with
        // the latest real gRPC progress snapshot.
        if (row.breakdownDialog != null
                && row.breakdownDialog.isDisplayable()) {
            row.breakdownDialog.refresh(progress);
        }

        connectionStatus.setText(
                "Job " + shortId(row.jobId)
                        + ": " + row.percent + "% - " + row.status);
    }

    private void showBreakdown(JobRow row) {
        if (row.progress == null) {
            JOptionPane.showMessageDialog(
                    this,
                    "Waiting for the first gRPC progress update.",
                    "Worker breakdown",
                    JOptionPane.INFORMATION_MESSAGE);
            return;
        }

        if (row.breakdownDialog != null
                && row.breakdownDialog.isDisplayable()) {
            row.breakdownDialog.toFront();
            row.breakdownDialog.refresh(row.progress);
            return;
        }

        row.breakdownDialog = new BreakdownDialog(this, row);
        row.breakdownDialog.setVisible(true);
    }

    private void closeApplication() {
        if (closing) {
            return;
        }

        int answer = JOptionPane.showConfirmDialog(
                this,
                "Close the client GUI and disconnect active client streams?",
                "Exit",
                JOptionPane.YES_NO_OPTION);

        if (answer != JOptionPane.YES_OPTION) {
            return;
        }

        closing = true;
        submitButton.setEnabled(false);
        connectionStatus.setText("Closing client...");

        jobExecutor.shutdownNow();
        dispose();
    }

    private static String stateText(JobState state) {
        switch (state) {
            case JOB_STATE_RUNNING:
                return "RUNNING";
            case JOB_STATE_COMPLETED:
                return "COMPLETED";
            case JOB_STATE_FAILED:
                return "FAILED";
            default:
                return state.name().replace("JOB_STATE_", "");
        }
    }

    private static String shortId(String id) {
        return id.length() > 8 ? id.substring(0, 8) : id;
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            try {
                UIManager.setLookAndFeel(
                        UIManager.getSystemLookAndFeelClassName());
            } catch (Exception ignored) {
                // Keep Swing's default look and feel.
            }

            new ClientGUI().setVisible(true);
        });
    }

    // ------------------------------------------------------------
    // Job table model
    // ------------------------------------------------------------

    private static final class JobRow {
        final String jobId;
        final String type;
        final SubmitJobRequest request;

        volatile JobProgress progress;
        volatile int percent;
        volatile String status = "SUBMITTING";
        volatile String result = "";
        volatile String note = "";
        volatile boolean finished;

        BreakdownDialog breakdownDialog;

        JobRow(SubmitJobRequest request) {
            this.request = request;
            this.jobId = request.getJobId();
            this.type = request.getType().name()
                    .replace("JOB_TYPE_", "");
        }
    }

    private static final class JobTableModel
            extends AbstractTableModel {

        private final String[] columns = {
                "Job ID", "Type", "Progress", "Status", "Result / Message"
        };

        private final List<JobRow> rows = new ArrayList<>();

        void addJob(JobRow row) {
            rows.add(row);
            int index = rows.size() - 1;
            fireTableRowsInserted(index, index);
        }

        JobRow getJobAt(int index) {
            if (index < 0 || index >= rows.size()) {
                return null;
            }
            return rows.get(index);
        }

        void refresh(JobRow job) {
            int index = rows.indexOf(job);
            if (index >= 0) {
                fireTableRowsUpdated(index, index);
            }
        }

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public int getColumnCount() {
            return columns.length;
        }

        @Override
        public String getColumnName(int column) {
            return columns[column];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            JobRow row = rows.get(rowIndex);

            switch (columnIndex) {
                case 0:
                    return shortId(row.jobId);
                case 1:
                    return row.type;
                case 2:
                    return row.percent;
                case 3:
                    return row.status;
                case 4:
                    return row.result;
                default:
                    return "";
            }
        }
    }

    // ------------------------------------------------------------
    // Real progress-bar renderer
    // ------------------------------------------------------------

    private static final class ProgressRenderer
            extends DefaultTableCellRenderer {

        @Override
        public Component getTableCellRendererComponent(
                JTable table,
                Object value,
                boolean selected,
                boolean focused,
                int row,
                int column) {

            int percent = value instanceof Number
                    ? ((Number) value).intValue() : 0;

            JProgressBar bar = new JProgressBar(0, 100);
            bar.setValue(Math.max(0, Math.min(100, percent)));
            bar.setStringPainted(true);
            bar.setString(percent + "%");
            bar.setBorderPainted(false);
            bar.setForeground(new Color(42, 130, 218));
            bar.setBackground(new Color(230, 235, 242));
            bar.setPreferredSize(new Dimension(170, 22));

            if (selected) {
                bar.setBackground(table.getSelectionBackground());
            }

            return bar;
        }
    }

    // ------------------------------------------------------------
    // Worker/task breakdown dialog
    // ------------------------------------------------------------

    private static final class BreakdownDialog extends JDialog {

        private final JobRow job;
        private final TaskTableModel taskModel = new TaskTableModel();
        private final JTable taskTable = new JTable(taskModel);
        private final JLabel summary = new JLabel();

        BreakdownDialog(JFrame owner, JobRow job) {
            super(owner,
                    "Worker Breakdown - " + shortId(job.jobId),
                    false);

            this.job = job;

            setLayout(new BorderLayout(10, 10));

            JPanel header = new JPanel();
            header.setLayout(new BoxLayout(
                    header, BoxLayout.Y_AXIS));
            header.setBorder(new EmptyBorder(12, 12, 4, 12));

            JLabel title = new JLabel(
                    job.type + " - " + shortId(job.jobId));
            title.setFont(
                    new Font("SansSerif", Font.BOLD, 17));

            header.add(title);
            header.add(Box.createVerticalStrut(6));
            header.add(summary);

            add(header, BorderLayout.NORTH);

            taskTable.setRowHeight(30);
            taskTable.setFillsViewportHeight(true);
            taskTable.setAutoCreateRowSorter(true);
            taskTable.getTableHeader().setReorderingAllowed(false);

            JScrollPane scroll = new JScrollPane(taskTable);
            scroll.setBorder(new EmptyBorder(0, 12, 0, 12));
            add(scroll, BorderLayout.CENTER);

            JButton close = new JButton("Close");
            close.addActionListener(e -> dispose());

            JPanel footer = new JPanel(
                    new FlowLayout(FlowLayout.RIGHT));
            footer.setBorder(new EmptyBorder(0, 12, 12, 12));
            footer.add(close);
            add(footer, BorderLayout.SOUTH);

            setSize(850, 330);
            setMinimumSize(new Dimension(750, 280));
            setLocationRelativeTo(owner);

            if (job.progress != null) {
                refresh(job.progress);
            }
        }

        void refresh(JobProgress progress) {
            summary.setText(String.format(
                    "Overall completion: %.1f%%    |    Status: %s    |    Coordinator: Worker %d (term %d)",
                    progress.getPercentComplete(),
                    stateText(progress.getState()),
                    progress.getCoordinatorId(),
                    progress.getTerm()));

            taskModel.update(progress.getTasksList());
        }
    }

    private static final class TaskTableModel
            extends AbstractTableModel {

        private final String[] columns = {
                "Task", "Worker", "Completed work", "Task progress",
                "State", "Attempt"
        };

        private List<TaskProgress> tasks = new ArrayList<>();

        void update(List<TaskProgress> latest) {
            tasks = new ArrayList<>(latest);
            fireTableDataChanged();
        }

        @Override
        public int getRowCount() {
            return tasks.size();
        }

        @Override
        public int getColumnCount() {
            return columns.length;
        }

        @Override
        public String getColumnName(int column) {
            return columns[column];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            TaskProgress task = tasks.get(rowIndex);

            switch (columnIndex) {
                case 0: {
                    String id = task.getTaskId();
                    int dash = id.lastIndexOf('-');
                    return dash >= 0 ? id.substring(dash + 1) : id;
                }
                case 1:
                    return task.getWorkerId() == 0
                            ? "Unassigned"
                            : "Worker " + task.getWorkerId();
                case 2:
                    return String.format(
                            "%,d / %,d",
                            task.getProcessed(),
                            task.getTotal());
                case 3:
                    return String.format(
                            "%.1f%%", task.getPercentComplete());
                case 4:
                    return taskStateText(task.getState());
                case 5:
                    return task.getAttempt()
                            + (task.getAttempt() > 1
                            ? " (reassigned)" : "");
                default:
                    return "";
            }
        }

        private static String taskStateText(TaskState state) {
            return state.name().replace("TASK_STATE_", "");
        }
    }
}
