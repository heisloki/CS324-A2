package org.client;

import org.example.bootstrap.BootstrapService;
import org.example.jobs.Job;
import org.example.jobs.JobResult;
import org.example.worker.WorkerInterface;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientGUI extends JFrame {

    // A job keeps retrying for this long if there is no coordinator (for example
    // while a new election is running after a term ended or the coordinator died).
    private static final long JOB_DEADLINE_MS = 20_000;
    private static final long RETRY_PAUSE_MS = 1_000;

    private JTextField numbersField;
    private JTextField startField;
    private JTextField endField;
    private JTextArea resultArea;

    private volatile BootstrapService bootstrap;

    // IMPORTANT: every network (RMI) call runs on this pool, never on the Swing
    // event thread. Blocking the event thread is what froze the window before.
    // Each submitted job gets its own thread, so one client can run many jobs at once.
    private final ExecutorService jobExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "client-job");
        t.setDaemon(true);
        return t;
    });
    private final AtomicInteger jobCounter = new AtomicInteger();

    // Numbers loaded from a CSV are kept here instead of being pasted into the text
    // field (a 100,000-number file is ~700 KB, which makes a JTextField very slow).
    private int[] csvData = null;
    private String csvSummary = null;

    public ClientGUI() {
        setTitle("CS324 Distributed Job Client");
        setSize(700, 700);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        createGUI();
        connectToBootstrap();
    }

    private void createGUI() {

        JPanel mainPanel = new JPanel(new BorderLayout(15, 15));
        mainPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));

        // ------------------------------------- HEADER -------------------------------------

        JLabel titleLabel = new JLabel("Distributed Job Client");
        titleLabel.setFont(new Font("Arial", Font.BOLD, 26));
        titleLabel.setHorizontalAlignment(SwingConstants.CENTER);

        JLabel subtitleLabel = new JLabel("Submit distributed computing jobs to the current coordinator");
        subtitleLabel.setHorizontalAlignment(SwingConstants.CENTER);

        JPanel headerPanel = new JPanel(new BorderLayout(5, 5));
        headerPanel.add(titleLabel, BorderLayout.NORTH);
        headerPanel.add(subtitleLabel, BorderLayout.SOUTH);
        mainPanel.add(headerPanel, BorderLayout.NORTH);

        // ------------------------------------- CENTER PANEL -------------------------------------

        JPanel centerPanel = new JPanel();
        centerPanel.setLayout(new BoxLayout(centerPanel, BoxLayout.Y_AXIS));

        // ------------------------------------- INPUT DATA SECTION -------------------------------------

        JPanel inputSection = new JPanel(new BorderLayout(10, 10));
        inputSection.setBorder(BorderFactory.createTitledBorder("1. Input Data"));

        JPanel numberPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 5));
        numberPanel.add(new JLabel("Numbers:"));

        numbersField = new JTextField(35);
        numbersField.setToolTipText("Enter numbers separated by commas");
        numberPanel.add(numbersField);

        JButton csvButton = new JButton("Load CSV");
        numberPanel.add(csvButton);

        inputSection.add(numberPanel, BorderLayout.WEST);

        // ------------------------------------- JOB SELECTION SECTION -------------------------------------

        JPanel jobSection = new JPanel(new BorderLayout(10, 10));
        jobSection.setBorder(BorderFactory.createTitledBorder("2. Select Job Type"));

        JPanel jobButtonPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 5));

        JButton maxButton = new JButton("MAX");
        JButton primeCountButton = new JButton("PRIMECOUNT");
        JButton primeSumButton = new JButton("PRIMESUM");

        jobButtonPanel.add(maxButton);
        jobButtonPanel.add(primeCountButton);
        jobButtonPanel.add(primeSumButton);

        jobSection.add(jobButtonPanel, BorderLayout.CENTER);

        // ------------------------------------- PRIMESUM RANGE SECTION -------------------------------------

        JPanel rangeSection = new JPanel(new GridLayout(1, 4, 10, 5));
        rangeSection.setBorder(BorderFactory.createTitledBorder(
                "3. PRIMESUM Range (only required for PRIMESUM)"));

        rangeSection.add(new JLabel("Start:"));
        startField = new JTextField();
        rangeSection.add(startField);

        rangeSection.add(new JLabel("End:"));
        endField = new JTextField();
        rangeSection.add(endField);

        // ------------------------------------- CLEAR BUTTON -------------------------------------

        JPanel controlPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton clearButton = new JButton("Clear");
        controlPanel.add(clearButton);

        // ------------------------------------- Add sections -------------------------------------

        centerPanel.add(inputSection);
        centerPanel.add(Box.createVerticalStrut(10));
        centerPanel.add(jobSection);
        centerPanel.add(Box.createVerticalStrut(10));
        centerPanel.add(rangeSection);
        centerPanel.add(Box.createVerticalStrut(5));
        centerPanel.add(controlPanel);

        mainPanel.add(centerPanel, BorderLayout.CENTER);

        // ------------------------------------- RESULT SECTION -------------------------------------

        resultArea = new JTextArea();
        resultArea.setEditable(false);
        resultArea.setFont(new Font("Monospaced", Font.PLAIN, 15));
        resultArea.setLineWrap(true);
        resultArea.setWrapStyleWord(true);
        resultArea.setMargin(new Insets(10, 10, 10, 10));

        JScrollPane scrollPane = new JScrollPane(resultArea);
        scrollPane.setPreferredSize(new Dimension(550, 180));
        scrollPane.setBorder(BorderFactory.createTitledBorder("4. Results & System Status"));

        mainPanel.add(scrollPane, BorderLayout.SOUTH);

        // ------------------------------------- BUTTON ACTIONS -------------------------------------

        maxButton.addActionListener(e -> submitMax());
        primeCountButton.addActionListener(e -> submitPrimeCount());
        primeSumButton.addActionListener(e -> submitPrimeSum());
        csvButton.addActionListener(e -> loadCSV());

        clearButton.addActionListener(e -> {
            numbersField.setText("");
            startField.setText("");
            endField.setText("");
            csvData = null;
            csvSummary = null;
            resultArea.setText("");
        });

        add(mainPanel);
    }

    // ------------------------------------- Thread-safe logging -------------------------------------

    // Safe to call from any thread: the text is appended on the Swing event thread.
    private void log(String text) {
        SwingUtilities.invokeLater(() -> {
            resultArea.append(text);
            resultArea.setCaretPosition(resultArea.getDocument().getLength());
        });
    }

    // ------------------------------------- Connect to Bootstrap -------------------------------------

    private void connectToBootstrap() {
        // Runs in the background so a slow or missing Bootstrap can't freeze the window.
        jobExecutor.submit(() -> {
            try {
                WorkerInterface coordinator = findCoordinator();
                log("Connected to Bootstrap Node.\n");
                if (coordinator == null) {
                    log("No coordinator is currently available.\n");
                } else {
                    log("Current Coordinator: Worker " + coordinator.getId() + "\n");
                }
            } catch (Exception e) {
                log("Could not connect to Bootstrap Node.\n\n"
                        + "Make sure BootstrapNode is running.\n\n"
                        + "Error: " + e.getMessage() + "\n");
            }
        });
    }

    private synchronized BootstrapService ensureBootstrap() throws Exception {
        if (bootstrap == null) {
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);
            bootstrap = (BootstrapService) registry.lookup("BootstrapService");
        }
        return bootstrap;
    }

    // Asks Bootstrap who the coordinator is right now (null if there isn't one yet).
    // Each job asks for itself, so concurrent jobs never share a stale reference.
    private WorkerInterface findCoordinator() throws Exception {
        try {
            return ensureBootstrap().getCoordinator();
        } catch (Exception e) {
            bootstrap = null; // reconnect next time
            throw e;
        }
    }

    // ------------------------------------- Input helpers -------------------------------------

    private int[] getInputNumbers() {
        String text = numbersField.getText();
        if (csvData != null && text.equals(csvSummary)) {
            return csvData; // user hasn't edited the loaded-CSV placeholder
        }
        return parseNumbers(text);
    }

    // ------------------------------------- MAX -------------------------------------

    private void submitMax() {
        try {
            int[] numbers = getInputNumbers();
            if (numbers.length == 0) {
                showError("Please enter some numbers.");
                return;
            }
            submitJob(new Job("MAX", numbers));
        } catch (NumberFormatException e) {
            showError("Please enter valid numbers separated by commas.");
        }
    }

    // ------------------------------------- PRIMECOUNT -------------------------------------

    private void submitPrimeCount() {
        try {
            int[] numbers = getInputNumbers();
            if (numbers.length == 0) {
                showError("Please enter some numbers.");
                return;
            }
            submitJob(new Job("PRIMECOUNT", numbers));
        } catch (NumberFormatException e) {
            showError("Please enter valid numbers separated by commas.");
        }
    }

    // ------------------------------------- PRIMESUM -------------------------------------

    private void submitPrimeSum() {
        try {
            int start = Integer.parseInt(startField.getText().trim());
            int end = Integer.parseInt(endField.getText().trim());

            if (start > end) {
                showError("Start must be less than or equal to End.");
                return;
            }
            submitJob(new Job("PRIMESUM", new int[]{start, end}));
        } catch (NumberFormatException e) {
            showError("Please enter valid start and end numbers.");
        }
    }

    // ------------------------------------- Submit job to current coordinator -------------------------------------

    // Returns immediately. The job runs on its own background thread, so the window
    // stays responsive and you can submit more jobs while earlier ones are running.
    private void submitJob(Job job) {
        final int jobNo = jobCounter.incrementAndGet();
        log("\n[Job #" + jobNo + "] " + job.getType() + " submitted...\n");
        jobExecutor.submit(() -> runJob(jobNo, job));
    }

    private void runJob(int jobNo, Job job) {
        String tag = "[Job #" + jobNo + "] ";
        long deadline = System.currentTimeMillis() + JOB_DEADLINE_MS;
        boolean announcedWait = false;

        while (System.currentTimeMillis() < deadline) {
            try {
                WorkerInterface coordinator = findCoordinator();

                if (coordinator == null) {
                    if (!announcedWait) {
                        log(tag + "No coordinator right now (election in progress?) - waiting...\n");
                        announcedWait = true;
                    }
                    Thread.sleep(RETRY_PAUSE_MS);
                    continue;
                }

                int coordinatorId = coordinator.getId();
                log(tag + "Sending to coordinator Worker " + coordinatorId + "...\n");

                long startNanos = System.nanoTime();
                JobResult result = coordinator.submitJob(job);
                long millis = (System.nanoTime() - startNanos) / 1_000_000;

                log(tag + "DONE in " + millis + " ms\n"
                        + "  Result: " + result.getResult() + "\n"
                        + "  Value: " + result.getValue() + "\n");
                return;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Most often: the coordinator's 5-job term just ended (or it died) and a
                // new election is running. Wait a moment and ask Bootstrap again.
                log(tag + "Coordinator changed or unavailable - retrying...\n");
                try {
                    Thread.sleep(RETRY_PAUSE_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        log(tag + "FAILED - no coordinator accepted the job within "
                + (JOB_DEADLINE_MS / 1000) + " seconds. Please try again.\n");
    }

    // ------------------------------------- Load CSV -------------------------------------

    private void loadCSV() {

        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Select CSV File");

        int result = fileChooser.showOpenDialog(this);
        if (result != JFileChooser.APPROVE_OPTION) {
            return;
        }

        File file = fileChooser.getSelectedFile();

        if (!file.getName().toLowerCase().endsWith(".csv")) {
            showError("Please select a CSV file.");
            return;
        }

        List<Integer> loaded = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {

            String line;
            while ((line = reader.readLine()) != null) {
                for (String value : line.split(",")) {
                    value = value.trim();
                    if (!value.isEmpty()) {
                        loaded.add(Integer.parseInt(value));
                    }
                }
            }

        } catch (NumberFormatException e) {
            showError("The CSV must contain only whole numbers separated by commas.\n" + e.getMessage());
            return;
        } catch (Exception e) {
            showError("Could not read CSV file.\n" + e.getMessage());
            return;
        }

        csvData = new int[loaded.size()];
        for (int i = 0; i < csvData.length; i++) {
            csvData[i] = loaded.get(i);
        }

        // Show a short placeholder instead of hundreds of KB of text.
        csvSummary = "[" + csvData.length + " numbers loaded from " + file.getName() + "]";
        numbersField.setText(csvSummary);

        resultArea.setText("CSV loaded successfully.\n"
                + "File: " + file.getName() + "\n"
                + csvData.length + " numbers ready to submit.\n");
    }

    // ------------------------------------- Convert comma-separated numbers into int[] -------------------------------------

    private int[] parseNumbers(String input) {

        if (input == null || input.trim().isEmpty()) {
            return new int[0];
        }

        String[] parts = input.split(",");
        List<Integer> numbers = new ArrayList<>();

        for (String part : parts) {
            String value = part.trim();
            if (!value.isEmpty()) {
                numbers.add(Integer.parseInt(value));
            }
        }

        int[] result = new int[numbers.size()];
        for (int i = 0; i < numbers.size(); i++) {
            result[i] = numbers.get(i);
        }
        return result;
    }

    // ------------------------------------- Error dialog -------------------------------------

    private void showError(String message) {
        JOptionPane.showMessageDialog(this, message, "Input Error", JOptionPane.ERROR_MESSAGE);
    }

    // ------------------------------------- Main -------------------------------------

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            ClientGUI gui = new ClientGUI();
            gui.setVisible(true);
        });
    }
}