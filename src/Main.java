import java.nio.file.*;
import java.util.*;
import com.google.gson.Gson;

interface Scheduler {
    void schedule(List<Process> processes, int contextSwitch);
    List<String> getExecutionOrder(); // For returning execution sequence
    List<ProcessResult> getProcessResults();
    double getAverageWaitingTime();
    double getAverageTurnaroundTime();
}

class Process {
    String ProcessName;
    int arrivalTime;
    int burstTime;
    int remainingTime;
    int priority;
    int effectivePriority;
    int quantum;
    int waitingTime;
    int turnaroundTime;
    int waitStartTime = -1; // For aging calculation
    List<Integer> quantumHistory = new ArrayList<>();

    Process(String n, int a, int b, int p, int q) {
        ProcessName = n;
        arrivalTime = a;
        burstTime = b;
        remainingTime = b;
        priority = p;
        effectivePriority = p;
        quantum = q;
    }
}

class RoundRobinScheduler implements Scheduler {
    private List<Process> processes;
    private Queue<Process> q = new LinkedList<>();
    private List<String> order = new ArrayList<>();
    private int time = 0;
    private int quantum;
    RoundRobinScheduler(int q) {
        quantum = q; }

    public void schedule(List<Process> input, int cs) {
        processes = new ArrayList<>();
        for (Process p : input)
            processes.add(new Process(p.ProcessName, p.arrivalTime, p.burstTime, p.priority, p.quantum));
        processes.sort(Comparator.comparingInt(p -> p.arrivalTime));
        int idx = 0, finished = 0;
        while (finished < processes.size()) {
            while (idx < processes.size() && processes.get(idx).arrivalTime <= time)
                q.add(processes.get(idx++));
            // Idle CPU
            if (q.isEmpty()) {
                time++;
                continue;
            }
            Process p = q.poll();
            order.add(p.ProcessName);
            int run = Math.min(quantum, p.remainingTime);
            p.remainingTime -= run;
            time += run;

            while (idx < processes.size() && processes.get(idx).arrivalTime <= time)
                q.add(processes.get(idx++));
            if (p.remainingTime > 0)
                q.add(p);
            else {
                finished++;
                p.turnaroundTime = time - p.arrivalTime;
                p.waitingTime = p.turnaroundTime - p.burstTime;
            }
            if (!q.isEmpty()) time += cs;
        }
    }
    public List<String> getExecutionOrder() {
        return order; }
    public List<ProcessResult> getProcessResults() {
        List<ProcessResult> r = new ArrayList<>();
        for (Process p : processes)
            r.add(new ProcessResult(p.ProcessName, p.waitingTime, p.turnaroundTime, null));
        return r;
    }

    public double getAverageWaitingTime() {
        return processes.stream().mapToInt(p -> p.waitingTime).average().orElse(0);
    }

    public double getAverageTurnaroundTime() {
        return processes.stream().mapToInt(p -> p.turnaroundTime).average().orElse(0);
    }
}

class SJFScheduler implements Scheduler {
    private List<Process> processList;
    private List<String> order = new ArrayList<>();
    private int currentTime = 0;

    public void schedule(List<Process> processes, int cs) {
        this.processList = processes;
        int completed = 0;
        Process running = null;

        while (completed < processList.size()) {
            List<Process> readyQueue = new ArrayList<>();
            for (Process proc : processList)
                if (proc.arrivalTime <= currentTime && proc.remainingTime > 0)
                    readyQueue.add(proc);
            if (readyQueue.isEmpty()) {
                currentTime++;
                continue;
            }
            // Here we select process with the shortest remaining time
            Process selected = readyQueue.stream()
                    .min(Comparator.comparingInt(proc -> proc.remainingTime))
                    .get();
            // Context switch if preempted
            if (running != null && running != selected)
                currentTime += cs;
            running = selected;
            if (order.isEmpty() || !order.get(order.size() - 1).equals(running.ProcessName))
                order.add(running.ProcessName);
            // Execute 1 time unit
            running.remainingTime--;
            currentTime++;
            if (running.remainingTime == 0) {
                completed++;
                running.turnaroundTime = currentTime - running.arrivalTime;
                running.waitingTime = running.turnaroundTime - running.burstTime;
            }
        }
    }

    public List<String> getExecutionOrder() {
        return order;
    }
    public List<ProcessResult> getProcessResults() {
        List<ProcessResult> r = new ArrayList<>();
        for (Process proc : processList)
            r.add(new ProcessResult(proc.ProcessName, proc.waitingTime, proc.turnaroundTime, null));
        return r;
    }
    public double getAverageWaitingTime() {
        return processList.stream().mapToInt(proc -> proc.waitingTime).average().orElse(0);
    }
    public double getAverageTurnaroundTime() {
        return processList.stream().mapToInt(proc -> proc.turnaroundTime).average().orElse(0);
    }
}
class PriorityScheduler implements Scheduler {

    // Input Fields
    private List<Process> processes = new ArrayList<>();
    private int contextSwitch;
    private int agingInterval;

    // Result Fields
    private List<String> executionOrder = new ArrayList<>();
    private List<ProcessResult> resultList = new ArrayList<>();
    private double avgWaitingTime;
    private double avgTurnaroundTime;

    public PriorityScheduler(int agingInterval) {
        this.agingInterval = agingInterval;
    }

    @Override
    public void schedule(List<Process> input, int contextSwitch) {

        // Deep copy (same as original)
        for (Process p : input) {
            Process c = new Process(
                    p.ProcessName,
                    p.arrivalTime,
                    p.burstTime,
                    p.priority,
                    p.quantum
            );
            processes.add(c);
        }

        this.contextSwitch = contextSwitch;

        processes.sort(Comparator.comparingInt(p -> p.arrivalTime));

        List<Process> readyQueue = new ArrayList<>();
        int currentTime = 0;
        Process currentProcess = null;
        String lastExecutedProcess = null;

        while (true) {

            // 1. Add newly arrived processes
            for (Process p : processes) {
                if (p.arrivalTime <= currentTime &&
                        !readyQueue.contains(p) &&
                        p.remainingTime > 0) {
                    readyQueue.add(p);
                    p.waitStartTime = currentTime;
                }
            }

            // 2. Update priorities (AGING) — Same logic
            updatePriority(readyQueue, currentProcess, currentTime);

            // 3. Handle Preemption — Same logic
            if (currentProcess != null && !readyQueue.isEmpty()) {
                if (readyQueue.contains(currentProcess)) {

                    Process highestPriorityInQueue =
                            getHighestPriorityProcess(readyQueue);

                    if (highestPriorityInQueue != currentProcess &&
                            highestPriorityInQueue.effectivePriority
                                    <= currentProcess.effectivePriority) {

                        if (!currentProcess.ProcessName
                                .equals(highestPriorityInQueue.ProcessName)) {

                            currentTime += contextSwitch;
                            recordExecution(currentProcess.ProcessName);
                            checkInContextSwitch(readyQueue, currentTime);
                            updatePriority(readyQueue, currentProcess, currentTime);
                        }

                        currentProcess.waitStartTime = currentTime - contextSwitch;
                        currentProcess = getHighestPriorityProcess(readyQueue);

                        if (highestPriorityInQueue != currentProcess) {
                            currentTime += contextSwitch;
                        }
                    }
                }
            }

            // 4. Select if idle — Same logic
            if (currentProcess == null && !readyQueue.isEmpty()) {

                Process highestPriorityInQueue =
                        getHighestPriorityProcess(readyQueue);

                if (lastExecutedProcess != null &&
                        !lastExecutedProcess.equals(highestPriorityInQueue.ProcessName)) {

                    currentTime += contextSwitch;
                    recordExecution(highestPriorityInQueue.ProcessName);
                    checkInContextSwitch(readyQueue, currentTime);
                    updatePriority(readyQueue, currentProcess, currentTime);
                }

                currentProcess = getHighestPriorityProcess(readyQueue);

                if (highestPriorityInQueue != currentProcess) {
                    currentTime += contextSwitch;
                }

                checkInContextSwitch(readyQueue, currentTime);
                updatePriority(readyQueue, currentProcess, currentTime);
            }

            // 5. Termination Check — Same logic
            if (currentProcess == null) {
                boolean allDone = true;
                for (Process p : processes) {
                    if (p.remainingTime > 0) {
                        allDone = false;
                        break;
                    }
                }
                if (allDone) break;
                currentTime++;
                continue;
            }

            // 6. Execute 1 unit — Same logic
            recordExecution(currentProcess.ProcessName);
            currentProcess.remainingTime--;
            currentProcess.waitStartTime = -1;
            currentTime++;

            // 7. Completion — Same logic
            if (currentProcess.remainingTime == 0) {

                currentProcess.turnaroundTime =
                        currentTime - currentProcess.arrivalTime;

                currentProcess.waitingTime =
                        currentProcess.turnaroundTime
                                - currentProcess.burstTime;

                resultList.add(new ProcessResult(
                        currentProcess.ProcessName,
                        currentProcess.waitingTime,
                        currentProcess.turnaroundTime,
                        null
                ));

                readyQueue.remove(currentProcess);
                lastExecutedProcess = currentProcess.ProcessName;
                currentProcess = null;
            } else {
                lastExecutedProcess = currentProcess.ProcessName;
            }
        }

        calculateFinalResults();
    }

    // Decrease effective priority if process waits too long
    private void updatePriority(List<Process> readyQueue,
                                Process currentProcess,
                                int currentTime) {

        for (Process p : readyQueue) {
            int timeInQueue = currentTime - p.waitStartTime;

            if (p != currentProcess && timeInQueue >= agingInterval) {
                int priorityBoost = timeInQueue / agingInterval;
                p.effectivePriority =
                        Math.max(1, p.effectivePriority - priorityBoost);
                p.waitStartTime = currentTime;
            }
        }
    }

    private void checkInContextSwitch(List<Process> readyQueue, int currentTime) {
        for (Process p : processes) {
            if (p.arrivalTime <= currentTime &&
                    p.arrivalTime > (currentTime - contextSwitch) &&
                    !readyQueue.contains(p)) {

                readyQueue.add(p);
                p.waitStartTime = p.arrivalTime;
            }
        }
    }
    // Select process with highest priority
    private Process getHighestPriorityProcess(List<Process> readyQueue) {
        Process highest = readyQueue.get(0);
        for (Process p : readyQueue) {
            if (p.effectivePriority < highest.effectivePriority ||
                    (p.effectivePriority == highest.effectivePriority &&
                            p.arrivalTime < highest.arrivalTime)) {
                highest = p;
            }
        }
        return highest;
    }

    // Record execution order without duplicates
    private void recordExecution(String name) {
        if (executionOrder.isEmpty() ||
                !executionOrder.get(executionOrder.size() - 1).equals(name)) {
            executionOrder.add(name);
        }
    }

    private void calculateFinalResults() {
        double totalWait = 0, totalTurn = 0;
        for (ProcessResult p : resultList) {
            totalWait += p.waitingTime;
            totalTurn += p.turnaroundTime;
        }
        avgWaitingTime =
                Math.round((totalWait / processes.size()) * 100.0) / 100.0;
        avgTurnaroundTime =
                Math.round((totalTurn / processes.size()) * 100.0) / 100.0;
    }
    @Override
    public List<String> getExecutionOrder() {
        return executionOrder;
    }

    @Override
    public List<ProcessResult> getProcessResults() {
        return resultList;
    }

    @Override
    public double getAverageWaitingTime() {
        return avgWaitingTime;
    }

    @Override
    public double getAverageTurnaroundTime() {
        return avgTurnaroundTime;
    }
}

class AGScheduler implements Scheduler {
    private List<Process> processes;
    private final List<String> order = new ArrayList<>();
    private int currentTime = 0;

    @Override
    public void schedule(List<Process> input, int contextSwitch) {

        // reset outputs/state
        order.clear();
        currentTime = 0;

        // IMPORTANT: work on the SAME objects (so results go back to these processes)
        processes = new ArrayList<>(input);

        // sort by arrival then name for stable behavior
        processes.sort(Comparator.comparingInt(p -> p.arrivalTime));

        // init remaining + quantum history
        for (Process p : processes) {
            p.remainingTime = p.burstTime;
            p.waitingTime = 0;
            p.turnaroundTime = 0;

            p.quantumHistory.clear();
            p.quantumHistory.add(p.quantum); // start history with initial Q
        }

        int n = processes.size();
        int completed = 0;
        int arrivalIdx = 0;

        Deque<Process> readyQueue = new ArrayDeque<>();
        Process running = null;

        // add arrivals at time 0
        while (arrivalIdx < n && processes.get(arrivalIdx).arrivalTime <= currentTime) {
            readyQueue.addLast(processes.get(arrivalIdx++));
        }

        while (completed < n) {

            // CPU idle => jump to next arrival
            if (running == null && readyQueue.isEmpty()) {
                if (arrivalIdx < n) {
                    currentTime = Math.max(currentTime, processes.get(arrivalIdx).arrivalTime);
                    while (arrivalIdx < n && processes.get(arrivalIdx).arrivalTime <= currentTime) {
                        readyQueue.addLast(processes.get(arrivalIdx++));
                    }
                    continue;
                }
            }

            // choose next FCFS from ready queue
            if (running == null) {
                running = readyQueue.pollFirst();
                addExecutionSwitch(running.ProcessName);
            }

            int q = running.quantum;   // quantum for THIS cycle
            int used = 0;              // how much we already used of q

            int phase1 = ceilDiv(q, 4); // ceil(25%)
            int phase2 = ceilDiv(q, 4); // ceil(25%)

            // Phase 1: FCFS (ceil(25%Q))
            int run1 = Math.min(phase1, running.remainingTime);
            for (int step = 0; step < run1; step++) {
                tick(running);
                currentTime++; used++;

                while (arrivalIdx < n && processes.get(arrivalIdx).arrivalTime <= currentTime) {
                    readyQueue.addLast(processes.get(arrivalIdx++));
                }

                if (running.remainingTime == 0) break;
            }

            // finished early => case iv
            if (running.remainingTime == 0) {
                finish(running);
                completed++;
                running = null;
                continue;
            }

            // Boundary after phase 1 => Priority check (case ii)
            Process bestPrio = pickBestPriority(readyQueue);
            if (bestPrio != null && bestPrio.priority < running.priority) {
                int remainingQ = q - used;
                applyCaseII(running, remainingQ);
                readyQueue.addLast(running);

                readyQueue.remove(bestPrio);
                running = bestPrio;

                addExecutionSwitch(running.ProcessName);
                continue;
            }

            // Phase 2: Priority slice (ceil(25%Q))
            int run2 = Math.min(phase2, running.remainingTime);
            for (int step = 0; step < run2; step++) {
                tick(running);
                currentTime++; used++;

                while (arrivalIdx < n && processes.get(arrivalIdx).arrivalTime <= currentTime) {
                    readyQueue.addLast(processes.get(arrivalIdx++));
                }

                if (running.remainingTime == 0) break;
            }

            // finished early => case iv
            if (running.remainingTime == 0) {
                finish(running);
                completed++;
                running = null;
                continue;
            }

            // boundary after phase 2 => SJF check (case iii)
            Process bestSjf = pickShortestRemaining(readyQueue);
            if (bestSjf != null && bestSjf.remainingTime < running.remainingTime) {
                int remainingQ = q - used;
                applyCaseIII(running, remainingQ);
                readyQueue.addLast(running);

                readyQueue.remove(bestSjf);
                running = bestSjf;

                addExecutionSwitch(running.ProcessName);
                continue;
            }

            // Phase 3: remaining quantum (preemptive SJF)
            int remainingSlice = q - used;

            while (remainingSlice > 0 && running.remainingTime > 0) {
                tick(running);
                currentTime++; used++;
                remainingSlice--;

                while (arrivalIdx < n && processes.get(arrivalIdx).arrivalTime <= currentTime) {
                    readyQueue.addLast(processes.get(arrivalIdx++));
                }

                if (running.remainingTime == 0) break;

                // preempt anytime in phase 3 if shorter job appears (case iii)
                bestSjf = pickShortestRemaining(readyQueue);
                if (bestSjf != null && bestSjf.remainingTime < running.remainingTime) {
                    int remainingQ = q - used;
                    applyCaseIII(running, remainingQ);
                    readyQueue.addLast(running);

                    readyQueue.remove(bestSjf);
                    running = bestSjf;

                    addExecutionSwitch(running.ProcessName);
                    break;
                }
            }

            if (running == null) continue;

            // finished => case iv
            if (running.remainingTime == 0) {
                finish(running);
                completed++;
                running = null;
                continue;
            }

            // used full quantum but not finished => case i
            if (used == q && running.remainingTime > 0) {
                applyCaseI(running);
                readyQueue.addLast(running);
                running = null;
            }
        }
    }

    @Override
    public List<String> getExecutionOrder() {
        return order;
    }

    @Override
    public List<ProcessResult> getProcessResults() {
        List<ProcessResult> r = new ArrayList<>();
        for (Process p : processes) {
            r.add(new ProcessResult(p.ProcessName, p.waitingTime, p.turnaroundTime,
                    new ArrayList<>(p.quantumHistory)));
        }
        return r;
    }

    @Override
    public double getAverageWaitingTime() {
        return processes.stream().mapToInt(p -> p.waitingTime).average().orElse(0);
    }

    @Override
    public double getAverageTurnaroundTime() {
        return processes.stream().mapToInt(p -> p.turnaroundTime).average().orElse(0);
    }

    // ---------- helpers ----------
    private void addExecutionSwitch(String name) {
        if (order.isEmpty() || !order.get(order.size() - 1).equals(name)) {
            order.add(name);
        }
    }

    private void tick(Process p) {
        p.remainingTime--;
    }

    private void finish(Process p) {
        // quantum ends with 0 and must appear in history once
        p.quantum = 0;
        appendQuantumIfChanged(p, 0);

        p.turnaroundTime = currentTime - p.arrivalTime;
        p.waitingTime = p.turnaroundTime - p.burstTime;
    }

    // case i: quantum used fully
    private void applyCaseI(Process p) {
        p.quantum += 2;
        appendQuantumIfChanged(p, p.quantum);
    }

    // case ii: preempted at priority boundary => Q += ceil(remaining/2)
    private void applyCaseII(Process p, int remainingQ) {
        int inc = ceilDiv(remainingQ, 2);
        p.quantum += inc;
        appendQuantumIfChanged(p, p.quantum);
    }

    // case iii: preempted by SJF => Q += remainingQ
    private void applyCaseIII(Process p, int remainingQ) {
        p.quantum += remainingQ;
        appendQuantumIfChanged(p, p.quantum);
    }

    private void appendQuantumIfChanged(Process p, int q) {
        // avoid duplicates like [7,7,7,...]
        if (p.quantumHistory.isEmpty() || p.quantumHistory.get(p.quantumHistory.size() - 1) != q) {
            p.quantumHistory.add(q);
        }
    }

    private Process pickBestPriority(Deque<Process> readyQueue) {
        Process best = null;
        for (Process candidate : readyQueue) {
            if (best == null || candidate.priority < best.priority) best = candidate;
        }
        return best;
    }

    private Process pickShortestRemaining(Deque<Process> readyQueue) {
        Process best = null;
        for (Process candidate : readyQueue) {
            if (best == null || candidate.remainingTime < best.remainingTime) best = candidate;
        }
        return best;
    }

    private int ceilDiv(int a, int b) {
        return (a + b - 1) / b;
    }
}
public class Main {

    public static void main(String[] args) throws Exception {

        Gson gson = new Gson();
        Path root = Paths.get("src/testCases");

        try (DirectoryStream<Path> folders = Files.newDirectoryStream(root)) {
            for (Path folder : folders) {

                if (!Files.isDirectory(folder)) continue;

                System.out.println("\n==============================");
                System.out.println("Folder: " + folder.getFileName());
                System.out.println("==============================");

                try (DirectoryStream<Path> files =
                             Files.newDirectoryStream(folder, "*.json")) {

                    for (Path f : files) {

                        TestCase tc = gson.fromJson(Files.readString(f), TestCase.class);

                        System.out.println("\n----------------------------------");
                        System.out.println(tc.name == null ? "[AG Test Case]" : tc.name);
                        System.out.println("----------------------------------");

                        runTest(tc);
                    }
                }
            }
        }
    }

    static void runTest(TestCase tc) {

        int cs = tc.input.contextSwitch != null ? tc.input.contextSwitch : 0;
        int rrQ = tc.input.rrQuantum != null ? tc.input.rrQuantum : 1;
        int aging = tc.input.agingInterval != null ? tc.input.agingInterval : 5;

        List<Process> base = new ArrayList<>();
        for (ProcessJson p : tc.input.processes) {
            base.add(new Process(
                    p.name,
                    p.arrival,
                    p.burst,
                    p.priority,
                    p.quantum != null ? p.quantum : 1
            ));
        }

        //SJF
        if (tc.expectedOutput.SJF != null) {
            SJFScheduler sjf = new SJFScheduler();
            sjf.schedule(copy(base), cs);

            printDetails("SJF", sjf);
            printAndValidateAverages("SJF", sjf, tc.expectedOutput.SJF);
            validate("SJF", sjf, tc.expectedOutput.SJF);
        }

        // RoundRobin
        if (tc.expectedOutput.RR != null) {
            RoundRobinScheduler rr = new RoundRobinScheduler(rrQ);
            rr.schedule(copy(base), cs);

            printDetails("Round Robin", rr);
            printAndValidateAverages("Round Robin", rr, tc.expectedOutput.RR);
            validate("Round Robin", rr, tc.expectedOutput.RR);
        }

        // Priority
        if (tc.expectedOutput.Priority != null) {
            PriorityScheduler pr = new PriorityScheduler(aging);
            pr.schedule(copy(base), cs);

            printDetails("Priority", pr);
            printAndValidateAverages("Priority", pr, tc.expectedOutput.Priority);
            validate("Priority", pr, tc.expectedOutput.Priority);
        }

        // AG
        if (tc.expectedOutput.executionOrder != null) {
            AGScheduler ag = new AGScheduler();
            ag.schedule(copy(base), cs);

            printDetails("AG", ag);
            validateAG(ag, tc.expectedOutput);
        }
    }

    // Printing
    static void printDetails(String name, Scheduler s) {

        System.out.println("\n" + name + " Execution Order:");
        System.out.println(s.getExecutionOrder());

        System.out.println(name + " Waiting Time for each process:");
        for (ProcessResult pr : s.getProcessResults()) {
            System.out.println(pr.name + " = " + pr.waitingTime);
        }

        System.out.println(name + " Turnaround Time for each process:");
        for (ProcessResult pr : s.getProcessResults()) {
            System.out.println(pr.name + " = " + pr.turnaroundTime);
        }

        System.out.println(name + " Average Waiting Time = "
                + s.getAverageWaitingTime());

        System.out.println(name + " Average Turnaround Time = "
                + s.getAverageTurnaroundTime());
    }

    // Expected vs Actual
    static void printAndValidateAverages(String name,
                                         Scheduler s,
                                         SchedulerOutput expected) {

        double actualWT = s.getAverageWaitingTime();
        double actualTAT = s.getAverageTurnaroundTime();
        double expectedWT = expected.averageWaitingTime;
        double expectedTAT = expected.averageTurnaroundTime;

        System.out.println("\n" + name + " Average Comparison:");
        System.out.println("Expected Average Waiting Time = " + expectedWT);
        System.out.println("Actual   Average Waiting Time = " + actualWT);

        System.out.println("Expected Average Turnaround Time = " + expectedTAT);
        System.out.println("Actual   Average Turnaround Time = " + actualTAT);

        System.out.println("Average Waiting Time Match: " +
                (Math.abs(actualWT - expectedWT) < 0.01 ? "PASS" : "FAIL"));

        System.out.println("Average Turnaround Time Match: " +
                (Math.abs(actualTAT - expectedTAT) < 0.01 ? "PASS" : "FAIL"));
    }

    // Validation of AG
    static void validateAG(AGScheduler ag, ExpectedOutput e) {

        System.out.println("\nAG Quantum History (Expected):");
        for (ProcessResult pr : e.processResults) {
            System.out.println(pr.name + " -> " + pr.quantumHistory);
        }

        System.out.println("\nAG Quantum History (Actual):");
        for (ProcessResult pr : ag.getProcessResults()) {
            System.out.println(pr.name + " -> " + pr.quantumHistory);
        }

        double actualWT = ag.getAverageWaitingTime();
        double actualTAT = ag.getAverageTurnaroundTime();

        System.out.println("\nAG Average Comparison:");
        System.out.println("Expected Average Waiting Time = " + e.averageWaitingTime);
        System.out.println("Actual   Average Waiting Time = " + actualWT);

        System.out.println("Expected Average Turnaround Time = " + e.averageTurnaroundTime);
        System.out.println("Actual   Average Turnaround Time = " + actualTAT);

        System.out.println("Average Waiting Time Match: " +
                (Math.abs(actualWT - e.averageWaitingTime) < 0.01 ? "PASS" : "FAIL"));

        System.out.println("Average Turnaround Time Match: " +
                (Math.abs(actualTAT - e.averageTurnaroundTime) < 0.01 ? "PASS" : "FAIL"));

        System.out.println("\nAG Execution Order Match: " +
                ag.getExecutionOrder().equals(e.executionOrder));
    }

    // Order Validation
    static void validate(String name, Scheduler s, SchedulerOutput e) {

        System.out.println("\n" + name + " Execution Order Match: " +
                s.getExecutionOrder().equals(e.executionOrder));
    }

   // Process copying
    static List<Process> copy(List<Process> p) {
        List<Process> c = new ArrayList<>();
        for (Process x : p) {
            c.add(new Process(
                    x.ProcessName,
                    x.arrivalTime,
                    x.burstTime,
                    x.priority,
                    x.quantum
            ));
        }
        return c;
    }
}


// JSON Models
class TestCase {
    String name;
    InputData input;
    ExpectedOutput expectedOutput;
}

class InputData {
    List<ProcessJson> processes;
    Integer contextSwitch;
    Integer rrQuantum;
    Integer agingInterval;
}

class ProcessJson {
    String name;
    int arrival;
    int burst;
    int priority;
    Integer quantum;
}

class ExpectedOutput {
    SchedulerOutput SJF;
    SchedulerOutput RR;
    SchedulerOutput Priority;

    List<String> executionOrder;
    List<ProcessResult> processResults;
    double averageWaitingTime;
    double averageTurnaroundTime;
}

class SchedulerOutput {
    List<String> executionOrder;
    List<ProcessResult> processResults;
    double averageWaitingTime;
    double averageTurnaroundTime;
}

class ProcessResult {
    String name;
    int waitingTime;
    int turnaroundTime;
    List<Integer> quantumHistory;

    ProcessResult(String n, int w, int t, List<Integer> q) {
        name = n;
        waitingTime = w;
        turnaroundTime = t;
        quantumHistory = q;
    }
}