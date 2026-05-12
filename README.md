# CPU Schedulers Simulator

> A Java-based simulation framework for analyzing and validating classical CPU scheduling algorithms.

This project implements multiple CPU scheduling strategies commonly used in operating systems and evaluates their behavior through automated JSON-based test cases.
The simulator models process execution, scheduling decisions, context switching, waiting times, turnaround times, and dynamic quantum management.

## Implemented Algorithms
### Preemptive Shortest Job First (SJF)
Schedules processes based on the shortest remaining burst time with support for context switching.

### Round Robin (RR)
Implements time-sharing process scheduling using configurable quantum values.

### Priority Scheduling
Preemptive priority scheduling with starvation prevention using aging.

### AG Scheduling
Hybrid scheduling algorithm combining FCFS, Priority Scheduling, and Shortest Job First. Includes dynamic quantum updates and execution history tracking.

## Features
- Execution order simulation
- Context switching handling
- Waiting and turnaround time calculation
- Dynamic quantum management
- Starvation prevention using aging
- Automated scheduler validation using JSON test cases
- Modular scheduler architecture

## Example Process Set
```text
Process   Arrival   Burst   Priority   Quantum
P1        0         17      4          7
P2        2         6       7          9
P3        5         11      3          4
P4        15        4       6          6
```

## Automated Validation
The simulator parses structured JSON test cases and validates:
- Execution order
- Waiting time
- Turnaround time
- Average waiting time
- Average turnaround time
- Quantum history updates

## Technologies
Java  
Gson  
Scheduling Algorithms  
Object-Oriented Programming
