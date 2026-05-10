// ═══════════════════════════════════════════════════════════════════════════
//  Multithreaded Task Queue — C++17 Single-File Implementation
//
//  Compile:  g++ -std=c++17 -pthread -o task_queue main.cpp
//  Run:      ./task_queue
// ═══════════════════════════════════════════════════════════════════════════

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <string>
#include <sstream>
#include <vector>        // Only for vector<thread> workers — per class design spec

using namespace std;

// ─────────────────────────────────────────────────────────────────────────────
// STRUCTS
// ─────────────────────────────────────────────────────────────────────────────

struct Task {
    int    id;
    string name;
    bool   is_empty;   // true = slot is available, false = task lives here
};

struct FailedTask {
    int    id;
    string name;
    string error_reason;
    int    failed_by_worker;
};

// ─────────────────────────────────────────────────────────────────────────────
// CLASS: TaskQueue
//
// WHY A CLASS INSTEAD OF GLOBAL VARIABLES?
//   Global variables are shared across the entire program — any thread or
//   function can accidentally read or overwrite them. In multithreaded code
//   that causes subtle, hard-to-reproduce bugs (data races).
//
//   A class bundles all state (queue, locks, counters) into one self-contained
//   object. Each TaskQueue instance has its own private memory, so the three
//   scenarios in main() run completely independently without interfering.
//   You simply create another object — no copy-paste, no name clashes.
// ─────────────────────────────────────────────────────────────────────────────

class TaskQueue {
private:

    // ── Task buffer (circular array) ─────────────────────────────────────────
    Task queue_array[100];   // fixed-size storage — max 100 slots
    int  front;              // index of the next task to dequeue
    int  back;               // index where the next enqueued task goes
    int  count;              // number of tasks currently in the buffer
    int  max_size;           // actual capacity passed in constructor (≤ 100)

    // ── Dead Letter Queue (DLQ) ───────────────────────────────────────────────
    //   In real distributed systems (AWS SQS, RabbitMQ, Apache Kafka) a DLQ
    //   is where messages go after repeated failures, so they are never silently
    //   dropped. Operators can inspect them, replay them after a bug fix, or
    //   trigger alerts. We do the same here — failed tasks land in dlq[].
    FailedTask dlq[100];
    int        dlq_count;

    // ── Counters ──────────────────────────────────────────────────────────────
    int  done_count;       // tasks finished (success + failure)
    int  total_submitted;  // tasks ever enqueued — used for wait_until_done()

    // ── Shutdown flag ─────────────────────────────────────────────────────────
    bool stop_workers;

    // ── Locks ─────────────────────────────────────────────────────────────────
    mutex queue_lock;   // protects queue_array, front, back, count, done_count
    mutex dlq_lock;     // protects dlq[] and dlq_count separately from queue
    //
    // WHY TWO SEPARATE MUTEXES FOR QUEUE vs DLQ?
    //   If one lock guarded both, a worker saving a failure would block every
    //   other worker from dequeuing their next task. Two locks let the main
    //   queue keep moving while a failure is being recorded.

    // ── Thread-safe output ────────────────────────────────────────────────────
    mutex print_lock;
    //
    // WHY safe_print?
    //   cout is not thread-safe. Two threads writing simultaneously produce
    //   garbled output, e.g.:
    //       "[Worker 1] Co[Worker 2] Picked up task #3..."
    //   print_lock ensures only one thread writes to cout at a time.
    //   EVERY cout in this project goes through safe_print.

    // ── Condition variables ───────────────────────────────────────────────────
    condition_variable task_available;   // wakes workers when a task is added
    condition_variable slot_available;   // wakes enqueue() when a slot is freed
    //
    // WHY TWO SEPARATE CONDITION VARIABLES?
    //   task_available targets workers  — "there is work to pick up".
    //   slot_available targets the producer — "there is room to add more".
    //   Merging them into one variable would wake the wrong waiter half the
    //   time (spurious wakeups with no matching condition), causing subtle
    //   stalls or busy-spin bugs.

    // ── Workers ───────────────────────────────────────────────────────────────
    vector<thread> workers;
    int            num_workers;

    // ─────────────────────────────────────────────────────────────────────────
    // PRIVATE: safe_print
    //   All output in this project routes through here so cout is never
    //   called from two threads at the same time.
    // ─────────────────────────────────────────────────────────────────────────
    void safe_print(string msg) {
        lock_guard<mutex> lk(print_lock);
        cout << msg << "\n";
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PRIVATE: save_to_dlq
    //   Called when a task throws an exception. Locks only dlq_lock so the
    //   main queue remains unblocked while failure details are recorded.
    // ─────────────────────────────────────────────────────────────────────────
    void save_to_dlq(Task t, string reason, int worker_id) {
        lock_guard<mutex> lk(dlq_lock);

        if (dlq_count >= 100) {
            // Use a temporary string to avoid holding both locks at once
            string warn = "[DLQ] WARNING: DLQ is full! Task #"
                          + to_string(t.id) + " cannot be recorded.";
            lock_guard<mutex> plk(print_lock);
            cout << warn << "\n";
            return;
        }

        dlq[dlq_count].id              = t.id;
        dlq[dlq_count].name            = t.name;
        dlq[dlq_count].error_reason    = reason;
        dlq[dlq_count].failed_by_worker = worker_id;
        dlq_count++;

        string msg = "[DLQ] Task #" + to_string(t.id)
                   + " → \"" + t.name + "\" | Reason: " + reason;
        lock_guard<mutex> plk(print_lock);
        cout << msg << "\n";
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PRIVATE: worker_loop
    //   Each worker thread runs this indefinitely, dequeuing and processing
    //   tasks one at a time until shutdown() is called.
    //
    //   WHY try/catch IS PLACED OUTSIDE THE LOCK:
    //     If we held queue_lock while doing the CPU work or catching an
    //     exception, every other worker would be blocked from picking up their
    //     next task for the entire duration of this task.  We hold the lock
    //     only for the instant we need to read/write shared state (dequeue).
    //     Once the task is in our local variable, we release the lock and work
    //     independently — the queue can keep moving in parallel.
    // ─────────────────────────────────────────────────────────────────────────
    void worker_loop(int worker_id) {
        while (true) {
            Task task;

            // ── Critical section: dequeue ─────────────────────────────────
            {
                unique_lock<mutex> lk(queue_lock);

                // Sleep until there is at least one task OR shutdown signal
                task_available.wait(lk, [this]() {
                    return count > 0 || stop_workers;
                });

                // Shutdown: if the queue is drained, exit cleanly
                if (stop_workers && count == 0) {
                    safe_print("[Worker " + to_string(worker_id)
                               + "] Shutting down. Goodbye.");
                    return;
                }

                // Grab task from the front of the circular buffer
                task = queue_array[front];
                queue_array[front].is_empty = true;
                front = (front + 1) % max_size;
                count--;

                // Tell enqueue() a slot opened up (backpressure relief)
                slot_available.notify_one();

            }  // ← queue_lock released here — CPU work happens with no lock

            safe_print("[Worker " + to_string(worker_id)
                       + "] Picked up task #" + to_string(task.id)
                       + ": \"" + task.name + "\"");

            // ── Process task (try/catch outside the lock) ─────────────────
            try {
                // Simulate real CPU work (prevent compiler from optimising away)
                volatile int x = 0;
                for (int i = 0; i < 50000000; i++) x++;

                // Every task whose id is divisible by 5 deliberately fails —
                // this mimics a real-world error rate so the DLQ gets exercised.
                if (task.id % 5 == 0) {
                    throw runtime_error("Simulated failure for task #"
                                        + to_string(task.id));
                }

                safe_print("[Worker " + to_string(worker_id)
                           + "] Completed task #" + to_string(task.id)
                           + " ✓");

            } catch (runtime_error& e) {
                safe_print("[Worker " + to_string(worker_id)
                           + "] ERROR on task #" + to_string(task.id)
                           + ": " + e.what());
                save_to_dlq(task, e.what(), worker_id);

            } catch (...) {
                safe_print("[Worker " + to_string(worker_id)
                           + "] UNKNOWN ERROR on task #" + to_string(task.id));
                save_to_dlq(task, "Unknown error", worker_id);
            }

            // Count this task done regardless of success or failure
            {
                lock_guard<mutex> lk(queue_lock);
                done_count++;
            }
        }
    }

public:

    // ─────────────────────────────────────────────────────────────────────────
    // CONSTRUCTOR
    //   Initialises all counters and circular-buffer indices, then spawns
    //   exactly num_workers threads — each runs worker_loop.
    // ─────────────────────────────────────────────────────────────────────────
    TaskQueue(int num_workers, int max_queue_size)
        : front(0), back(0), count(0),
          max_size(max_queue_size),
          dlq_count(0), done_count(0),
          total_submitted(0), stop_workers(false),
          num_workers(num_workers)
    {
        // Mark every slot as empty so stale data can never be dequeued
        for (int i = 0; i < 100; i++) {
            queue_array[i].is_empty = true;
            queue_array[i].id       = 0;
            queue_array[i].name     = "";
        }

        // Spawn worker threads; each owns its ID for log messages
        for (int i = 1; i <= num_workers; i++) {
            workers.push_back(thread(&TaskQueue::worker_loop, this, i));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PUBLIC: enqueue
    //
    //   WHAT IS BACKPRESSURE?
    //     Backpressure slows the producer when consumers cannot keep up.
    //     Without it, submitting 1000 tasks to a queue of size 5 would require
    //     unbounded memory or silently drop tasks. With backpressure, enqueue()
    //     waits until a worker frees a slot — the producer is naturally throttled
    //     to the speed of the consumers. This is the same mechanism used in TCP
    //     flow control, Kafka topic partitions, and async I/O pipelines.
    // ─────────────────────────────────────────────────────────────────────────
    void enqueue(int id, string name) {
        int snapshot_count;

        {
            unique_lock<mutex> lk(queue_lock);

            // BACKPRESSURE: block here if the queue is at capacity
            slot_available.wait(lk, [this]() {
                return count < max_size;
            });

            // Write the new task into the circular buffer
            queue_array[back].id       = id;
            queue_array[back].name     = name;
            queue_array[back].is_empty = false;
            back = (back + 1) % max_size;
            count++;
            total_submitted++;
            snapshot_count = count;   // capture before releasing lock

            // Wake one sleeping worker
            task_available.notify_one();
        }
        // Print after releasing queue_lock to keep the critical section short
        safe_print("[Queue] Task #" + to_string(id)
                   + " enqueued: \"" + name + "\""
                   + "  |  waiting: " + to_string(snapshot_count)
                   + "/" + to_string(max_size));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PUBLIC: wait_until_done
    //   Busy-waits until every submitted task has been processed (success or
    //   failure). In production you'd use a condition_variable here to avoid
    //   burning CPU, but the spec calls for a busy wait for clarity.
    // ─────────────────────────────────────────────────────────────────────────
    void wait_until_done() {
        while (done_count < total_submitted) {}
        safe_print("[Queue] All " + to_string(total_submitted)
                   + " tasks processed.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PUBLIC: shutdown
    //   Sets the stop flag, wakes every worker so they can check it, then
    //   joins all threads.
    //
    //   WHY join()?
    //     join() blocks the caller until a thread finishes. Without it, the
    //     TaskQueue destructor could run while workers are still executing,
    //     destroying the mutexes and condition variables they depend on —
    //     an instant crash or silent memory corruption. join() guarantees
    //     every worker has cleanly exited before we return.
    // ─────────────────────────────────────────────────────────────────────────
    void shutdown() {
        {
            lock_guard<mutex> lk(queue_lock);
            stop_workers = true;
        }
        // Wake ALL sleeping workers so each can see stop_workers == true
        task_available.notify_all();

        for (thread& t : workers) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PUBLIC: print_report
    //   Prints a formatted summary after all tasks have been processed.
    // ─────────────────────────────────────────────────────────────────────────
    void print_report() {
        int completed_ok = total_submitted - dlq_count;

        cout << "\n";
        cout << "  ╔══════════"
                "══════════"
                "══════════"
                "════╗\n";
        cout << "  ║       TASK QUEUE REPORT          ║\n";
        cout << "  ╚══════════"
                "══════════"
                "══════════"
                "════╝\n";

        cout << "  Total submitted : " << total_submitted  << "\n";
        cout << "  Completed OK    : " << completed_ok     << "\n";
        cout << "  Failed (DLQ)    : " << dlq_count        << "\n";
        cout << "  Workers used    : " << num_workers       << "\n";
        cout << "  Queue capacity  : " << max_size          << "\n";

        if (dlq_count > 0) {
            cout << "\n  --- Failed Tasks (Dead Letter Queue) ---\n";
            for (int i = 0; i < dlq_count; i++) {
                cout << "  [" << (i + 1) << "]"
                     << " Task #"   << dlq[i].id
                     << " | \""     << dlq[i].name << "\""
                     << " | Worker " << dlq[i].failed_by_worker
                     << " | Reason: " << dlq[i].error_reason
                     << "\n";
            }
            cout << "  Tip: These tasks can be retried or sent to an alert system.\n";
        }
        cout << "\n";
    }
};

// ═════════════════════════════════════════════════════════════════════════════
// MAIN — Three scenarios showing the system is configurable, not hardcoded
// ═════════════════════════════════════════════════════════════════════════════

int main() {

    // ── Scenario 1: Small team, small queue ──────────────────────────────────
    // 2 workers share a queue that holds at most 5 tasks at once.
    // Backpressure kicks in as soon as 5 tasks are buffered — enqueue()
    // blocks until a worker frees a slot before adding the next task.
    cout << "\n========== Scenario 1: Small Team (2 workers, queue=5) ==========\n";
    {
        TaskQueue q1(2, 5);

        string names[] = {
            "Task A", "Task B", "Task C", "Task D", "Task E",
            "Task F", "Task G", "Task H", "Task I", "Task J"
        };
        for (int i = 0; i < 10; i++) {
            q1.enqueue(i + 1, names[i]);
        }

        q1.wait_until_done();
        q1.shutdown();
        q1.print_report();
    }

    // ── Scenario 2: Larger team, bigger queue ────────────────────────────────
    // 5 workers and a generous buffer of 20 — all 15 frames can queue
    // immediately with no backpressure stalls.
    cout << "\n========== Scenario 2: Larger Team (5 workers, queue=20) ==========\n";
    {
        TaskQueue q2(5, 20);

        for (int i = 1; i <= 15; i++) {
            q2.enqueue(i, "Render frame " + to_string(i));
        }

        q2.wait_until_done();
        q2.shutdown();
        q2.print_report();
    }

    // ── Scenario 3: Gen AI inference queue ───────────────────────────────────
    // 3 GPU workers process NLP jobs. The queue is capped at 8 to mirror
    // a real GPU memory budget — you cannot buffer unlimited inference jobs.
    cout << "\n========== Scenario 3: Gen AI Inference Queue (3 GPU workers, queue=8) ==========\n";
    {
        TaskQueue q3(3, 8);

        string ai_tasks[] = {
            "Summarize document",
            "Translate to Hindi",
            "Generate code snippet",
            "Answer: What is RAG?",
            "Classify email",
            "Extract entities",
            "Sentiment analysis",
            "Generate image caption"
        };

        for (int i = 0; i < 8; i++) {
            q3.enqueue(i + 1, ai_tasks[i]);
        }

        q3.wait_until_done();
        q3.shutdown();
        q3.print_report();
    }

    return 0;
}
