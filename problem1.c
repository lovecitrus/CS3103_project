/* Student starter for Problem 1. Complete the TODOs before submission. */
/* Build on Linux: gcc -std=gnu11 -Wall -Wextra -pthread problem1.c -o problem1 */
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <semaphore.h>

/* Edit these macros and recompile to change the configuration. */
#define BUFFER_A_SIZE 5
#define BUFFER_B_SIZE 5
#define NUM_PRODUCERS 3
#define NUM_WORKERS 3
#define NUM_CONSUMERS 2
#define ITEMS_PER_PRODUCER 4

typedef struct {
    int id;
    int producer_id;
    int value;
} item_t;

typedef struct {
    int id;
    int producer_id;
    int worker_id;
    long result;
} result_t;

typedef struct {
    item_t data[BUFFER_A_SIZE];
    int in;
    int out;
    sem_t mutex;
    sem_t empty;
    sem_t full;
} input_buffer_t;

typedef struct {
    result_t data[BUFFER_B_SIZE];
    int in;
    int out;
    sem_t mutex;
    sem_t empty;
    sem_t full;
} output_buffer_t;

typedef struct {

    input_buffer_t input;
    output_buffer_t output;
    /* TODO: add shared result records and termination state as needed.
       You may extend these structures and add synchronization. */
    /* Counters start at zero in the anonymous mapping. The last producer
       sends one stop item per worker; the last worker sends one per consumer. */
    int producers_finished; /* protected by input.mutex */
    int workers_finished;   /* protected by output.mutex */

    /* Consumers update these under output.mutex while removing a result. */
    result_t results[NUM_PRODUCERS * ITEMS_PER_PRODUCER];
    int occurrences[NUM_PRODUCERS * ITEMS_PER_PRODUCER];
    int out_of_range_count;
} shared_t;

/* Generate this producer's input items and insert them into Buffer A. */
static void producer(shared_t *sh, int producer_id)
{
    /* TODO: for j = 0 .. ITEMS_PER_PRODUCER-1, generate an item with
       id = producer_id * ITEMS_PER_PRODUCER + j, producer_id as supplied,
       and value = id + 1. Insert it into sh->input with synchronization.
       Generate the item outside the buffer's critical section.
       Use BUFFER_A_SIZE for circular-buffer wraparound.
       Complete this producer's part of your termination protocol. */

    int is_last = 0;
    for (int j = 0; j < ITEMS_PER_PRODUCER; ++j) {
        item_t item;
        item.id = producer_id * ITEMS_PER_PRODUCER + j;
        item.producer_id = producer_id;
        item.value = item.id + 1; // Generate the item outside the critical section

        sem_wait(&sh->input.empty);
        sem_wait(&sh->input.mutex);
        sh->input.data[sh->input.in] = item;
        sh->input.in = (sh->input.in + 1) % BUFFER_A_SIZE;
        if (j == ITEMS_PER_PRODUCER - 1) {
            sh->producers_finished++;
            is_last = (sh->producers_finished == NUM_PRODUCERS);
        }
        sem_post(&sh->input.mutex);
        sem_post(&sh->input.full);
    }

    if (is_last) {
        const item_t stop = { .id = -1 };

        for (int i = 0; i < NUM_WORKERS; ++i) {
            sem_wait(&sh->input.empty);
            sem_wait(&sh->input.mutex);
            sh->input.data[sh->input.in] = stop;
            sh->input.in = (sh->input.in + 1) % BUFFER_A_SIZE;
            sem_post(&sh->input.mutex);
            sem_post(&sh->input.full);
        }
    }
    fprintf(stderr, "Producer %d: TODO\n", producer_id);
}


/* Transfer input items through the computation stage into Buffer B. */
static void worker(shared_t *sh, int worker_id)
{
    /* TODO: remove an item from sh->input and release its lock.
       Compute (long)item.value * item.value outside the critical section.
       Preserve id and producer_id, and record this worker_id.
       Insert the result into sh->output using BUFFER_B_SIZE for wraparound.
       Follow your termination protocol; an empty buffer is not completion. */
    const int stop_id = -1;

    for (;;) {
        sem_wait(&sh->input.full);
        sem_wait(&sh->input.mutex);
        item_t item = sh->input.data[sh->input.out];
        sh->input.out = (sh->input.out + 1) % BUFFER_A_SIZE;
        sem_post(&sh->input.mutex);
        sem_post(&sh->input.empty);

        if (item.id == stop_id) {
            break;
        }

        result_t result = {
            .id = item.id,
            .producer_id = item.producer_id,
            .worker_id = worker_id,
            .result = (long)item.value * item.value
        };

        sem_wait(&sh->output.empty);
        sem_wait(&sh->output.mutex);
        sh->output.data[sh->output.in] = result;
        sh->output.in = (sh->output.in + 1) % BUFFER_B_SIZE;
        sem_post(&sh->output.mutex);
        sem_post(&sh->output.full);
    }

    sem_wait(&sh->output.mutex);
    sh->workers_finished++;
    int is_last = (sh->workers_finished == NUM_WORKERS);
    sem_post(&sh->output.mutex);

    if (is_last) {
        const result_t stop = { .id = stop_id };
        for (int i = 0; i < NUM_CONSUMERS; ++i) {
            sem_wait(&sh->output.empty);
            sem_wait(&sh->output.mutex);
            sh->output.data[sh->output.in] = stop;
            sh->output.in = (sh->output.in + 1) % BUFFER_B_SIZE;
            sem_post(&sh->output.mutex);
            sem_post(&sh->output.full);
        }
    }
    fprintf(stderr, "Worker %d: TODO\n", worker_id);
}

/* Collect Buffer B results into synchronized shared records. */
static void consumer(shared_t *sh, int consumer_id)
{
    /* TODO: remove results from sh->output and save them in shared memory.
       Synchronize shared updates and retain enough information for the
       parent to detect missing, duplicate, or out-of-range item IDs.
       Exclude termination records and follow your termination protocol.
       The parent prints the final results in ascending item ID order. */
    const int stop_id = -1;
    const int total_items = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
    result_t result;

    do {
        sem_wait(&sh->output.full);
        sem_wait(&sh->output.mutex);
        result = sh->output.data[sh->output.out];
        sh->output.out = (sh->output.out + 1) % BUFFER_B_SIZE;

        if (result.id >= 0 && result.id < total_items) {
            sh->results[result.id] = result;
            sh->occurrences[result.id]++;
        } else if (result.id != stop_id) {
            sh->out_of_range_count++;
        }

        sem_post(&sh->output.mutex);
        sem_post(&sh->output.empty);
    } while (result.id != stop_id);

    fprintf(stderr, "Consumer %d: TODO\n", consumer_id);
}

/* Initialize the shared buffer indices and process-shared semaphores. */
static int init_shared(shared_t *sh)
{
    /* TODO: initialize any added shared records, synchronization, and
       termination state here, before any child process is created. */
    memset(sh, 0, sizeof(*sh));

    if (sem_init(&sh->input.mutex, 1, 1) == -1 ||
        sem_init(&sh->input.empty, 1, BUFFER_A_SIZE) == -1 ||
        sem_init(&sh->input.full, 1, 0) == -1 ||
        sem_init(&sh->output.mutex, 1, 1) == -1 ||
        sem_init(&sh->output.empty, 1, BUFFER_B_SIZE) == -1 ||
        sem_init(&sh->output.full, 1, 0) == -1) {
        perror("sem_init");
        return -1;
    }
    return 0;
}

/* Wait for a child (any child if pid is -1), retrying interrupted waits. */
static pid_t wait_child(pid_t pid, int *status)
{
    pid_t child;
    do {
        child = waitpid(pid, status, 0);
    } while (child == -1 && errno == EINTR);
    return child;
}

/* Check the collected IDs, fields and counts; return 1 if all are correct. */
static int verify_results(const shared_t *sh)
{
    const int total_items = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
    long collected = sh->out_of_range_count;
    int valid = 1;

    if (sh->out_of_range_count != 0) {
        fprintf(stderr, "Out-of-range results: %d\n", sh->out_of_range_count);
        valid = 0;
    }
    for (int id = 0; id < total_items; ++id) {
        int occurrences = sh->occurrences[id];
        collected += occurrences;
        if (occurrences != 1) {
            fprintf(stderr, "ID %d occurred %d times (expected 1).\n", id, occurrences);
            valid = 0;
            continue;
        }

        result_t row = sh->results[id];
        long value = (long)id + 1;
        if (row.id != id || row.producer_id != id / ITEMS_PER_PRODUCER ||
            row.worker_id < 0 || row.worker_id >= NUM_WORKERS ||
            row.result != value * value) {
            fprintf(stderr, "Incorrect result for ID %d.\n", id);
            valid = 0;
        }
    }
    if (collected != total_items) {
        fprintf(stderr, "Collected %ld results (expected %d).\n", collected, total_items);
        valid = 0;
    }
    return valid;
}

/* Create shared resources, launch all roles, verify results, and clean up. */
int main(void)
{
    shared_t *sh = mmap(NULL, sizeof(*sh), PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (sh == MAP_FAILED) {
        perror("mmap");
        return EXIT_FAILURE;
    }

    if (init_shared(sh) != 0) {
        munmap(sh, sizeof(*sh));
        return EXIT_FAILURE;
    }

    const int total_items = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
    pid_t children[NUM_PRODUCERS + NUM_WORKERS + NUM_CONSUMERS];
    int created = 0;
    int children_ok = 0;

    for (int i = 0; i < NUM_PRODUCERS; ++i) {
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); goto reap_children; }
        if (pid == 0) { producer(sh, i); _exit(EXIT_SUCCESS); }
        children[created++] = pid;
    }

    for (int i = 0; i < NUM_WORKERS; ++i) {
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); goto reap_children; }
        if (pid == 0) { worker(sh, i); _exit(EXIT_SUCCESS); }
        children[created++] = pid;
    }

    for (int i = 0; i < NUM_CONSUMERS; ++i) {
        pid_t pid = fork();
        if (pid < 0) { perror("fork"); goto reap_children; }
        if (pid == 0) { consumer(sh, i); _exit(EXIT_SUCCESS); }
        children[created++] = pid;
    }
    children_ok = 1;

    /* All process groups must be started before waiting for children.
       TODO: add any parent-side actions required by your termination
       protocol at the appropriate point; do not wait for a process that
       still needs a termination action from the parent. */
    /* The last producer and the last worker send the stop records. */
    /* TODO: collect and check each child's exit status, rather than
       discarding it with wait(NULL). */
reap_children:
    while (children_ok && created > 0) {
        int status;
        pid_t child = wait_child(-1, &status);
        if (child == -1) {
            if (errno == ECHILD)
                created = 0;
            perror("waitpid");
            children_ok = 0;
            break;
        }
        for (int i = 0; i < created; ++i) {
            if (children[i] == child) {
                children[i] = children[--created];
                break;
            }
        }
        if (!WIFEXITED(status) || WEXITSTATUS(status) != EXIT_SUCCESS) {
            fprintf(stderr, "Child %ld failed.\n", (long)child);
            children_ok = 0;
        }
    }
    if (!children_ok) {
        /* A missing stage can leave siblings blocked on a semaphore. */
        for (int i = 0; i < created; ++i)
            kill(children[i], SIGKILL);
        for (int i = 0; i < created; ++i)
            wait_child(children[i], NULL);
    }

    /* TODO: before unmapping shared memory, verify the collected results:
       - every ID from 0 to NUM_PRODUCERS * ITEMS_PER_PRODUCER - 1 occurs once;
       - producer_id and result match the generated item;
       - worker_id is within 0 .. NUM_WORKERS-1;
       - the total is correct, with no out-of-range IDs.
       Print the results in ascending item ID order.
       Set verification_ok only after verification and child-status checks
       have succeeded. This placeholder is not a completed verifier. */
    int verification_ok = children_ok && verify_results(sh);
    if (verification_ok) {
        printf("id  producer_id  worker_id  result\n");
        for (int id = 0; id < total_items; ++id) {
            result_t row = sh->results[id];
            printf("%-3d %-12d %-10d %ld\n", row.id, row.producer_id, row.worker_id, row.result);
        }
    }

    /* TODO: destroy any additional semaphores you initialize. */
    sem_destroy(&sh->input.mutex);
    sem_destroy(&sh->input.empty);
    sem_destroy(&sh->input.full);
    sem_destroy(&sh->output.mutex);
    sem_destroy(&sh->output.empty);
    sem_destroy(&sh->output.full);
    munmap(sh, sizeof(*sh));

    if (!verification_ok) {
        fprintf(stderr, "Pipeline verification failed.\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
