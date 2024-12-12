#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <limits.h>
#include "constants.h"
#include "parser.h"
#include "operations.h"

// Mutex e variáveis de condição para a fila de backups
pthread_mutex_t thread_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t backup_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t backup_cond = PTHREAD_COND_INITIALIZER;

// Estrutura para guardar pedidos de backup
typedef struct {
    char backup_file[PATH_MAX];
} backup_task_t;

static backup_task_t *backup_queue = NULL;
static int backup_queue_size = 0;
static int backup_queue_start = 0, backup_queue_end = 0, backup_queue_count = 0;
static int program_terminating = 0;
static pthread_t backup_thread;

// Insere um pedido de backup na fila
static void enqueue_backup(const char *file) {
    pthread_mutex_lock(&backup_mutex);
    while (backup_queue_count == backup_queue_size && !program_terminating)
        pthread_cond_wait(&backup_cond, &backup_mutex);
    if (!program_terminating) {
        strncpy(backup_queue[backup_queue_end].backup_file, file, PATH_MAX);
        backup_queue_end = (backup_queue_end + 1) % backup_queue_size;
        backup_queue_count++;
        pthread_cond_signal(&backup_cond);
    }
    pthread_mutex_unlock(&backup_mutex);
}

// Retira um pedido de backup da fila
static int dequeue_backup(char *out) {
    pthread_mutex_lock(&backup_mutex);
    while (backup_queue_count == 0 && !program_terminating)
        pthread_cond_wait(&backup_cond, &backup_mutex);
    if (program_terminating && backup_queue_count == 0) {
        pthread_mutex_unlock(&backup_mutex);
        return 0;
    }
    strncpy(out, backup_queue[backup_queue_start].backup_file, PATH_MAX);
    backup_queue_start = (backup_queue_start + 1) % backup_queue_size;
    backup_queue_count--;
    pthread_cond_signal(&backup_cond);
    pthread_mutex_unlock(&backup_mutex);
    return 1;
}

// Thread dedicada ao processamento de backups
static void *backup_thread_func(void *arg) {
    (void)arg;
    char f[PATH_MAX];
    while (dequeue_backup(f)) {
        int fd = open(f, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd != -1) {
            kvs_show(fd);
            close(fd);
        } else
            perror("Erro backup");
    }
    return NULL;
}

// Processa um único ficheiro .job
static void process_file(const char *job_file_path, const char *output_file_path) {
    static int backup_count = 0;
    int job_fd = open(job_file_path, O_RDONLY);
    if (job_fd == -1) {
        perror("Erro job");
        return;
    }
    int out_fd = open(output_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd == -1) {
        perror("Erro out");
        close(job_fd);
        return;
    }

    enum Command cmd;
    while ((cmd = get_next(job_fd)) != EOC) {
        switch (cmd) {
        case CMD_WRITE: {
            char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0}, values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
            size_t n = parse_write(job_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
            kvs_write(n, keys, values);
            break;
        }
        case CMD_READ: {
            char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
            size_t n = parse_read_delete(job_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
            n > 0 ? kvs_read(n, keys, out_fd) : dprintf(out_fd, "READ: ERROR\n");
            break;
        }
        case CMD_DELETE: {
            char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
            size_t n = parse_read_delete(job_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
            n > 0 ? kvs_delete(n, keys, out_fd) : dprintf(out_fd, "DELETE: ERROR\n");
            break;
        }
        case CMD_BACKUP: {
            backup_count++;
            const char *dot = strrchr(output_file_path, '.');
            int base_len = dot ? (int)(dot - output_file_path) : (int)strlen(output_file_path);
            char base_name[PATH_MAX];
            snprintf(base_name, sizeof(base_name), "%.*s", base_len, output_file_path);
            char backup_file[PATH_MAX];
            if (snprintf(backup_file, sizeof(backup_file), "%s-%d.bck", base_name, backup_count) < (int)sizeof(backup_file))
                enqueue_backup(backup_file);
            break;
        }
        case CMD_SHOW:
            kvs_show(out_fd);
            break;
        case CMD_WAIT: {
            unsigned int d;
            if (parse_wait(job_fd, &d, NULL) == 0)
                kvs_wait(d);
            break;
        }
        case CMD_HELP:
            // Mantém as mensagens iguais ao código base do professor
            printf( 
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n"
                "  HELP\n"
            );
            break;
        case CMD_EMPTY:
            // Não faz nada
            break;
        case CMD_INVALID:
            dprintf(out_fd, "INVALID COMMAND\n");
            break;
        case EOC:
            break;
        }
    }
    close(job_fd);
    close(out_fd);
}

// Cria uma thread para cada ficheiro .job, respeitando o limite max_threads
static void *thread_process_file(void *arg) {
    char **paths = (char **)arg;
    process_file(paths[0], paths[1]);
    free(paths[0]);
    free(paths[1]);
    free(paths);
    return NULL;
}

// Processa a diretoria, encontrando ficheiros .job e criando threads para cada um
static void process_directory(const char *dir_path, int max_threads) {
    DIR *dir = opendir(dir_path);
    if (!dir) {
        perror("Erro diretoria");
        return;
    }
    pthread_t *threads = malloc(sizeof(pthread_t) * (size_t)max_threads);
    if (!threads) {
        perror("Erro malloc threads");
        closedir(dir);
        return;
    }
    for (int i = 0; i < max_threads; i++)
        threads[i] = 0;
    int active = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        const char *ext = strrchr(entry->d_name, '.');
        if (!ext || strcmp(ext, ".job") != 0)
            continue;

        char *j = malloc(PATH_MAX), *o = malloc(PATH_MAX);
        if (!j || !o) {
            free(j);
            free(o);
            perror("Erro malloc");
            break;
        }
        snprintf(j, PATH_MAX, "%s%s%s", dir_path, (dir_path[strlen(dir_path) - 1] == '/' ? "" : "/"), entry->d_name);
        snprintf(o, PATH_MAX, "%s%s%.*s.out", dir_path, (dir_path[strlen(dir_path) - 1] == '/' ? "" : "/"), (int)(ext - entry->d_name), entry->d_name);

        pthread_mutex_lock(&thread_lock);
        // Espera até haver uma vaga de thread
        while (active >= max_threads) {
            for (int i = 0; i < max_threads; i++) {
                if (threads[i]) {
                    pthread_join(threads[i], NULL);
                    threads[i] = 0;
                    active--;
                    break;
                }
            }
        }
        pthread_mutex_unlock(&thread_lock);

        char **paths = malloc(2 * sizeof(char *));
        if (!paths) {
            free(j);
            free(o);
            perror("Erro malloc paths");
            break;
        }
        paths[0] = j;
        paths[1] = o;

        int assigned = 0;
        for (int i = 0; i < max_threads; i++) {
            if (!threads[i]) {
                if (pthread_create(&threads[i], NULL, thread_process_file, paths) != 0) {
                    perror("Erro thread");
                    free(j);
                    free(o);
                    free(paths);
                } else {
                    pthread_mutex_lock(&thread_lock);
                    active++;
                    pthread_mutex_unlock(&thread_lock);
                    assigned = 1;
                    break;
                }
            }
        }
        if (!assigned) {
            // Se não foi possível criar thread, libertar memória
            free(j);
            free(o);
            free(paths);
        }
    }
    closedir(dir);
    // Espera que todas as threads terminem
    for (int i = 0; i < max_threads; i++)
        if (threads[i])
            pthread_join(threads[i], NULL);
    free(threads);
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Uso: %s <dir> <max_backups> <max_threads>\n", argv[0]);
        return EXIT_FAILURE;
    }
    int max_backups = atoi(argv[2]), max_threads = atoi(argv[3]);
    if (max_backups <= 0 || max_threads <= 0) {
        fprintf(stderr, "Valores invalidos.\n");
        return EXIT_FAILURE;
    }
    // Inicializa a KVS
    if (kvs_init() != 0) {
        fprintf(stderr, "Falha kvs_init\n");
        return EXIT_FAILURE;
    }

    // Inicializa fila de backups e a thread de backup
    backup_queue_size = max_backups;
    backup_queue = malloc(sizeof(backup_task_t) * (size_t)backup_queue_size);
    if (!backup_queue) {
        perror("Erro fila backup");
        kvs_terminate();
        return EXIT_FAILURE;
    }
    if (pthread_create(&backup_thread, NULL, backup_thread_func, NULL) != 0) {
        perror("Erro thread backup");
        free(backup_queue);
        kvs_terminate();
        return EXIT_FAILURE;
    }

    // Processa os ficheiros .job na diretoria
    process_directory(argv[1], max_threads);

    // Sinaliza o término do programa à thread de backup
    pthread_mutex_lock(&backup_mutex);  
    program_terminating = 1;
    pthread_cond_broadcast(&backup_cond);
    pthread_mutex_unlock(&backup_mutex);

    // Espera a thread de backup terminar
    pthread_join(backup_thread, NULL);
    free(backup_queue);

    // Termina a KVS
    if (kvs_terminate() != 0) {
        fprintf(stderr, "Falha kvs_terminate\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
