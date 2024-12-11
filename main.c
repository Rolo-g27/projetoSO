#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
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

#define MAX_THREADS 1
sem_t backup_sem;
pthread_mutex_t thread_lock = PTHREAD_MUTEX_INITIALIZER;

void process_file(const char *job_file_path, const char *output_file_path);


void initialize_backup_sem(int max_backups) {
    if (sem_init(&backup_sem, 0, (unsigned int)max_backups) == -1) {
        perror("Erro ao inicializar o semáforo");
        exit(EXIT_FAILURE);
    }
}

void perform_backup(const char *backup_file) {
    int backup_fd = open(backup_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (backup_fd == -1) {
        perror("Erro ao criar o ficheiro de backup");
        exit(EXIT_FAILURE);
    }

    kvs_show(backup_fd);
    close(backup_fd);
    exit(EXIT_SUCCESS);
}

void *thread_process_file(void *arg) {
    char **paths = (char **)arg;
    const char *job_file_path = paths[0];
    const char *output_file_path = paths[1];

    process_file(job_file_path, output_file_path);

    free(paths[0]);
    free(paths[1]);
    free(paths);

    return NULL;
}


void process_file(const char *job_file_path, const char *output_file_path) {
    static int backup_count = 0;

    int job_fd = open(job_file_path, O_RDONLY);
    if (job_fd == -1) {
        perror("Erro ao abrir o ficheiro .job");
        return;
    }

    int out_fd = open(output_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd == -1) {
        perror("Erro ao criar o ficheiro .out");
        close(job_fd);
        return;
    }

    enum Command cmd;
    while ((cmd = get_next(job_fd)) != EOC) {
        switch (cmd) {
            case CMD_WRITE: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_pairs = parse_write(job_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                kvs_write(num_pairs, keys, values);
                break;
            }
            case CMD_READ: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_keys = parse_read_delete(job_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

                if (num_keys > 0) {
                    kvs_read(num_keys, keys, out_fd);
                } else {
                    dprintf(out_fd, "READ: ERROR\n");
                }
                break;
            }

            case CMD_DELETE: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_keys = parse_read_delete(job_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

                if (num_keys > 0) {
                    kvs_delete(num_keys, keys, out_fd);
                } else {
                    dprintf(out_fd, "DELETE: ERROR\n");
                }
                break;
            }
            case CMD_BACKUP: {
                sem_wait(&backup_sem);

                backup_count++;

                // Extrair o nome base sem a extensão ".out"
                char base_name[PATH_MAX];
                snprintf(base_name, sizeof(base_name), "%.*s",
                         (int)(strrchr(output_file_path, '.') - output_file_path),
                         output_file_path);

                // Garantir que o tamanho do backup_file nunca ultrapasse PATH_MAX
                char backup_file[PATH_MAX];
                int len = snprintf(backup_file, sizeof(backup_file), "%s-%d.bck", base_name, backup_count);
                if (len < 0 || len >= (int)sizeof(backup_file)) {
                    fprintf(stderr, "Erro: Nome do arquivo de backup excede o tamanho máximo permitido.\n");
                    sem_post(&backup_sem);
                    break;
                }

                pid_t pid = fork();
                if (pid == -1) {
                    perror("Erro ao criar processo para backup");
                    sem_post(&backup_sem);
                } else if (pid == 0) {
                    perform_backup(backup_file);
                } else {
                    waitpid(pid, NULL, 0);
                    sem_post(&backup_sem);
                }
                break;
            }
            case CMD_SHOW:
                kvs_show(out_fd);
                break;
                
            case CMD_WAIT: {
                unsigned int delay;
                if (parse_wait(job_fd, &delay, NULL) == 0) {
                        printf("Waiting...\n");
                    kvs_wait(delay);
                }
                break;
            }
            case CMD_HELP:
                printf( 
                    "Available commands:\n"
                    "  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n" // Not implemented
                    "  HELP\n"
                );
            
            case CMD_EMPTY:
                break;
            case CMD_INVALID:
                dprintf(out_fd, "INVALID COMMAND\n");
                break;
            case EOC:
                break;
            default:
                break;
        }
    }

    close(job_fd);
    close(out_fd);
}


void process_directory(const char *directory_path, int max_threads) {
    DIR *dir = opendir(directory_path);
    if (!dir) {
        perror("Erro ao abrir a diretoria");
        return;
    }

    pthread_t threads[MAX_THREADS] = {0};
    int active_threads = 0;

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        const char *ext = strrchr(entry->d_name, '.');
        if (!ext || strcmp(ext, ".job") != 0) {
            continue;
        }

        char *job_file_path = malloc(PATH_MAX);
        char *output_file_path = malloc(PATH_MAX);

        if (!job_file_path || !output_file_path) {
            fprintf(stderr, "Erro de alocação de memória\n");
            free(job_file_path);
            free(output_file_path);
            closedir(dir);
            return;
        }

        snprintf(job_file_path, PATH_MAX, "%s%s%s",
                 directory_path,
                 (directory_path[strlen(directory_path) - 1] == '/' ? "" : "/"),
                 entry->d_name);

        snprintf(output_file_path, PATH_MAX, "%s%s%.*s.out",
                 directory_path,
                 (directory_path[strlen(directory_path) - 1] == '/' ? "" : "/"),
                 (int)(ext - entry->d_name),
                 entry->d_name);

        pthread_mutex_lock(&thread_lock);
        while (active_threads >= max_threads) {
            for (int i = 0; i < MAX_THREADS; i++) {
                if (threads[i]) {
                    pthread_join(threads[i], NULL);
                    threads[i] = 0;
                    active_threads--;
                    break;
                }
            }
        }
        pthread_mutex_unlock(&thread_lock);

        char **paths = malloc(2 * sizeof(char *));
        if (!paths) {
            fprintf(stderr, "Erro de alocação de memória\n");
            free(job_file_path);
            free(output_file_path);
            closedir(dir);
            return;
        }

        paths[0] = job_file_path;
        paths[1] = output_file_path;

        int thread_assigned = 0;
        for (int i = 0; i < MAX_THREADS; i++) {
            if (!threads[i]) {
                if (pthread_create(&threads[i], NULL, thread_process_file, paths) != 0) {
                    perror("Erro ao criar thread");
                    free(job_file_path);
                    free(output_file_path);
                    free(paths);
                } else {
                    pthread_mutex_lock(&thread_lock);
                    active_threads++;
                    pthread_mutex_unlock(&thread_lock);
                    thread_assigned = 1;
                }
                break;
            }
        }

        if (!thread_assigned) {
            free(job_file_path);
            free(output_file_path);
            free(paths);
        }
    }

    closedir(dir);

    for (int i = 0; i < MAX_THREADS; i++) {
        if (threads[i]) {
            pthread_join(threads[i], NULL);
        }
    }
}




int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Uso: %s <diretoria_dos_ficheiros_job> <threads> <backups>\n", argv[0]);
        return EXIT_FAILURE;
    }

    int max_threads = atoi(argv[2]);
    int max_backups = atoi(argv[3]);

    if (max_threads <= 0 || max_threads > MAX_THREADS) {
        fprintf(stderr, "Erro: O número de threads deve estar entre 1 e %d\n", MAX_THREADS);
        return EXIT_FAILURE;
    }

    initialize_backup_sem(max_backups);

    if (kvs_init() != 0) {
        fprintf(stderr, "Falha na inicialização do KVS\n");
        return EXIT_FAILURE;
    }

    process_directory(argv[1], max_threads);

    if (kvs_terminate() != 0) {
        fprintf(stderr, "Falha na terminação do KVS\n");
        return EXIT_FAILURE;
    }

    sem_destroy(&backup_sem);
    return EXIT_SUCCESS;
}