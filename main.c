#include <dirent.h>
#include <fcntl.h>
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

sem_t backup_sem;

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
                    kvs_read(num_keys, keys, out_fd); // Passa o descritor do ficheiro de saída
                } else {
                    dprintf(out_fd, "READ: ERROR\n");
                }
                break;
            }

            case CMD_DELETE: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_keys = parse_read_delete(job_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

                if (num_keys > 0) {
                    kvs_delete(num_keys, keys, out_fd); // Passa o descritor do ficheiro de saída
                } else {
                    dprintf(out_fd, "DELETE: ERROR\n");
                }
                break;
            }
            case CMD_BACKUP: {
                sem_wait(&backup_sem);

                backup_count++;
                char backup_file[PATH_MAX];
                snprintf(backup_file, sizeof(backup_file), "%s-%d.bck", output_file_path, backup_count);

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
                    kvs_wait(delay);
                }
                break;
            }
            case CMD_HELP:
            case CMD_EMPTY:
                break;
            case CMD_INVALID:
                dprintf(out_fd, "INVALID COMMAND\n");
                break;
            case EOC:
        // Marca o fim dos comandos. Nenhuma ação necessária.
            break;
            default:
                break;
        }
    }

    close(job_fd);
    close(out_fd);
}

void process_directory(const char *directory_path) {
    DIR *dir = opendir(directory_path);
    if (!dir) {
        perror("Erro ao abrir a diretoria");
        return;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        const char *ext = strrchr(entry->d_name, '.');
        if (!ext || strcmp(ext, ".job") != 0) {
            continue;
        }

        char job_file_path[PATH_MAX];
        snprintf(job_file_path, sizeof(job_file_path), "%s/%s", directory_path, entry->d_name);

        char output_file_path[PATH_MAX];
        snprintf(output_file_path, sizeof(output_file_path), "%s/%.*s.out",
                 directory_path,
                 (int)(ext - entry->d_name),
                 entry->d_name);

        process_file(job_file_path, output_file_path);
    }

    closedir(dir);
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Uso: %s <diretoria_dos_ficheiros_job> <threads> <backups>\n", argv[0]);
        return EXIT_FAILURE;
    }

    int max_backups = atoi(argv[3]);
    initialize_backup_sem(max_backups);

    if (kvs_init() != 0) {
        fprintf(stderr, "Falha na inicialização do KVS\n");
        return EXIT_FAILURE;
    }

    process_directory(argv[1]);

    if (kvs_terminate() != 0) {
        fprintf(stderr, "Falha na terminação do KVS\n");
        return EXIT_FAILURE;
    }

    sem_destroy(&backup_sem);
    return EXIT_SUCCESS;
}
