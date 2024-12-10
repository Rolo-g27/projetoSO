#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <limits.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

// Processa um único ficheiro `.job`
void process_file(const char *job_file_path, const char *output_file_path) {
    int job_fd = open(job_file_path, O_RDONLY);
    if (job_fd == -1) {
        perror("Erro ao abrir o ficheiro .job");
        return;
    }

    int out_fd = open(output_file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644); // 0644 define permissões padrão
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

                if (num_pairs > 0 && kvs_write(num_pairs, keys, values) == 0) {
                    dprintf(out_fd, "WRITE: OK\n");
                } else {
                    dprintf(out_fd, "WRITE: ERROR\n");
                }
                break;
            }
            case CMD_READ: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_keys = parse_read_delete(job_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

                if (num_keys > 0) {
                    dprintf(out_fd, "READ: [");
                    for (size_t i = 0; i < num_keys; i++) {
                        dprintf(out_fd, "(%s,KVSREAD)", keys[i]);
                    }
                    dprintf(out_fd, "]\n");
                } else {
                    dprintf(out_fd, "READ: ERROR\n");
                }
                break;
            }
            case CMD_DELETE: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_keys = parse_read_delete(job_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

                if (num_keys > 0) {
                    dprintf(out_fd, "DELETE: [");
                    for (size_t i = 0; i < num_keys; i++) {
                        dprintf(out_fd, "(%s,KVSMISSING)", keys[i]);
                    }
                    dprintf(out_fd, "]\n");
                } else {
                    dprintf(out_fd, "DELETE: ERROR\n");
                }
                break;
            }
            case CMD_SHOW:
                dprintf(out_fd, "SHOW: BEGIN\n");
                dprintf(out_fd, "SHOW: END\n");
                break;
            case CMD_WAIT: {
                unsigned int delay;
                if (parse_wait(job_fd, &delay, NULL) == 0) {
                    kvs_wait(delay);
                    dprintf(out_fd, "WAIT: %u ms\n", delay);
                } else {
                    dprintf(out_fd, "WAIT: ERROR\n");
                }
                break;
            }
            case CMD_BACKUP:
                dprintf(out_fd, "BACKUP: NOT IMPLEMENTED\n");
                break;
            case CMD_HELP:
                dprintf(out_fd, "HELP: NOT SUPPORTED\n");
                break;
            case CMD_EMPTY:
                dprintf(out_fd, "EMPTY COMMAND\n");
                break;
            case EOC:
                dprintf(out_fd, "END OF COMMANDS\n");
                break;
            case CMD_INVALID:
                dprintf(out_fd, "INVALID COMMAND\n");
                break;
            default:
                dprintf(out_fd, "UNKNOWN COMMAND\n");
                break;
        }
    }

    close(job_fd);
    close(out_fd);
}

// Processa todos os ficheiros `.job` num diretório
void process_directory(const char *directory_path) {
    DIR *dir = opendir(directory_path);
    if (!dir) {
        perror("Erro ao abrir a diretoria");
        return;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        // Verifica se o nome termina com ".job"
        const char *ext = strrchr(entry->d_name, '.');
        if (!ext || strcmp(ext, ".job") != 0) {
            continue;
        }

        char job_file_path[PATH_MAX];
        snprintf(job_file_path, sizeof(job_file_path), "%s/%s", directory_path, entry->d_name);

        // Gera o nome do ficheiro de saída como `<nome_base>.out`
        char output_file_path[PATH_MAX];
        snprintf(output_file_path, sizeof(output_file_path), "%s/%.*s.out",
                 directory_path,
                 (int)(ext - entry->d_name), // Copia o nome base (sem extensão)
                 entry->d_name);

        printf("Processing job file: %s\n", job_file_path);
        printf("Output file will be: %s\n", output_file_path);

        process_file(job_file_path, output_file_path);
    }

    closedir(dir);
}

int main(int argc, char *argv[]) {
    if (argc != 4) { // Verifica apenas se o diretório foi passado
        fprintf(stderr, "Uso: %s <diretoria_dos_ficheiros_job>\n", argv[0]);
        return EXIT_FAILURE;
    }

    if (kvs_init() != 0) {
        fprintf(stderr, "Falha na inicialização do KVS\n");
        return EXIT_FAILURE;
    }

    process_directory(argv[1]);

    if (kvs_terminate() != 0) {
        fprintf(stderr, "Falha na terminação do KVS\n");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
