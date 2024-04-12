#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <dirent.h>
#include <pthread.h>

#define MAX_LINE_LENGTH 256

char directorio[MAX_LINE_LENGTH] = "";
int NUM_PROCESOS;
pthread_mutex_t mutex;
static int headerWritten = 0; // Variable global para controlar la escritura de la cabecera

typedef struct {
    char **files;
    int count;
} ThreadData;

void leerArchivo() {
    FILE *file, *logFile;
    char line[MAX_LINE_LENGTH];
    char *key, *value;
    char archivo[MAX_LINE_LENGTH] = "";
    char ficherolog[MAX_LINE_LENGTH] = "ficherolog.txt"; // Asignación directa si el archivo siempre es el mismo
    char SIMULATE_SLEEP[MAX_LINE_LENGTH] = "";

    logFile = fopen(ficherolog, "a"); // Abre el archivo de log para añadir mensajes

    // Abre el fichero de properties
    file = fopen("config.txt", "r");
    if (!file) {
        perror("No se pudo abrir el archivo");
        fprintf(logFile, "Error: No se pudo abrir el archivo 'config.txt'\n");
        fclose(logFile);
        return;
    }
    // Lee el fichero línea por línea
    while (fgets(line, MAX_LINE_LENGTH, file)) {
        // Elimina el salto de línea al final, si existe
        line[strcspn(line, "\n")] = 0;

        // Divide la línea en clave y valor
        key = strtok(line, "=");
        value = strtok(NULL, "=");

        // Comprueba si la clave y el valor son válidos y guarda los datos
        if (key && value) {
            if (strcmp(key, "directorio") == 0) {
                strncpy(directorio, value, MAX_LINE_LENGTH);
            } else if (strcmp(key, "archivo") == 0) {
                strncpy(archivo, value, MAX_LINE_LENGTH);
            } else if(strcmp(key, "ficherolog") == 0) {
                strncpy(ficherolog, value, MAX_LINE_LENGTH);
            } else if(strcmp(key, "NUM_PROCESOS") == 0) {
                NUM_PROCESOS = atoi(value); // Convert NUM_PROCESOS to integer
            } else if(strcmp(key, "SIMULATE_SLEEP") == 0) {
                strncpy(SIMULATE_SLEEP, value, MAX_LINE_LENGTH);
            } 
        }
    }

    // Cierra el fichero
    fclose(file);
    // Imprime y guarda las variables para verificar
    printf("Directorio: %s\n", directorio);
    fprintf(logFile, "Directorio: %s\n", directorio);
    printf("Archivo: %s\n", archivo);
    fprintf(logFile, "Archivo: %s\n", archivo);
    printf("Fichero de Log: %s\n", ficherolog);
    fprintf(logFile, "Fichero de Log: %s\n", ficherolog);
    printf("Numero de procesos: %d\n", NUM_PROCESOS);
    fprintf(logFile, "Numero de procesos: %d\n", NUM_PROCESOS);
    printf("SLEEP: %s\n", SIMULATE_SLEEP);
    fprintf(logFile, "SLEEP: %s\n", SIMULATE_SLEEP);

    fclose(logFile); // Cierra el archivo de log
}

void* leerficheros(void* arg) {
    ThreadData *data = (ThreadData *)arg;
    FILE *consolidatedFile = fopen("consolidado.csv", "a");
    FILE *logFile = fopen("ficherolog.txt", "a"); // Abre el archivo de log para añadir mensajes

    for (int i = 0; i < data->count; i++) {
        char filePath[MAX_LINE_LENGTH];
        snprintf(filePath, sizeof(filePath), "%s/%s", directorio, data->files[i]);
        FILE* csvFile = fopen(filePath, "r");
        if (csvFile) {
            char line[MAX_LINE_LENGTH];
            int isFirstLine = 1; // Variable para controlar la primera línea (cabecera) de cada archivo
            while (fgets(line, sizeof(line), csvFile)) {
                if (isFirstLine) {
                    pthread_mutex_lock(&mutex);
                    if (!headerWritten) {
                        fputs(line, consolidatedFile);
                        fprintf(logFile, "%s", line); // Escribe la cabecera en el log
                        headerWritten = 1; // Se marca que la cabecera ya fue escrita
                    }
                    pthread_mutex_unlock(&mutex);
                    isFirstLine = 0; // Se marca para no volver a entrar en este bloque para el archivo actual
                    continue; // Salta el resto del ciclo para la primera línea después de escribirla (si es necesario)
                }
                pthread_mutex_lock(&mutex);
                fputs(line, consolidatedFile);
                fprintf(logFile, "%s", line); // Escribe cada línea en el log
                pthread_mutex_unlock(&mutex);
            }
            fclose(csvFile);
        } else {
            perror("Error abriendo archivo CSV");
            fprintf(logFile, "Error: No se pudo abrir el archivo CSV '%s'\n", filePath);
        }
    }
    fclose(consolidatedFile);
    fclose(logFile); // Cierra el archivo de log
    return NULL;
}

int main() {
    pthread_mutex_init(&mutex, NULL);
    leerArchivo();

    DIR *dir = opendir(directorio);
    struct dirent *entry;
    char *files[50]; // Asumiendo que hay hasta 50 archivos CSV, ajustar según sea necesario
    int totalFiles = 0;

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type == DT_REG && strstr(entry->d_name, ".csv")) {
            files[totalFiles++] = strdup(entry->d_name);
        }
    }
    closedir(dir);

    int filesPerThread = totalFiles / NUM_PROCESOS;
    int remainingFiles = totalFiles % NUM_PROCESOS;
    pthread_t threads[NUM_PROCESOS];
    ThreadData data[NUM_PROCESOS];

    for (int i = 0, j = 0; i < NUM_PROCESOS; i++) {
        data[i].files = &files[j];
        data[i].count = (i < remainingFiles) ? filesPerThread + 1 : filesPerThread;
        j += data[i].count;
        pthread_create(&threads[i], NULL, leerficheros, (void *)&data[i]);
    }

    for (int i = 0; i < NUM_PROCESOS; i++) {
        pthread_join(threads[i], NULL);
    }

    pthread_mutex_destroy(&mutex);
    for (int i = 0; i < totalFiles; i++) {
        free(files[i]); // Libera la memoria de los nombres de los archivos
    }

    return 0;
}
