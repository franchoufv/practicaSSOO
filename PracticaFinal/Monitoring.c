#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>

#define MAX_TRANSACCIONES 10000
#define LIMITE_TRANSACCIONES 5
#define LIMITE_RETIROS 3

typedef struct {
    char IdOperacion[10];
    char IdUsuario[50];
    char FECHA_INICIO[20];
    char FECHA_FIN[20];
    char IdTipoOperacion[25];
    float Importe;  // Cambiado de int cantidad a float Importe
    float Saldo;
    char Estado[25];
} Transaccion;

Transaccion transacciones[MAX_TRANSACCIONES];
int num_transacciones = 0;
pthread_mutex_t mutex; // Mutex para controlar el acceso a transacciones[]

void leer_transacciones(const char* archivo) {
    FILE* file = fopen(archivo, "r");
    if (file == NULL) {
        perror("Error al abrir el archivo");
        exit(EXIT_FAILURE);
    }

    char buffer[1024];
    while (fgets(buffer, sizeof(buffer), file)) {
        pthread_mutex_lock(&mutex); // Proteger la escritura en transacciones[]
        if (sscanf(buffer, "%[^;];%[^;];%[^;];%[^;];%[^;];%f;%f€;%[^;\n]",
               transacciones[num_transacciones].IdOperacion,
               transacciones[num_transacciones].FECHA_INICIO,
               transacciones[num_transacciones].FECHA_FIN,
               transacciones[num_transacciones].IdUsuario,
               transacciones[num_transacciones].IdTipoOperacion,
               &transacciones[num_transacciones].Importe,
               &transacciones[num_transacciones].Saldo,
               transacciones[num_transacciones].Estado) == 8) {
            num_transacciones++;
            printf("Transacción %d cargada correctamente.\n", num_transacciones);
        } else {
            printf("Error al parsear la línea: %s\n", buffer);
        }
        pthread_mutex_unlock(&mutex); // Liberar el mutex después de actualizar transacciones[]
    }
    fclose(file);
    printf("Total de transacciones cargadas: %d\n", num_transacciones);
}

// Extrae la fecha en formato "dia/mes/año"
void extraer_fecha_dmy(const char* fechaHora, char* fecha) {
    sscanf(fechaHora, "%[^ ]", fecha); // Extrae hasta el primer espacio (fecha sin hora)
}

// Extrae la fecha y la hora (sin minutos) en formato "dia/mes/año hora"
void extraer_fecha_hora(const char* fechaHora, char* fecha) {
    sscanf(fechaHora, "%[^:]", fecha); // Extrae hasta el primer dos puntos (incluye la hora)
}

// Función auxiliar para verificar si una operación es de retiro (saldo negativo)
int esRetiro(float saldo) {
    return saldo < 0;
}

void* patron1() {
    pthread_mutex_lock(&mutex);
    printf("Procesando %d transacciones en el patrón 1.\n", num_transacciones);
    for (int i = 0; i < num_transacciones; i++) {
        int contador = 1; // Contador inicia en 1 para contar la transacción actual
        char fecha_hora_i[20];
        extraer_fecha_hora(transacciones[i].FECHA_INICIO, fecha_hora_i);
        for (int j = i + 1; j < num_transacciones; j++) {
            char fecha_hora_j[20];
            extraer_fecha_hora(transacciones[j].FECHA_INICIO, fecha_hora_j);
            if (strcmp(transacciones[i].IdUsuario, transacciones[j].IdUsuario) == 0 &&
                strcmp(fecha_hora_i, fecha_hora_j) == 0) {
                contador++;
            }
        }
        if (contador > LIMITE_TRANSACCIONES) {
            printf("%s ha dado positivo en el patron 1.\n", transacciones[i].IdUsuario);
        }
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}

void* patron2(void* arg) {
    pthread_mutex_lock(&mutex);
    printf("Procesando %d transacciones en el patrón 2.\n", num_transacciones);
    for (int i = 0; i < num_transacciones; i++) {
        int contador_retiros = esRetiro(transacciones[i].Saldo) ? 1 : 0;
        char fecha_i[20];
        extraer_fecha_dmy(transacciones[i].FECHA_INICIO, fecha_i);
        for (int j = i + 1; j < num_transacciones; j++) {
            char fecha_j[20];
            extraer_fecha_dmy(transacciones[j].FECHA_INICIO, fecha_j);
            if (strcmp(transacciones[i].IdUsuario, transacciones[j].IdUsuario) == 0 &&
                strcmp(fecha_i, fecha_j) == 0 && esRetiro(transacciones[j].Saldo)) {
                contador_retiros++;
            }
        }
        if (contador_retiros > LIMITE_RETIROS) {
            printf("%s ha dado positivo en el patron 2.\n", transacciones[i].IdUsuario);
        }
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}

void* patron3(void* arg) {
    pthread_mutex_lock(&mutex);
    printf("Procesando %d transacciones en el patrón 3.\n", num_transacciones);
    for (int i = 0; i < num_transacciones; i++) {
        int contador_errores = strcmp(transacciones[i].Estado, "Error") == 0 ? 1 : 0;
        char fecha_i[20];
        extraer_fecha_dmy(transacciones[i].FECHA_INICIO, fecha_i);
        for (int j = i + 1; j < num_transacciones; j++) {
            char fecha_j[20];
            extraer_fecha_dmy(transacciones[j].FECHA_INICIO, fecha_j);
            if (strcmp(transacciones[i].IdUsuario, transacciones[j].IdUsuario) == 0 &&
                strcmp(fecha_i, fecha_j) == 0 && strcmp(transacciones[j].Estado, "Error") == 0) {
                contador_errores++;
            }
        }
        if (contador_errores > 3) {
            printf("%s ha dado positivo en el patron 3.\n", transacciones[i].IdUsuario);
        }
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}

void* patron4(void* arg) {
    pthread_mutex_lock(&mutex);
    printf("Procesando %d transacciones en el patrón 4.\n", num_transacciones);
    for (int i = 0; i < num_transacciones; i++) {
        int compra01 = 0, compra02 = 0, compra03 = 0, compra04 = 0;
        char fecha_i[20];
        extraer_fecha_dmy(transacciones[i].FECHA_INICIO, fecha_i);
        for (int j = 0; j < num_transacciones; j++) {
            char fecha_j[20];
            extraer_fecha_dmy(transacciones[j].FECHA_INICIO, fecha_j);
            if (strcmp(transacciones[i].IdUsuario, transacciones[j].IdUsuario) == 0 &&
                strcmp(fecha_i, fecha_j) == 0) {
                if (strcmp(transacciones[j].IdTipoOperacion, "COMPRA00") == 0) compra01 = 1;
                if (strcmp(transacciones[j].IdTipoOperacion, "COMPRA01") == 0) compra02 = 1;
                if (strcmp(transacciones[j].IdTipoOperacion, "COMPRA02") == 0) compra03 = 1;
                if (strcmp(transacciones[j].IdTipoOperacion, "COMPRA03") == 0) compra04 = 1;
            }
        }
        if (compra01 && compra02 && compra03 && compra04) {
            printf("%s ha dado positivo en el patron 4.\n", transacciones[i].IdUsuario);
        }
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}

void* patron5(void* arg) {
    pthread_mutex_lock(&mutex);
    printf("Procesando %d transacciones en el patrón 5.\n", num_transacciones);
    for (int i = 0; i < num_transacciones; i++) {
        float total_retirado = 0, total_ingresado = 0;
        char fecha_i[20];
        extraer_fecha_dmy(transacciones[i].FECHA_INICIO, fecha_i);
        for (int j = 0; j < num_transacciones; j++) {
            char fecha_j[20];
            extraer_fecha_dmy(transacciones[j].FECHA_INICIO, fecha_j);
            if (strcmp(transacciones[i].IdUsuario, transacciones[j].IdUsuario) == 0 &&
                strcmp(fecha_i, fecha_j) == 0) {
                if (esRetiro(transacciones[j].Saldo)) total_retirado += transacciones[j].Saldo;
                else total_ingresado += transacciones[j].Saldo;
            }
        }
        if (total_retirado < total_ingresado) {
            printf("%s ha dado positivo en el patron 5 por tener más retiros que ingresos en un día.\n", transacciones[i].IdUsuario);
        }
    }
    pthread_mutex_unlock(&mutex);
    return NULL;
}

int main() {
    pthread_t thread1, thread2, thread3, thread4, thread5;
    pthread_mutex_init(&mutex, NULL); // Inicializar el mutex

    leer_transacciones("consolidado.csv");

     //Crear hilos para los patrones
    if (pthread_create(&thread1, NULL, patron1, NULL) != 0 ||
        pthread_create(&thread2, NULL, patron2, NULL) != 0 ||
        pthread_create(&thread3, NULL, patron3, NULL) != 0 ||
        pthread_create(&thread4, NULL, patron4, NULL) != 0 ||
        pthread_create(&thread5, NULL, patron5, NULL) != 0) {
        perror("pthread_create");
        exit(EXIT_FAILURE);
    }
    //     Esperar a que los hilos terminen
    if (pthread_join(thread1, NULL) != 0 ||
        pthread_join(thread2, NULL) != 0 ||
        pthread_join(thread3, NULL) != 0 ||
        pthread_join(thread4, NULL) != 0 ||
        pthread_join(thread5, NULL) != 0) {
        perror("pthread_join");
        exit(EXIT_FAILURE);
    }

    pthread_mutex_destroy(&mutex); // Destruir el mutex al finalizar

    return 0;
}
