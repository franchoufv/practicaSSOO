#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main() {
    srand(time(NULL)); // Inicializa la semilla del generador de números aleatorios
    int i;
    for (i = 1; i <= 50; i++) {
        char filename[20];
        sprintf(filename, "archivo_%02d.csv", i);
        FILE *file = fopen(filename, "w");
        if (file == NULL) {
            printf("Error al crear el archivo %s\n", filename);
            exit(1);
        }
        // Escribe contenido en el archivo CSV
        fprintf(file, "IdOperacion;FECHA_INICIO;FECHA_FIN;IdUsuario;IdTipoOperación;Importe;Saldo;Estado\n");
        int j;
        for (j = 1; j <= 10; j++) { // Genera 10 líneas de datos aleatorios
            time_t t = time(NULL);
            struct tm *tm = localtime(&t);
            tm->tm_year = 124; // Ajusta el año a 2024
            tm->tm_mon = rand() % 12; // Mes aleatorio
            tm->tm_mday = rand() % 28 + 1; // Día aleatorio
            tm->tm_hour = rand() % 24; // Hora aleatoria
            tm->tm_min = rand() % 60; // Minutos aleatorios
            char fecha_inicio[20], fecha_fin[20];
            strftime(fecha_inicio, sizeof(fecha_inicio), "%d/%m/%Y %H:%M", tm);

            int minutos_extra = rand() % 60 + 1; // Genera un número aleatorio de minutos para agregar
            tm->tm_min += minutos_extra; // Ajusta los minutos para la fecha de fin
            mktime(tm); // Normaliza la fecha en caso de que los minutos hayan pasado de 60
            strftime(fecha_fin, sizeof(fecha_fin), "%d/%m/%Y %H:%M", tm);

            fprintf(file, "OPE%03d;%s;%s;USER%03d;COMPRA%02d;%d;%d.0€;%s\n",
                    j, fecha_inicio, fecha_fin, rand() % 100, rand() % 4, rand() % 5, rand() % 1001 - 500, rand() % 2 ? "Correcto" : "Error");
        }
        fclose(file);
    }
    printf("Se han creado 50 archivos CSV con datos aleatorios.\n");
    return 0;
}
