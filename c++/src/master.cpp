#include <mpi.h>
#include <iostream>
#include <cstdlib>
#include <string>

int main(int argc, char* argv[])
{
    MPI_Init(&argc, &argv);

    int numProcs, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &numProcs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int numSlaves = 0;

    // El proceso maestro (rank 0) lee el argumento de la línea de comandos
    if (rank == 0) {
        if (argc != 2) {
            std::cerr << "Uso: " << argv[0] << " <numSlaves>" << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        try {
            numSlaves = std::stoi(argv[1]);
        } catch (const std::invalid_argument& e) {
            std::cerr << "Argumento inválido: " << argv[1] << " no es un número válido." << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    // Difundir el número de esclavos a todos los procesos
    MPI_Bcast(&numSlaves, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        // Ejecutar el script de Python solo en el proceso maestro
        int result = system("python code/preprocessing_data.py");
        if (result != 0) {
            std::cerr << "Error al ejecutar el script de Python (preprocessing_data.py)." << std::endl;
            MPI_Finalize();
            return 1;
        }

        std::cout << "Script de Python (preprocessing_data.py) ejecutado con éxito." << std::endl;

        std::string command2 = "python3 code/preprocessing_codvar.py " + std::to_string(numSlaves) + " " + "csv/resProcessingData/cod_varian.csv";
        int result2 = std::system(command2.c_str());
        if (result2 != 0) {
            std::cerr << "Error al ejecutar el script de Python (preprocessing_codvar.py)." << std::endl;
            MPI_Finalize();
            return 1;
        }

        std::cout << "Script de Python (preprocessing_codvar.py) ejecutado con éxito." << std::endl;
    }

    // Asegurar que todos los procesos esperen a que el maestro termine con la inicialización
    MPI_Barrier(MPI_COMM_WORLD);

    // Cada proceso esclavo ejecuta su tarea
    if (rank < numSlaves) {
        std::string command = "./slave " + std::to_string(rank);
        int result = std::system(command.c_str());

        if (result == 0) {
            std::cout << "Esclavo " << rank << " ejecutado con éxito." << std::endl;
        } else {
            std::cerr << "Error al ejecutar el esclavo " << rank << "." << std::endl;
        }
    }

    MPI_Finalize();
    return 0;
}
