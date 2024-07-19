#include <iostream>
#include <omp.h>

int main()
{
#pragma omp parallel num_threads(2)
    {
        // Aquí puedes ejecutar el comando para el esclavo, si lo necesitas
        std::string command = "./slave"; // Nombre del programa esclavo
        int result = std::system(command.c_str());

        if (result == 0)
        {
            std::cout << "Esclavo ejecutado " << omp_get_thread_num() << "con éxito." << std::endl;
        }
        else
        {
            std::cerr << "Error al ejecutar el " << omp_get_thread_num() << "esclavo." << std::endl;
        }
    }

    return 0;
}
