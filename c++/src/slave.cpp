#include <iostream>
#include <cstdlib>

int main()
{
    // Ejecutar el script de Python usando la línea de comandos
    int result = std::system("python3 code/algoritmo.py");

    if (result == 0)
    {
        std::cout << "Script ejecutado con éxito." << std::endl;
    }
    else
    {
        std::cerr << "Error al ejecutar el script." << std::endl;
    }

    return result;
}
