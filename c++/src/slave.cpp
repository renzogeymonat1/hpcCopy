#include <iostream>
#include <cstdlib>
#include <string>
#include <omp.h>
#include <fstream>
#include <sstream>
#include <set>


int main(int argc, char* argv[]){

    // Verificar que se haya pasado el argumento numSlaves
    if (argc != 2) {
        std::cerr << "Uso: " << argv[0] << " <numSlave>" << std::endl;
        return 1;
    }

    // Convertir el argumento a un entero
    int numSlave = std::stoi(argv[1]);

    // Ejecutar el script de Python usando la línea de comandos
    std::string pythonPath = "C:\\Python312\\python.exe"; // Ruta del ejecutable de Python
    std::string scriptPath = "C:\\Users\\renzo\\Desktop\\hpc\\code\\algoritmo.py";
    std::string command = pythonPath + " " + scriptPath + " " + std::to_string(numSlave);
    int result = std::system(command.c_str());

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
