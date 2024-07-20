#include <iostream>
#include <omp.h>
#include <fstream>
#include <sstream>
#include <set>
#include <string>


int main(int argc, char* argv[])
{
    // Verificar que se haya pasado el argumento numSlaves
    if (argc != 2) {
        std::cerr << "Uso: " << argv[0] << " <numSlaves>" << std::endl;
        return 1;
    }

    // Convertir el argumento a un entero
    int numSlaves = std::stoi(argv[1]);

    // Ruta al intérprete de Python (por ejemplo, python o python3) y al script de Python
    // Ejecuto procesing data
    const char* pythonInterpreter = "python";
    const char* scriptPath = "c:\\Users\\renzo\\Desktop\\hpc\\code\\preprocessing_data.py";

    // Comando para ejecutar el script de Python
    std::string command = std::string(pythonInterpreter) + " " + scriptPath;

    // Ejecutar el script de Python
    int result = system(command.c_str());

    // Comprobar el resultado de la ejecución
    if (result != 0) {
        std::cerr << "Error al ejecutar el script de Python (preprossesing_data.py)." << std::endl;
        return 1;
    }

    std::cout << "Script de Python (preprossesing_data.py) ejecutado con éxito." << std::endl;

    // Ruta al intérprete de Python y al script de Python
    const char* scriptPath2 = "C:\\Users\\renzo\\Desktop\\hpc\\code\\preprocessing_codvar.py";
    
    // Ruta del archivo CSV
    std::string ruta_csv = "C:\\Users\\renzo\\Desktop\\hpc\\csv\\resProcessingData\\cod_varian.csv";

    // Comando para ejecutar el script de Python con los parámetros
    std::string command2 = std::string(pythonInterpreter) + " " + scriptPath2 + " " + std::to_string(numSlaves) + " " + ruta_csv;

    // Ejecutar el script de Python
    int result2 = std::system(command2.c_str());

    // Comprobar el resultado de la ejecución
    if (result2 != 0) {
        std::cerr << "Error al ejecutar el script de Python (preprocessing_codvar.py)." << std::endl;
        return 1;
    }

    std::cout << "Script de Python (preprocessing_codvar.py) ejecutado con éxito." << std::endl;

    #pragma omp parallel
    {
        #pragma omp for
        for (int i = 0; i < numSlaves; ++i) {
            // Construir el comando para ejecutar el esclavo con el parámetro de la iteración
            std::string command = ".\\slave.exe " + std::to_string(i);

            // Ejecutar el comando
            int result = std::system(command.c_str());

            if (result == 0) {
                #pragma omp critical
                {
                    std::cout << "Esclavo " << i << " ejecutado con éxito." << std::endl;
                }
            } else {
                #pragma omp critical
                {
                    std::cerr << "Error al ejecutar el esclavo " << i << "." << std::endl;
                }
            }
        }
    }

    return 0;

}





