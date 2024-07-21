#include <iostream>
#include <string>
#include <stdexcept>
#include <array>
#include <nlohmann/json.hpp>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

// Función para configurar el servidor y recibir datos
void receive_data(std::string& json_str, int slave_num) {
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0) {
        throw std::runtime_error("No se pudo crear el socket.");
    }

    sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(6500 + slave_num);
    server_address.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_sock, (struct sockaddr*)&server_address, sizeof(server_address)) < 0) {
        throw std::runtime_error("Error al hacer bind.");
    }

    if (listen(server_sock, 1) < 0) {
        throw std::runtime_error("Error al escuchar.");
    }

    std::cout << "Esperando conexión..." << std::endl;
    int client_sock = accept(server_sock, nullptr, nullptr);
    if (client_sock < 0) {
        throw std::runtime_error("Error al aceptar conexión.");
    }

    std::array<char, 1024> buffer;
    std::string received_data;
    ssize_t bytes_received;

    // Leer datos hasta que se cierre la conexión
    while ((bytes_received = recv(client_sock, buffer.data(), buffer.size(), 0)) > 0) {
        received_data.append(buffer.data(), bytes_received);
    }

    json_str = received_data;

    close(client_sock);
    close(server_sock);
}

// Función para ejecutar el comando
void execute_command(const std::string& command) {
    int result = system(command.c_str());
    if (result != 0) {
        std::cerr << "Error al ejecutar el script Python." << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Uso: " << argv[0] << " <numSlave>" << std::endl;
        return 1;
    }

    int numSlave = std::stoi(argv[1]);

    // Construir el comando a ejecutar
    std::string command = "python3 code/algoritmo.py " + std::to_string(numSlave);

    // Crear un thread para recibir datos
    std::string json_str;
    std::thread receiver(receive_data, std::ref(json_str), numSlave);

    // Esperar un momento para asegurar que el servidor está escuchando
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Ejecutar el comando en el thread principal
    execute_command(command);

    // Esperar a que el thread de recepción termine
    receiver.join();

    // Procesar el JSON recibido
    try {
        auto json_output = nlohmann::json::parse(json_str);
        std::cout << "Script ejecutado con éxito." << std::endl;
        std::cout << "Contenido del JSON:" << std::endl;
        std::cout << json_output.dump(4) << std::endl;
    } catch (const nlohmann::json::parse_error& e) {
        std::cerr << "Error al analizar el JSON: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
