#include <iostream>
#include <string>
#include <stdexcept>
#include <array>
#include <nlohmann/json.hpp>
#include <thread>
#include <mpi.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

// Función para enviar datos a través de un socket
void send_data(int client_sock, const std::string& data) {
    size_t total_sent = 0;
    size_t data_length = data.size();
    while (total_sent < data_length) {
        ssize_t sent = send(client_sock, data.data() + total_sent, data_length - total_sent, 0);
        if (sent < 0) {
            throw std::runtime_error("Error al enviar datos.");
        }
        total_sent += sent;
    }
}

// Función para manejar al cliente y recibir datos por sockets
void handle_client(std::string& json_str, int slave_num, const std::string& data) {
    std::string data_send = data + "END_OF_DATA";
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

    std::cout << "Esperando conexión en el hilo del cliente..." << std::endl;
    int client_sock = accept(server_sock, nullptr, nullptr);
    if (client_sock < 0) {
        throw std::runtime_error("Error al aceptar conexión.");
    }

    // Enviar los dataframes al cliente
    send_data(client_sock, data_send);

    // Leer los datos recibidos del cliente
    std::array<char, 1024> buffer;
    std::string received_data;
    ssize_t bytes_received;

    while (true) {
        bytes_received = recv(client_sock, buffer.data(), buffer.size(), 0);
        if (bytes_received > 0) {
            received_data.append(buffer.data(), bytes_received);
        } else if (bytes_received == 0) {
            // El cliente ha cerrado la conexión
            std::cout << "Cliente ha cerrado la conexión." << std::endl;
            break;
        } else {
            // Error en la recepción
            std::cerr << "Error al recibir datos." << std::endl;
            break;
        }
    }

    close(client_sock);
    close(server_sock);

    // Procesar el JSON recibido
    try {
        json_str = received_data;
        auto json_output = nlohmann::json::parse(received_data);
        std::cout << "Script ejecutado con éxito." << std::endl;
        std::cout << "Contenido del JSON:" << std::endl;
        //std::cout << json_output.dump(4) << std::endl;
    } catch (const nlohmann::json::parse_error& e) {
        std::cerr << "Error al analizar el JSON: " << e.what() << std::endl;
        throw;
    }
}

// Función para ejecutar el comando
void execute_command(const std::string& command) {
    int result = system(command.c_str());
    if (result != 0) {
        std::cerr << "Error al ejecutar el script Python." << std::endl;
    }
}

int main(int argc, char* argv[]) {
    // Inicializar MPI
    MPI_Init(&argc, &argv);
    int world_rank;
    int world_size;

    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // Imprimir el valor de world_rank
    std::cout << "Soy el proceso con rank " << world_rank << " de " << world_size << " procesos." << std::endl;

    if (world_rank != 0) {
        int numSlave;
        MPI_Status status;

        MPI_Recv(&numSlave, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);

        int fragment_size;
        MPI_Recv(&fragment_size, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, &status);

        std::string data(fragment_size, ' ');
        MPI_Recv(&data[0], fragment_size, MPI_CHAR, 0, 2, MPI_COMM_WORLD, &status);

        // Crear un thread para manejar al cliente
        std::string json_str;
        std::thread client_thread(handle_client, std::ref(json_str), numSlave, data);

        // Esperar un momento para asegurar que el servidor está escuchando
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Ejecutar el comando en el thread principal
        std::string command = "python3 code/algoritmo.py " + std::to_string(numSlave);
        execute_command(command);

        // Esperar a que el thread del cliente termine
        client_thread.join();
        MPI_Barrier(MPI_COMM_WORLD);
        // Enviar el JSON al proceso maestro
        int json_size = json_str.size();
        MPI_Send(&json_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(json_str.c_str(), json_str.size(), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }
    
    MPI_Finalize();
    return 0;
}