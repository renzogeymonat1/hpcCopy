#include <mpi.h>
#include <iostream>
#include <cstdlib>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thread>
#include <array>
#include <unistd.h>
#include <vector>
#include <nlohmann/json.hpp>

// Función para recibir datos a través de un socket
void receive_data(std::string& json_str, int port) {
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0) {
        throw std::runtime_error("No se pudo crear el socket.");
    }

    sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(port);
    server_address.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_sock, (struct sockaddr*)&server_address, sizeof(server_address)) < 0) {
        throw std::runtime_error("Error al hacer bind.");
    }

    if (listen(server_sock, 1) < 0) {
        throw std::runtime_error("Error al escuchar.");
    }

    int client_sock = accept(server_sock, nullptr, nullptr);
    if (client_sock < 0) {
        throw std::runtime_error("Error al aceptar conexión.");
    }

    std::array<char, 1024> buffer;
    std::string received_data;
    ssize_t bytes_received;

    while ((bytes_received = recv(client_sock, buffer.data(), buffer.size(), 0)) > 0) {
        received_data.append(buffer.data(), bytes_received);
    }

    close(client_sock);
    close(server_sock);

    json_str = received_data;
}

// Function to split a JSON array into fragments
std::vector<nlohmann::json> split_dataframe(const nlohmann::json& dataframe, int num_fragments) {
    if (!dataframe.is_array()) {
        throw std::invalid_argument("Dataframe is not an array.");
    }

    int total_rows = dataframe.size();
    int rows_per_fragment = total_rows / num_fragments;
    int remaining_rows = total_rows % num_fragments;

    std::vector<nlohmann::json> fragments;
    int start = 0;
    for (int i = 0; i < num_fragments; ++i) {
        int end = start + rows_per_fragment + (i < remaining_rows ? 1 : 0);
        nlohmann::json fragment = nlohmann::json::array();
        for (int j = start; j < end; ++j) {
            fragment.push_back(dataframe[j]);
        }
        fragments.push_back(fragment);
        start = end;
    }
    return fragments;
}

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    int numProcs;
    int rank;
    MPI_Comm_size(MPI_COMM_WORLD, &numProcs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        if (argc != 2) {
            std::cerr << "Uso: " << argv[0] << " <numSlaves>" << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        std::string preprocessing_data;
        std::thread client_thread(receive_data, std::ref(preprocessing_data), 65432);
        std::this_thread::sleep_for(std::chrono::seconds(1));

        int result = system("python3 code/preprocessing_data.py");
        if (result != 0) {
            std::cerr << "Error al ejecutar el script de Python (preprocessing_data.py)." << std::endl;
            MPI_Finalize();
            return 1;
        }

        int num_fragments = std::stoi(argv[1]);
        client_thread.join();
        auto data_result = nlohmann::json::parse(preprocessing_data);
        std::vector<nlohmann::json> cod_varian_fragments = split_dataframe(data_result["cod_varian"], num_fragments);
        // Medir el tiempo de inicio
        double start_time = MPI_Wtime();

        for (int i = 1; i <= num_fragments; ++i) {
            nlohmann::json fragment;
            fragment["paradas"] = data_result["paradas"];
            fragment["paradas_lineas_direc"] = data_result["paradas_lineas_direc"];
            fragment["cod_varian"] = cod_varian_fragments[i - 1];
            fragment["df_cant_viajes_franja"] = data_result["df_cant_viajes_franja"];

            std::string fragment_str = fragment.dump();
            int fragment_size = fragment_str.size();

            MPI_Send(&i, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(&fragment_size, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
            MPI_Send(fragment_str.c_str(), fragment_size, MPI_CHAR, i, 2, MPI_COMM_WORLD);
        }

        std::cout << "Script de Python (preprocessing_data.py) ejecutado con éxito." << std::endl;

        MPI_Barrier(MPI_COMM_WORLD);

        std::vector<std::string> result_data(num_fragments);

        // Recibir resultados de los procesos esclavos
        for (int i = 1; i <= num_fragments; ++i) {
            int result_size;
            MPI_Recv(&result_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            std::string received_data(result_size, ' ');
            MPI_Recv(&received_data[0], result_size, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            result_data[i - 1] = received_data; // Guardar en el vector
        }

        // Medir el tiempo de finalización
        double end_time = MPI_Wtime();
        double elapsed_time = end_time - start_time;
        std::cout << "Tiempo total de ejecución: " << elapsed_time << " segundos." << std::endl;

        // Mostrar resultados
        std::cout << "Resultados recibidos de los esclavos:" << std::endl;
        // for (int i = 0; i < num_fragments; ++i) {
        //     std::cout << "Esclavo " << i + 1 << ": " << result_data[i] << std::endl;
        // }
    }

    MPI_Finalize();
    return 0;
}
