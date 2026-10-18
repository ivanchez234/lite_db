#include "database/database.h"
#include "server/shutdown.h"
#include "server/tcp_server.h"

#include <exception>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
    lite_db::install_shutdown_handlers();

    try {
        const int         port = argc > 1 ? std::stoi(argv[1]) : 5555;
        const std::string host = argc > 2 ? argv[2] : "127.0.0.1";

        Database db("wal.log");
        std::cout << "[System] Loading configuration from setup.yaml..." << std::endl;
        db.load_config("setup.yaml");
        db.recover_from_wal();

        TcpServer server(port, &db, 5, host);
        if (!server.open()) return 1;

        std::cout << "[System] Press Ctrl+C to stop the server" << std::endl;
        server.run();   
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[System] Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
