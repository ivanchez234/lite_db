#include "shutdown.h"
#include <atomic>

#ifdef _WIN32
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #ifndef NOMINMAX
        #define NOMINMAX
    #endif
    #include <windows.h>
#else
    #include <csignal>
#endif

namespace {

std::atomic<bool> g_shutdown_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free,
              "флаг остановки должен быть lock-free, иначе его нельзя трогать из обработчика сигнала");

#ifdef _WIN32

BOOL WINAPI on_console_event(DWORD event) {
    switch (event) {
        case CTRL_C_EVENT:
        case CTRL_BREAK_EVENT:
            lite_db::request_shutdown();
            return TRUE;

        case CTRL_CLOSE_EVENT:
            
            lite_db::request_shutdown();
            ::Sleep(4500);
            return TRUE;

        default:
            return FALSE;
    }
}

#endif

} // namespace

#ifndef _WIN32
extern "C" void lite_db_on_signal(int) {
    lite_db::request_shutdown();
}
#endif

namespace lite_db {

void install_shutdown_handlers() {
#ifdef _WIN32
    ::SetConsoleCtrlHandler(on_console_event, TRUE);
#else
    struct sigaction stop_action{};
    stop_action.sa_handler = lite_db_on_signal;
    sigemptyset(&stop_action.sa_mask);
    stop_action.sa_flags = 0;
    sigaction(SIGINT,  &stop_action, nullptr);
    sigaction(SIGTERM, &stop_action, nullptr);

    struct sigaction ignore_action{};
    ignore_action.sa_handler = SIG_IGN;
    sigemptyset(&ignore_action.sa_mask);
    sigaction(SIGPIPE, &ignore_action, nullptr);
#endif
}

bool shutdown_requested() noexcept {
    return g_shutdown_requested.load();
}

void request_shutdown() noexcept {
    g_shutdown_requested.store(true);
}

} // namespace lite_db
