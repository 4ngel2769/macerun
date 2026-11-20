idf_component_register(SRCS "net_server.c" "net_errors.c"
                    INCLUDE_DIRS "include"
                    REQUIRES proto config lwip)
