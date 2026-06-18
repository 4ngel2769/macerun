idf_component_register(SRCS "proto_framing.c" "proto_profile.c" "proto_server.c" "proto_container.c"
                    INCLUDE_DIRS "include"
                    REQUIRES config world log nvs_flash)
