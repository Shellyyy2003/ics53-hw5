#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#define MAX_ARG 100
#define MAX_STRING 100

int open_clientfd(char *hostname, char *port) {
    int clientfd;
    struct addrinfo hints, *listp, *p;
    
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICSERV;
    hints.ai_flags |= AI_ADDRCONFIG;

    int err = getaddrinfo(hostname, port, &hints, &listp);
    if (err != 0) { fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err)); exit(1); }
    
    for (p = listp; p; p = p->ai_next) {
        if ((clientfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) < 0) continue;
        if (connect(clientfd, p->ai_addr, p->ai_addrlen) != -1) break;
        close(clientfd);
    }

    freeaddrinfo(listp);
    if (!p) return -1;
    else return clientfd;
}

int main(int argc, char **argv) {
    char cmd[MAX_STRING];
    char temp[MAX_STRING];
    char *arg[MAX_ARG];
    int count = 0;
    char buf[MAX_STRING];

    int clientfd = open_clientfd(argv[1], argv[2]);
    if (clientfd == -1) { printf("Failed to open server"); exit(1); }

    while(printf(">")) { // Main Loop
        fgets(cmd, sizeof(cmd), stdin);
        char *cmd_ptr = cmd;
        
        while (sscanf(cmd_ptr, "%s", temp)) { // Parse arguments
            arg[count++] = strdup(temp);
            cmd_ptr = strchr(cmd_ptr, ' ');
            if (cmd_ptr == NULL) break;
            cmd_ptr++;
        }

        if (strcmp(arg[0], "list") == 0 && count == 1) {
            snprintf(buf, MAX_STRING, "%s\n", arg[0]);
            write(clientfd, buf, strlen(buf));
            read(clientfd, buf, MAX_STRING);
            fputs(buf, stdout);
        } else if (strcmp(arg[0], "price") == 0 && count == 3) {
            snprintf(buf, MAX_STRING, "%s %s %s\n", arg[0], arg[1], arg[2]);
            write(clientfd, buf, strlen(buf));
            read(clientfd, buf, MAX_STRING);
            fputs(buf, stdout);
        } else if (strcmp(arg[0], "changePrice") == 0 && count == 4) {
            snprintf(buf, MAX_STRING, "%s %s %s %s\n", arg[0], arg[1], arg[2], arg[3]);
            write(clientfd, buf, strlen(buf));
            read(clientfd, buf, MAX_STRING);
            fputs(buf, stdout);
        } else if (strcmp(arg[0], "quit") == 0 && count == 1) {
            snprintf(buf, MAX_STRING, "%s\n", arg[0]);
            write(clientfd, buf, strlen(buf));
            break;
        } else printf("Invalid syntax\n");

        while (count > 0) {
            free(arg[count - 1]);
            --count;
        }
    }
    close(clientfd);
    return 0;
}