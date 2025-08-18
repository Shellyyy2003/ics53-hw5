#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <ctype.h>

static int send_all(int fd, const char *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = send(fd, buf + off, len - off, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

static int read_line(int fd, char *out, size_t cap) {
    size_t n = 0;
    while (1) {
        char c;
        ssize_t r = recv(fd, &c, 1, 0);
        if (r == 0) { // EOF
            if (n == 0) return 0;
            out[n] = 0; return (int)n;
        }
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (c == '\n') { out[n] = 0; return (int)n; }
        if (c == '\r') continue;
        if (n + 1 < cap) out[n++] = c; // truncate if too long
    }
}

static void rstrip(char *s) {
    size_t n = strlen(s);
    while (n && (s[n-1] == '\n' || s[n-1] == '\r' || isspace((unsigned char)s[n-1]))) s[--n] = 0;
}

static void lskip_spaces(const char **p) {
    while (**p == ' ') (*p)++;
}

// parse "MM/DD/YYYY"
static int parse_date(const char *s) {
    int m=0,d=0,y=0;
    if (sscanf(s, "%d/%d/%d", &m, &d, &y) != 3) return 0;
    if (m<1 || m>12 || d<1 || d>31 || y<1800 || y>3000) return 0;
    return 1;
}

// syntax check exactly per spec; if ok, return 1; else 0
static int syntax_ok(const char *line) {
    if (strcmp(line, "list") == 0) return 1;
    if (strcmp(line, "quit") == 0) return 1;

    if (strncmp(line, "price ", 6) == 0) {
        const char *p = line + 6;
        // SYMBOL, DATE
        // find comma
        const char *comma = strchr(p, ',');
        if (!comma) return 0;
        // symbol must be non-empty and no spaces at ends
        if (comma == p) return 0;
        // after comma optional space
        const char *q = comma + 1;
        if (*q == ' ') q++;
        if (*q == 0) return 0;
        // q should be date
        if (!parse_date(q)) return 0;
        return 1;
    }

    if (strncmp(line, "changePrice ", 12) == 0) {
        const char *p = line + 12;
        // SYMBOL, DATE, PRICE
        const char *c1 = strchr(p, ',');
        if (!c1 || c1 == p) return 0;
        const char *q = c1 + 1;
        if (*q == ' ') q++;
        const char *c2 = strchr(q, ',');
        if (!c2 || c2 == q) return 0;
        // q..c2-1 is date
        char date[64]; size_t dn = (size_t)(c2 - q);
        if (dn >= sizeof(date)) return 0;
        memcpy(date, q, dn); date[dn] = 0;
        if (!parse_date(date)) return 0;
        const char *r = c2 + 1;
        if (*r == ' ') r++;
        if (*r == 0) return 0;
        // price is a number
        char *end; errno = 0; (void)strtod(r, &end);
        if (errno != 0 || end == r) return 0;
        return 1;
    }
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <host> <port>\n", argv[0]);
        return 1;
    }
    const char *host = argv[1], *port = argv[2];

    // connect
    struct addrinfo hints, *res, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_STREAM;
    int err = getaddrinfo(host, port, &hints, &res);
    if (err != 0) { fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err)); return 1; }

    int cfd = -1;
    for (rp = res; rp; rp = rp->ai_next) {
        cfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (cfd == -1) continue;
        if (connect(cfd, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(cfd); cfd = -1;
    }
    freeaddrinfo(res);
    if (cfd < 0) { perror("connect"); return 1; }

    char line[2048];
    while (1) {
        printf("> ");
        fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) break;
        rstrip(line);
        if (line[0] == 0) continue; // ignore empty

        if (!syntax_ok(line)) {
            printf("Invalid syntax\n");
            continue; // do not send to server
        }

        if (strcmp(line, "quit") == 0) {
            char msg[16];
            snprintf(msg, sizeof(msg), "quit\n");
            (void)send_all(cfd, msg, strlen(msg));
            break;
        }

        // send with newline
        size_t L = strlen(line);
        char *msg = malloc(L + 2);
        if (!msg) { perror("malloc"); break; }
        memcpy(msg, line, L);
        msg[L] = '\n'; msg[L+1] = 0;
        if (send_all(cfd, msg, L + 1) < 0) { perror("send"); free(msg); break; }
        free(msg);

        // read one response line
        char resp[4096];
        int r = read_line(cfd, resp, sizeof(resp));
        if (r < 0) { printf("Server closed\n"); break; }

        // changePrice success => empty line => print nothing
        if (resp[0] != 0) printf("%s\n", resp);
    }

    close(cfd);
    return 0;
}
