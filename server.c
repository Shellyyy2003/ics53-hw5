// server.c
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
    int y, m, d;        // parsed date
    int key;            // y*10000+m*100+d
    double close, open, high, low;
    long long volume;
} PriceRow;

typedef struct {
    char *symbol;       // e.g., "AAPL"
    char *filepath;     // original CSV path
    PriceRow *rows;
    size_t nrows, cap;
} Stock;

typedef struct {
    Stock *list;
    size_t nstocks, cap;
} StockDB;

/* ------------------- small utils ------------------- */
static void die(const char *msg) { perror(msg); exit(1); }

static char *trim_crlf(char *s) {
    size_t n = strlen(s);
    while (n && (s[n-1]=='\n' || s[n-1]=='\r')) s[--n] = 0;
    return s;
}

static int make_key(int y, int m, int d) { return y*10000 + m*100 + d; }

static int cmp_rows(const void *a, const void *b) {
    const PriceRow *ra = (const PriceRow *)a, *rb = (const PriceRow *)b;
    if (ra->key < rb->key) return -1;
    if (ra->key > rb->key) return 1;
    return 0;
}

static int find_row_idx(const Stock *s, int key) {
    // binary search in s->rows (sorted by key)
    size_t lo=0, hi=s->nrows;
    while (lo < hi) {
        size_t mid = (lo+hi)/2;
        int k = s->rows[mid].key;
        if (k == key) return (int)mid;
        if (k < key) lo = mid+1; else hi = mid;
    }
    return -1;
}

/* ------------------- dynamic arrays ------------------- */
static void db_init(StockDB *db){ db->list=NULL; db->nstocks=0; db->cap=0; }
static void db_push(StockDB *db, Stock st){
    if (db->nstocks==db->cap){ db->cap = db->cap? db->cap*2:4; db->list = realloc(db->list, db->cap*sizeof(Stock)); if(!db->list) die("realloc");}
    db->list[db->nstocks++] = st;
}
static void stock_init(Stock *s){
    s->symbol=NULL; s->filepath=NULL; s->rows=NULL; s->nrows=0; s->cap=0;
}
static void stock_push_row(Stock *s, PriceRow r){
    if (s->nrows==s->cap){ s->cap = s->cap? s->cap*2:64; s->rows=realloc(s->rows, s->cap*sizeof(PriceRow)); if(!s->rows) die("realloc rows");}
    s->rows[s->nrows++] = r;
}

/* ------------------- parsing helpers ------------------- */

static bool parse_date(const char *t, int *y, int *m, int *d) {
    // format: MM/DD/YYYY
    int mm=0, dd=0, yy=0;
    if (sscanf(t, "%d/%d/%d", &mm, &dd, &yy) != 3) return false;
    if (mm<1 || mm>12 || dd<1 || dd>31 || yy<1800 || yy>3000) return false;
    *m=mm; *d=dd; *y=yy; return true;
}

static bool parse_money(const char *t, double *out) {
    // input looks like "$209.05" or maybe "209.05"
    char *end;
    if (t[0]=='$') t++;
    errno=0;
    double v = strtod(t, &end);
    if (errno!=0 || end==t) return false;
    *out = v; return true;
}

static bool parse_ll(const char *t, long long *out) {
    char *end; errno=0; long long v = strtoll(t, &end, 10);
    if (errno!=0 || end==t) return false; *out=v; return true;
}

/* ------------------- CSV loader ------------------- */

static char *xstrdup(const char *s){ size_t n=strlen(s)+1; char *p=malloc(n); if(!p) die("malloc"); memcpy(p,s,n); return p; }

static bool load_one_csv(const char *path, Stock *out) {
    stock_init(out);
    out->filepath = xstrdup(path);

    FILE *fp = fopen(path, "r");
    if (!fp) { perror(path); return false; }

    char *line = NULL; size_t cap = 0; ssize_t len;

    // line 1: symbol
    if ((len=getline(&line, &cap, fp)) < 0) { fclose(fp); free(line); return false; }
    trim_crlf(line);
    out->symbol = xstrdup(line);

    // line 2: header (ignored but validate a bit)
    if ((len=getline(&line, &cap, fp)) < 0) { fclose(fp); free(line); return false; }

    // remaining: rows
    while ((len=getline(&line, &cap, fp)) >= 0) {
        trim_crlf(line);
        if (*line == 0) continue;

        // split by commas into 6 fields
        char *fields[6] = {0};
        int cnt = 0;
        char *tok = strtok(line, ",");
        while (tok && cnt < 6) {
            while (*tok == ' ') tok++;
            fields[cnt++] = tok;
            tok = strtok(NULL, ",");
        }

        int y,m,d; double close,open,high,low; long long vol;
        if (!parse_date(fields[0], &y,&m,&d)) continue;
        if (!parse_money(fields[1], &close)) continue;
        if (!parse_ll(fields[2], &vol)) continue;
        if (!parse_money(fields[3], &open)) continue;
        if (!parse_money(fields[4], &high)) continue;
        if (!parse_money(fields[5], &low)) continue;

        PriceRow r = { .y=y,.m=m,.d=d,.key=make_key(y,m,d),
                       .close=close,.open=open,.high=high,.low=low,.volume=vol };
        stock_push_row(out, r);
    }
    free(line);
    fclose(fp);

    // sort rows by date to enable binary search & stable rewrite order
    qsort(out->rows, out->nrows, sizeof(PriceRow), cmp_rows);
    return true;
}

static bool load_csvs(StockDB *db, int filec, char **files) {
    db_init(db);
    for (int i=0;i<filec;i++){
        Stock s;
        if (!load_one_csv(files[i], &s)) {
            fprintf(stderr, "Failed to load %s\n", files[i]);
            return false;
        }
        db_push(db, s);
    }
    return true;
}

/* ------------------- DB find helpers ------------------- */

static int find_stock_idx(const StockDB *db, const char *sym) {
    for (size_t i=0;i<db->nstocks;i++) {
        if (strcmp(db->list[i].symbol, sym)==0) return (int)i;
    }
    return -1;
}

/* ------------------- file rewrite after changePrice ------------------- */

static bool rewrite_csv(const Stock *s) {
    // write from in-memory rows; keep first two lines as spec
    char tmppath[4096];
    snprintf(tmppath, sizeof(tmppath), "%s.tmp", s->filepath);

    FILE *fp = fopen(tmppath, "w");
    if (!fp) { perror("fopen tmp"); return false; }

    // line1: symbol
    fprintf(fp, "%s\n", s->symbol);
    // line2: fixed header
    fprintf(fp, "Date,Close/Last,Volume,Open,High,Low\n");

    for (size_t i=0;i<s->nrows;i++){
        const PriceRow *r = &s->rows[i];
        fprintf(fp, "%02d/%02d/%04d,", r->m, r->d, r->y);
        fprintf(fp, "$%.2f,", r->close);
        fprintf(fp, "%lld,", r->volume);
        fprintf(fp, "$%.2f,$%.2f,$%.2f\n", r->open, r->high, r->low);
    }
    if (fflush(fp)!=0) { fclose(fp); return false; }
    int fd = fileno(fp);
    if (fd!=-1) fsync(fd);
    fclose(fp);

    if (rename(tmppath, s->filepath) != 0) {
        perror("rename");
        unlink(tmppath);
        return false;
    }
    return true;
}

/* ------------------- networking: line-based recv/send ------------------- */

static ssize_t send_all(int fd, const char *buf, size_t n) {
    size_t off=0;
    while (off < n) {
        ssize_t w = send(fd, buf+off, n-off, 0);
        if (w < 0) {
            if (errno==EINTR) continue;
            return -1;
        }
        off += (size_t)w;
    }
    return (ssize_t)off;
}

static int read_line(int fd, char *out, size_t cap) {
    // read until '\n' (discard '\r'); return bytes written (excluding '\n'), or -1 on error, 0 on EOF
    size_t n=0;
    while (1) {
        char c;
        ssize_t r = recv(fd, &c, 1, 0);
        if (r == 0) { // EOF
            if (n==0) return 0;
            out[n]=0; return (int)n;
        }
        if (r < 0) {
            if (errno==EINTR) continue;
            return -1;
        }
        if (c == '\n') { out[n]=0; return (int)n; }
        if (c == '\r') continue;
        if (n+1 < cap) { out[n++] = c; } // else drop extra chars
    }
}

/* ------------------- command handling ------------------- */

static void send_invalid(int fd){ const char *s="Invalid syntax\n"; send_all(fd, s, strlen(s)); }

static void handle_list(int fd, const StockDB *db) {
    // build "AAPL, MSFT, SBUX\n"
    char buf[8192]; buf[0]=0;
    for (size_t i=0;i<db->nstocks;i++){
        if (i) strncat(buf, ", ", sizeof(buf)-strlen(buf)-1);
        strncat(buf, db->list[i].symbol, sizeof(buf)-strlen(buf)-1);
    }
    strncat(buf, "\n", sizeof(buf)-strlen(buf)-1);
    send_all(fd, buf, strlen(buf));
}

static bool parse_after_comma_space(char **p) {
    // accept optional space after comma
    if (**p != ',') return false;
    (*p)++;
    if (**p == ' ') (*p)++;
    return true;
}

static void handle_price(int fd, const StockDB *db, const char *arg) {
    // expect: SYMBOL, MM/DD/YYYY
    // arg starts at first non-space after command name
    // copy to tmp to safely modify
    char tmp[1024]; strncpy(tmp, arg, sizeof(tmp)-1); tmp[sizeof(tmp)-1]=0;
    char *p = tmp;

    // read SYMBOL (until comma or space/comma)
    char *sym = p;
    while (*p && *p!=',' && *p!='\r' && *p!='\n') p++;
    if (*p!=',' && *p!=',') { send_invalid(fd); return; } // guard if weird char
    // split
    if (*p!=',') { /* no-op */ }
    *p = 0; p++;
    if (*p==' ') p++;
    char *date = p;
    if (!*sym || !*date) { send_invalid(fd); return; }

    int y,m,d;
    if (!parse_date(date, &y,&m,&d)) { send_invalid(fd); return; }

    int si = find_stock_idx(db, sym);
    if (si<0) { send_invalid(fd); return; }
    int key = make_key(y,m,d);
    int ri = find_row_idx(&db->list[si], key);
    if (ri<0) { send_invalid(fd); return; }

    char out[128];
    snprintf(out, sizeof(out), "$%.2f\n", db->list[si].rows[ri].close);
    send_all(fd, out, strlen(out));
}

static void handle_change(int fd, StockDB *db, const char *arg) {
    // expect: SYMBOL, MM/DD/YYYY, PRICE
    char tmp[1024]; strncpy(tmp, arg, sizeof(tmp)-1); tmp[sizeof(tmp)-1]=0;
    char *p = tmp;

    // SYMBOL
    char *sym = p;
    while (*p && *p!=',') p++;
    if (*p != ',') { send_invalid(fd); return; }
    *p=0; p++;
    if (*p==' ') p++;
    if (!*sym) { send_invalid(fd); return; }

    // DATE
    char *date = p;
    while (*p && *p!=',') p++;
    if (*p != ',') { send_invalid(fd); return; }
    *p=0; p++;
    if (*p==' ') p++;
    if (!*date) { send_invalid(fd); return; }

    // PRICE
    char *price_s = p;
    if (!*price_s) { send_invalid(fd); return; }
    double newp;
    if (!parse_money(price_s, &newp)) { send_invalid(fd); return; }

    int y,m,d;
    if (!parse_date(date, &y,&m,&d)) { send_invalid(fd); return; }

    int si = find_stock_idx(db, sym);
    if (si<0) { send_invalid(fd); return; }
    Stock *s = &db->list[si];

    int key = make_key(y,m,d);
    int ri = find_row_idx(s, key);
    if (ri<0) { send_invalid(fd); return; }

    // update memory
    s->rows[ri].close = newp;

    // rewrite CSV to persist
    if (!rewrite_csv(s)) { send_invalid(fd); return; }

    // success: send empty line (just "\n")
    const char *ok = "\n";
    send_all(fd, ok, strlen(ok));
}

/* ------------------- main server loop ------------------- */

static int tcp_listen(const char *port) {
    struct addrinfo hints, *res, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    int err = getaddrinfo(NULL, port, &hints, &res);
    if (err != 0) { fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err)); exit(1); }

    int sfd = -1;
    for (rp = res; rp; rp = rp->ai_next) {
        sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sfd == -1) continue;
        int yes=1; setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        if (bind(sfd, rp->ai_addr, rp->ai_addrlen)==0) break;
        close(sfd); sfd=-1;
    }
    freeaddrinfo(res);
    if (sfd==-1) die("bind");
    if (listen(sfd, 1) != 0) die("listen");
    return sfd;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <CSV...> <port>\n", argv[0]);
        return 1;
    }
    const char *port = argv[argc-1];
    int filec = argc - 2;

    StockDB db;
    if (!load_csvs(&db, filec, &argv[1])) {
        fprintf(stderr, "Failed to load CSVs\n");
        return 1;
    }

    int lfd = tcp_listen(port);
    struct sockaddr_storage cli; socklen_t clilen = sizeof(cli);
    int cfd = accept(lfd, (struct sockaddr*)&cli, &clilen);
    if (cfd < 0) die("accept");
    // only accept single client
    close(lfd);

    char line[2048];
    while (1) {
        int n = read_line(cfd, line, sizeof(line));
        if (n < 0) die("recv");
        if (n == 0) break; // client closed
        // print the raw command to server stdout (spec requirement)
        printf("%s\n", line);
        fflush(stdout);

        // dispatch
        if (strncmp(line, "list", 4)==0 && (line[4]==0)) {
            handle_list(cfd, &db);
        } else if (strncmp(line, "price ", 6)==0) {
            handle_price(cfd, &db, line+6);
        } else if (strncmp(line, "changePrice ", 12)==0) {
            handle_change(cfd, &db, line+12);
        } else if (strcmp(line, "quit")==0) {
            // close and exit; no need to send response
            close(cfd);
            return 0;
        } else {
            send_invalid(cfd);
        }
    }

    close(cfd);
    return 0;
}
