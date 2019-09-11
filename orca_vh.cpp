/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <cstring>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <mysql/mysql.h>

#include <sys/time.h>
#include "b64.hpp"
#include "nlohmann/json.hpp"

/**
 * To run this demo, please download the ANN_SIFT1M dataset from
 *
 *   http://corpus-texmex.irisa.fr/
 *
 * and unzip it to the sudirectory sift1M.
 **/

/*****************************************************
 * I/O functions for fvecs and ivecs
 *****************************************************/

float * fvecs_read (const char *fname,
                    size_t *d_out, size_t *n_out)
{
    FILE *f = fopen(fname, "r");
    if(!f) {
        fprintf(stderr, "could not open %s\n", fname);
        perror("");
        abort();
    }
    int d;
    fread(&d, 1, sizeof(int), f);
    assert((d > 0 && d < 1000000) || !"unreasonable dimension");
    fseek(f, 0, SEEK_SET);
    struct stat st;
    fstat(fileno(f), &st);
    size_t sz = st.st_size;
    assert(sz % ((d + 1) * 4) == 0 || !"weird file size");
    size_t n = sz / ((d + 1) * 4);

    *d_out = d; *n_out = n;
    float *x = new float[n * (d + 1)];
    size_t nr = fread(x, sizeof(float), n * (d + 1), f);
    assert(nr == n * (d + 1) || !"could not read whole file");

    // shift array to remove row headers
    for(size_t i = 0; i < n; i++)
        memmove(x + i * d, x + 1 + i * (d + 1), d * sizeof(*x));

    fclose(f);
    return x;
}

// not very clean, but works as long as sizeof(int) == sizeof(float)
int *ivecs_read(const char *fname, size_t *d_out, size_t *n_out)
{
    return (int*)fvecs_read(fname, d_out, n_out);
}

double elapsed ()
{
    struct timeval tv;
    gettimeofday (&tv, nullptr);
    return  tv.tv_sec + tv.tv_usec * 1e-6;
}

void vec_test(const float *const xv, const size_t numElem, const int d)
{
  namespace b64 = base64;

  char b64_vec[16*1024];
  size_t b64_vec_chars = sizeof(b64_vec);
  char raw_vec[24*1024];
  size_t raw_vec_sz = sizeof(raw_vec);

  int i = 0;
  for (; i < numElem; i++) {
    b64::encode((char const *)(xv + i * d), sizeof(xv[0] * d), b64_vec, &b64_vec_chars);
    b64::decode((char const *)b64_vec, b64_vec_chars, raw_vec, &raw_vec_sz);
    if (raw_vec_sz != sizeof(xv[0] * d)) {
      printf ("ERROR! size mismatch in raw vector to baase64 encode to raw decode mismatch at vector %d got %ld, expected %ld\n", i, raw_vec_sz, sizeof(xv[0]) * d);
    }
    else if (memcmp((char const *)(xv + i * d), raw_vec, sizeof(xv[0] * d)) != 0) {
      printf ("ERROR! data mismatch in raw vector to baase64 encode to raw decode at vector %d\n", i);
    }
  }
}

void *myDBLocalConnect(unsigned int port, char *user, char *pass, char *db) {
  const char *server = "127.0.0.1";

  void *conn = (void *)mysql_init(NULL);
  if (!mysql_real_connect((MYSQL *)conn, server, user, pass, db, port, NULL, 0)) {
    fprintf(stderr, "Connect to %s on %s:%u failed: %s\n", db, server, port, mysql_error((MYSQL *)conn));
    return NULL;
  }
  return conn;
}

void
insertRawVector(uint16_t op, float * rvec, uint16_t dim) {
  char b64vec[(sizeof(rvec[0]) * dim * 4) / 3  + 200];
  // ensure NULL termination
  b64vec_chars = sizeof(b64vec) - 1;
  b64::encode((char const *)rvec, sizeof(rvec[0] * dim), b64vec, &b64_vec_chars);
  b64vec[b64_vec_chars] = 0;
  json vec;
  vec['v'] = b64vec;
  insertJsonVector(op, vec);
}

char vector_buffer[512 * 1024];
char * vbuf_ptr = vector_buffer;
unsigned short insert_count = 0;
unsigned short max_extended_insert = 100;

insertJsonVector(uint16_t op, json &vec) {
{
  size_t len;
  if (vector_buffer == vbuf_ptr) {
    len = snprintf(vbuf_ptr, "INSERT INTO vectors (op16, jstr60k) VALUES ");
    vbuf_ptr += len;
    printf("extended insert of size %d: %s\n", vbuf_ptr - vector_buffer, vector_buffer);
  }
  size_t prev_len = len;
  size_t cur_cap = sizeof(vector_buffer) - (vbuf_ptr - vector_buffer);

  char value_buf[2 * 1024];
  size_t insert_len = snprintf(inser_value, sizeof(value_buf), "(%d, '%s')", op, vec.dump());
  insert_count++ ;
  if (cur_cap < insert_len || insert_count > max_extended_insert) {
    printf("issuing extended insert of size %d: %s\n", vbuf_ptr - vector_buffer, vector_buffer);
    insertExtended(query);
    // reset and start over
    vbuf_ptr = vector_buffer;
    insert_count = 0;
    insertJsonVector(op, vec);
  }
}

void
insertExtended(char * query) {
    void conn = myDBLocalConnect(3306, user, pw, db_name);
    (void)mysql_query((MYSQL *)conn, query);
    mysql_close((MYSQL *)conn);
}

int main()
{
    double t0 = elapsed();

    size_t d;
    size_t nt;
    float *xt;
    {
        printf ("[%.3f s] Loading train set\n", elapsed() - t0);

        xt = fvecs_read("sift1M/sift_learn.fvecs", &d, &nt);

        printf ("[%.3f s] Done Loading train set d=%ld N=%ld\n",
                elapsed() - t0, d, nt);
    }

    size_t nb;
    float *xb;
    {
        printf ("[%.3f s] Loading database\n", elapsed() - t0);

        size_t d2;
        xb = fvecs_read("sift1M/sift_base.fvecs", &d2, &nb);
        assert(d == d2 || !"dataset does not have same dimension as train set");
        printf ("[%.3f s] Indexing database, d=%ld N=%ld\n",
                elapsed() - t0, d2, nb);

    }

    size_t nq;
    float *xq;
    {
        printf ("[%.3f s] Loading queries\n", elapsed() - t0);

        size_t d2;
        xq = fvecs_read("sift1M/sift_query.fvecs", &d2, &nq);
        assert(d == d2 || !"query does not have same dimension as train set");
        printf ("[%.3f s] Queries database, d=%ld N=%ld\n",
		elapsed() - t0, d2, nq);
    }

    vec_test(xq, nq, d);
    vec_test(xb, nb, d);    
    vec_test(xt, nt, d);
    
    char user [] = { "vectoruser" };
    char pw [] = { "vectorpw" };
    char server [] = { "127.0.0.1" };
    void *conn = myDBLocalConnect(3306, user, pw, NULL);

    char db_name [] = { "vector_db" };
    char vec_table_name [] = { "vectors" };
    char vec_table_schema[] = { "(op16 smallint unsigned default 0, "
				"jstr60k varchar(61440) default '{}')" };
    char query[1600];
    snprintf(query, sizeof(query), "CREATE DATABASE IF NOT EXISTS %s", db_name);
    (void)mysql_query((MYSQL *)conn, query);
    mysql_close((MYSQL *)conn);

    conn = myDBLocalConnect(3306, user, pw, db_name);
    // conn = (void *)mysql_init(NULL);
    // (void)mysql_real_connect((MYSQL *)conn, server, user, pw, db_name, 3306, NULL, 0);
    // snprintf(query, sizeof(query), "CREATE TABLE IF NOT EXISTS %s", vec_table_name);
    snprintf(query, sizeof(query), "CREATE TABLE IF NOT EXISTS %s %s", vec_table_name, vec_table_schema);
    (void)mysql_query((MYSQL *)conn, query);
    mysql_close((MYSQL *)conn);

    // Add vectors to the table

    delete [] xq;
    delete [] xb;
    delete [] xt;
    return 0;
}
