/**
 * Enocde Sift1M Vectors in base64. Insert and retrieve from mysql.
 * Modeled after fassis code examples.
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

using json = nlohmann::json;
namespace b64 = base64;

// Just values, belong in a public AI header someplace
const uint16_t OP_VECTOR = 151;
const uint16_t CLASS_A_SIFT = 400;
const uint16_t CLASS_B_SIFT_TYPE_TRAIN = 401;
const uint16_t CLASS_B_SIFT_TYPE_BASE = 402; // the general population of sift vectors
const uint16_t CLASS_B_SIFT_TYPE_QUERY = 403;

// Maxium Raw Vector Size for padding
const uint32_t MAX_RAW_VEC_SZ = sizeof(float) * 2 * 1024;  // 2K floats is our maximum

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

void *myDBLocalConnect(unsigned int port, const char * user, const char * pass, const char * db) {
  const char *server = "127.0.0.1";
  void *conn = (void *)mysql_init(NULL);
  if (!mysql_real_connect((MYSQL *)conn, server, user, pass, db, port, NULL, 0)) {
    fprintf(stderr, "Connect to %s on %s:%u failed: %s\n", db, server, port, mysql_error((MYSQL *)conn));
    return NULL;
  }
  return conn;
}

void insertJsonVector(uint16_t op, uint16_t class_a, uint16_t class_b, json &vec);
void insertExtended(char * query, const char * const db_name, const char * const user, const char * const pw);
void insertJsonVectorFlush();

void insertRawVector(uint16_t op, uint16_t class_a, uint16_t class_b, float * rvec, uint16_t dim) {
  namespace b64 = base64;
  char b64_vec[(sizeof(rvec[0]) * dim * 4) / 3  + 60];
  // ensure NULL termination
  size_t b64_vec_chars = sizeof(b64_vec) - 1;
  b64::encode((char const *)rvec, sizeof(rvec[0]) * dim, b64_vec, &b64_vec_chars);
  b64_vec[b64_vec_chars] = 0;
  // printf("b64_vec: %s\n", b64_vec);
  json jvec;
  jvec["v"] = std::string(b64_vec);
  insertJsonVector(op, class_a, class_b, jvec);
}

void insertRawVectorFlush() {
  insertJsonVectorFlush();
}

// not much speed difference between buffer sizes, the 2M buffer is probably a second
// faster using local host
// char vector_buffer[512 * 1024]; // see note below
char vector_buffer[2 * 1024 * 1024]; // see note below
// char vector_buffer[1 * 1024 * 1024]; // see note below
char * vbuf_ptr = vector_buffer;
unsigned short insert_count = 0;
// unsigned short max_extended_insert = 100;
// unsigned short max_extended_insert = 500;
// unsigned short max_extended_insert = 1000; // with vector bufer size of 512K, 1000 SIFT vectors tests the overflow logic
unsigned short max_extended_insert = 2500; // with vector buffer size of 512K, 1000 SIFT vectors tests the overflow logic
                                           // and with vector buffer size of 1024K, which also tests the overflow logic, and
                                           // the newer flush logic

void insertJsonVectorFlush() {
  // TODO: should be constants
  const char db_name [] = { "vector_db" };
  const char user [] = {"vectoruser"};
  const char pw [] = {"vectorpw"};
  if (vbuf_ptr - vector_buffer > 0) {
    insertExtended(vector_buffer, db_name, user, pw);
    vbuf_ptr = vector_buffer;
    vector_buffer[0] = 0;
    insert_count = 0;
  }
}

// Batches up the actual db writes
void insertJsonVector(uint16_t op, uint16_t class_a, uint16_t class_b, json &jvec) {
  size_t len;
  bool overflow;
  char vec_table_name[] = { "vectors" };

  if (vector_buffer == vbuf_ptr) {
    len = snprintf(vbuf_ptr, sizeof(vector_buffer), "INSERT INTO %s (op16, cla16, clb16, jstr60k) VALUES ", vec_table_name);
    vbuf_ptr += len;
    // printf("extended insert of size %ld: %s\n", vbuf_ptr - vector_buffer, vector_buffer);
  }
  size_t prev_len = len;
  size_t cur_cap = sizeof(vector_buffer) - (vbuf_ptr - vector_buffer);
  // printf("cur_cap: %ld\n", cur_cap);

  char value_buf[2 * 1024];
  size_t insert_len = snprintf(vbuf_ptr, cur_cap, "%s(%d, %d, %d, '%s')", insert_count == 0 ? "" : ", ", op, class_a, class_b, jvec.dump().c_str());
  // printf("value buf at %ld: %s\n", vbuf_ptr - vector_buffer, value_buf);
  insert_count++;
  overflow = false;
  if (insert_len >= cur_cap) {
    printf("overflow at %ld with cur_cap: %ld\n", vbuf_ptr - vector_buffer, cur_cap);
    // overflow, put back null and perform the insert
    vector_buffer[sizeof(vector_buffer) - 1] = 0;
    // printf("insert overflow before trunc: %s\n", vbuf_ptr);
    *vbuf_ptr = 0;
    // printf("insert overflow after trunc: %s\n", vbuf_ptr);
    overflow = true;
  }
  if (overflow || insert_count >= max_extended_insert) {
    //  printf("issuing extended insert of size %ld: %s\n", vbuf_ptr - vector_buffer, vector_buffer);
    double t0 = elapsed();
    insertJsonVectorFlush();
    if (overflow) {
      // repeat because the one that we tried to add
      // overflows the available space.
      insertJsonVector(op, class_a, class_b, jvec);
    }
  } else {
    // string stuff together
    // if (insert_count != 1) snprintf()
    vbuf_ptr += insert_len;
  }
}

void insertExtended(char * query, const char * const db_name, const char * const user, const char * const pw) {
  // char db_name [] = { "vector_db" };
  // char user [] = {"vectoruser"};
  // char pw [] = {"vectorpw"};

  void * conn = myDBLocalConnect(3306, user, pw, db_name);
  (void)mysql_query((MYSQL *)conn, query);
  // printf("ExtendedInsert query: %s\n", query);
  mysql_close((MYSQL *)conn);
}

unsigned long retrieveRawVectors(uint16_t cla16, uint16_t clb16) {
  char query_buf [512];
  snprintf(query_buf, sizeof(query_buf), "SELECT jstr60k FROM vectors WHERE op16 = %d AND cla16 = %u AND clb16 = %u",
           (unsigned int)OP_VECTOR, (unsigned int)cla16, (unsigned int)clb16);
    
  char db_name [] = { "vector_db" };
  char user [] = {"vectoruser"};
  char pw [] = {"vectorpw"};

  unsigned long num_rows = 0;
  void * conn = myDBLocalConnect(3306, user, pw, db_name);
  (void)mysql_query((MYSQL *)conn, query_buf);
  MYSQL_RES * result = mysql_store_result((MYSQL *)conn);
  if (result) {
    num_rows = mysql_num_rows(result);
    // printf ("Number of rows: %lu\n", num_rows);
  } else {
    printf("Error: %s\n", mysql_error((MYSQL *)conn));
  }
  int i;
  char * raw_buf_alloc = NULL;
  char * raw_ptr = NULL;
  size_t raw_vec_sz;
  unsigned int first_vec_sz;
  unsigned int raw_buf_alloc_sz;
  for (i=0; i < num_rows; i++) {
    MYSQL_ROW row = mysql_fetch_row(result);
    unsigned int num_fields = mysql_num_fields(result);
    // this shoudl be 1
    // does what you expect row[0] is *the* value
    // printf("row: %u fields: %u row value: %s\n", i, num_fields, row[0]) ;
    json jvec = json::parse(row[0]);
    std::string b64_vec = jvec["v"];
    // printf("row: %d, len: %lu, b64_vec: %s\n", i, b64_vec.size(), b64_vec.c_str());
    if (!raw_buf_alloc) {
      first_vec_sz = b64_vec.size() * 3 / 4;
      raw_buf_alloc_sz = num_rows * first_vec_sz;
      raw_ptr = raw_buf_alloc = (char *)malloc(raw_buf_alloc_sz + MAX_RAW_VEC_SZ);
    }
    if ((raw_buf_alloc + raw_buf_alloc_sz) - raw_ptr < first_vec_sz) {
      // Error out of space
      printf("ERROR: (rba + rbas) - raw_ptr: %lu\n", (raw_buf_alloc + raw_buf_alloc_sz) - raw_ptr);
    }
    else {
      raw_vec_sz = (raw_buf_alloc + raw_buf_alloc_sz) - raw_ptr;
      b64::decode((char const *)b64_vec.c_str(), b64_vec.size(), raw_ptr, &raw_vec_sz);
    }
    if (raw_vec_sz != first_vec_sz) {
      // Error unexpected buffer decode size
      printf("ERROR: decode size: %lu vs expected decode size: %u\n", raw_vec_sz, first_vec_sz);
      break;
    }
    raw_ptr += raw_vec_sz;
  }
  if (num_rows > 0) {
    mysql_free_result(result);
    free(raw_buf_alloc);
  }
  mysql_close((MYSQL *)conn);
  return num_rows;
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

  // tests the base64 encoding/decoding of the SIFT vectors
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
                              "cla16 smallint unsigned default 0, "
                              "clb16 smallint unsigned default 0, "
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

  // Add vectors to the db/table
  double db_insert_start = elapsed();
  int i;
  for (i = 0; i < nq; i++) {
    insertRawVector(OP_VECTOR, CLASS_A_SIFT, CLASS_B_SIFT_TYPE_QUERY, xq + i, d);
  }
  insertRawVectorFlush();
  printf("inserted %ld vectors in %.3fs\n", nq, elapsed() - db_insert_start);
  db_insert_start = elapsed();
  for (i = 0; i < nt; i++) {
    insertRawVector(OP_VECTOR, CLASS_A_SIFT, CLASS_B_SIFT_TYPE_TRAIN, xt + i, d);
  }
  insertRawVectorFlush();
  printf("inserted %ld vectors in %.3fs\n", nt, elapsed() - db_insert_start);
  db_insert_start = elapsed();
  for (i = 0; i < nb; i++) {
    insertRawVector(OP_VECTOR, CLASS_A_SIFT, CLASS_B_SIFT_TYPE_BASE, xb + i, d);
  }
  insertRawVectorFlush();
  printf("inserted %ld vectors in %.3fs\n", nb, elapsed() - db_insert_start);

  unsigned long num_rows;
  double db_retrieve_start = elapsed();
  num_rows = retrieveRawVectors(CLASS_A_SIFT, CLASS_B_SIFT_TYPE_QUERY);
  printf("retrieveded %ld vectors in %.3fs\n", num_rows, elapsed() - db_insert_start);

  db_retrieve_start = elapsed();
  num_rows = retrieveRawVectors(CLASS_A_SIFT, CLASS_B_SIFT_TYPE_TRAIN);
  printf("retrieveded %ld vectors in %.3fs\n", num_rows, elapsed() - db_insert_start);

  db_retrieve_start = elapsed();
  num_rows = retrieveRawVectors(CLASS_A_SIFT, CLASS_B_SIFT_TYPE_BASE);
  printf("retrieveded %ld vectors in %.3fs\n", num_rows, elapsed() - db_insert_start);

  delete [] xq;
  delete [] xb;
  delete [] xt;
  return 0;
}
