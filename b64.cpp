#include <iostream>
#include <math.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>

#include "base64.hpp"

int
main(int argc, char **argv)
{
  using namespace std;
  namespace b64 = base64;

  const uint8_t block[] = { 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff };

  char small_block[10];
  char medium_block[20];
  char big_block[2048];

  size_t out_size = sizeof(small_block) - 1;
  b64::encode((char const *)block, sizeof(block), small_block, &out_size);
  small_block[out_size] = 0;
  cout << "small block is " << small_block << endl;

  out_size = sizeof(medium_block) - 1;
  b64::encode((char const *)block, sizeof(block), medium_block, &out_size);
  medium_block[out_size] = 0;
  cout << "medium block is " << medium_block << endl;

  out_size = sizeof(big_block) - 1;
  b64::encode((char const *)block, sizeof(block), big_block, &out_size);
  big_block[out_size] = 0;
  cout << "big block is " << big_block << endl;

  char out_buf[1024];
  size_t out_buf_sz = sizeof(out_buf);
  b64::decode(big_block, out_size, (void *)out_buf, &out_buf_sz);
  if (memcmp(block, out_buf, out_buf_sz) != 0) {
    cout << "ERROR: out_buf and block are not equal" << endl;
  }

  const unsigned int n_trials = 100;
  unsigned int test_len[n_trials];
  srandom(((1961*365+6*30)+13)*24+17);
  unsigned int i = 0;
  for (; i < sizeof(test_len); i++) {
    test_len[i] = (unsigned int)((random() & 0xFFF000) >> 12);
  }
  srandom(1832*6955*17);
  uint8_t test_buffer[8*1024];
  memset((void *)test_buffer, 0, sizeof(test_buffer));
  char b64_out[16*1024];
  void *decode_out;
  for (i=0; i < n_trials; i++) {
    unsigned int j = 0;
    for (; j < test_len[j]; j++) {
      test_buffer[j] = (random() & 0xFF00000) >> 20;
    }
    size_t b64_out_sz = sizeof(b64_out);
    b64::encode((char const *)test_buffer, test_len[i], b64_out, &b64_out_sz);
    cout << "test " << i << " length " << test_len[i] << " (" << test_len[i] % 3 << ") " << " encode size " << b64_out_sz << " (" << b64_out_sz % 4 << ")" << endl;
    if (b64_out_sz % 4 == 1) {
      cout << "ERROR: Bad encode out length, " << b64_out_sz << ", residue is 1, should be 0, 3, 2 mod 4" << endl;
    }
    // malloc so we know if decode crosses boundaries that it shouldn't
    decode_out = malloc(test_len[i]);
    size_t decode_out_sz = test_len[i];
    b64::decode(b64_out, b64_out_sz, decode_out, &decode_out_sz);
    if (decode_out_sz != test_len[i]) {
      cout << "ERROR at test " << i << "  lengths do not match " << decode_out_sz << " and " << test_len[i] << endl;
    }
    if (memcmp(test_buffer, decode_out, decode_out_sz) != 0) {
      cout << "ERROR: at " << i << " << test_buffer and decode are not equal" << endl;
      b64_out[b64_out_sz] = 0;
      cout << "b64(" << b64_out_sz << "): " << b64_out << endl;
    }
    free(decode_out);
    decode_out = NULL;
  }
  return 0;
}
