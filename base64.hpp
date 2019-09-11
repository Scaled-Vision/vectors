#ifndef __base64_hpp__
#define __base64_hpp__

#include <vector>
#include <stdint.h>

namespace base64 {
  static const char encodeLookup[] = {"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"};
  void encode(const void * in, const size_t in_size, char *out, size_t *out_size_p) {
    size_t out_limit;
    if (in_size == 0) {
      /* easy */
      return;
    };
    if (out_size_p) {
      out_limit = *out_size_p;
    } else {
      /* hanging rope */
      out_limit = (1U<<31) - 1;
    }
    uint8_t *out_p = (uint8_t *)out;
    uint32_t temp;
    uint8_t const * in_p = (uint8_t const *)in;
    size_t in_limit = in_size;
    /* Out_limit big enough? Avoid checking in loops. Adjust
     * how much input we can drink accordingly */
    if (out_limit < (4 * in_limit) / 3) {
      in_limit = (out_limit * 3) / 4;
    }
    int i = 0;
    while((in_limit - i) > 0) {
      if ((in_limit - i) >= 3) {
	temp = (*in_p++) << 16;
	temp += (*in_p++) << 8;
	temp += (*in_p++);
	uint32_t out_val;
	// little endian
	out_val = encodeLookup[(temp  & 0x00FC0000) >> 18];
	out_val |= encodeLookup[(temp & 0x0003F000) >> 12] << 8;
	out_val |= encodeLookup[(temp & 0x00000FC0) >> 6] << 16;
	out_val |= encodeLookup[(temp & 0x0000003F)] << 24;
	*((uint32_t *)out_p) = out_val;
	out_p += 4;
	i += 3;
      } else if ((in_limit - i) == 2) {
	temp = (*in_p++) << 16;
	temp += (*in_p++) << 8;
	uint16_t out_val_s;
	out_val_s = encodeLookup[(temp & 0x00FC0000) >> 18];
	out_val_s |= encodeLookup[(temp & 0x0003F000) >> 12] << 8;
	*((uint16_t *)out_p) = out_val_s;
	out_p += 2;
	*out_p = encodeLookup[(temp & 0x00000FC0) >> 6 ];
	out_p++;
	i += 2;
      } else {
	/* in_limit - i should never be zero here */
	temp = (*in_p++) << 16;
	uint16_t out_val_s;
	out_val_s = encodeLookup[(temp & 0x00FC0000) >> 18];
	out_val_s |= encodeLookup[(temp & 0x0003F000) >> 12] << 8;
	*((uint16_t *)out_p) = out_val_s;
	out_p += 2;
	i += 1;
      }
    }
    if (out_size_p) {
      *out_size_p = out_p - (uint8_t *)out;
    }
  }


  unsigned char inline _decode_one(const unsigned char x) {
    unsigned char ret;
    if (x >= 'A' && x <= 'Z') ret = x - 'A';
    else if (x >= 'a' && x <= 'z') ret = x - ('a' - ('Z' - 'A' + 1));
    /* - ('0' - ('Z' - 'A' + 1) - 'z' - 'a' + 1) wraps to - 0xfc, which is + 4 */
    else if (x >= '0' && x <= '9') ret = x + 4; 
    else if (x == '+') ret = 0x3e;
    else if (x == '/') ret = 0x3f;
    return ret;
  }

  void decode(char const * in, const size_t in_size, void *out, size_t *out_size_p) {
    size_t out_limit;
    if (in_size == 0) {
      /* easy */
      return;
    };
    if (out_size_p) {
      out_limit = *out_size_p;
    } else {
      /* hanging rope */
      out_limit = (1U<<31) - 1;
    }
    uint8_t *out_p = (uint8_t *)out;
    uint8_t const * in_p = (uint8_t const *)in;
    size_t in_limit = in_size;
    /* Out_limit big enough? Avoid checking in loops. Adjust
     * how much input we can drink accordingly */
    if (out_limit < (3 * in_limit) / 4) {
      in_limit = (out_limit * 4) / 3;
    }
    unsigned int i = 0;
    while(i < in_limit) {
      uint32_t tmp = 0;
      if (in_limit - i >= 4) {
	// 4 base 64 bytes, 24 bits, into 3 bytes
	int j = 0;
	for(; j < 4; j++) {
	  tmp <<= 6;
	  uint8_t x = *(in_p + j);
	  tmp |= _decode_one(x);
	}
	*out_p++ = (tmp >> 16) & 0xFF;
	*out_p++ = (tmp >> 8) & 0xFF;
	*out_p++ = tmp  & 0xFF;
	in_p += j;
	i += j;
      } else if (in_limit - i == 3) {
	// decode 3 into 2 bytes
	uint8_t x = *(in_p);
	tmp |= _decode_one(x);
	tmp <<= 6;
	x = *(in_p + 1);
	tmp |= _decode_one(x);
	tmp <<= 6;
	x = *(in_p + 2);
	tmp |= _decode_one(x);
	/* XX+XXXXYYYY+YYZZZZZ */
	/*   |   b1   |  b0    */
	*out_p++ = (tmp >> 16) & 0xFF;
	*out_p++ = (tmp >> 8) & 0xFF;
	// done, so skipping in_p += 3;
	i += 3;
      } else if (in_limit - i == 2) {
	// decode 2 into 1 byte
	uint8_t x = *(in_p);
	tmp |= _decode_one(x);
	tmp <<= 6;
	x = *(in_p + 1);
	tmp |= _decode_one(x);
	/* +XXXXXXYY+YYYY */
	/* |   b0   |     */
	*out_p++ = (tmp >> 4) & 0xFF;
	// done, so skipping in_p += 3;
	i += 2;
      } else if (in_limit - i == 1) {
	// error, let it go, can't do anything about
	// caller supplying an incorrectly sized input
	// buffer.
      }
    }
    if (out_size_p) {
      *out_size_p = out_p - (uint8_t *)out;
    }
  }

  void encode(std::vector<double>in, char *out, size_t *out_size_p) {
  }
}
#endif // __base64_hpp__
