#ifndef KVUNICODE_H

#define KVUNICODE_H

#if !__APPLE__
#include "unicode/utypes.h"
#include "unicode/uloc.h"
#include "unicode/utext.h"
#include "unicode/localpointer.h"
#include "unicode/parseerr.h"
#include "unicode/ubrk.h"
#include "unicode/urep.h"
#include "unicode/utrans.h"
#include "unicode/parseerr.h"
#include "unicode/uenum.h"
#include "unicode/uset.h"
#include "unicode/putil.h"
#include "unicode/uiter.h"
#include "unicode/ustring.h"
#else
#if defined(__CHAR16_TYPE__)
typedef __CHAR16_TYPE__ UChar;
#else
typedef uint16_t UChar;
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

void kv_unicode_init(void);
void kv_unicode_deinit(void);

unsigned int kv_u_get_length(const UChar * word);
UChar * kv_from_utf8(const char * word);
char * kv_to_utf8(const UChar * word);
char * kv_transliterate(const UChar * text, int length);

#ifdef __cplusplus
}
#endif

#endif
