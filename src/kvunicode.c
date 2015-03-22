#include "kvunicode.h"

#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#if __APPLE__
#include <CoreFoundation/CoreFoundation.h>
#endif

#include "ConvertUTF.h"

#if !__APPLE__
// Transliteration helpers.

typedef struct XReplaceable {
    UChar* text;    /* MUST BE null-terminated */
} XReplaceable;

static void InitXReplaceable(XReplaceable* rep, const UChar* str, int length)
{
    if (length == 0) {
        length = u_strlen(str);
    }
    rep->text = (UChar*) malloc(sizeof(* rep->text) * (length + 1));
    rep->text[length] = 0;
    u_strncpy(rep->text, str, length);
}

static void FreeXReplaceable(XReplaceable* rep)
{
    if (rep->text != NULL) {
        free(rep->text);
        rep->text = NULL;
    }
}

/* UReplaceableCallbacks callback */
static int32_t Xlength(const UReplaceable* rep)
{
    const XReplaceable* x = (const XReplaceable*)rep;
    return u_strlen(x->text);
}

/* UReplaceableCallbacks callback */
static UChar XcharAt(const UReplaceable* rep, int32_t offset)
{
    const XReplaceable* x = (const XReplaceable*)rep;
    return x->text[offset];
}

/* UReplaceableCallbacks callback */
static UChar32 Xchar32At(const UReplaceable* rep, int32_t offset)
{
    const XReplaceable* x = (const XReplaceable*)rep;
    return x->text[offset];
}

/* UReplaceableCallbacks callback */
static void Xreplace(UReplaceable* rep, int32_t start, int32_t limit,
                     const UChar* text, int32_t textLength)
{
    XReplaceable* x = (XReplaceable*)rep;
    int32_t newLen = Xlength(rep) + limit - start + textLength;
    UChar* newText = (UChar*) malloc(sizeof(UChar) * (newLen+1));
    u_strncpy(newText, x->text, start);
    u_strncpy(newText + start, text, textLength);
    u_strcpy(newText + start + textLength, x->text + limit);
    free(x->text);
    x->text = newText;
}

/* UReplaceableCallbacks callback */
static void Xcopy(UReplaceable* rep, int32_t start, int32_t limit, int32_t dest)
{
    XReplaceable* x = (XReplaceable*)rep;
    int32_t newLen = Xlength(rep) + limit - start;
    UChar* newText = (UChar*) malloc(sizeof(UChar) * (newLen+1));
    u_strncpy(newText, x->text, dest);
    u_strncpy(newText + dest, x->text + start, limit - start);
    u_strcpy(newText + dest + limit - start, x->text + dest);
    free(x->text);
    x->text = newText;
}

/* UReplaceableCallbacks callback */
static void Xextract(UReplaceable* rep, int32_t start, int32_t limit, UChar* dst)
{
    XReplaceable* x = (XReplaceable*)rep;
    int32_t len = limit - start;
    u_strncpy(dst, x->text, len);
}

static void InitXReplaceableCallbacks(UReplaceableCallbacks* callbacks)
{
    callbacks->length = Xlength;
    callbacks->charAt = XcharAt;
    callbacks->char32At = Xchar32At;
    callbacks->replace = Xreplace;
    callbacks->extract = Xextract;
    callbacks->copy = Xcopy;
}


// init and deinit.

static UReplaceableCallbacks s_xrepVtable;
static UTransliterator * s_trans = NULL;
static pthread_mutex_t s_lock = PTHREAD_MUTEX_INITIALIZER;
static int s_initialized = 0;
static int pthread_once_t s_once = PTHREAD_ONCE_INIT;

static void kv_unicode_init(void)
{
    pthread_mutex_lock(&s_lock);
    if (!s_initialized) {
        UChar urules[1024];
        UErrorCode status = U_ZERO_ERROR;
        u_strFromUTF8(urules, sizeof(urules), NULL, "Any-Latin; NFD; Lower; [:nonspacing mark:] remove; nfc", -1, &status);
        LIDX_ASSERT(status == U_ZERO_ERROR);
        
        UParseError parseError;
        s_trans = utrans_openU(urules, -1, UTRANS_FORWARD,
                               NULL, -1, &parseError, &status);
        LIDX_ASSERT(status == U_ZERO_ERROR);
        
        InitXReplaceableCallbacks(&s_xrepVtable);
        s_initialized = 1;
    }
    pthread_mutex_unlock(&s_lock);
}

static void kv_unicode_deinit(void)
{
    utrans_close(s_trans);
}
#endif

unsigned int kv_u_get_length(const UChar * word)
{
    unsigned int length = 0;
    while (* word != 0) {
        word ++;
        length ++;
    }
    return length;
}

// UTF <-> UTF16

UChar * kv_from_utf8(const char * word)
{
    size_t len = strlen(word);
    const UTF8 * source = (const UTF8 *) word;
    UTF16 * target = (UTF16 *) malloc((len + 1) * sizeof(* target));
    UTF16 * targetStart = target;
    ConvertUTF8toUTF16(&source, source + len,
                       &targetStart, targetStart + len, lenientConversion);
    unsigned int utf16length = (unsigned int) (targetStart - target);
    target[utf16length] = 0;
    return (UChar *) target;
}

char * kv_to_utf8(const UChar * word)
{
    unsigned int len = kv_u_get_length(word);
    const UTF16 * source = (const UTF16 *) word;
    UTF8 * target = (UTF8 *) malloc(len * 6 + 1);
    UTF8 * targetStart = target;
    ConvertUTF16toUTF8(&source, source + len,
                       &targetStart, targetStart + len * 6 + 1, lenientConversion);
    unsigned int utf8length = (unsigned int) (targetStart - target);
    target[utf8length] = 0;
    return (char *) target;
}

// transliterate to ASCII

char * kv_transliterate(const UChar * text, int length)
{
#if __APPLE__
    if (length == -1) {
        length = kv_u_get_length(text);
    }
    
    int is_ascii = 1;
    const UChar * p = text;
    for(int i = 0 ; i < length ; i ++) {
        if ((* p < 32) || (* p >= 127)) {
        //if (!isalnum(* p)) {
            is_ascii = 0;
            break;
        }
        p ++;
    }
    
    if (is_ascii) {
        char * result = malloc(length + 1);
        char * q = result;
        for(int i = 0 ; i < length ; i ++) {
            * q = tolower(text[i]);
            q ++;
        }
        * q = 0;
        return result;
    }
    
    CFMutableStringRef cfStr = CFStringCreateMutable(NULL, 0);
    CFStringAppendCharacters(cfStr, (const UniChar *) text, length);
    CFStringTransform(cfStr, NULL, CFSTR("Any-Latin; NFD; Lower; [:nonspacing mark:] remove; nfc"), false);
    CFIndex resultLength = CFStringGetLength(cfStr);
    char * buffer = (char *) malloc(resultLength + 1);
    buffer[resultLength] = 0;
    CFStringGetCString(cfStr, buffer, resultLength + 1, kCFStringEncodingUTF8);
    CFRelease(cfStr);
    return buffer;
#else
    if (length == -1) {
        length = u_strlen(text);
    }
    
    pthread_once(&s_once, kv_unicode_init);
    
    XReplaceable xrep;
    InitXReplaceable(&xrep, text, length);
    UErrorCode status = U_ZERO_ERROR;
    
    int32_t limit = length;
    utrans_trans(s_trans, (UReplaceable *) &xrep, &s_xrepVtable, 0, &limit, &status);
    if (status != U_ZERO_ERROR) {
        goto free_xrep;
    }
    
    char * result = lidx_to_utf8(xrep.text);
    FreeXReplaceable(&xrep);
    
    return result;
    
free_xrep:
    FreeXReplaceable(&xrep);
err:
    return NULL;
#endif
}
