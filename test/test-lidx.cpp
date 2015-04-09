#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "sfts.h"

#define INDEXER 0

static double current_time(void)
{
    struct timeval val;
    gettimeofday(&val, NULL);
    return (double) val.tv_sec + (double) val.tv_usec / 1000000.;
}

int main(int argc, char ** argv)
{
    sfts * index = sfts_new("/Users/dvh/kvdb-test/db.sfts");
    sfts_open(index);

    unsigned int doc_count = 0;
  
    if (argc == 1) {
        char path[1024];
        char buffer[16384];
        char uidstr[256];
        char dirname[] = "/Users/dvh/Mail/dinh.viet.hoa@gmail.com/[Gmail].All Mail/cur";
        DIR * dir = opendir(dirname);
        if (dir == NULL) {
            fprintf(stderr, "can't open dir\n");
            exit(EXIT_FAILURE);
        }
  
        double start_time = current_time();
        struct dirent * dp;
        //sfts_transaction_begin(index);
        while ((dp = readdir(dir)) != NULL) {
            const char * filename = dp->d_name;
    
            if (filename[0] == '.') {
                continue;
            }
    
            char * p = strstr(filename, "U=");
            if (p == NULL) {
                continue;
            }
            char * p2 = strstr(p, ",");
            if (p2 == NULL) {
                continue;
            }
    
            snprintf(uidstr, sizeof(uidstr), "%s", p + 2);
            uidstr[p2 - p - 2] = 0;
            unsigned long long uid = strtoull(uidstr, NULL, 10);
    
            snprintf(path, sizeof(path), "%s/%s", dirname, filename);
            //printf("%s ", uidstr);
            //fflush(stdout);
    
            FILE * f = fopen(path, "r");
            size_t count = fread(buffer, 1, sizeof(buffer), f);
            fclose(f);
    
            if (count >= 16384) {
                count = 16383;
            }
            buffer[count] = 0;
    
            sfts_set(index, uid, buffer);
            doc_count ++;
            if (doc_count % 100 == 0) {
                //sfts_transaction_commit(index);
                //sfts_flush(index);
                double time = current_time();
                fprintf(stderr, "progress: %i %lf\n", doc_count, time - start_time);
                start_time = time;
                //sfts_transaction_begin(index);
            }
        }
        //sfts_transaction_commit(index);
        closedir(dir);
        
#if 0
        //lidx_set(index, 3242432423, "2014年6月23日ビジット・ジャパン（VJ）事業ベトナム市場で使用するパンフレット、ギブアウェイを募集！");
        lidx_set(index, 3242432423, "coin");
        lidx_set(index, 3242432421, "coin foobar");
  
        lidx_remove(index, 3242432421);
#endif
    }
    else {
  
        uint64_t * docsids;
        size_t count;
        sfts_search(index, argv[1], sfts_search_kind_prefix /* sfts_search_kind_substr */,
                    &docsids, &count);
        printf("found %i\n", (int) count);
    }
  
    fprintf(stderr, "closing: %i\n", doc_count);
    sfts_close(index);
    sfts_free(index);
    fprintf(stderr, "done\n");
    exit(EXIT_SUCCESS);
}