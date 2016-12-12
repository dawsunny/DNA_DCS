#ifndef __DC_DEBUG_H_   //1 if
#define __DC_DEBUG_H_

#include <stdio.h>
#include <string.h>
#include <time.h>

//debug switch
#define __DEBUG_ON_  0

#define DC_ERROR(format, arg...)    \
    do{ \
        time_t mytime = time(NULL);  \
        char *mytime_str = ctime(&mytime);  \
        mytime_str[strlen(mytime_str) - 1] = '\0';  \
        printf("[%s] ", mytime_str); \
        printf("(%s, %d) ", __FILE__, __LINE__);   \
        printf(format, ##arg);  \
        fflush(stdout); \
    }while(0)


#if __DEBUG_ON_  //2 if

#define DC_PRINT(format, arg...)    \
    do{ \
        time_t mytime = time(NULL);  \
        char *mytime_str = ctime(&mytime);  \
        mytime_str[strlen(mytime_str) - 1] = '\0';  \
        printf("[%s] ", mytime_str); \
        printf(format, ##arg);  \
        fflush(stdout); \
    }while(0)

/*
#define DC_ERROR(format, arg...)    \
    do{ \
        time_t mytime = time(NULL);  \
        char *mytime_str = ctime(&mytime);  \
        mytime_str[strlen(mytime_str) - 1] = '\0';  \
        printf("[%s] ", mytime_str); \
        printf("(%s, %d) ", __FILE__, __LINE__);   \
        printf(format, ##arg);  \
        fflush(stdout); \
    }while(0)
*/

#else

#define DC_PRINT(format, arg...)
//#define DC_ERROR(format, arg...)

#endif  //2 if

#endif  //1 if
