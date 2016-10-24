/*-=================================================-*/
/*  Copyright (c) 2005  NCIC                         */
/*  National Center of Intelligent Computing System. */
/*  All rights reserved.                             */
/*                                                   */
/*  Author list:                                     */
/*  Rongfeng Tang                                    */
/*-=================================================-*/
/* $Header: $ */
/* $Id: DCS_debug.h 528 2006-10-20 01:37:29Z rf_tang $ */
#ifndef __DCS_DEBUG_H__
#define __DCS_DEBUG_H__
/*
 * DCS_log_file_len declares the extern varible of a log file's length,
 * which must be defined in each module.
 */
//#ifndef DCS_LOG_FILE_PATH
//#define DCS_LOG_FILE_PATH "/tmp/log/"
//#endif

#ifdef __KERNEL__

#define DCS_ERROR(format, a...)                                       \
do {                                                            \
       	printk("ERROR in (%s, %d): ", __FILE__, __LINE__);    \
	printk(format, ## a); \
} while (0) 

#ifndef __DCS_DEBUG__

#define  DCS_DEBUG(format, a...) 
#define  DCS_MSG(format, a...) 
#define  DCS_ENTER(format, a...) 
#define  DCS_LEAVE(format, a...) 

#else

#define DCS_DEBUG(format, a...)                                \
        do {                                                            \
        	printk("(%s, %d): ",  __FILE__, __LINE__);                  \
 	        printk(format, ## a);                                       \
    	} while (0) 

#define DCS_MSG(format, a...)                                  \
     	do {                                                            \
         	printk (format, ## a);                                       \
     	} while (0) 

#define DCS_ENTER(format, a...)                                  \
     	do {                                                            \
         	printk (format, ## a);                                       \
     	} while (0)

#define DCS_LEAVE(format, a...)                                  \
     	do {                                                            \
         	printk (format, ## a);                                       \
     	} while (0)


#endif

#endif

#ifndef __KERNEL__

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
//extern int errno;
#define DCS_MAX_LOG_FILE_LEN    (5000000)
static int dcs_log_file_len;
#define DCS_ERROR(format, a...)                                       \
         do { \
	     time_t mytime ;  \
	     char *tmp_time_buf = NULL;  \
             if (dcs_log_file_len > DCS_MAX_LOG_FILE_LEN) {           \
                 ftruncate (STDERR_FILENO, 0);                          \
                 lseek (STDERR_FILENO, 0, SEEK_SET);                    \
                 dcs_log_file_len = 0;                                 \
             } else                                                     \
                 dcs_log_file_len ++;                                  \
	     mytime = time (NULL); \
	     tmp_time_buf = ctime(&mytime); \
	     tmp_time_buf[strlen(tmp_time_buf) - 1] = '\0'; \
	     fprintf(stderr,"[%s][%ld]: ", tmp_time_buf, pthread_self()); \
	     fprintf (stderr, format, ## a); \
	     fflush(stderr);						\
         } while (0) 


#ifndef __DCS_DEBUG__

#define DCS_DEBUG(format, a...)      
#define DCS_MSG(format, a...)   
#define DCS_ENTER(format, a...)
#define DCS_LEAVE(format, a...)

#else

#define DCS_DEBUG(format, a...)                                \
         do {     		 \
	     time_t mytime; \
	     char *tmp_time_buf = NULL; \
             if (dcs_log_file_len > DCS_MAX_LOG_FILE_LEN) {           \
                 ftruncate (STDERR_FILENO, 0);                          \
                 lseek (STDERR_FILENO, 0, SEEK_SET);                    \
                 dcs_log_file_len = 0;                                 \
             } else                                                     \
                 dcs_log_file_len ++;                                  \
	     mytime = time (NULL); \
	     tmp_time_buf = ctime(&mytime); \
	     tmp_time_buf[strlen(tmp_time_buf) - 1] = '\0'; \
	     fprintf(stderr, "[%s][%ld]: ", tmp_time_buf, pthread_self()); \
             fprintf(stderr, "(%s, %d): ",  __FILE__, __LINE__);         \
             fprintf(stderr, format, ## a);                              \
    	     fflush(stderr);					\
         } while (0) 

#define DCS_MSG(format, a...)                                  \
         do {                                                           \
	     time_t mytime ;  \
	     char *tmp_time_buf = NULL;  \
             if (dcs_log_file_len > DCS_MAX_LOG_FILE_LEN) {           \
                 ftruncate (STDERR_FILENO, 0);                          \
                 lseek (STDERR_FILENO, 0, SEEK_SET);                    \
                 dcs_log_file_len = 0;                                 \
             } else                                                     \
                 dcs_log_file_len ++;                                  \
	     mytime = time (NULL); \
	     tmp_time_buf = ctime(&mytime); \
	     tmp_time_buf[strlen(tmp_time_buf) - 1] = '\0'; \
	     fprintf(stderr, "[%s][%ld]: ", tmp_time_buf, pthread_self()); \
             fprintf (stderr, format, ## a);                            \
             fflush(stderr);	        		\
         } while (0)

#define DCS_ENTER(format, a...)                                  \
         do {                                                           \
	     time_t mytime ;  \
	     char *tmp_time_buf = NULL;  \
             if (dcs_log_file_len > DCS_MAX_LOG_FILE_LEN) {           \
                 ftruncate (STDERR_FILENO, 0);                          \
                 lseek (STDERR_FILENO, 0, SEEK_SET);                    \
                 dcs_log_file_len = 0;                                 \
             } else                                                     \
                 dcs_log_file_len ++;                                  \
	     mytime = time (NULL); \
	     tmp_time_buf = ctime(&mytime); \
	     tmp_time_buf[strlen(tmp_time_buf) - 1] = '\0'; \
	     fprintf(stderr, "[%s][%ld]: ", tmp_time_buf, pthread_self()); \
             fprintf (stderr, format, ## a);                            \
             fflush(stderr);	        		\
         } while (0)

#define DCS_LEAVE(format, a...)                                  \
         do {                                                           \
	     time_t mytime ;  \
	     char *tmp_time_buf = NULL;  \
             if (dcs_log_file_len > DCS_MAX_LOG_FILE_LEN) {           \
                 ftruncate (STDERR_FILENO, 0);                          \
                 lseek (STDERR_FILENO, 0, SEEK_SET);                    \
                 dcs_log_file_len = 0;                                 \
             } else                                                     \
                 dcs_log_file_len ++;                                  \
	     mytime = time (NULL); \
	     tmp_time_buf = ctime(&mytime); \
	     tmp_time_buf[strlen(tmp_time_buf) - 1] = '\0'; \
	     fprintf(stderr, "[%s][%ld]: ", tmp_time_buf, pthread_self()); \
             fprintf (stderr, format, ## a);                            \
             fflush(stderr);	        		\
         } while (0)


#endif

#endif

#endif

