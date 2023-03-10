/*
 * @Author: your name
 * @Date: 2021-04-25 17:46:17
 * @LastEditTime: 2021-05-07 00:39:13
 * @LastEditors: your name
 * @Description: In User Settings Edit
 * @FilePath: /RDMA_middleware/include/dhmp_log.h
 */
#ifndef DHMP_LOG_H
#define DHMP_LOG_H

enum dhmp_log_level{
	DHMP_LOG_LEVEL_ERROR,
	DHMP_LOG_LEVEL_WARN,
	DHMP_LOG_LEVEL_INFO,
	DHMP_LOG_LEVEL_DEBUG,
	DHMP_LOG_LEVEL_TRACE,
	DHMP_LOG_LEVEL_MID,
	DHMP_LOG_LEVEL_LAST
};

extern enum dhmp_log_level global_log_level;

void dhmp_log_impl(const char *file, unsigned line, const char *func,
					unsigned log_level, const char *fmt, ...);

#define dhmp_log(level, fmt, ...)\
	do{\
		if(level < DHMP_LOG_LEVEL_LAST && level<=global_log_level)\
			dhmp_log_impl(__FILE__, __LINE__, __func__,\
							level, fmt, ## __VA_ARGS__);\
	}while(0)

#define	ERROR_LOG(fmt, ...)		dhmp_log(DHMP_LOG_LEVEL_ERROR, fmt, \
									## __VA_ARGS__)
#define	WARN_LOG(fmt, ...) 		dhmp_log(DHMP_LOG_LEVEL_WARN, fmt, \
									## __VA_ARGS__)
#define INFO_LOG(fmt, ...)		dhmp_log(DHMP_LOG_LEVEL_INFO, fmt, \
									## __VA_ARGS__)	
#define DEBUG_LOG(fmt, ...)		dhmp_log(DHMP_LOG_LEVEL_DEBUG, fmt, \
									## __VA_ARGS__)
#define TRACE_LOG(fmt, ...)		dhmp_log(DHMP_LOG_LEVEL_TRACE, fmt,\
									## __VA_ARGS__)
#define MID_LOG(fmt, ...)		dhmp_log(DHMP_LOG_LEVEL_MID, fmt,\
									## __VA_ARGS__)

void cal_time(struct timespec * start, struct timespec *end, const char * commit);			
extern struct timespec SP_start, SP_end;
extern unsigned long long mica_total_time;

#endif
