#ifndef CONFIG_H_
#define CONFIG_H_

// # #define DEBUG_ENABLED

#define DEFAULT_CONFIG "/etc/dp-ap-logger/logger.conf"
#define DEFAULT_COMMIT_COUNT 100
#define DEFAULT_COMMIT_HARD_LIMIT 1000

#define	TYPE_SSL 1
#define SSL_SUFFIX "-ssl"

#define LOG_FORMAT "%s : %s"
#define LOG_DEBUG_LABEL "Debug"
#define LOG_INFO_LABEL "Info"
#define LOG_WARNING_LABEL "Warning"
#define LOG_ERROR_LABEL "Error"
#define LOG_FATAL_LABEL "Fatal error"

#define LOG_DEBUG 	0
#define LOG_INFO 	1
#define LOG_WARNING 2
#define LOG_ERROR 	3
#define LOG_FATAL	4

#define FAILURE_SLEEP_MULTIPLIER 2
#define FAILURE_SLEEP_MAX 	 20

#ifdef DEBUG_ENABLED
	#define LOG_MIN_FILE_LEVEL	0
	#define LOG_MIN_OUT_LEVEL	0
	#define LOG_MIN_ERR_LEVEL	2
#else
	#define LOG_MIN_FILE_LEVEL	1
	#define LOG_MIN_OUT_LEVEL	1
	#define LOG_MIN_ERR_LEVEL	2
#endif

#endif /*CONFIG_H_*/
