/*
 * utilities
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */


#include "utils.h"
#include <cstdlib>
#include <time.h>
#include <math.h>
#include <sys/time.h>
#include <assert.h>


void doublesec2timespec(double sec, struct timespec *ts) {
  struct timeval tv;
  if (gettimeofday(&tv, NULL) == 0) {
    double integ;
    double fract = modf(sec, &integ);
    ts->tv_sec = tv.tv_sec + (time_t)integ;
    ts->tv_nsec = (long)(tv.tv_usec * 1000.0 + fract * 999999000);
    if (ts->tv_nsec >= 1000000000) {
      ts->tv_nsec -= 1000000000;
      ts->tv_sec++;
    }
  } else {
    ts->tv_sec = time(NULL) + 1;
    ts->tv_nsec = 0;
  }
}

