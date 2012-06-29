/*
 * Debugging utilities.
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#ifndef DEBUG_H
#define DEBUG_H


#ifdef DEBUG
#define TRACE(fmt, ...)     do { fprintf(stderr, "%s: %d: %s: " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__); } while (0)
#else
#define TRACE(fmt, ...)     ((void)0)
#endif /* DEBUG */


#endif /* DEBUG_H */

